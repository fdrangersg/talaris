//! `ConnectionState` —— Pool 内单条连接的状态机
//!
//! Pool 持唯一 [`Proactor`]，各 conn 的 IO 方法接 `proactor: &mut Proactor`
//! 参数；公开 API 通过 [`crate::Pool`] + `ConnHandle` 操作连接。
//!
//! 不对外暴露——`pub(crate)`。
//!
//! 字段语义、状态机、buffer 生命周期、inflight 限制完全沿用 `connection.rs`
//! 模块文档，不再复述。

// `.expect("buf_ring …")` 等是 invariant 断言（on_connect_cqe 一定先注册），
// 走到 panic 等于 driver state machine 已坏 —— 此时 HFT 进程应立即崩并由
// supervisor 重启，而不是继续吞错。
#![allow(clippy::expect_used)]

use std::io;
use std::net::SocketAddr;

use crate::connection::{ConnectionConfig, ConnectionError, State};
use crate::proactor::{
    BufferRing, Completion, Domain, OpKind, Proactor, SockAddr, SqeFlags, TcpSocket, UserData,
};
use crate::tls::TlsAdapter;
use crate::ws::{ConnState as WsConnState, WsClient, WsConfig};

/// 单连接驱动状态。Pool 持 `Vec<ConnectionState>`，每条都有独立的 socket /
/// buf_ring / send_buf / ws；唯一共享的是 Pool 拥有的 [`Proactor`]。
pub(crate) struct ConnectionState {
    pub(crate) socket: TcpSocket,
    /// `submit_connect` 期间 kernel 读这块；必须随 self 一起活。
    pub(crate) addr: SockAddr,
    pub(crate) tls: Option<TlsAdapter>,
    pub(crate) ws: WsClient,
    pub(crate) state: State,
    /// `None` 直到 TCP connect 完成。drop 前必须 `unregister`（Pool 负责）。
    pub(crate) buf_ring: Option<BufferRing>,
    /// kernel 通过 SQE 持着 `send_buf.as_ptr().add(send_head)` 直到 Send CQE
    /// 回来。**不变式：`send_inflight = true` 期间 `send_buf` 不得 push /
    /// extend，`send_head` 不得移动**（任一操作可能触发 realloc / memmove，
    /// 导致 kernel 端 dangling 指针）。所有 in-flight 期间产生的 egress 字节
    /// 走 `tls_pending_out` 累加，由 `try_submit_send` 在 `send_inflight` 解除
    /// 后合入。
    ///
    /// `send_head` cursor 替换早期 `drain(..n)`：partial-send 的 `on_send_cqe`
    /// 现在是 O(1) head 自增（早期 O(n) memmove）；保留 `Vec<u8>` 是因为
    /// `tls.egress_plaintext(&[u8], &mut Vec<u8>)` 直接 push 到 send_buf 末端。
    pub(crate) send_buf: Vec<u8>,
    pub(crate) send_head: usize,
    /// TLS 层在 in-flight 期间想发的密文累加器（**永远不直接交给 kernel**）。
    /// `on_recv_cqe` 在处理 TLS handshake reply / re-key / alert 时 append 到这里；
    /// `try_submit_send` 在 `!send_inflight` 时把它 drain 到 `send_buf` 一并提交。
    /// 命名沿用 plan.md / connection.rs 的 `tls_*` 前缀。
    pub(crate) tls_pending_out: Vec<u8>,
    pub(crate) send_inflight: bool,
    pub(crate) multishot_armed: bool,
    pub(crate) ws_handshake_begun: bool,
    pub(crate) cfg: ConnectionConfig,
}

impl ConnectionState {
    /// 不 submit 任何 SQE。状态 `Init`，等 caller 调 `submit_connect`。
    pub(crate) fn new(cfg: ConnectionConfig, addr: SocketAddr) -> Result<Self, ConnectionError> {
        let sock_addr = SockAddr::from_std(addr);
        let domain = match addr {
            SocketAddr::V4(_) => Domain::V4,
            SocketAddr::V6(_) => Domain::V6,
        };
        let socket = TcpSocket::new(domain)?;
        socket.set_nodelay(true)?;

        let tls = if cfg.use_tls {
            Some(match cfg.tls_config.clone() {
                Some(config) => TlsAdapter::new_client_with_config(&cfg.host, config)?,
                None => TlsAdapter::new_client(&cfg.host)?,
            })
        } else {
            None
        };

        let ws_cfg = WsConfig::new(cfg.host.clone(), cfg.path.clone());
        let ws = WsClient::new_client(ws_cfg)?;

        let init_cap = cfg.buf_ring_slot_size as usize;
        Ok(Self {
            socket,
            addr: sock_addr,
            tls,
            ws,
            state: State::Init,
            buf_ring: None,
            send_buf: Vec::with_capacity(init_cap),
            send_head: 0,
            tls_pending_out: Vec::with_capacity(init_cap),
            send_inflight: false,
            multishot_armed: false,
            ws_handshake_begun: false,
            cfg,
        })
    }

    #[inline]
    pub(crate) const fn conn_id(&self) -> u32 {
        self.cfg.conn_id
    }

    #[inline]
    pub(crate) const fn state(&self) -> State {
        self.state
    }

    pub(crate) fn assert_open(&self) -> Result<(), ConnectionError> {
        if matches!(self.state, State::Open) {
            Ok(())
        } else {
            Err(ConnectionError::InvalidState(self.state))
        }
    }

    pub(crate) fn submit_connect(
        &mut self,
        proactor: &mut Proactor,
    ) -> Result<(), ConnectionError> {
        let ud = UserData::new(OpKind::Connect, u64::from(self.cfg.conn_id));
        // SAFETY: self.addr 与 self 同寿命；CQE 回来前不会被 move/drop
        unsafe {
            proactor.submit_connect(self.socket.as_raw_fd(), &self.addr, ud, SqeFlags::NONE)?;
        }
        self.state = State::Connecting;
        Ok(())
    }

    pub(crate) fn try_submit_send(
        &mut self,
        proactor: &mut Proactor,
    ) -> Result<(), ConnectionError> {
        if self.send_inflight {
            // 不变式：in-flight 期间不动 send_buf / send_head。任何 egress 都堆
            // 在 tls_pending_out / ws.tx_buf 里，下轮 pump 拿到 Send CQE 后再合入。
            return Ok(());
        }

        if matches!(self.state, State::Init | State::Connecting | State::Closed) {
            return Ok(());
        }

        // 在 push 任何新字节前 compact send_buf：把 [send_head..] move 到 front
        // 并 reset head。让"未发完的尾部 + 新字节"在 send_buf 里连续，下次
        // submit_send 一次性把它们打包给 kernel。head==0 时 noop（hot path）。
        self.compact_send_buf_if_needed();

        // 1) 把上一轮 in-flight 期间 on_recv_cqe 累加的 TLS 密文吐进 send_buf。
        //    保留顺序：tls_pending_out（更早入队）排在 ws 新字节前。
        if !self.tls_pending_out.is_empty() {
            self.send_buf.extend_from_slice(&self.tls_pending_out);
            self.tls_pending_out.clear();
        }

        // 2) 把 ws 待发字节合入 send_buf。
        //    早期版本守卫了 `send_buf.is_empty()` — partial-send 后 send_buf 残留
        //    会让新 ws 字节永远卡在 ws.tx_buf。这里 !send_inflight（顶部已 gate）
        //    保证 kernel 已交回 send_buf 所有权，append 不会让 in-flight 指针失效。
        let ws_tx_len = self.ws.pending_tx().len();
        if ws_tx_len > 0 {
            if let Some(tls) = &mut self.tls {
                tls.egress_plaintext(self.ws.pending_tx(), &mut self.send_buf)?;
            } else {
                self.send_buf.extend_from_slice(self.ws.pending_tx());
            }
            self.ws.ack_tx(ws_tx_len);
        }

        if let Some(tls) = &mut self.tls {
            // 空 egress 把 rustls 主动想发的 handshake / re-key 字节流出
            tls.egress_plaintext(&[], &mut self.send_buf)?;
        }

        // 实际未发送字节 = send_buf.len() - send_head
        let pending = self.send_buf.len().saturating_sub(self.send_head);
        if pending == 0 {
            return Ok(());
        }

        let ud = UserData::new(OpKind::Send, u64::from(self.cfg.conn_id));
        let len = u32::try_from(pending).unwrap_or(u32::MAX);
        // SAFETY: send_buf 是 self 的 Vec，CQE 回来前不会 drop/realloc/compact
        // （send_inflight=true 阻塞 compact_send_buf_if_needed 和 extend）
        unsafe {
            proactor.submit_send(
                self.socket.as_raw_fd(),
                self.send_buf.as_ptr().add(self.send_head),
                len,
                ud,
                SqeFlags::NONE,
            )?;
        }
        self.send_inflight = true;
        Ok(())
    }

    /// 必要时把 `send_buf[send_head..]` move 到 front + reset head。仅在
    /// `!send_inflight` 时调用安全（in-flight 期间 kernel 持着 ptr+head）。
    fn compact_send_buf_if_needed(&mut self) {
        debug_assert!(!self.send_inflight);
        if self.send_head == 0 {
            return;
        }
        if self.send_head == self.send_buf.len() {
            self.send_buf.clear();
        } else {
            self.send_buf.drain(..self.send_head);
        }
        self.send_head = 0;
    }

    pub(crate) fn try_rearm_multishot(
        &mut self,
        proactor: &mut Proactor,
    ) -> Result<(), ConnectionError> {
        if self.multishot_armed {
            return Ok(());
        }
        if matches!(self.state, State::Init | State::Connecting | State::Closed) {
            return Ok(());
        }
        let Some(ring) = self.buf_ring.as_ref() else {
            return Ok(());
        };
        let bgid = ring.bgid();
        // SAFETY: buf_ring 持有效 ring 注册；fd 在 self 寿命内有效
        unsafe {
            proactor.submit_recv_multishot(
                self.socket.as_raw_fd(),
                bgid,
                UserData::new(OpKind::Recv, u64::from(self.cfg.conn_id)),
            )?;
        }
        self.multishot_armed = true;
        Ok(())
    }

    pub(crate) fn handle_completion(
        &mut self,
        proactor: &mut Proactor,
        c: Completion,
    ) -> Result<(), ConnectionError> {
        let kind = c
            .user_data
            .kind()
            .ok_or_else(|| ConnectionError::UnknownOpKind(c.user_data.raw()))?;
        match kind {
            OpKind::Connect => self.on_connect_cqe(proactor, c),
            OpKind::Send => self.on_send_cqe(c),
            OpKind::Recv => self.on_recv_cqe(c),
            OpKind::Close => {
                self.state = State::Closed;
                Ok(())
            }
            OpKind::Nop => Ok(()),
        }
    }

    fn on_connect_cqe(
        &mut self,
        proactor: &mut Proactor,
        c: Completion,
    ) -> Result<(), ConnectionError> {
        c.to_result().map_err(ConnectionError::ConnectFailed)?;

        let ring = BufferRing::new(
            proactor,
            self.cfg.bgid,
            self.cfg.buf_ring_entries,
            self.cfg.buf_ring_slot_size,
        )?;
        let bgid = ring.bgid();
        self.buf_ring = Some(ring);
        // SAFETY: buf_ring 现在 own 了 ring 注册；fd 仍然有效
        unsafe {
            proactor.submit_recv_multishot(
                self.socket.as_raw_fd(),
                bgid,
                UserData::new(OpKind::Recv, u64::from(self.cfg.conn_id)),
            )?;
        }
        self.multishot_armed = true;

        self.state = if self.tls.is_some() {
            State::TlsHandshake
        } else {
            State::WsHandshake
        };
        if self.tls.is_none() && !self.ws_handshake_begun {
            self.ws.begin_handshake()?;
            self.ws_handshake_begun = true;
        }
        Ok(())
    }

    fn on_send_cqe(&mut self, c: Completion) -> Result<(), ConnectionError> {
        self.send_inflight = false;
        let n = c.to_result().map_err(ConnectionError::SendFailed)?;
        // O(1) head 自增。早期 `drain(..n)` 在 partial-send 时是 O(n) memmove。
        // head 追上 len 时整体 reset，避免 send_buf 无限增长。
        self.send_head += n;
        if self.send_head >= self.send_buf.len() {
            self.send_buf.clear();
            self.send_head = 0;
        }
        Ok(())
    }

    fn on_recv_cqe(&mut self, c: Completion) -> Result<(), ConnectionError> {
        if !c.has_more() {
            self.multishot_armed = false;
        }

        let Some(bid) = c.buffer_id() else {
            return match c.to_result() {
                Ok(0) => {
                    self.state = State::Closed;
                    Err(ConnectionError::PeerClosed)
                }
                Ok(_) => Ok(()),
                Err(e) if is_recv_buffer_ring_exhausted(&e) => {
                    // ENOBUFS 不是 bug 而是 backpressure 信号：kernel 端 head 追上
                    // 了 ring tail，没空闲 buffer 可分配。两个前提保证下一轮
                    // try_rearm_multishot 不会立刻再 ENOBUFS：
                    //   1. 同 batch 内的数据 CQE 已经在前面被处理 + recycle()
                    //      过（drain_completions 的 sink 顺序是 CQE 入队顺序）
                    //   2. multishot 在收到 ENOBUFS 时自动终止（F_MORE=0），所以
                    //      kernel 不会在我们 rearm 之前再消耗一格
                    // 真要忙转，只可能是 caller 的 sink 不调 recycle —— 那是用法
                    // 错误，warn log 已经露头。
                    self.multishot_armed = false;
                    tracing::warn!(
                        conn_id = self.cfg.conn_id,
                        bgid = self.buf_ring.as_ref().map_or(0, BufferRing::bgid),
                        "recv multishot provided-buffer ring exhausted; will rearm next pump"
                    );
                    Ok(())
                }
                Err(e) => Err(ConnectionError::RecvFailed(e)),
            };
        };

        let n = c.to_result().map_err(ConnectionError::RecvFailed)?;

        if n == 0 {
            self.buf_ring
                .as_mut()
                .expect("buf_ring 应在 on_connect_cqe 注册")
                .recycle(bid);
            self.state = State::Closed;
            return Err(ConnectionError::PeerClosed);
        }

        // Raw pointer split borrow（详见 connection.rs 模块文档同名段落）。
        let bytes_ptr = self
            .buf_ring
            .as_ref()
            .expect("buf_ring")
            .buffer(bid)
            .as_ptr();
        // SAFETY: Proactor !Sync + 处理完才 recycle + buf_storage 是 Box<[u8]>，
        // 三条保证 bytes 视图在本函数内全程有效（详见 connection.rs 长注释）。
        let bytes: &[u8] = unsafe { std::slice::from_raw_parts(bytes_ptr, n) };

        let recv_result = if let Some(tls) = &mut self.tls {
            // **不变式**：tls_pending_out 是 in-flight 安全累加器，**绝不 clear**——
            // 它由 try_submit_send 在 `!send_inflight` 时 drain。这里直接 append
            // 让 rustls 把 handshake reply / re-key / alert 密文堆进去。
            let ws = &mut self.ws;
            tls.ingest_ciphertext(bytes, &mut self.tls_pending_out, |plaintext| {
                ws.feed_recv(plaintext);
            })?;
            if !tls.is_handshaking()
                && matches!(self.state, State::TlsHandshake)
                && !self.ws_handshake_begun
            {
                // ALPN 校验：通告了 http/1.1 还不够，server 可能忽略；这里 enforce
                tls.verify_alpn()?;
                self.state = State::WsHandshake;
                self.ws.begin_handshake()?;
                self.ws_handshake_begun = true;
            }
            // peer 发了 close_notify → 推 driver 到 Closing。Open 之后再发生
            // 不应该把 state 拉回 TlsHandshake；只在尚未 Closed 时推一步。
            if tls.received_close_notify() && !matches!(self.state, State::Closed | State::Closing)
            {
                self.state = State::Closing;
            }
            Ok(())
        } else {
            self.ws.feed_recv(bytes);
            Ok::<_, ConnectionError>(())
        };

        self.buf_ring.as_mut().expect("buf_ring").recycle(bid);

        recv_result
    }

    /// pump 主循环末尾调一次：WS 内部状态切到 Closed 时，外层 state 同步到
    /// Closing（不直接跳 Closed —— 仍可能有未发完的 close 帧或未到的 close CQE）。
    pub(crate) fn sync_ws_close_state(&mut self) {
        if matches!(self.ws.state(), WsConnState::Closed)
            && !matches!(self.state, State::Closing | State::Closed)
        {
            self.state = State::Closing;
        }
    }

    /// HandshakeComplete 在 ws.poll_event() 出来时不直接改 self.state；这里
    /// 显式同步：ws 进 Open 就把 driver state 也推到 Open。
    pub(crate) fn sync_ws_open_state(&mut self) {
        if matches!(self.ws.state(), WsConnState::Open) && !matches!(self.state, State::Open) {
            self.state = State::Open;
        }
    }
}

fn is_recv_buffer_ring_exhausted(err: &io::Error) -> bool {
    err.raw_os_error() == Some(libc::ENOBUFS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_recv_buffer_ring_exhaustion() {
        let err = io::Error::from_raw_os_error(libc::ENOBUFS);
        assert!(is_recv_buffer_ring_exhausted(&err));

        let err = io::Error::from_raw_os_error(libc::ECONNRESET);
        assert!(!is_recv_buffer_ring_exhausted(&err));
    }
}
