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

use std::net::SocketAddr;
use std::{fmt, io};

use crate::connection::{ConnectionConfig, ConnectionError, IngressStats, State};
use crate::observability::LatencyHistograms;
use crate::proactor::{
    BufferRing, Completion, Domain, OpKind, Proactor, SockAddr, SqeFlags, TcpSocket, UserData,
};
use crate::tls::TlsAdapter;
use crate::ws::{
    ConnState as WsConnState, DataEvent as WsDataEvent, DataEventMeta, MarkedDataEvent, WsClient,
    WsConfig,
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum WsIngressState {
    Clean,
    Dirty,
}

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
    /// Reused only when `plain_recv_batch_copy_max_bytes > 0`: consecutive
    /// plain recv CQEs can be copied here and parsed as one larger WS slice.
    plain_recv_batch_scratch: Vec<u8>,
    /// TLS 层在 in-flight 期间想发的密文累加器（**永远不直接交给 kernel**）。
    /// `on_recv_cqe` 在处理 TLS handshake reply / re-key / alert 时 append 到这里；
    /// `try_submit_send` 在 `!send_inflight` 时把它 drain 到 `send_buf` 一并提交。
    /// 命名沿用 plan.md / connection.rs 的 `tls_*` 前缀。
    pub(crate) tls_pending_out: Vec<u8>,
    pub(crate) send_inflight: bool,
    pub(crate) multishot_armed: bool,
    pub(crate) ws_handshake_begun: bool,
    /// Data-pump hint: at least one plaintext slice reached `ws.feed_recv` since
    /// the last WebSocket drain. TLS recv CQEs that only extend a partial record
    /// leave this false, so the data hot path can skip a guaranteed no-op drain.
    ws_ingress: WsIngressState,
    pub(crate) ingress_stats: IngressStats,
    marked_recv_sequence: u64,
    marked_message_sequence: u64,
    observability_histograms: Option<LatencyHistograms>,
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

        let mut ws_cfg = cfg
            .ws_config
            .clone()
            .unwrap_or_else(|| WsConfig::new(cfg.host.clone(), cfg.path.clone()));
        ws_cfg.host.clone_from(&cfg.host);
        ws_cfg.path.clone_from(&cfg.path);
        let ws = WsClient::new_client(ws_cfg)?;

        let init_cap = cfg.buf_ring_slot_size as usize;
        let send_cap = cfg.send_buffer_initial_capacity.unwrap_or(init_cap);
        let tls_pending_out_cap = cfg.tls_pending_out_initial_capacity.unwrap_or(init_cap);
        let observability_histograms = if cfg.record_observability_histograms {
            Some(LatencyHistograms::new()?)
        } else {
            None
        };
        Ok(Self {
            socket,
            addr: sock_addr,
            tls,
            ws,
            state: State::Init,
            buf_ring: None,
            send_buf: Vec::with_capacity(send_cap),
            send_head: 0,
            plain_recv_batch_scratch: Vec::new(),
            tls_pending_out: Vec::with_capacity(tls_pending_out_cap),
            send_inflight: false,
            multishot_armed: false,
            ws_handshake_begun: false,
            ws_ingress: WsIngressState::Clean,
            ingress_stats: IngressStats::default(),
            marked_recv_sequence: 0,
            marked_message_sequence: 0,
            observability_histograms,
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

    #[inline]
    pub(crate) const fn ingress_stats(&self) -> IngressStats {
        self.ingress_stats
    }

    pub(crate) fn write_prometheus_metrics<W: fmt::Write>(&self, out: &mut W) -> fmt::Result {
        let conn_id = self.cfg.conn_id;
        if let Some(histograms) = &self.observability_histograms {
            histograms.write_prometheus_cumulative(conn_id, out)?;
        }
        self.write_ingress_prometheus_metrics(out)
    }

    pub(crate) fn write_prometheus_metrics_and_reset_interval<W: fmt::Write>(
        &mut self,
        out: &mut W,
    ) -> fmt::Result {
        let conn_id = self.cfg.conn_id;
        if let Some(histograms) = &mut self.observability_histograms {
            histograms.write_prometheus_interval_and_reset(conn_id, out)?;
        }
        self.write_ingress_prometheus_metrics(out)
    }

    fn write_ingress_prometheus_metrics<W: fmt::Write>(&self, out: &mut W) -> fmt::Result {
        let conn_id = self.cfg.conn_id;
        let stats = self.ingress_stats;
        writeln!(
            out,
            "talaris_ingress_recv_data_cqes_total{{conn_id=\"{conn_id}\"}} {}",
            stats.recv_data_cqes
        )?;
        writeln!(
            out,
            "talaris_ingress_recv_bytes_total{{conn_id=\"{conn_id}\"}} {}",
            stats.recv_bytes
        )?;
        writeln!(
            out,
            "talaris_ingress_recv_multishot_rearms_total{{conn_id=\"{conn_id}\"}} {}",
            stats.recv_multishot_rearms
        )?;
        writeln!(
            out,
            "talaris_ingress_recv_ring_exhaustions_total{{conn_id=\"{conn_id}\"}} {}",
            stats.recv_ring_exhaustions
        )?;
        writeln!(
            out,
            "talaris_ingress_plain_recv_batches_total{{conn_id=\"{conn_id}\"}} {}",
            stats.plain_recv_batches
        )?;
        writeln!(
            out,
            "talaris_ingress_plain_recv_batch_cqes_total{{conn_id=\"{conn_id}\"}} {}",
            stats.plain_recv_batch_cqes
        )?;
        writeln!(
            out,
            "talaris_ingress_plain_recv_copied_batches_total{{conn_id=\"{conn_id}\"}} {}",
            stats.plain_recv_copied_batches
        )?;
        writeln!(
            out,
            "talaris_ingress_plain_recv_copied_bytes_total{{conn_id=\"{conn_id}\"}} {}",
            stats.plain_recv_copied_bytes
        )?;
        writeln!(
            out,
            "talaris_ingress_plaintext_chunks_total{{conn_id=\"{conn_id}\"}} {}",
            stats.plaintext_chunks
        )?;
        writeln!(
            out,
            "talaris_ingress_plaintext_bytes_total{{conn_id=\"{conn_id}\"}} {}",
            stats.plaintext_bytes
        )?;
        writeln!(
            out,
            "talaris_ingress_ws_data_drains_total{{conn_id=\"{conn_id}\"}} {}",
            stats.ws_data_drains
        )?;
        writeln!(
            out,
            "talaris_ingress_ws_data_drain_skips_total{{conn_id=\"{conn_id}\"}} {}",
            stats.ws_data_drain_skips
        )?;
        writeln!(
            out,
            "talaris_ingress_ws_data_events_total{{conn_id=\"{conn_id}\"}} {}",
            stats.ws_data_events
        )?;
        writeln!(
            out,
            "talaris_ingress_ws_text_events_total{{conn_id=\"{conn_id}\"}} {}",
            stats.ws_text_events
        )?;
        writeln!(
            out,
            "talaris_ingress_ws_binary_events_total{{conn_id=\"{conn_id}\"}} {}",
            stats.ws_binary_events
        )
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
        self.record_recv_multishot_rearm();
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

    pub(crate) fn handle_completion_data<F>(
        &mut self,
        proactor: &mut Proactor,
        c: Completion,
        mut sink: F,
    ) -> Result<usize, ConnectionError>
    where
        F: for<'a> FnMut(WsDataEvent<'a>),
    {
        let kind = c
            .user_data
            .kind()
            .ok_or_else(|| ConnectionError::UnknownOpKind(c.user_data.raw()))?;
        match kind {
            OpKind::Connect => {
                self.on_connect_cqe(proactor, c)?;
                Ok(0)
            }
            OpKind::Send => {
                self.on_send_cqe(c)?;
                Ok(0)
            }
            OpKind::Recv => self.on_recv_cqe_data(c, &mut sink),
            OpKind::Close => {
                self.state = State::Closed;
                Ok(0)
            }
            OpKind::Nop => Ok(0),
        }
    }

    #[inline]
    pub(crate) fn can_handle_plain_recv_data_batch(&self, c: Completion) -> bool {
        self.tls.is_none()
            && matches!(self.state, State::Open)
            && c.user_data.kind() == Some(OpKind::Recv)
            && c.result > 0
            && c.buffer_id().is_some()
    }

    pub(crate) fn handle_plain_recv_data_batch<F>(
        &mut self,
        completions: &[Completion],
        sink: &mut F,
    ) -> Result<usize, ConnectionError>
    where
        F: for<'a> FnMut(WsDataEvent<'a>),
    {
        debug_assert!(self.tls.is_none());
        let batch_cqes = u64::try_from(completions.len()).unwrap_or(u64::MAX);
        if let Some(total_bytes) = self.plain_recv_batch_copy_len(completions) {
            self.record_plain_recv_batch(
                batch_cqes,
                Some(u64::try_from(total_bytes).unwrap_or(u64::MAX)),
            );
            return self.handle_plain_recv_data_batch_copied(completions, total_bytes, sink);
        }

        self.record_plain_recv_batch(batch_cqes, None);
        self.handle_plain_recv_data_batch_slices(completions, sink)
    }

    fn plain_recv_batch_copy_len(&self, completions: &[Completion]) -> Option<usize> {
        let max_bytes = self.cfg.plain_recv_batch_copy_max_bytes;
        if max_bytes == 0 {
            return None;
        }

        let mut total = 0_usize;
        for &c in completions {
            if c.result <= 0 || c.buffer_id().is_none() {
                return None;
            }
            #[allow(clippy::cast_sign_loss)]
            let n = c.result as usize;
            total = total.checked_add(n)?;
            if total > max_bytes {
                return None;
            }
        }
        Some(total)
    }

    fn handle_plain_recv_data_batch_copied<F>(
        &mut self,
        completions: &[Completion],
        total_bytes: usize,
        sink: &mut F,
    ) -> Result<usize, ConnectionError>
    where
        F: for<'a> FnMut(WsDataEvent<'a>),
    {
        self.plain_recv_batch_scratch.clear();
        self.plain_recv_batch_scratch.reserve(total_bytes);

        let mut plaintext_chunks = 0_u64;
        for &c in completions {
            if !c.has_more() {
                self.multishot_armed = false;
            }

            let bid = c
                .buffer_id()
                .expect("copy batch only accepts provided-buffer recv CQEs");
            #[allow(clippy::cast_sign_loss)]
            let n = c.result as usize;
            self.record_recv_data(n);
            plaintext_chunks = plaintext_chunks.saturating_add(1);

            let bytes_ptr = self
                .buf_ring
                .as_ref()
                .expect("buf_ring")
                .buffer(bid)
                .as_ptr();
            // SAFETY: the selected provided buffer remains valid until it is
            // recycled below; bytes are copied into scratch before recycle.
            let bytes: &[u8] = unsafe { std::slice::from_raw_parts(bytes_ptr, n) };
            self.plain_recv_batch_scratch.extend_from_slice(bytes);
            self.buf_ring.as_mut().expect("buf_ring").recycle(bid);
        }

        debug_assert_eq!(self.plain_recv_batch_scratch.len(), total_bytes);

        let mut drained_events = 0_usize;
        let mut text_events = 0_u64;
        let mut binary_events = 0_u64;
        let bytes = self.plain_recv_batch_scratch.as_slice();
        let result = self
            .ws
            .drain_data_events_from_ingress(bytes, |ev| {
                drained_events = drained_events.saturating_add(1);
                match ev {
                    WsDataEvent::Text(_) => text_events = text_events.saturating_add(1),
                    WsDataEvent::Binary(_) => binary_events = binary_events.saturating_add(1),
                }
                sink(ev);
            })
            .map_err(ConnectionError::Ws);

        self.record_plaintext(plaintext_chunks, total_bytes as u64);
        self.record_ws_data_drains(plaintext_chunks, 0);
        self.record_ws_data_events(text_events, binary_events);

        result.map(|_| drained_events)
    }

    fn handle_plain_recv_data_batch_slices<F>(
        &mut self,
        completions: &[Completion],
        sink: &mut F,
    ) -> Result<usize, ConnectionError>
    where
        F: for<'a> FnMut(WsDataEvent<'a>),
    {
        let mut drained_events = 0_usize;
        let mut plaintext_chunks = 0_u64;
        let mut plaintext_bytes = 0_u64;
        let mut text_events = 0_u64;
        let mut binary_events = 0_u64;
        let mut first_err = None;

        for &c in completions {
            if !c.has_more() {
                self.multishot_armed = false;
            }

            let Some(bid) = c.buffer_id() else {
                if first_err.is_none() {
                    first_err = Some(match c.to_result() {
                        Ok(0) => {
                            self.state = State::Closed;
                            ConnectionError::PeerClosed
                        }
                        Ok(_) => ConnectionError::InvalidState(self.state),
                        Err(e) if is_recv_buffer_ring_exhausted(&e) => {
                            self.record_recv_ring_exhaustion();
                            self.multishot_armed = false;
                            continue;
                        }
                        Err(e) => ConnectionError::RecvFailed(e),
                    });
                }
                continue;
            };

            let n = match c.to_result() {
                Ok(0) => {
                    self.buf_ring.as_mut().expect("buf_ring").recycle(bid);
                    self.state = State::Closed;
                    if first_err.is_none() {
                        first_err = Some(ConnectionError::PeerClosed);
                    }
                    continue;
                }
                Ok(n) => n,
                Err(e) => {
                    self.buf_ring.as_mut().expect("buf_ring").recycle(bid);
                    if first_err.is_none() {
                        first_err = Some(ConnectionError::RecvFailed(e));
                    }
                    continue;
                }
            };

            self.record_recv_data(n);

            if first_err.is_none() {
                plaintext_chunks = plaintext_chunks.saturating_add(1);
                plaintext_bytes = plaintext_bytes.saturating_add(n as u64);
                let bytes_ptr = self
                    .buf_ring
                    .as_ref()
                    .expect("buf_ring")
                    .buffer(bid)
                    .as_ptr();
                // SAFETY: the selected provided buffer remains owned by this
                // connection until it is recycled below, after the sink returns.
                let bytes: &[u8] = unsafe { std::slice::from_raw_parts(bytes_ptr, n) };
                let result = self
                    .ws
                    .drain_data_events_from_ingress(bytes, |ev| {
                        drained_events = drained_events.saturating_add(1);
                        match ev {
                            WsDataEvent::Text(_) => text_events = text_events.saturating_add(1),
                            WsDataEvent::Binary(_) => {
                                binary_events = binary_events.saturating_add(1);
                            }
                        }
                        sink(ev);
                    })
                    .map_err(ConnectionError::Ws);
                if let Err(e) = result
                    && first_err.is_none()
                {
                    first_err = Some(e);
                }
            }

            self.buf_ring.as_mut().expect("buf_ring").recycle(bid);
        }

        self.record_plaintext(plaintext_chunks, plaintext_bytes);
        self.record_ws_data_drains(plaintext_chunks, 0);
        self.record_ws_data_events(text_events, binary_events);

        first_err.map_or(Ok(drained_events), Err)
    }

    pub(crate) fn handle_completion_data_marked<F>(
        &mut self,
        proactor: &mut Proactor,
        c: Completion,
        mut sink: F,
    ) -> Result<usize, ConnectionError>
    where
        F: for<'a> FnMut(MarkedDataEvent<'a>),
    {
        let kind = c
            .user_data
            .kind()
            .ok_or_else(|| ConnectionError::UnknownOpKind(c.user_data.raw()))?;
        match kind {
            OpKind::Connect => {
                self.on_connect_cqe(proactor, c)?;
                Ok(0)
            }
            OpKind::Send => {
                self.on_send_cqe(c)?;
                Ok(0)
            }
            OpKind::Recv => self.on_recv_cqe_data_marked(c, &mut sink),
            OpKind::Close => {
                self.state = State::Closed;
                Ok(0)
            }
            OpKind::Nop => Ok(0),
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
        self.record_recv_multishot_rearm();

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
                    self.record_recv_ring_exhaustion();
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

        self.record_recv_data(n);
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
            let mut fed_plaintext = false;
            tls.ingest_ciphertext(bytes, &mut self.tls_pending_out, |plaintext| {
                ws.feed_recv(plaintext);
                fed_plaintext = true;
            })?;
            if fed_plaintext {
                self.ws_ingress = WsIngressState::Dirty;
            }
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
            self.ws_ingress = WsIngressState::Dirty;
            Ok::<_, ConnectionError>(())
        };

        self.buf_ring.as_mut().expect("buf_ring").recycle(bid);

        recv_result
    }

    #[allow(clippy::too_many_lines)]
    fn on_recv_cqe_data<F>(&mut self, c: Completion, sink: &mut F) -> Result<usize, ConnectionError>
    where
        F: for<'a> FnMut(WsDataEvent<'a>),
    {
        if !c.has_more() {
            self.multishot_armed = false;
        }

        let Some(bid) = c.buffer_id() else {
            return match c.to_result() {
                Ok(0) => {
                    self.state = State::Closed;
                    Err(ConnectionError::PeerClosed)
                }
                Ok(_) => Ok(0),
                Err(e) if is_recv_buffer_ring_exhausted(&e) => {
                    self.record_recv_ring_exhaustion();
                    self.multishot_armed = false;
                    tracing::warn!(
                        conn_id = self.cfg.conn_id,
                        bgid = self.buf_ring.as_ref().map_or(0, BufferRing::bgid),
                        "recv multishot provided-buffer ring exhausted; will rearm next pump"
                    );
                    Ok(0)
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

        self.record_recv_data(n);
        let bytes_ptr = self
            .buf_ring
            .as_ref()
            .expect("buf_ring")
            .buffer(bid)
            .as_ptr();
        // SAFETY: Same invariant as on_recv_cqe: the buffer is not recycled until
        // recv_result is built, and Proactor is single-thread owned.
        let bytes: &[u8] = unsafe { std::slice::from_raw_parts(bytes_ptr, n) };

        let recv_result = if let Some(tls) = &mut self.tls {
            let ws = &mut self.ws;
            let mut fed_plaintext = false;
            let mut drained_events = 0_usize;
            let mut ws_error = None;
            let mut plaintext_chunks = 0_u64;
            let mut plaintext_bytes = 0_u64;
            let mut text_events = 0_u64;
            let mut binary_events = 0_u64;
            if self.cfg.track_ingress_stats {
                tls.ingest_ciphertext(bytes, &mut self.tls_pending_out, |plaintext| {
                    fed_plaintext = true;
                    plaintext_chunks = plaintext_chunks.saturating_add(1);
                    plaintext_bytes = plaintext_bytes.saturating_add(plaintext.len() as u64);
                    match ws.drain_data_events_from_ingress(plaintext, |ev| {
                        drained_events = drained_events.saturating_add(1);
                        match ev {
                            WsDataEvent::Text(_) => text_events = text_events.saturating_add(1),
                            WsDataEvent::Binary(_) => {
                                binary_events = binary_events.saturating_add(1);
                            }
                        }
                        sink(ev);
                    }) {
                        Ok(_) => {}
                        Err(e) if ws_error.is_none() => {
                            ws_error = Some(e);
                        }
                        Err(_) => {}
                    }
                })?;
            } else {
                tls.ingest_ciphertext(bytes, &mut self.tls_pending_out, |plaintext| {
                    fed_plaintext = true;
                    match ws.drain_data_events_from_ingress(plaintext, |ev| {
                        drained_events = drained_events.saturating_add(1);
                        sink(ev);
                    }) {
                        Ok(_) => {}
                        Err(e) if ws_error.is_none() => {
                            ws_error = Some(e);
                        }
                        Err(_) => {}
                    }
                })?;
            }
            if !tls.is_handshaking()
                && matches!(self.state, State::TlsHandshake)
                && !self.ws_handshake_begun
            {
                tls.verify_alpn()?;
                self.state = State::WsHandshake;
                self.ws.begin_handshake()?;
                self.ws_handshake_begun = true;
            }
            if tls.received_close_notify() && !matches!(self.state, State::Closed | State::Closing)
            {
                self.state = State::Closing;
            }
            self.record_ws_data_drain_attempt(fed_plaintext);
            if self.cfg.track_ingress_stats {
                self.record_plaintext(plaintext_chunks, plaintext_bytes);
                self.record_ws_data_events(text_events, binary_events);
            }
            match ws_error {
                Some(e) => Err(ConnectionError::Ws(e)),
                None => Ok(drained_events),
            }
        } else {
            let mut drained_events = 0_usize;
            let result = if self.cfg.track_ingress_stats {
                let mut text_events = 0_u64;
                let mut binary_events = 0_u64;
                let result = self
                    .ws
                    .drain_data_events_from_ingress(bytes, |ev| {
                        drained_events = drained_events.saturating_add(1);
                        match ev {
                            WsDataEvent::Text(_) => text_events = text_events.saturating_add(1),
                            WsDataEvent::Binary(_) => {
                                binary_events = binary_events.saturating_add(1);
                            }
                        }
                        sink(ev);
                    })
                    .map(|_| drained_events)
                    .map_err(ConnectionError::Ws);
                self.record_plaintext(1, n as u64);
                self.record_ws_data_events(text_events, binary_events);
                result
            } else {
                self.ws
                    .drain_data_events_from_ingress(bytes, |ev| {
                        drained_events = drained_events.saturating_add(1);
                        sink(ev);
                    })
                    .map(|_| drained_events)
                    .map_err(ConnectionError::Ws)
            };
            self.record_ws_data_drain_attempt(true);
            result
        };

        self.buf_ring.as_mut().expect("buf_ring").recycle(bid);

        recv_result
    }

    #[allow(clippy::too_many_lines)]
    fn on_recv_cqe_data_marked<F>(
        &mut self,
        c: Completion,
        sink: &mut F,
    ) -> Result<usize, ConnectionError>
    where
        F: for<'a> FnMut(MarkedDataEvent<'a>),
    {
        if !c.has_more() {
            self.multishot_armed = false;
        }

        let Some(bid) = c.buffer_id() else {
            return match c.to_result() {
                Ok(0) => {
                    self.state = State::Closed;
                    Err(ConnectionError::PeerClosed)
                }
                Ok(_) => Ok(0),
                Err(e) if is_recv_buffer_ring_exhausted(&e) => {
                    self.record_recv_ring_exhaustion();
                    self.multishot_armed = false;
                    tracing::warn!(
                        conn_id = self.cfg.conn_id,
                        bgid = self.buf_ring.as_ref().map_or(0, BufferRing::bgid),
                        "recv multishot provided-buffer ring exhausted; will rearm next pump"
                    );
                    Ok(0)
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

        let recv_sequence = self.marked_recv_sequence;
        let sampled = self
            .cfg
            .observability_sample_rate
            .should_sample_sequence(recv_sequence);
        let recv_meta = DataEventMeta::recv_observed_now(recv_sequence, sampled);
        self.marked_recv_sequence = self.marked_recv_sequence.saturating_add(1);
        self.record_recv_data(n);
        let bytes_ptr = self
            .buf_ring
            .as_ref()
            .expect("buf_ring")
            .buffer(bid)
            .as_ptr();
        // SAFETY: Same invariant as on_recv_cqe: the buffer is not recycled until
        // recv_result is built, and Proactor is single-thread owned.
        let bytes: &[u8] = unsafe { std::slice::from_raw_parts(bytes_ptr, n) };

        let recv_result = if let Some(tls) = &mut self.tls {
            let ws = &mut self.ws;
            let marked_message_sequence = &mut self.marked_message_sequence;
            let observability_histograms = &mut self.observability_histograms;
            let mut fed_plaintext = false;
            let mut plaintext_chunks = 0_u64;
            let mut plaintext_bytes = 0_u64;
            let mut drained_events = 0_usize;
            let mut text_events = 0_u64;
            let mut binary_events = 0_u64;
            let mut chunk_index = 0_u16;
            let mut ws_error = None;
            tls.ingest_ciphertext(bytes, &mut self.tls_pending_out, |plaintext| {
                fed_plaintext = true;
                plaintext_chunks = plaintext_chunks.saturating_add(1);
                plaintext_bytes = plaintext_bytes.saturating_add(plaintext.len() as u64);
                let base_meta = recv_meta.plaintext_ready_now(chunk_index);
                chunk_index = chunk_index.saturating_add(1);
                let chunk_events_before = drained_events;
                let drain_result = ws.drain_data_events_from_ingress_marked_with_message_sequence(
                    plaintext,
                    base_meta,
                    marked_message_sequence,
                    |ev| {
                        let meta = ev.meta();
                        if let Some(histograms) = observability_histograms.as_mut() {
                            histograms.record_message(meta);
                        }
                        drained_events = drained_events.saturating_add(1);
                        match ev {
                            MarkedDataEvent::Text { .. } => {
                                text_events = text_events.saturating_add(1);
                            }
                            MarkedDataEvent::Binary { .. } => {
                                binary_events = binary_events.saturating_add(1);
                            }
                        }
                        sink(ev);
                    },
                );
                if drained_events > chunk_events_before
                    && let Some(histograms) = observability_histograms.as_mut()
                {
                    histograms.record_plaintext_chunk(base_meta);
                }
                match drain_result {
                    Ok(_) => {}
                    Err(e) if ws_error.is_none() => {
                        ws_error = Some(e);
                    }
                    Err(_) => {}
                }
            })?;
            if !tls.is_handshaking()
                && matches!(self.state, State::TlsHandshake)
                && !self.ws_handshake_begun
            {
                tls.verify_alpn()?;
                self.state = State::WsHandshake;
                self.ws.begin_handshake()?;
                self.ws_handshake_begun = true;
            }
            if tls.received_close_notify() && !matches!(self.state, State::Closed | State::Closing)
            {
                self.state = State::Closing;
            }
            self.record_ws_data_drain_attempt(fed_plaintext);
            self.record_plaintext(plaintext_chunks, plaintext_bytes);
            self.record_ws_data_events(text_events, binary_events);
            match ws_error {
                Some(e) => Err(ConnectionError::Ws(e)),
                None => Ok(drained_events),
            }
        } else {
            let mut drained_events = 0_usize;
            let mut text_events = 0_u64;
            let mut binary_events = 0_u64;
            let base_meta = recv_meta.plaintext_ready_at(recv_meta.transport_recv_mono_nanos, 0);
            let marked_message_sequence = &mut self.marked_message_sequence;
            let observability_histograms = &mut self.observability_histograms;
            let chunk_events_before = drained_events;
            let result = self
                .ws
                .drain_data_events_from_ingress_marked_with_message_sequence(
                    bytes,
                    base_meta,
                    marked_message_sequence,
                    |ev| {
                        let meta = ev.meta();
                        if let Some(histograms) = observability_histograms.as_mut() {
                            histograms.record_message(meta);
                        }
                        drained_events = drained_events.saturating_add(1);
                        match ev {
                            MarkedDataEvent::Text { .. } => {
                                text_events = text_events.saturating_add(1);
                            }
                            MarkedDataEvent::Binary { .. } => {
                                binary_events = binary_events.saturating_add(1);
                            }
                        }
                        sink(ev);
                    },
                )
                .map(|_| drained_events)
                .map_err(ConnectionError::Ws);
            if drained_events > chunk_events_before
                && let Some(histograms) = observability_histograms.as_mut()
            {
                histograms.record_plaintext_chunk(base_meta);
            }
            self.record_ws_data_drain_attempt(true);
            self.record_plaintext(1, n as u64);
            self.record_ws_data_events(text_events, binary_events);
            result
        };

        self.buf_ring.as_mut().expect("buf_ring").recycle(bid);

        recv_result
    }

    #[inline]
    fn record_recv_data(&mut self, bytes: usize) {
        if !self.cfg.track_ingress_stats {
            return;
        }
        self.ingress_stats.recv_data_cqes = self.ingress_stats.recv_data_cqes.saturating_add(1);
        self.ingress_stats.recv_bytes = self.ingress_stats.recv_bytes.saturating_add(bytes as u64);
    }

    #[inline]
    fn record_recv_multishot_rearm(&mut self) {
        if self.cfg.track_ingress_stats {
            self.ingress_stats.recv_multishot_rearms =
                self.ingress_stats.recv_multishot_rearms.saturating_add(1);
        }
    }

    #[inline]
    fn record_recv_ring_exhaustion(&mut self) {
        if self.cfg.track_ingress_stats {
            self.ingress_stats.recv_ring_exhaustions =
                self.ingress_stats.recv_ring_exhaustions.saturating_add(1);
        }
    }

    #[inline]
    fn record_plain_recv_batch(&mut self, cqes: u64, copied_bytes: Option<u64>) {
        if self.cfg.track_ingress_stats {
            self.ingress_stats.plain_recv_batches =
                self.ingress_stats.plain_recv_batches.saturating_add(1);
            self.ingress_stats.plain_recv_batch_cqes = self
                .ingress_stats
                .plain_recv_batch_cqes
                .saturating_add(cqes);
            if let Some(bytes) = copied_bytes {
                self.ingress_stats.plain_recv_copied_batches = self
                    .ingress_stats
                    .plain_recv_copied_batches
                    .saturating_add(1);
                self.ingress_stats.plain_recv_copied_bytes = self
                    .ingress_stats
                    .plain_recv_copied_bytes
                    .saturating_add(bytes);
            }
        }
    }

    #[inline]
    fn record_plaintext(&mut self, chunks: u64, bytes: u64) {
        if self.cfg.track_ingress_stats {
            self.ingress_stats.plaintext_chunks =
                self.ingress_stats.plaintext_chunks.saturating_add(chunks);
            self.ingress_stats.plaintext_bytes =
                self.ingress_stats.plaintext_bytes.saturating_add(bytes);
        }
    }

    #[inline]
    fn record_ws_data_drain_attempt(&mut self, dirty: bool) {
        if self.cfg.track_ingress_stats {
            if dirty {
                self.record_ws_data_drains(1, 0);
            } else {
                self.record_ws_data_drains(0, 1);
            }
        }
    }

    #[inline]
    fn record_ws_data_drains(&mut self, drains: u64, skips: u64) {
        if self.cfg.track_ingress_stats {
            self.ingress_stats.ws_data_drains =
                self.ingress_stats.ws_data_drains.saturating_add(drains);
            self.ingress_stats.ws_data_drain_skips =
                self.ingress_stats.ws_data_drain_skips.saturating_add(skips);
        }
    }

    #[inline]
    fn record_ws_data_events(&mut self, text_events: u64, binary_events: u64) {
        if self.cfg.track_ingress_stats {
            self.ingress_stats.ws_text_events = self
                .ingress_stats
                .ws_text_events
                .saturating_add(text_events);
            self.ingress_stats.ws_binary_events = self
                .ingress_stats
                .ws_binary_events
                .saturating_add(binary_events);
            self.ingress_stats.ws_data_events = self
                .ingress_stats
                .ws_data_events
                .saturating_add(text_events.saturating_add(binary_events));
        }
    }

    /// Generic pump drains the WebSocket state machine unconditionally after a
    /// CQE batch. Clear the data-pump hint so switching APIs cannot observe it.
    #[inline]
    pub(crate) fn clear_ws_ingress_dirty(&mut self) {
        self.ws_ingress = WsIngressState::Clean;
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
