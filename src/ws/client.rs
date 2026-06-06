//! `WsClient` —— RFC 6455 WS client 最高层 API
//!
//! 装配 `FrameParser` + handshake + close + auto-pong + fragment 重组。
//! 字节进字节出，不依赖任何 runtime；caller 负责把 `pending_tx()` 出来的字节
//! 写到 socket，把 socket 收到的字节 `feed_recv()` 进来。
//!
//! Lifecycle：
//! 1. `new_client(cfg)` 构造
//! 2. `begin_handshake()` 把 GET Upgrade 请求字节塞 tx_buf
//! 3. caller 把 tx_buf send 出去，读 socket 把 bytes feed 回来
//! 4. `poll_event()` 出来第一个事件是 `HandshakeComplete`
//! 5. 之后 `poll_event()` 出 Text / Binary / Ping / Pong / Close
//! 6. `send_text/binary/ping/pong/close()` 主动发
//! 7. 主动发 Close 后状态 → `Closing`；收到合法 Close 后 queue echo 并进入 `Closed`

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::module_name_repetitions
)]

use super::close::{
    CloseCode, CloseError, encode_close_payload, is_valid_endpoint_sent, parse_close_payload,
};
use super::frame::{FrameError, FrameHeader, MAX_HEADER_LEN, OpCode, encode_header, parse_header};
use super::handshake::{
    HandshakeError, UpgradeRequest, encode_upgrade_request, generate_key, verify_upgrade_response,
};
use super::mask::mask_inplace;
use super::parser::{FeedOutcome, FrameEvent, FrameParser};
use crate::cursor_buf::CursorBuf;
use crate::http::{HttpError, parse_response};
use thiserror::Error;

/// 默认单条 message 最大长度（fragmented 累计）：8 MiB。
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

/// 默认单帧 payload 上限：16 MiB。
pub const DEFAULT_MAX_FRAME_PAYLOAD: u64 = 16 * 1024 * 1024;

/// `WsClient` outbound mask key pool 字节数。
///
/// 这是内联数组大小，当前不是运行时调参项；公开该常量便于 bench/report
/// 把不可调的底层结构也纳入参数表。
pub const MASK_POOL_BYTES: usize = 256;

/// Configuration
#[derive(Debug, Clone)]
pub struct WsConfig {
    pub host: String,
    pub path: String,
    pub subprotocols: Vec<String>,
    pub origin: Option<String>,
    /// 单条 message 最大长度（fragmented 累计），默认 8 MiB
    pub max_message_size: usize,
    /// 单帧 payload 上限，默认 16 MiB
    pub max_frame_payload: u64,
    /// `recv_buf` 初始容量。`None` 表示 `max_message_size + MAX_HEADER_LEN`。
    ///
    /// 这只是初始 heap capacity，不是协议上限；真实 message 上限仍由
    /// [`Self::max_message_size`] 控制。
    pub initial_recv_buffer_capacity: Option<usize>,
    /// fragmented message assembly buffer 初始容量。`None` 表示 `max_message_size`。
    pub initial_message_buffer_capacity: Option<usize>,
    /// outbound `tx_buf` 初始容量。`None` 表示 `max_message_size + MAX_HEADER_LEN`。
    pub initial_tx_buffer_capacity: Option<usize>,
    /// 收到 Ping 时自动回 Pong（默认 true）
    pub auto_pong: bool,
    /// Hot-path opt-in: trust server text payloads are valid UTF-8 and skip
    /// per-frame validation before constructing `&str`.
    assume_text_utf8: bool,
}

impl WsConfig {
    #[must_use]
    pub fn new(host: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            path: path.into(),
            subprotocols: Vec::new(),
            origin: None,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_frame_payload: DEFAULT_MAX_FRAME_PAYLOAD,
            initial_recv_buffer_capacity: None,
            initial_message_buffer_capacity: None,
            initial_tx_buffer_capacity: None,
            auto_pong: true,
            assume_text_utf8: false,
        }
    }

    #[must_use]
    pub const fn with_max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = bytes;
        self
    }

    #[must_use]
    pub const fn with_max_frame_payload(mut self, bytes: u64) -> Self {
        self.max_frame_payload = bytes;
        self
    }

    #[must_use]
    pub const fn with_initial_recv_buffer_capacity(mut self, bytes: usize) -> Self {
        self.initial_recv_buffer_capacity = Some(bytes);
        self
    }

    #[must_use]
    pub const fn with_initial_message_buffer_capacity(mut self, bytes: usize) -> Self {
        self.initial_message_buffer_capacity = Some(bytes);
        self
    }

    #[must_use]
    pub const fn with_initial_tx_buffer_capacity(mut self, bytes: usize) -> Self {
        self.initial_tx_buffer_capacity = Some(bytes);
        self
    }

    #[must_use]
    pub const fn with_initial_buffer_capacities(
        mut self,
        recv_bytes: usize,
        message_bytes: usize,
        tx_bytes: usize,
    ) -> Self {
        self.initial_recv_buffer_capacity = Some(recv_bytes);
        self.initial_message_buffer_capacity = Some(message_bytes);
        self.initial_tx_buffer_capacity = Some(tx_bytes);
        self
    }

    #[must_use]
    pub const fn with_auto_pong(mut self, on: bool) -> Self {
        self.auto_pong = on;
        self
    }

    /// Skip UTF-8 validation for incoming Text messages.
    ///
    /// # Safety
    ///
    /// The caller must guarantee every server Text payload is valid UTF-8 for
    /// the whole connection lifetime. If the peer sends invalid UTF-8 while
    /// this option is enabled, constructing `&str` with unchecked conversion is
    /// undefined behavior.
    #[must_use]
    pub const unsafe fn with_assume_text_utf8_unchecked(mut self, on: bool) -> Self {
        self.assume_text_utf8 = on;
        self
    }
}

/// 当前连接状态
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConnState {
    /// handshake 请求已发，等 101 响应
    Connecting,
    /// handshake 完，data 阶段
    Open,
    /// close 已发或已收，等 peer close
    Closing,
    /// WS 层不再接收新事件；caller 仍应先 flush `pending_tx()` 中已排队的 close frame
    Closed,
}

/// 外部可见事件
#[derive(Debug)]
pub enum Event<'a> {
    /// handshake 完，可以发数据了
    HandshakeComplete,
    /// 完整 text 消息（UTF-8 已校验，或由 unsafe config 显式承诺）
    Text(&'a str),
    /// 完整 binary 消息
    Binary(&'a [u8]),
    /// 收到 Ping（payload 可见；若 `auto_pong` 开启，Pong 已排入 `pending_tx()`）
    Ping(&'a [u8]),
    /// 收到 Pong
    Pong(&'a [u8]),
    /// 收到 Close
    Close { code: u16, reason: &'a str },
}

/// 只包含业务 data message 的轻量事件。
#[derive(Debug)]
pub enum DataEvent<'a> {
    /// 完整 text 消息（UTF-8 已校验，或由 unsafe config 显式承诺）
    Text(&'a str),
    /// 完整 binary 消息
    Binary(&'a [u8]),
}

#[derive(Debug, Error)]
pub enum WsError {
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("handshake error: {0}")]
    Handshake(#[from] HandshakeError),
    #[error("http parse error: {0}")]
    Http(#[from] HttpError),
    #[error("close payload error: {0}")]
    Close(#[from] CloseError),
    #[error("protocol error: {0}")]
    Protocol(&'static str),
    #[error("operation not allowed in websocket state {0:?}")]
    InvalidState(ConnState),
    #[error("text frame had invalid UTF-8")]
    Utf8Invalid,
    #[error("assembled message exceeded max_message_size")]
    MessageTooLarge,
    #[error("internal state error: {0}")]
    Internal(&'static str),
}

#[derive(Debug, Clone, Copy)]
enum EmitKind {
    HandshakeComplete,
    Text,
    Binary,
    BorrowedText,
    BorrowedBinary,
    Ping,
    Pong,
    Close,
}

#[derive(Debug)]
struct BorrowedPayload {
    payload_start: usize,
    payload_end: usize,
    frame_len: usize,
}

#[derive(Debug)]
pub struct WsClient {
    state: ConnState,
    parser: FrameParser,
    /// raw bytes from socket / TLS layer; drained as parser consumes。
    /// partial-frame consume 是 O(1) head 自增，
    recv_buf: CursorBuf,
    /// assembled data message payload。整体 clear 不 drain，仍用 `Vec<u8>`。
    msg_buf: Vec<u8>,
    /// 未分片、单帧已完整落在 recv_buf 时直接借用 payload，跳过 recv_buf→msg_buf copy。
    /// `poll_event()` 下一次进入时才 consume 整帧，保证上一轮 Event slice 有效。
    borrowed_payload: Option<BorrowedPayload>,
    /// 当前 fragmented data message 的初始 opcode（Text 或 Binary）；None = 不在 fragmented 中间
    msg_opcode: Option<OpCode>,
    /// control frame payload buf（≤125 bytes）
    ctl_buf: [u8; 125],
    ctl_len: u8,
    /// 当前帧的 opcode（FrameStart 设、FrameEnd 清）
    cur_opcode: Option<OpCode>,
    cur_fin: bool,
    /// 待发字节。`tx_buf` 是 underlying Vec；`tx_head` 是已确认发出字节的 cursor。
    /// `pending_tx()` 返回 `&tx_buf[tx_head..]`，`ack_tx(n)` 仅自增 head（O(1)）;
    /// 早期 `Vec::drain(..n)` 是 partial-send hot path 上的 O(n) memmove。
    /// 不切 CursorBuf 是因为 `encode_upgrade_request` 直接 push `&mut Vec<u8>`。
    tx_buf: Vec<u8>,
    tx_head: usize,
    config: WsConfig,
    /// handshake 用的 client key（base64）
    client_key: String,
    /// 防止 caller 重复 append GET Upgrade 请求。
    handshake_started: bool,
    /// 上一次 poll_event 已经 emit 过的 kind，下次进 poll 时按此清缓存
    last_emitted: Option<EmitKind>,
    /// CSPRNG mask-key pool。RFC §10.3 要求 mask key "unpredictable"，所以不能
    /// 用 xorshift / PCG 之类纯递推 PRNG。这里把 `ring::SystemRandom` 一次填满
    /// 一块 buffer 摊薄 syscall 成本：每 `MASK_POOL_BYTES / 4` 次 next_mask
    /// 才 refill 一次。
    mask_pool: [u8; MASK_POOL_BYTES],
    /// `mask_pool` 中下一个未消费 mask key 的字节偏移。`>= MASK_POOL_BYTES`
    /// 时下一次 next_mask 触发 refill。
    mask_pool_cursor: usize,
    /// CSPRNG handle。`SystemRandom::new()` 是 cheap（thread-local），但仍然
    /// 持久化以表达"这条 ws 的 mask key 来源固定"的语义。
    mask_rng: ring::rand::SystemRandom,
}

impl WsClient {
    /// 构造，但不发 handshake——调 [`begin_handshake`](Self::begin_handshake)
    pub fn new_client(config: WsConfig) -> Result<Self, WsError> {
        use ring::rand::SecureRandom;

        let key = generate_key()?;
        let default_msg_cap = config.max_message_size;
        let default_io_cap = default_msg_cap
            .checked_add(MAX_HEADER_LEN)
            .ok_or(WsError::Protocol("max_message_size overflow"))?;
        let recv_cap = config
            .initial_recv_buffer_capacity
            .unwrap_or(default_io_cap);
        let msg_cap = config
            .initial_message_buffer_capacity
            .unwrap_or(default_msg_cap);
        let tx_cap = config.initial_tx_buffer_capacity.unwrap_or(default_io_cap);
        let mask_rng = ring::rand::SystemRandom::new();
        let mut mask_pool = [0_u8; MASK_POOL_BYTES];
        mask_rng
            .fill(&mut mask_pool)
            .map_err(|_| WsError::Handshake(HandshakeError::RngFailure))?;
        Ok(Self {
            state: ConnState::Connecting,
            parser: FrameParser::new(),
            recv_buf: CursorBuf::with_capacity(recv_cap),
            msg_buf: Vec::with_capacity(msg_cap),
            borrowed_payload: None,
            msg_opcode: None,
            ctl_buf: [0; 125],
            ctl_len: 0,
            cur_opcode: None,
            cur_fin: false,
            tx_buf: Vec::with_capacity(tx_cap),
            tx_head: 0,
            config,
            client_key: key,
            handshake_started: false,
            last_emitted: None,
            mask_pool,
            mask_pool_cursor: 0,
            mask_rng,
        })
    }

    /// 编码 GET Upgrade 请求字节到 tx_buf。lifecycle 步 2。
    ///
    /// 只能在新建 client 的 `Connecting` 初始阶段调用一次；重复调用返回
    /// `WsError::Protocol`，避免 append 第二份 GET Upgrade 请求。
    pub fn begin_handshake(&mut self) -> Result<(), WsError> {
        if self.state != ConnState::Connecting {
            return Err(WsError::InvalidState(self.state));
        }
        if self.handshake_started {
            return Err(WsError::Protocol("handshake already begun"));
        }
        // 一般 handshake 是开局第一次 push，tx_head==0；这里 compact 是防御性
        // （如果 caller 复用同一 WsClient 实例做重连的话）。
        self.compact_tx_if_needed(256);
        let subprotos_refs: Vec<&str> = self
            .config
            .subprotocols
            .iter()
            .map(String::as_str)
            .collect();
        let req = UpgradeRequest {
            host: &self.config.host,
            path: &self.config.path,
            key: &self.client_key,
            subprotocols: &subprotos_refs,
            origin: self.config.origin.as_deref(),
        };
        encode_upgrade_request(&mut self.tx_buf, &req);
        self.handshake_started = true;
        Ok(())
    }

    /// 喂明文字节（TLS 解密后或裸 TCP）
    pub fn feed_recv(&mut self, bytes: &[u8]) {
        self.recv_buf.extend_from_slice(bytes);
    }

    /// 取下一个事件。返回的 Event 借用 self 内部缓冲，必须在下次调用前消费。
    pub fn poll_event(&mut self) -> Option<Result<Event<'_>, WsError>> {
        // 清掉上次 emit 的状态
        self.clear_after_emit();

        loop {
            if self.state == ConnState::Closed {
                return None;
            }

            match self.try_emit_borrowed_data() {
                Ok(BorrowedResult::Emit(kind)) => {
                    self.last_emitted = Some(kind);
                    return Some(Ok(self.build_event(kind)));
                }
                Ok(BorrowedResult::WaitForMore) => return None,
                Ok(BorrowedResult::Fallback) => {}
                Err(e) => {
                    if self.state != ConnState::Closing {
                        self.state = ConnState::Closed;
                    }
                    return Some(Err(e));
                }
            }

            match self.advance() {
                Ok(AdvanceResult::Emit(kind)) => {
                    self.last_emitted = Some(kind);
                    return Some(Ok(self.build_event(kind)));
                }
                Ok(AdvanceResult::Progressed) => continue,
                Ok(AdvanceResult::NeedMore) => return None,
                Err(e) => {
                    if self.state != ConnState::Closing {
                        self.state = ConnState::Closed;
                    }
                    return Some(Err(e));
                }
            }
        }
    }

    /// Common inbound market-data path：完整、未分片的 Text/Binary frame 已全部落在
    /// `recv_buf` 时，直接借用 payload 给 Event。其它情况回退到流式 parser。
    fn try_emit_borrowed_data(&mut self) -> Result<BorrowedResult, WsError> {
        if self.state != ConnState::Open
            || !self.parser.is_idle()
            || self.msg_opcode.is_some()
            || self.recv_buf.is_empty()
        {
            return Ok(BorrowedResult::Fallback);
        }

        let Some((header, header_len)) = parse_header(self.recv_buf.as_slice()).map_err(|e| {
            self.queue_close(CloseCode::ProtocolError.as_u16(), "frame parse error");
            WsError::Frame(e)
        })?
        else {
            return Ok(BorrowedResult::WaitForMore);
        };

        if header.mask.is_some() {
            self.queue_close(
                CloseCode::ProtocolError.as_u16(),
                "server sent masked frame",
            );
            return Err(WsError::Frame(FrameError::ServerSentMaskedFrame));
        }
        if !header.fin || !matches!(header.opcode, OpCode::Text | OpCode::Binary) {
            return Ok(BorrowedResult::Fallback);
        }
        if header.payload_len > self.config.max_frame_payload {
            self.queue_close(CloseCode::MessageTooBig.as_u16(), "frame too large");
            return Err(WsError::MessageTooLarge);
        }

        let payload_len = usize::try_from(header.payload_len).map_err(|_| {
            self.queue_close(CloseCode::MessageTooBig.as_u16(), "frame too large");
            WsError::MessageTooLarge
        })?;
        if payload_len > self.config.max_message_size {
            self.queue_close(
                CloseCode::MessageTooBig.as_u16(),
                "message exceeds max_message_size",
            );
            return Err(WsError::MessageTooLarge);
        }
        let Some(frame_len) = header_len.checked_add(payload_len) else {
            self.queue_close(CloseCode::MessageTooBig.as_u16(), "frame too large");
            return Err(WsError::MessageTooLarge);
        };
        if self.recv_buf.len() < frame_len {
            return Ok(BorrowedResult::WaitForMore);
        }

        let payload_end = frame_len;
        let payload = &self.recv_buf.as_slice()[header_len..payload_end];
        let kind = match header.opcode {
            OpCode::Text => {
                if !self.config.assume_text_utf8 && std::str::from_utf8(payload).is_err() {
                    self.queue_close(CloseCode::InvalidPayload.as_u16(), "invalid utf-8");
                    return Err(WsError::Utf8Invalid);
                }
                EmitKind::BorrowedText
            }
            OpCode::Binary => EmitKind::BorrowedBinary,
            _ => unreachable!("guarded above"),
        };
        self.borrowed_payload = Some(BorrowedPayload {
            payload_start: header_len,
            payload_end,
            frame_len,
        });
        Ok(BorrowedResult::Emit(kind))
    }

    /// Drain WS events but only surface Text/Binary data messages to `sink`.
    ///
    /// 完整、未分片的 Text/Binary frame 直接 dispatch；其它 frame 回退到
    /// [`poll_event`](Self::poll_event)。因此 Ping/Pong/Close、fragmentation、
    /// UTF-8 校验和 auto-pong 语义都与通用路径一致。适合交易所 feed：
    /// Text JSON 和 Binary SBE 都会被分发，control frame 由 WS 层处理。
    ///
    /// 返回这一轮处理掉的 WS event 数量（包含被内部消费的 Ping/Pong/Close）。
    pub fn drain_data_events<F>(&mut self, mut sink: F) -> Result<usize, WsError>
    where
        F: FnMut(DataEvent<'_>),
    {
        let mut events = 0_usize;
        loop {
            // poll_event 的 borrowed payload 在下一次 poll 才 consume。data-only
            // fast path 也必须先完成这步，才能安全继续看 recv_buf 的下一帧。
            self.clear_after_emit();
            let (direct_events, direct_result) = self.try_drain_data_events(&mut sink)?;
            events += direct_events;
            match direct_result {
                DirectDataResult::Drained => continue,
                DirectDataResult::WaitForMore => return Ok(events),
                DirectDataResult::Fallback => {}
            }

            let Some(res) = self.poll_event() else {
                return Ok(events);
            };
            let ev = res?;
            events += 1;
            match ev {
                Event::Text(s) => sink(DataEvent::Text(s)),
                Event::Binary(bytes) => sink(DataEvent::Binary(bytes)),
                Event::HandshakeComplete
                | Event::Ping(_)
                | Event::Pong(_)
                | Event::Close { .. } => {}
            }
        }
    }

    /// Feed a newly-arrived plaintext chunk and drain data events.
    ///
    /// Hot path: when `bytes` contains complete, unfragmented Text/Binary
    /// frames and the internal parser is idle, payloads are borrowed directly
    /// from `bytes` and dispatched to `sink` without copying the plaintext into
    /// `recv_buf`.
    ///
    /// Fallback: partial frames, control frames, fragmented messages, handshakes
    /// or any existing buffered state copy only the remaining bytes into
    /// `recv_buf`, then continue through the full WebSocket state machine.
    pub fn drain_data_events_from_ingress<F>(
        &mut self,
        bytes: &[u8],
        mut sink: F,
    ) -> Result<usize, WsError>
    where
        F: for<'a> FnMut(DataEvent<'a>),
    {
        self.clear_after_emit();
        if bytes.is_empty() {
            return self.drain_data_events(sink);
        }

        if self.can_drain_ingress_directly() {
            let (events, consumed, result) =
                self.try_drain_ingress_data_events(bytes, &mut sink)?;
            match result {
                DirectDataResult::Drained => return Ok(events),
                DirectDataResult::WaitForMore => {
                    self.recv_buf.extend_from_slice(&bytes[consumed..]);
                    return Ok(events);
                }
                DirectDataResult::Fallback => {
                    self.recv_buf.extend_from_slice(&bytes[consumed..]);
                    return self
                        .drain_data_events(&mut sink)
                        .map(|fallback_events| events + fallback_events);
                }
            }
        }

        self.feed_recv(bytes);
        self.drain_data_events(sink)
    }

    /// data-only 常见路径：完整单帧直接借 recv_buf payload 给 sink，回调返回后
    /// 立即 consume。control / fragmented frame 返回 Fallback，由 poll_event
    /// 完整状态机接手。
    fn try_drain_data_events<F>(
        &mut self,
        sink: &mut F,
    ) -> Result<(usize, DirectDataResult), WsError>
    where
        F: FnMut(DataEvent<'_>),
    {
        if self.state != ConnState::Open
            || !self.parser.is_idle()
            || self.msg_opcode.is_some()
            || self.recv_buf.is_empty()
        {
            return Ok((0, DirectDataResult::Fallback));
        }

        let mut consumed = 0_usize;
        let mut events = 0_usize;
        loop {
            let bytes = &self.recv_buf.as_slice()[consumed..];
            if bytes.is_empty() {
                self.recv_buf.consume(consumed);
                return Ok((events, DirectDataResult::Drained));
            }

            let header = match parse_server_data_header_fast(bytes) {
                Ok(FastDataHeaderResult::Parsed(header)) => header,
                Ok(FastDataHeaderResult::NeedMore) => {
                    self.recv_buf.consume(consumed);
                    return Ok((events, DirectDataResult::WaitForMore));
                }
                Ok(FastDataHeaderResult::Fallback) => {
                    self.recv_buf.consume(consumed);
                    return Ok((events, DirectDataResult::Fallback));
                }
                Err(e) => {
                    self.recv_buf.consume(consumed);
                    self.queue_close(CloseCode::ProtocolError.as_u16(), "frame parse error");
                    return Err(WsError::Frame(e));
                }
            };

            if header.payload_len_u64 > self.config.max_frame_payload {
                self.recv_buf.consume(consumed);
                self.queue_close(CloseCode::MessageTooBig.as_u16(), "frame too large");
                return Err(WsError::MessageTooLarge);
            }

            let payload_len = header.payload_len;
            if payload_len > self.config.max_message_size {
                self.recv_buf.consume(consumed);
                self.queue_close(
                    CloseCode::MessageTooBig.as_u16(),
                    "message exceeds max_message_size",
                );
                return Err(WsError::MessageTooLarge);
            }
            let Some(frame_len) = header.header_len.checked_add(payload_len) else {
                self.recv_buf.consume(consumed);
                self.queue_close(CloseCode::MessageTooBig.as_u16(), "frame too large");
                return Err(WsError::MessageTooLarge);
            };
            if bytes.len() < frame_len {
                self.recv_buf.consume(consumed);
                return Ok((events, DirectDataResult::WaitForMore));
            }

            let payload = &bytes[header.header_len..frame_len];
            match header.opcode {
                OpCode::Text => {
                    let text = if self.config.assume_text_utf8 {
                        // SAFETY: Enabled only through `unsafe`
                        // `WsConfig::with_assume_text_utf8_unchecked`, whose
                        // caller promises all server text payloads are UTF-8.
                        unsafe { std::str::from_utf8_unchecked(payload) }
                    } else if let Ok(text) = std::str::from_utf8(payload) {
                        text
                    } else {
                        self.recv_buf.consume(consumed);
                        self.queue_close(CloseCode::InvalidPayload.as_u16(), "invalid utf-8");
                        return Err(WsError::Utf8Invalid);
                    };
                    sink(DataEvent::Text(text));
                }
                OpCode::Binary => sink(DataEvent::Binary(payload)),
                _ => unreachable!("guarded above"),
            }
            consumed += frame_len;
            events += 1;
        }
    }

    #[inline]
    fn can_drain_ingress_directly(&self) -> bool {
        self.state == ConnState::Open
            && self.parser.is_idle()
            && self.msg_opcode.is_none()
            && self.recv_buf.is_empty()
            && self.borrowed_payload.is_none()
    }

    /// Direct data-only path over caller-owned ingress bytes. Returns
    /// `(events, consumed, result)`.
    fn try_drain_ingress_data_events<F>(
        &mut self,
        mut bytes: &[u8],
        sink: &mut F,
    ) -> Result<(usize, usize, DirectDataResult), WsError>
    where
        F: for<'a> FnMut(DataEvent<'a>),
    {
        let original_len = bytes.len();
        let mut events = 0_usize;
        loop {
            if bytes.is_empty() {
                return Ok((events, original_len, DirectDataResult::Drained));
            }

            let header = match parse_server_data_header_fast(bytes) {
                Ok(FastDataHeaderResult::Parsed(header)) => header,
                Ok(FastDataHeaderResult::NeedMore) => {
                    return Ok((
                        events,
                        original_len - bytes.len(),
                        DirectDataResult::WaitForMore,
                    ));
                }
                Ok(FastDataHeaderResult::Fallback) => {
                    return Ok((
                        events,
                        original_len - bytes.len(),
                        DirectDataResult::Fallback,
                    ));
                }
                Err(e) => {
                    self.queue_close(CloseCode::ProtocolError.as_u16(), "frame parse error");
                    return Err(WsError::Frame(e));
                }
            };

            if header.payload_len_u64 > self.config.max_frame_payload {
                self.queue_close(CloseCode::MessageTooBig.as_u16(), "frame too large");
                return Err(WsError::MessageTooLarge);
            }

            let payload_len = header.payload_len;
            if payload_len > self.config.max_message_size {
                self.queue_close(
                    CloseCode::MessageTooBig.as_u16(),
                    "message exceeds max_message_size",
                );
                return Err(WsError::MessageTooLarge);
            }
            let Some(frame_len) = header.header_len.checked_add(payload_len) else {
                self.queue_close(CloseCode::MessageTooBig.as_u16(), "frame too large");
                return Err(WsError::MessageTooLarge);
            };
            if bytes.len() < frame_len {
                return Ok((
                    events,
                    original_len - bytes.len(),
                    DirectDataResult::WaitForMore,
                ));
            }

            let payload = &bytes[header.header_len..frame_len];
            match header.opcode {
                OpCode::Text => {
                    let text = if self.config.assume_text_utf8 {
                        // SAFETY: Enabled only through `unsafe`
                        // `WsConfig::with_assume_text_utf8_unchecked`, whose
                        // caller promises all server text payloads are UTF-8.
                        unsafe { std::str::from_utf8_unchecked(payload) }
                    } else {
                        std::str::from_utf8(payload).map_err(|_| {
                            self.queue_close(CloseCode::InvalidPayload.as_u16(), "invalid utf-8");
                            WsError::Utf8Invalid
                        })?
                    };
                    sink(DataEvent::Text(text));
                }
                OpCode::Binary => sink(DataEvent::Binary(payload)),
                _ => unreachable!("guarded above"),
            }
            events += 1;
            bytes = &bytes[frame_len..];
        }
    }

    /// 主动发 Text。`payload` 必须是合法 UTF-8。仅在 `ConnState::Open` 生效。
    pub fn send_text(&mut self, payload: &[u8]) -> Result<(), WsError> {
        if std::str::from_utf8(payload).is_err() {
            return Err(WsError::Utf8Invalid);
        }
        self.assert_can_send_data()?;
        self.write_frame(true, OpCode::Text, payload);
        Ok(())
    }

    pub fn send_binary(&mut self, payload: &[u8]) -> Result<(), WsError> {
        self.assert_can_send_data()?;
        self.write_frame(true, OpCode::Binary, payload);
        Ok(())
    }

    /// 主动 Ping（payload ≤ 125 字节）
    pub fn send_ping(&mut self, payload: &[u8]) -> Result<(), WsError> {
        if payload.len() > 125 {
            return Err(WsError::Protocol("ping payload > 125 bytes"));
        }
        self.assert_can_send_data()?;
        self.write_frame(true, OpCode::Ping, payload);
        Ok(())
    }

    /// 主动 Pong（payload ≤ 125 字节）。
    ///
    /// 收到 Ping 时默认会自动 queue Pong；这个 API 用于手动响应 Ping（例如关闭
    /// `auto_pong` 后由业务接管）或发送交易所允许的 unsolicited Pong。
    pub fn send_pong(&mut self, payload: &[u8]) -> Result<(), WsError> {
        if payload.len() > 125 {
            return Err(WsError::Protocol("pong payload > 125 bytes"));
        }
        self.assert_can_send_data()?;
        self.write_frame(true, OpCode::Pong, payload);
        Ok(())
    }

    /// 发 Close（code 必须 endpoint-sendable，参见 RFC §7.4.2）。仅 `Open` 状态会
    /// queue Close frame 并进入 `Closing`；`Connecting` 返回 `InvalidState`。
    /// 重复调用幂等：已 Closing / Closed 时 no-op，避免发第二个 Close frame
    /// （RFC §5.5.1：每端最多一个 Close）。
    pub fn send_close(&mut self, code: u16, reason: &str) -> Result<(), WsError> {
        if matches!(self.state, ConnState::Closing | ConnState::Closed) {
            return Ok(());
        }
        if self.state != ConnState::Open {
            return Err(WsError::InvalidState(self.state));
        }
        if !is_valid_endpoint_sent(code) {
            return Err(WsError::Close(CloseError::InvalidCode(code)));
        }
        if reason.len() > 123 {
            return Err(WsError::Protocol("close reason > 123 bytes"));
        }
        let mut payload = [0_u8; 125];
        let n = encode_close_payload(&mut payload, code, reason);
        self.write_frame(true, OpCode::Close, &payload[..n]);
        self.state = ConnState::Closing;
        Ok(())
    }

    /// 数据帧 / Ping / Pong 的发送前置检查。仅 Open 允许。
    /// 早期版本在 Connecting 状态把 data frame 提前塞进 `tx_buf` 会把 GET upgrade
    /// 请求和数据帧拼到一起（畸形 wire format），release 路径下静默错路。
    fn assert_can_send_data(&self) -> Result<(), WsError> {
        if self.state == ConnState::Open {
            Ok(())
        } else {
            Err(WsError::InvalidState(self.state))
        }
    }

    /// 待发字节（caller 写到 socket）。返回的 slice 总是从 `tx_head` 开始。
    #[must_use]
    pub fn pending_tx(&self) -> &[u8] {
        // `tx_head <= tx_buf.len()` 由 ack_tx / push 路径维护
        &self.tx_buf[self.tx_head..]
    }

    /// 通知已发出 N 字节。caller 必须保证 `n <= pending_tx().len()`；O(1) cursor
    /// 自增，head 追上 len 时整体 reset（仍 O(1)）。早期实现是
    /// `Vec::drain(..n)`，partial-send 时是 O(n) memmove。
    pub fn ack_tx(&mut self, n: usize) {
        debug_assert!(n <= self.pending_tx().len());
        self.tx_head += n;
        if self.tx_head == self.tx_buf.len() {
            self.tx_buf.clear();
            self.tx_head = 0;
        }
    }

    /// `tx_buf` 在 `tx_head > 0` 但即将 capacity-exceed 时做一次 compact —— 把
    /// `tx_buf[tx_head..]` move 到 front，head 归零。`encode_upgrade_request`
    /// 直接 push 到 `&mut Vec<u8>`，所以在它之前调用。
    fn compact_tx_if_needed(&mut self, extra: usize) {
        if self.tx_head == 0 {
            return;
        }
        if self.tx_buf.len() + extra > self.tx_buf.capacity() {
            self.tx_buf.drain(..self.tx_head);
            self.tx_head = 0;
        }
    }

    #[inline]
    #[must_use]
    pub const fn state(&self) -> ConnState {
        self.state
    }

    // ─── internals ──────────────────────────────────────────────────────────

    /// 推进一步：处理 handshake 或 frame parser
    fn advance(&mut self) -> Result<AdvanceResult, WsError> {
        match self.state {
            ConnState::Connecting => self.advance_handshake(),
            ConnState::Open | ConnState::Closing => self.advance_frame(),
            ConnState::Closed => Ok(AdvanceResult::NeedMore),
        }
    }

    fn advance_handshake(&mut self) -> Result<AdvanceResult, WsError> {
        let parsed = parse_response(self.recv_buf.as_slice())?;
        let (status, end) = match parsed {
            Some((r, end)) => {
                let offered: Vec<&str> = self
                    .config
                    .subprotocols
                    .iter()
                    .map(String::as_str)
                    .collect();
                verify_upgrade_response(&r, &self.client_key, &offered)?;
                (r.status, end)
            }
            None => return Ok(AdvanceResult::NeedMore),
        };
        debug_assert_eq!(status, 101);
        self.recv_buf.consume(end);
        self.state = ConnState::Open;
        Ok(AdvanceResult::Emit(EmitKind::HandshakeComplete))
    }

    fn advance_frame(&mut self) -> Result<AdvanceResult, WsError> {
        let (consumed, action) = {
            let outcome = match self.parser.feed_one(self.recv_buf.as_slice()) {
                Ok(outcome) => outcome,
                Err(e) => {
                    self.queue_close(CloseCode::ProtocolError.as_u16(), "frame parse error");
                    return Err(WsError::Frame(e));
                }
            };
            match outcome {
                FeedOutcome::NeedMore { consumed } => (consumed, Action::NeedMore),
                FeedOutcome::Event {
                    consumed,
                    event: FrameEvent::FrameStart(h),
                } => (consumed, Action::FrameStart(h)),
                FeedOutcome::Event {
                    consumed,
                    event: FrameEvent::PayloadChunk(slice),
                } => {
                    if let Some(op) = self.cur_opcode {
                        if op.is_control() {
                            let start = self.ctl_len as usize;
                            let end = start + slice.len();
                            self.ctl_buf[start..end].copy_from_slice(slice);
                            self.ctl_len = end as u8;
                        } else {
                            self.msg_buf.extend_from_slice(slice);
                        }
                    }
                    (consumed, Action::PayloadChunk)
                }
                FeedOutcome::Event {
                    consumed,
                    event: FrameEvent::FrameEnd,
                } => (consumed, Action::FrameEnd),
            }
        };

        self.recv_buf.consume(consumed);

        match action {
            Action::NeedMore => Ok(AdvanceResult::NeedMore),
            Action::FrameStart(h) => {
                self.on_frame_start(h)?;
                Ok(AdvanceResult::Progressed)
            }
            Action::PayloadChunk => Ok(AdvanceResult::Progressed),
            Action::FrameEnd => match self.on_frame_end()? {
                Some(kind) => Ok(AdvanceResult::Emit(kind)),
                None => Ok(AdvanceResult::Progressed),
            },
        }
    }

    fn on_frame_start(&mut self, h: FrameHeader) -> Result<(), WsError> {
        if h.payload_len > self.config.max_frame_payload {
            self.queue_close(CloseCode::MessageTooBig.as_u16(), "frame too large");
            return Err(WsError::MessageTooLarge);
        }

        if h.opcode.is_control() {
            self.ctl_len = 0;
            self.cur_opcode = Some(h.opcode);
            self.cur_fin = h.fin;
            return Ok(());
        }

        match h.opcode {
            OpCode::Continuation => {
                if self.msg_opcode.is_none() {
                    self.queue_close(
                        CloseCode::ProtocolError.as_u16(),
                        "continuation without start",
                    );
                    return Err(WsError::Protocol("continuation without start"));
                }
            }
            OpCode::Text | OpCode::Binary => {
                if self.msg_opcode.is_some() {
                    self.queue_close(
                        CloseCode::ProtocolError.as_u16(),
                        "new data frame mid-fragmentation",
                    );
                    return Err(WsError::Protocol("nested data frame"));
                }
                self.msg_opcode = Some(h.opcode);
                self.msg_buf.clear();
            }
            _ => return Err(WsError::Internal("non-data non-control opcode")),
        }

        // 累计消息大小检查
        let prospective = self.msg_buf.len().saturating_add(h.payload_len as usize);
        if prospective > self.config.max_message_size {
            self.queue_close(
                CloseCode::MessageTooBig.as_u16(),
                "message exceeds max_message_size",
            );
            return Err(WsError::MessageTooLarge);
        }

        self.cur_opcode = Some(h.opcode);
        self.cur_fin = h.fin;
        Ok(())
    }

    fn on_frame_end(&mut self) -> Result<Option<EmitKind>, WsError> {
        let opcode = self
            .cur_opcode
            .take()
            .ok_or(WsError::Internal("FrameEnd without FrameStart"))?;
        let fin = self.cur_fin;

        if opcode.is_control() {
            return match opcode {
                OpCode::Ping => {
                    if self.config.auto_pong {
                        // pong with same payload
                        let len = self.ctl_len as usize;
                        let mut payload = [0_u8; 125];
                        payload[..len].copy_from_slice(&self.ctl_buf[..len]);
                        self.write_frame(true, OpCode::Pong, &payload[..len]);
                    }
                    Ok(Some(EmitKind::Ping))
                }
                OpCode::Pong => Ok(Some(EmitKind::Pong)),
                OpCode::Close => {
                    self.on_received_close()?;
                    Ok(Some(EmitKind::Close))
                }
                _ => Err(WsError::Internal("bad control opcode")),
            };
        }

        if !fin {
            // 还在 fragmented，等下一帧 continuation
            return Ok(None);
        }

        let msg_opcode = self
            .msg_opcode
            .take()
            .ok_or(WsError::Internal("FrameEnd FIN without active message"))?;

        match msg_opcode {
            OpCode::Text => {
                if !self.config.assume_text_utf8 && std::str::from_utf8(&self.msg_buf).is_err() {
                    self.queue_close(CloseCode::InvalidPayload.as_u16(), "invalid utf-8");
                    return Err(WsError::Utf8Invalid);
                }
                Ok(Some(EmitKind::Text))
            }
            OpCode::Binary => Ok(Some(EmitKind::Binary)),
            _ => Err(WsError::Internal("non-data msg_opcode")),
        }
    }

    fn on_received_close(&mut self) -> Result<(), WsError> {
        // 解析 ctl_buf
        let payload = &self.ctl_buf[..self.ctl_len as usize];
        let parsed = parse_close_payload(payload).map_err(WsError::Close);

        let (echo_code, _echo_reason) = match parsed {
            Ok(Some((code, reason))) => (code, reason),
            Ok(None) => (CloseCode::Normal.as_u16(), ""),
            Err(e) => {
                // peer 发了违法 close payload — 我们回 1002
                self.queue_close(CloseCode::ProtocolError.as_u16(), "bad close payload");
                self.state = ConnState::Closing;
                return Err(e);
            }
        };

        // RFC §5.5.1：收到 close 后必须 echo 一个 close（如果还没发过）
        if self.state == ConnState::Open {
            self.queue_close(echo_code, "");
        }
        self.state = ConnState::Closed;
        Ok(())
    }

    fn queue_close(&mut self, code: u16, reason: &str) {
        // 重复 queue 守卫：已 Closing/Closed 则不再发第二个 Close frame。
        // 触发场景：protocol error 在 on_received_close 已 queue 过 echo 后又
        // 触到一次（fragmented invalid utf-8 + 紧跟 Close payload error 等组合）。
        if matches!(self.state, ConnState::Closing | ConnState::Closed) {
            return;
        }
        let mut payload = [0_u8; 125];
        let n = encode_close_payload(&mut payload, code, reason);
        self.write_frame(true, OpCode::Close, &payload[..n]);
        if self.state == ConnState::Open {
            self.state = ConnState::Closing;
        }
    }

    /// 编码一帧（带 mask）到 tx_buf
    fn write_frame(&mut self, fin: bool, opcode: OpCode, payload: &[u8]) {
        let mask = self.next_mask();
        let mut hdr = [0_u8; MAX_HEADER_LEN];
        let n = encode_header(&mut hdr, fin, opcode, Some(mask), payload.len() as u64);
        // 推 frame 前 compact 一次 —— 把 partial-send 留下的 head>0 数据移到
        // front，避免 tx_buf 无限增长（每帧 capacity check 一次，head==0 时
        // O(1) noop，hot path 没有损耗）。
        self.compact_tx_if_needed(n + payload.len());
        self.tx_buf.extend_from_slice(&hdr[..n]);
        let payload_start = self.tx_buf.len();
        self.tx_buf.extend_from_slice(payload);
        let payload_slice = &mut self.tx_buf[payload_start..];
        mask_inplace(payload_slice, mask);
    }

    /// CSPRNG-backed mask key。从 `mask_pool` 取 4 字节，pool 耗尽时 refill。
    /// 满足 RFC §10.3 "MUST be unpredictable" 的要求 —— 早期 xorshift64 版本
    /// 是纯递推 PRNG，违规。refill 失败时只能 panic，因为 caller 已经把消息
    /// 入到 send 队列、无法回退；SystemRandom 在 Linux 上失败相当于内核 entropy
    /// 完全枯竭，HFT 部署上等同于不可恢复故障。
    fn next_mask(&mut self) -> [u8; 4] {
        if self.mask_pool_cursor + 4 > MASK_POOL_BYTES {
            use ring::rand::SecureRandom;
            self.mask_rng
                .fill(&mut self.mask_pool)
                .expect("SystemRandom::fill must not fail on supported platforms");
            self.mask_pool_cursor = 0;
        }
        let i = self.mask_pool_cursor;
        let key = [
            self.mask_pool[i],
            self.mask_pool[i + 1],
            self.mask_pool[i + 2],
            self.mask_pool[i + 3],
        ];
        self.mask_pool_cursor = i + 4;
        key
    }

    fn build_event(&self, kind: EmitKind) -> Event<'_> {
        match kind {
            EmitKind::HandshakeComplete => Event::HandshakeComplete,
            EmitKind::Text => {
                let s = if self.config.assume_text_utf8 {
                    // SAFETY: Enabled only through `unsafe`
                    // `WsConfig::with_assume_text_utf8_unchecked`, whose caller
                    // promises all server text payloads are UTF-8.
                    unsafe { std::str::from_utf8_unchecked(&self.msg_buf) }
                } else {
                    // utf-8 已在 on_frame_end 校验
                    std::str::from_utf8(&self.msg_buf).unwrap_or("")
                };
                Event::Text(s)
            }
            EmitKind::Binary => Event::Binary(&self.msg_buf),
            EmitKind::BorrowedText => {
                let payload = self.borrowed_payload();
                let s = if self.config.assume_text_utf8 {
                    // SAFETY: Enabled only through `unsafe`
                    // `WsConfig::with_assume_text_utf8_unchecked`, whose caller
                    // promises all server text payloads are UTF-8.
                    unsafe { std::str::from_utf8_unchecked(payload) }
                } else {
                    // utf-8 已在 try_emit_borrowed_data 校验
                    std::str::from_utf8(payload)
                        .unwrap_or_else(|_| unreachable!("borrowed text must retain valid utf-8"))
                };
                Event::Text(s)
            }
            EmitKind::BorrowedBinary => Event::Binary(self.borrowed_payload()),
            EmitKind::Ping => Event::Ping(&self.ctl_buf[..self.ctl_len as usize]),
            EmitKind::Pong => Event::Pong(&self.ctl_buf[..self.ctl_len as usize]),
            EmitKind::Close => {
                // EmitKind::Close 只在 on_received_close() 返回 Ok 后入队，所以
                // parse_close_payload 这里**必定 Ok**——`Err` 分支早已在 on_received_close
                // 短路。`Ok(None)` 是 RFC §7.4.1 的 "No Status Rcvd" 默认 1005。
                let payload = &self.ctl_buf[..self.ctl_len as usize];
                match parse_close_payload(payload) {
                    Ok(Some((code, reason))) => Event::Close { code, reason },
                    Ok(None) => Event::Close {
                        code: 1005,
                        reason: "",
                    },
                    Err(_) => unreachable!("on_received_close gates Err before EmitKind::Close"),
                }
            }
        }
    }

    fn clear_after_emit(&mut self) {
        match self.last_emitted.take() {
            None => {}
            Some(EmitKind::HandshakeComplete) => {}
            Some(EmitKind::Text | EmitKind::Binary) => {
                self.msg_buf.clear();
            }
            Some(EmitKind::BorrowedText | EmitKind::BorrowedBinary) => {
                let borrowed = self
                    .borrowed_payload
                    .take()
                    .expect("borrowed emit must retain payload range");
                self.recv_buf.consume(borrowed.frame_len);
            }
            Some(EmitKind::Ping | EmitKind::Pong | EmitKind::Close) => {
                self.ctl_len = 0;
            }
        }
    }

    fn borrowed_payload(&self) -> &[u8] {
        let borrowed = self
            .borrowed_payload
            .as_ref()
            .expect("borrowed emit must retain payload range");
        &self.recv_buf.as_slice()[borrowed.payload_start..borrowed.payload_end]
    }
}

#[derive(Debug)]
enum Action {
    NeedMore,
    FrameStart(FrameHeader),
    PayloadChunk,
    FrameEnd,
}

#[derive(Debug)]
enum AdvanceResult {
    Emit(EmitKind),
    Progressed,
    NeedMore,
}

enum BorrowedResult {
    Emit(EmitKind),
    WaitForMore,
    Fallback,
}

enum DirectDataResult {
    Drained,
    WaitForMore,
    Fallback,
}

#[derive(Debug)]
struct FastDataHeader {
    opcode: OpCode,
    header_len: usize,
    payload_len: usize,
    payload_len_u64: u64,
}

#[derive(Debug)]
enum FastDataHeaderResult {
    Parsed(FastDataHeader),
    NeedMore,
    Fallback,
}

#[inline]
fn parse_server_data_header_fast(buf: &[u8]) -> Result<FastDataHeaderResult, FrameError> {
    if buf.len() < 2 {
        return Ok(FastDataHeaderResult::NeedMore);
    }

    let b0 = buf[0];
    let b1 = buf[1];
    if (b0 & 0x70) != 0 {
        return Err(FrameError::RsvBitsSet);
    }
    if (b1 & 0x80) != 0 {
        return Err(FrameError::ServerSentMaskedFrame);
    }
    if (b0 & 0x80) == 0 {
        return Ok(FastDataHeaderResult::Fallback);
    }

    let opcode = match b0 & 0x0F {
        0x1 => OpCode::Text,
        0x2 => OpCode::Binary,
        0x0 | 0x8 | 0x9 | 0xA => return Ok(FastDataHeaderResult::Fallback),
        other => return Err(FrameError::InvalidOpCode(other)),
    };

    let len7 = b1 & 0x7F;
    let (header_len, payload_len, payload_len_u64) = match len7 {
        0..=125 => (2_usize, usize::from(len7), u64::from(len7)),
        126 => {
            if buf.len() < 4 {
                return Ok(FastDataHeaderResult::NeedMore);
            }
            let val = u16::from_be_bytes([buf[2], buf[3]]);
            if val < 126 {
                return Err(FrameError::NonMinimalPayloadLength);
            }
            (4_usize, usize::from(val), u64::from(val))
        }
        127 => return Ok(FastDataHeaderResult::Fallback),
        _ => unreachable!("len7 is masked to 7 bits"),
    };

    Ok(FastDataHeaderResult::Parsed(FastDataHeader {
        opcode,
        header_len,
        payload_len,
        payload_len_u64,
    }))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;

    fn mk_client() -> WsClient {
        WsClient::new_client(WsConfig::new("example.com", "/ws")).unwrap()
    }

    /// 模拟 server 发给 client 的（unmasked）单帧 text
    fn server_text(payload: &[u8]) -> Vec<u8> {
        let mut buf = vec![0_u8; MAX_HEADER_LEN];
        let n = super::super::frame::encode_header(
            &mut buf,
            true,
            OpCode::Text,
            None,
            payload.len() as u64,
        );
        buf.truncate(n);
        buf.extend_from_slice(payload);
        buf
    }

    fn server_binary(payload: &[u8]) -> Vec<u8> {
        let mut buf = vec![0_u8; MAX_HEADER_LEN];
        let n = super::super::frame::encode_header(
            &mut buf,
            true,
            OpCode::Binary,
            None,
            payload.len() as u64,
        );
        buf.truncate(n);
        buf.extend_from_slice(payload);
        buf
    }

    fn server_close(code: u16, reason: &str) -> Vec<u8> {
        let mut payload = [0_u8; 125];
        let n = encode_close_payload(&mut payload, code, reason);
        let mut buf = vec![0_u8; MAX_HEADER_LEN];
        let hn = super::super::frame::encode_header(&mut buf, true, OpCode::Close, None, n as u64);
        buf.truncate(hn);
        buf.extend_from_slice(&payload[..n]);
        buf
    }

    fn server_control(opcode: OpCode, payload: &[u8]) -> Vec<u8> {
        let mut buf = vec![0_u8; MAX_HEADER_LEN];
        let hn =
            super::super::frame::encode_header(&mut buf, true, opcode, None, payload.len() as u64);
        buf.truncate(hn);
        buf.extend_from_slice(payload);
        buf
    }

    fn fake_101_response(client_key: &str) -> Vec<u8> {
        let accept = super::super::handshake::compute_accept(client_key);
        let mut s = String::new();
        s.push_str("HTTP/1.1 101 Switching Protocols\r\n");
        s.push_str("Upgrade: websocket\r\n");
        s.push_str("Connection: Upgrade\r\n");
        s.push_str(&format!("Sec-WebSocket-Accept: {accept}\r\n"));
        s.push_str("\r\n");
        s.into_bytes()
    }

    fn mk_open_client() -> WsClient {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        match c.poll_event() {
            Some(Ok(Event::HandshakeComplete)) => {}
            other => panic!("expected HandshakeComplete, got {other:?}"),
        }
        assert_eq!(c.state(), ConnState::Open);
        c
    }

    #[test]
    fn handshake_then_text_then_close() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        assert!(!c.pending_tx().is_empty());
        // ack everything
        let n = c.pending_tx().len();
        c.ack_tx(n);

        // feed 101 response
        let resp = fake_101_response(&c.client_key);
        c.feed_recv(&resp);
        match c.poll_event() {
            Some(Ok(Event::HandshakeComplete)) => {}
            other => panic!("expected HandshakeComplete, got {other:?}"),
        }
        assert_eq!(c.state(), ConnState::Open);

        // feed a text frame
        c.feed_recv(&server_text(b"hi"));
        match c.poll_event() {
            Some(Ok(Event::Text(s))) => assert_eq!(s, "hi"),
            other => panic!("expected Text, got {other:?}"),
        }

        // server sends close 1000
        c.feed_recv(&server_close(1000, "bye"));
        match c.poll_event() {
            Some(Ok(Event::Close { code, reason })) => {
                assert_eq!(code, 1000);
                assert_eq!(reason, "bye");
            }
            other => panic!("expected Close, got {other:?}"),
        }
        assert_eq!(c.state(), ConnState::Closed);
        // tx_buf should have echoed close
        assert!(!c.pending_tx().is_empty());
    }

    #[test]
    fn begin_handshake_rejects_duplicate_call() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        let pending_len = c.pending_tx().len();

        let err = c.begin_handshake().unwrap_err();
        assert!(matches!(err, WsError::Protocol("handshake already begun")));
        assert_eq!(c.pending_tx().len(), pending_len);
    }

    #[test]
    fn complete_text_uses_borrowed_payload_path() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event(); // consume HandshakeComplete

        c.feed_recv(&server_text(b"borrowed"));
        match c.poll_event() {
            Some(Ok(Event::Text(s))) => assert_eq!(s, "borrowed"),
            other => panic!("expected borrowed Text, got {other:?}"),
        }
        assert!(c.msg_buf.is_empty());
        assert!(c.borrowed_payload.is_some());

        assert!(c.poll_event().is_none());
        assert!(c.borrowed_payload.is_none());
        assert!(c.recv_buf.is_empty());
    }

    #[test]
    fn split_text_waits_then_uses_borrowed_payload_path() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event(); // consume HandshakeComplete

        let frame = server_text(b"split");
        c.feed_recv(&frame[..4]);
        assert!(c.poll_event().is_none());
        assert!(c.parser.is_idle());
        assert!(c.msg_buf.is_empty());

        c.feed_recv(&frame[4..]);
        match c.poll_event() {
            Some(Ok(Event::Text(s))) => assert_eq!(s, "split"),
            other => panic!("expected borrowed Text, got {other:?}"),
        }
        assert!(c.msg_buf.is_empty());
        assert!(c.borrowed_payload.is_some());
    }

    #[test]
    fn ping_triggers_auto_pong() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event(); // consume HandshakeComplete

        // construct unmasked Ping frame
        let mut frame = vec![0_u8; MAX_HEADER_LEN];
        let n = super::super::frame::encode_header(&mut frame, true, OpCode::Ping, None, 4);
        frame.truncate(n);
        frame.extend_from_slice(b"ping");
        c.feed_recv(&frame);

        match c.poll_event() {
            Some(Ok(Event::Ping(p))) => assert_eq!(p, b"ping"),
            other => panic!("{other:?}"),
        }
        // tx_buf has the auto-pong
        let tx = c.pending_tx();
        assert!(!tx.is_empty());
        // pong header byte0 = 0x8A (FIN=1, opcode=Pong)
        assert_eq!(tx[0], 0x8A);
        // payload is masked, can't compare directly, but length should be header + 4
    }

    #[test]
    fn drain_data_events_handles_control_and_dispatches_text_binary() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event(); // consume HandshakeComplete
        c.ack_tx(c.pending_tx().len());

        let mut wire = Vec::new();
        wire.extend_from_slice(&server_text(b"{\"op\":\"subscribed\"}"));
        wire.extend_from_slice(&server_control(OpCode::Ping, b"hb"));
        wire.extend_from_slice(&server_binary(b"\x01\x02\x03\x04"));
        c.feed_recv(&wire);

        let mut data = Vec::new();
        let events = c
            .drain_data_events(|ev| match ev {
                DataEvent::Text(s) => data.push(format!("text:{s}")),
                DataEvent::Binary(bytes) => data.push(format!("binary:{}", bytes.len())),
            })
            .unwrap();

        assert_eq!(events, 3);
        assert_eq!(
            data,
            vec![
                "text:{\"op\":\"subscribed\"}".to_owned(),
                "binary:4".to_owned()
            ]
        );
        assert!(
            !c.pending_tx().is_empty(),
            "Ping should have queued an auto-pong"
        );
    }

    #[test]
    fn drain_data_events_from_ingress_dispatches_direct_data_frames() {
        let mut c = mk_open_client();
        let mut wire = Vec::new();
        wire.extend_from_slice(&server_text(b"alpha"));
        wire.extend_from_slice(&server_binary(b"bravo"));

        let mut data = Vec::new();
        let events = c
            .drain_data_events_from_ingress(&wire, |ev| match ev {
                DataEvent::Text(s) => data.push(format!("text:{s}")),
                DataEvent::Binary(bytes) => {
                    data.push(format!("binary:{}", std::str::from_utf8(bytes).unwrap()));
                }
            })
            .unwrap();

        assert_eq!(events, 2);
        assert_eq!(
            data,
            vec!["text:alpha".to_owned(), "binary:bravo".to_owned()]
        );
        assert!(
            c.recv_buf.is_empty(),
            "complete direct frames should not copy"
        );
    }

    #[test]
    fn drain_data_events_from_ingress_dispatches_extended_16bit_text_frame() {
        let mut c = mk_open_client();
        let payload = vec![b'x'; 130];
        let wire = server_text(&payload);

        let mut text_len = 0_usize;
        let events = c
            .drain_data_events_from_ingress(&wire, |ev| match ev {
                DataEvent::Text(s) => text_len = s.len(),
                DataEvent::Binary(_) => panic!("unexpected binary"),
            })
            .unwrap();

        assert_eq!(events, 1);
        assert_eq!(text_len, payload.len());
        assert!(
            c.recv_buf.is_empty(),
            "complete direct frames should not copy"
        );
    }

    #[test]
    fn drain_data_events_from_ingress_buffers_partial_frame() {
        let mut c = mk_open_client();
        let wire = server_text(b"partial");
        let split_at = wire.len() - 2;

        let mut data = Vec::new();
        let events = c
            .drain_data_events_from_ingress(&wire[..split_at], |ev| match ev {
                DataEvent::Text(s) => data.push(s.to_owned()),
                DataEvent::Binary(_) => panic!("unexpected binary"),
            })
            .unwrap();
        assert_eq!(events, 0);
        assert!(!c.recv_buf.is_empty(), "partial frame must be buffered");

        let events = c
            .drain_data_events_from_ingress(&wire[split_at..], |ev| match ev {
                DataEvent::Text(s) => data.push(s.to_owned()),
                DataEvent::Binary(_) => panic!("unexpected binary"),
            })
            .unwrap();
        assert_eq!(events, 1);
        assert_eq!(data, vec!["partial".to_owned()]);
    }

    #[test]
    fn drain_data_events_from_ingress_falls_back_for_control_frames() {
        let mut c = mk_open_client();
        let mut wire = Vec::new();
        wire.extend_from_slice(&server_text(b"before"));
        wire.extend_from_slice(&server_control(OpCode::Ping, b"hb"));
        wire.extend_from_slice(&server_text(b"after"));

        let mut data = Vec::new();
        let events = c
            .drain_data_events_from_ingress(&wire, |ev| match ev {
                DataEvent::Text(s) => data.push(s.to_owned()),
                DataEvent::Binary(_) => panic!("unexpected binary"),
            })
            .unwrap();

        assert_eq!(events, 3);
        assert_eq!(data, vec!["before".to_owned(), "after".to_owned()]);
        assert!(
            !c.pending_tx().is_empty(),
            "Ping fallback should still queue auto-pong"
        );
    }

    #[test]
    fn outgoing_text_is_masked() {
        let mut c = mk_client();
        // 必须先把 ws 推到 Open，才允许 send_text（state guard）
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event();
        assert_eq!(c.state(), ConnState::Open);
        c.send_text(b"hello").unwrap();
        let tx = c.pending_tx();
        // byte0 = FIN | Text = 0x81
        assert_eq!(tx[0], 0x81);
        // byte1 high bit = MASK = 0x80, len = 5 → 0x85
        assert_eq!(tx[1], 0x85);
        // 4 bytes mask, then 5 bytes masked payload — total 2+4+5 = 11
        assert_eq!(tx.len(), 11);
        // unmask payload using mask key from tx[2..6]
        let key = [tx[2], tx[3], tx[4], tx[5]];
        let mut payload: Vec<u8> = tx[6..].to_vec();
        super::super::mask::mask_inplace(&mut payload, key);
        assert_eq!(&payload, b"hello");
    }

    #[test]
    fn outgoing_pong_is_masked() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event();
        assert_eq!(c.state(), ConnState::Open);

        c.send_pong(b"pong").unwrap();
        let tx = c.pending_tx();
        // byte0 = FIN | Pong = 0x8A
        assert_eq!(tx[0], 0x8A);
        // byte1 high bit = MASK = 0x80, len = 4 → 0x84
        assert_eq!(tx[1], 0x84);
        assert_eq!(tx.len(), 10);

        let key = [tx[2], tx[3], tx[4], tx[5]];
        let mut payload: Vec<u8> = tx[6..].to_vec();
        super::super::mask::mask_inplace(&mut payload, key);
        assert_eq!(&payload, b"pong");
    }

    #[test]
    fn send_rejects_invalid_payloads_in_release_path() {
        let mut c = mk_client();
        assert!(matches!(
            c.send_text(b"hello").unwrap_err(),
            WsError::InvalidState(ConnState::Connecting)
        ));
        assert!(matches!(
            c.send_pong(b"pong").unwrap_err(),
            WsError::InvalidState(ConnState::Connecting)
        ));

        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event();
        assert_eq!(c.state(), ConnState::Open);

        assert!(matches!(c.send_text(&[0xFF]), Err(WsError::Utf8Invalid)));
        assert!(matches!(
            c.send_ping(&[0_u8; 126]),
            Err(WsError::Protocol("ping payload > 125 bytes"))
        ));
        assert!(matches!(
            c.send_pong(&[0_u8; 126]),
            Err(WsError::Protocol("pong payload > 125 bytes"))
        ));
        assert!(matches!(
            c.send_close(1006, ""),
            Err(WsError::Close(CloseError::InvalidCode(1006)))
        ));
        assert!(matches!(
            c.send_close(1000, "x".repeat(124).as_str()),
            Err(WsError::Protocol("close reason > 123 bytes"))
        ));
    }

    #[test]
    fn frame_parse_error_queues_close_before_error() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event();

        // Server frames must not be masked.
        c.feed_recv(b"\x81\x85\x12\x34\x56\x78Hello");
        match c.poll_event() {
            Some(Err(WsError::Frame(FrameError::ServerSentMaskedFrame))) => {}
            other => panic!("{other:?}"),
        }
        assert_eq!(c.state(), ConnState::Closing);
        assert!(!c.pending_tx().is_empty());
    }

    #[test]
    fn invalid_close_payload_queues_protocol_close() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event();

        c.feed_recv(&server_control(OpCode::Close, &[0x03]));
        match c.poll_event() {
            Some(Err(WsError::Close(CloseError::OneByte))) => {}
            other => panic!("{other:?}"),
        }
        assert_eq!(c.state(), ConnState::Closing);
        assert!(!c.pending_tx().is_empty());
    }

    #[test]
    fn fragmented_text_message_assembled() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.ack_tx(c.pending_tx().len());
        c.feed_recv(&fake_101_response(&c.client_key));
        c.poll_event();

        // First fragment: Text, FIN=0, payload "hel"
        let mut buf = vec![0_u8; MAX_HEADER_LEN];
        let n = super::super::frame::encode_header(&mut buf, false, OpCode::Text, None, 3);
        buf.truncate(n);
        buf.extend_from_slice(b"hel");
        // Continuation: FIN=1, payload "lo"
        let mut buf2 = vec![0_u8; MAX_HEADER_LEN];
        let n2 = super::super::frame::encode_header(&mut buf2, true, OpCode::Continuation, None, 2);
        buf2.truncate(n2);
        buf2.extend_from_slice(b"lo");

        c.feed_recv(&buf);
        // drain events from first fragment (no Text event yet because FIN=0)
        while let Some(_ev) = c.poll_event() {
            // shouldn't yield Text yet
        }
        c.feed_recv(&buf2);
        match c.poll_event() {
            Some(Ok(Event::Text(s))) => assert_eq!(s, "hello"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn bad_handshake_status_rejected() {
        let mut c = mk_client();
        c.begin_handshake().unwrap();
        c.feed_recv(b"HTTP/1.1 400 Bad Request\r\n\r\n");
        match c.poll_event() {
            Some(Err(WsError::Handshake(HandshakeError::BadStatus(400)))) => {}
            other => panic!("{other:?}"),
        }
        assert_eq!(c.state(), ConnState::Closed);
    }
}
