//! RFC 6455 WebSocket client —— 纯 sync 字节驱动状态机
//!
//! 分层（自底向上）：
//! - `frame`     RFC §5.2 帧 codec（header encode/decode）
//! - `mask`      RFC §5.3 XOR mask（AVX2 + scalar fallback）
//! - `parser`    流式 FrameParser（≤14 字节 header partial buf，payload 不缓存）
//! - `close`     RFC §7 close 状态码 + payload 解析
//! - `handshake` RFC §4 client Upgrade 请求 + 响应校验
//! - `client`    WsClient（最高层，组合上面所有 + 自动 pong + close handshake）
//!
//! 客户端模式 only：Persephone 永远是 client，所有发出去的帧必须 mask，
//! 收进来的服务端帧必须 unmasked（违反即 protocol error）。

pub mod client;
pub mod close;
pub mod frame;
pub mod handshake;
pub mod mask;
pub mod parser;

pub use crate::observability::{DataEventMeta, MarkedDataEvent};
pub use client::{
    ConnState, DEFAULT_MAX_FRAME_PAYLOAD, DEFAULT_MAX_MESSAGE_SIZE, DataEvent, Event,
    MASK_POOL_BYTES, WsClient, WsConfig, WsError,
};
pub use close::CloseCode;
pub use frame::{FrameError, FrameHeader, MAX_HEADER_LEN, OpCode};
