//! talaris —— 专门为 HFT 行情订阅系统打造的 hot-path 通信层
//!
//! io_uring proactor + 自实现 WS (RFC 6455) + TLS + HTTP/1.1 codec。
//!
//! ## 当前 v0.1 scope
//!
//! - [`ws`]         RFC 6455 client core（handshake / frame / mask / parser / fragmentation / control / close）
//! - [`tls`]        rustls 字节驱动 adapter（ALPN http/1.1 requested + verified）
//! - [`http`]       最小 HTTP/1.1 codec（WS upgrade request/response；无 REST client）
//! - [`proactor`]   io_uring 原语：connect/recv/send/close、SQ/CQ sizing、taskrun flags、pin、provided BufferRing、multishot recv；暴露 IO_LINK flag
//! - [`pool`]       单线程 multi-conn driver：1 个 proactor 驱动 N 条 WS；`pump_data` 只把 Text/Binary data 交给业务，control frame 仍走完整 WS 状态机
//! - [`connection`] 公共配置 / 状态 / 错误类型
//!
//! ## API stance
//!
//! talaris 按 low-level HFT transport toolkit 发布，而不是只暴露一个高层
//! WebSocket client wrapper。`Pool` / `ConnectionConfig` 是推荐入口；`ws`、
//! `proactor`、`http`、`tls` 模块也有意公开，供需要自己组合 framing /
//! transport、做 benchmark A/B 或接入 venue-specific codec 的用户使用。
//! v0.1 期间底层 API 会随实盘 benchmark 结果继续演进。

#![forbid(unsafe_op_in_unsafe_fn)]
#![allow(clippy::borrow_as_ptr)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_ptr_alignment)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::decimal_bitwise_operands)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::explicit_iter_loop)]
#![allow(clippy::format_push_string)]
#![allow(clippy::indexing_slicing)]
#![allow(clippy::ip_constant)]
#![allow(clippy::iter_with_drain)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_fields_in_debug)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::needless_continue)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::redundant_pub_crate)]
#![cfg_attr(test, allow(clippy::float_cmp))]

pub mod connection;

pub mod pool;

pub mod ws;

pub mod tls;

pub mod proactor;

pub mod http;

pub(crate) mod cursor_buf;

pub(crate) mod connection_state;

pub use pool::{
    ConnHandle, DEFAULT_POOL_COMPLETION_BATCH_CAPACITY, DEFAULT_POOL_INITIAL_CONN_CAPACITY, Pool,
    PoolConfig,
};

#[cfg(test)]
pub(crate) mod test_helpers;
