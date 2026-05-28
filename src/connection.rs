//! `connection` —— Pool 内单条 conn 的公共类型
//!
//! [`State`] / [`ConnectionConfig`] / [`ConnectionError`] 是 [`crate::Pool`] 对外
//! API 共用的类型。Step 3 起 `Connection` thin wrapper 已删，单 conn 路径走
//! `Pool::new` + `Pool::connect_blocking`。
//!
//! 实际驱动逻辑（socket / TLS / WS / buf_ring / send_buf 状态机）见
//! [`crate::connection_state`]。

#![allow(clippy::module_name_repetitions)]

use std::io;

use thiserror::Error;

use crate::proactor::{BufferRingError, ProactorConfig, ProactorError};
use crate::tls::TlsError;
use crate::ws::WsError;

/// 单 buffer 字节数默认值。caller 可通过 [`ConnectionConfig::with_buf_ring`]
/// 覆盖。HFT 行情常见 200B-1KB 帧，4 KiB 一格足够装下不跨 boundary；订阅 book
/// snapshot (4-16 KiB+) 时可调大到 16/32 KiB 减少 CQE 个数。
pub const DEFAULT_BUF_RING_BUF_SIZE: u32 = 4 * 1024;

/// buffer ring entry 数默认值（必须 2^N）。256 × 4 KiB = 1 MiB 池子，避免
/// 行情突发时 multishot 在 user-space recycle 前耗尽 provided buffers。
pub const DEFAULT_BUF_RING_ENTRIES: u16 = 256;

/// Driver 状态机。
///
/// ```text
///   Init ──submit_connect──▶ Connecting
///     Connecting ──Connect CQE──▶ TlsHandshake (TLS) | WsHandshake (plain)
///     TlsHandshake ──tls.is_handshaking()==false──▶ WsHandshake
///     WsHandshake ──WsClient emits HandshakeComplete──▶ Open
///     Open ──send_close / peer Close──▶ Closing ──Close CQE──▶ Closed
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum State {
    Init,
    Connecting,
    TlsHandshake,
    WsHandshake,
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("dns resolution returned no addresses for {0}")]
    DnsEmpty(String),
    #[error("proactor: {0}")]
    Proactor(#[from] ProactorError),
    #[error("buf ring: {0}")]
    BufRing(#[from] BufferRingError),
    #[error("tls: {0}")]
    Tls(#[from] TlsError),
    #[error("ws: {0}")]
    Ws(#[from] WsError),
    #[error("operation not allowed in state {0:?}")]
    InvalidState(State),
    #[error("connect failed: {0}")]
    ConnectFailed(#[source] io::Error),
    #[error("recv failed: {0}")]
    RecvFailed(#[source] io::Error),
    #[error("send failed: {0}")]
    SendFailed(#[source] io::Error),
    #[error("peer closed connection")]
    PeerClosed,
    #[error("CQE returned unknown OpKind: raw user_data = 0x{0:016x}")]
    UnknownOpKind(u64),
    /// Pool 的 `conn_id` 或 `bgid` 计数器溢出。当前 v1 不回收 id，长跑 reconnect
    /// 累计到 28-bit `conn_id` 上限 / 16-bit `bgid` 上限就报这个。修复路径：
    /// 给 Pool 加 free-list 复用槽位。
    #[error("pool {0} id space exhausted; restart or implement id reuse")]
    IdSpaceExhausted(&'static str),
}

/// 构造单条 conn 的参数。`proactor` 字段透传给 [`Pool::new`](crate::Pool::new) 时
/// 用；`conn_id` / `bgid` 由 [`Pool`](crate::Pool) 内部分配，caller 不应自己设。
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub use_tls: bool,
    /// 透传给 [`Proactor::new`](crate::proactor::Proactor::new)。HFT 部署开 SQ_POLL +
    /// pin kthread 到 client 线程的 SMT sibling（[`with_sq_poll`](Self::with_sq_poll)）。
    pub proactor: ProactorConfig,
    /// 本 conn 的路由 token。由 [`Pool`](crate::Pool) 在 `connect_blocking` 内分配；
    /// caller 直接构造时留默认 0 即可。低 28 位有效。
    pub conn_id: u32,
    /// 本 conn 独占的 buffer ring group id。同样由 Pool 分配。
    pub bgid: u16,
    /// multishot recv 用的 provided buffer 单个大小（字节）。kernel 每次 RX
    /// 最多写满这一格然后 post CQE。**取多大平衡 latency vs throughput**：
    /// 小（4 KiB 默认）→ 单帧不跨 boundary 概率高 / 单 CQE 处理快；
    /// 大（16-64 KiB）→ CQE 数下降 / 大 payload 不切碎，但 partial frame
    /// remainder 处理变贵。详见 [`Self::with_buf_ring`]。
    pub buf_ring_buf_size: u32,
    /// buffer ring entry 数。必须非零 2 的幂。`entries × buf_size` = 整池字节数；
    /// 太小会让 multishot 在 user space recycle 跟不上时频繁 ENOBUFS。
    pub buf_ring_entries: u16,
}

impl ConnectionConfig {
    #[must_use]
    pub fn new(host: impl Into<String>, port: u16, path: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port,
            path: path.into(),
            use_tls: true,
            proactor: ProactorConfig::default(),
            conn_id: 0,
            bgid: 0,
            buf_ring_buf_size: DEFAULT_BUF_RING_BUF_SIZE,
            buf_ring_entries: DEFAULT_BUF_RING_ENTRIES,
        }
    }

    #[must_use]
    pub const fn with_tls(mut self, on: bool) -> Self {
        self.use_tls = on;
        self
    }

    /// 启用 SQ_POLL + 可选钉 kthread CPU。详见 [`ProactorConfig`] doc。
    #[must_use]
    pub const fn with_sq_poll(mut self, idle_ms: u32, cpu: Option<u32>) -> Self {
        self.proactor.sq_poll_idle_ms = Some(idle_ms);
        self.proactor.sq_poll_cpu = cpu;
        self
    }

    /// 覆盖 multishot recv 的 provided buffer ring 配置。
    ///
    /// ## 经验法则
    ///
    /// `buf_size ≈ 20 × payload_size`（即每个 buffer 装 ~20 帧）。太小 → CQE
    /// 数量过多，每帧 dispatch 开销吃满；太大 → cache pressure 上来，memcpy
    /// 反超 CQE 摊销收益。实测 `benches/ws_ingress_single`：
    ///
    /// | payload   | 最优 buf_size | pump_binary vs tokio |
    /// |-----------|---------------|----------------------|
    /// | 200 B     | 4 KiB         | 1.33×                |
    /// | 400 B     | 8 KiB         | 1.21×                |
    /// | 800 B     | 16 KiB        | 1.01×                |
    /// | 4 KiB    | 32 KiB+       | （TODO）             |
    ///
    /// HFT trades/quotes 200-400B 用默认 4 KiB 就行；订阅 L2 book delta
    /// (400-1000B) 上 8 KiB；full snapshot (4-16 KiB) 上 32 KiB。
    ///
    /// ## entries
    ///
    /// 必须非零 2 的幂。整池字节 `entries × buf_size` 决定 burst buffering 能
    /// 撑多深；默认 256 × 4 KiB = 1 MiB。内核上限 `entries ≤ 32768`。`buf_size`
    /// 没有硬上限但 `entries × buf_size` 受限于进程 lockable memory（默认
    /// `RLIMIT_MEMLOCK`）。
    ///
    /// # Panics
    ///
    /// debug build 下 `entries == 0 || !entries.is_power_of_two()` 立刻 panic；
    /// release build 下 [`Pool::connect_blocking`](crate::Pool::connect_blocking)
    /// 时 [`BufferRing::new`] 会返 Err。
    #[must_use]
    pub const fn with_buf_ring(mut self, buf_size: u32, entries: u16) -> Self {
        debug_assert!(buf_size > 0, "buf_size must be > 0");
        debug_assert!(entries > 0 && entries.is_power_of_two(), "entries must be non-zero power of 2");
        self.buf_ring_buf_size = buf_size;
        self.buf_ring_entries = entries;
        self
    }
}
