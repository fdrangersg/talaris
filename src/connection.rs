//! `connection` —— Pool 内单条 conn 的公共类型
//!
//! [`State`] / [`ConnectionConfig`] / [`ConnectionError`] 是 [`crate::Pool`] 对外
//! API 共用的类型。公开 API 不再暴露单独的 `Connection` wrapper；单连接也通过
//! `Pool::new` + `Pool::connect_blocking` 驱动。
//!
//! 实际驱动逻辑（socket / TLS / WS / buf_ring / send_buf 状态机）见
//! `crate::connection_state`。

#![allow(clippy::module_name_repetitions)]

use std::{io, sync::Arc};

use thiserror::Error;

use crate::proactor::{BufferRingError, ProactorConfig, ProactorError};
use crate::tls::TlsError;
use crate::ws::WsError;

/// 调优点：如果 entries 太小，用户态还没 recycle，kernel 又要写数据，就可能撞 ENOBUFS；如果 buf_size 太小，大 payload 会被切成更多 CQE；如果 buf_size 太大，内存/cache 压力会上来，小行情包不一定划算
///
/// Provided buffer 内单 slot 字节数默认值。caller 可通过 [`ConnectionConfig::with_buf_ring`] 覆盖。
/// HFT 高频公开行情常见 10B-1KB 帧，4 KiB 能提高单个 CQE 覆盖常见小帧的概率，
/// 但 TCP / CQE 边界不等于 WebSocket frame 边界，parser 仍必须处理跨 CQE frame;
/// 此处仅为根据最小 size 分布情况设置的默认值，请以实际数据 frame size 分布来调整以下参数。
/// 影响单次 CQE 最多承载多少字节。
pub const DEFAULT_BUF_RING_SLOT_SIZE: u32 = 4 * 1024;

/// buffer ring entry 数默认值（必须 2^N）。256 × 4 KiB = 1 MiB 池子（即每条 Conn 的接收池内存），
/// 避免行情突发时 multishot 在 user-space recycle 前耗尽 provided buffers。
/// 影响 burst 时有多少个 buffer slot 可以同时借给 kernel。
pub const DEFAULT_BUF_RING_ENTRIES: u16 = 256;

/// Driver 状态机。
///
/// ```text
///     Init ──submit_connect──▶ Connecting
///     Connecting ──Connect CQE──▶ TlsHandshake (TLS) | WsHandshake (plain)
///     TlsHandshake ──TLS done + ALPN ok──▶ WsHandshake
///     WsHandshake ──WsClient emits HandshakeComplete──▶ Open
///     Open ──send_close / peer Close / protocol error──▶ Closing
///     Closing ──peer EOF / explicit close op / fatal I/O error──▶ Closed
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
    /// Pool 的 `conn_id` 或 `bgid` 计数器耗尽。当前 v1 不回收 id，长跑 reconnect
    /// 累计到 `UserData` 可编码的 conn_id 空间或 `u16` bgid 空间上限就报这个。
    /// 修复路径：给 Pool 加 free-list 复用槽位。
    #[error("pool {0} id space exhausted; restart or implement id reuse")]
    IdSpaceExhausted(&'static str),
}

/// 构造单条 conn 的参数。`proactor` 是创建 [`Pool`](crate::Pool) 时的便利默认值；
/// 单条 connect 实际使用 Pool 已持有的 proactor，`conn_id` / `bgid` 也由
/// [`Pool`](crate::Pool) 内部分配，caller 不应自己设。
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub use_tls: bool,
    /// 自定义 rustls client 配置。`None` 使用 webpki roots + `http/1.1` ALPN
    /// 的默认配置；私有 CA、session cache 或 crypto provider 调优可注入配置。
    pub tls_config: Option<Arc<rustls::ClientConfig>>,
    /// 构造 [`PoolConfig`](crate::PoolConfig) 时传给 [`Proactor::new`](crate::proactor::Proactor::new)。
    /// HFT 部署可测试 SQ_POLL + CPU pinning；把 SQ_POLL kthread 放在 client 线程的 SMT sibling 是一个候选拓扑，不是无条件最优（[`with_sq_poll`](Self::with_sq_poll)）。
    pub proactor: ProactorConfig,
    /// 本 conn 的路由 token。由 [`Pool`](crate::Pool) 在 `connect_blocking` 内分配；
    /// caller 直接构造时留默认 0 即可。低 28 位有效。
    pub conn_id: u32,
    /// 本 conn 独占的 buffer ring group id。同样由 Pool 分配。
    pub bgid: u16,
    /// multishot recv 用的 provided buffer 单个 slot 大小（字节）。kernel 每次 RX 最多写满这一格然后 post CQE。
    /// **取多大平衡 latency vs throughput**：
    /// 小（4 KiB 默认）→ CQE 粒度更细 / 单次 parser 输入更短 / 常见高频小帧更可能落在同一 CQE 内；
    /// 大（> 2 KiB）→ CQE 数下降 / 大 payload 不切碎，但 partial frame remainder 处理变贵。详见 [`Self::with_buf_ring`]。
    pub buf_ring_slot_size: u32,
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
            tls_config: None,
            proactor: ProactorConfig::default(),
            conn_id: 0,
            bgid: 0,
            buf_ring_slot_size: DEFAULT_BUF_RING_SLOT_SIZE,
            buf_ring_entries: DEFAULT_BUF_RING_ENTRIES,
        }
    }

    #[must_use]
    pub const fn with_tls(mut self, on: bool) -> Self {
        self.use_tls = on;
        self
    }

    /// 覆盖 TLS client 配置。caller 负责配置 root store；如果 server 返回 ALPN，
    /// transport 仍会校验它只能是 `http/1.1`。
    #[must_use]
    pub fn with_tls_config(mut self, config: Arc<rustls::ClientConfig>) -> Self {
        self.tls_config = Some(config);
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
    /// `slot_size ≈ 20 × payload_size`（即每个 buffer 装 ~20 帧）是调参起点，不是跨机器最优秀值
    /// 太小 → CQE 数量过多，每帧 dispatch 开销吃满；
    /// 太大 → cache pressure 上来，memcpy 反超 CQE 摊销收益。
    /// 内核版本、CPU、TLS/plain、`pump`/`pump_spin`、sink 逻辑都会改变最优点：
    ///
    /// ## entries
    ///
    /// 必须非零 2 的幂。整池字节 `entries × buf_size` 决定 burst buffering 能
    /// 撑多深；默认 256 × 4 KiB = 1 MiB。内核上限 `entries ≤ 32768`。`buf_size`
    /// 没有硬上限但 `entries × slot_size` 受限于进程 lockable memory（默认 `RLIMIT_MEMLOCK`）。
    ///
    /// # Panics
    ///
    /// debug build 下 `slot_size == 0 || entries == 0 || !entries.is_power_of_two()` 立刻 panic；
    /// release build 下 [`Pool::connect_blocking`](crate::Pool::connect_blocking) 时 [`crate::proactor::BufferRing::new`] 会返 Err。
    #[must_use]
    pub const fn with_buf_ring(mut self, slot_size: u32, entries: u16) -> Self {
        debug_assert!(slot_size > 0, "slot_size must be > 0");
        debug_assert!(
            entries > 0 && entries.is_power_of_two(),
            "entries must be non-zero power of 2"
        );
        self.buf_ring_slot_size = slot_size;
        self.buf_ring_entries = entries;
        self
    }
}
