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

use crate::observability::{ObservabilityError, ObservabilitySampleRate};
use crate::proactor::{BufferRingError, ProactorConfig, ProactorError, ProactorSetupFlags};
use crate::tls::TlsError;
use crate::ws::{WsConfig, WsError};

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
    #[error("observability: {0}")]
    Observability(#[from] ObservabilityError),
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

/// Opt-in ingress diagnostics. Disabled by default so production hot paths do not
/// pay for counters unless a caller explicitly enables them for tuning.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct IngressStats {
    /// Positive-length recv data CQEs handled by this connection.
    pub recv_data_cqes: u64,
    /// Ciphertext bytes carried by those CQEs.
    pub recv_bytes: u64,
    /// Times a recv multishot SQE was submitted or rearmed for this connection.
    pub recv_multishot_rearms: u64,
    /// Multishot recv terminations caused by provided-buffer ring exhaustion.
    pub recv_ring_exhaustions: u64,
    /// Consecutive plain TCP recv CQE runs handled by the data pump batch path.
    pub plain_recv_batches: u64,
    /// Total recv CQEs included in those plain TCP batch runs.
    pub plain_recv_batch_cqes: u64,
    /// Plain TCP batch runs parsed through the reusable copy scratch buffer.
    pub plain_recv_copied_batches: u64,
    /// Bytes copied into the reusable plain TCP batch scratch buffer.
    pub plain_recv_copied_bytes: u64,
    /// Plaintext chunks fed into the WebSocket parser. For TLS connections this
    /// counts rustls plaintext chunks; for plain TCP this counts recv CQEs.
    pub plaintext_chunks: u64,
    /// Plaintext bytes fed into the WebSocket parser.
    pub plaintext_bytes: u64,
    /// Data-pump CQEs that fed plaintext into the WebSocket receive buffer.
    pub ws_data_drains: u64,
    /// Data-pump CQEs that skipped WebSocket draining because no plaintext arrived.
    pub ws_data_drain_skips: u64,
    /// Text/Binary data messages emitted to the user's data sink.
    pub ws_data_events: u64,
    /// Text messages emitted to the user's data sink.
    pub ws_text_events: u64,
    /// Binary messages emitted to the user's data sink.
    pub ws_binary_events: u64,
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
    /// HFT 部署主要调 SQ/CQ 容量和 taskrun flags；线程 pinning 由
    /// [`crate::proactor::pin_current_thread_to`] 单独控制。
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
    /// 覆盖底层 [`WsClient`](crate::ws::WsClient) 配置。`host` / `path` 最终仍以
    /// 当前 `ConnectionConfig` 为准，避免 transport endpoint 和 WS handshake
    /// header 被调参配置意外改散。
    pub ws_config: Option<WsConfig>,
    /// `send_buf` 初始容量。`None` 表示沿用 `buf_ring_slot_size`。
    ///
    /// 这是 socket/TLS outbound staging buffer；真实 pending 字节仍会按需 grow。
    pub send_buffer_initial_capacity: Option<usize>,
    /// TLS in-flight 期间延迟合入 `send_buf` 的密文 staging buffer 初始容量。
    /// `None` 表示沿用 `buf_ring_slot_size`。
    pub tls_pending_out_initial_capacity: Option<usize>,
    /// Consecutive plain TCP recv CQEs in one data pump may be copied into a
    /// reusable scratch buffer and parsed as one larger WebSocket input slice.
    /// `0` disables copy aggregation. This only affects unmarked plain-WS data
    /// pumps; TLS and marked observability paths preserve per-CQE staging.
    pub plain_recv_batch_copy_max_bytes: usize,
    /// 收集 [`IngressStats`]。默认关闭，避免在生产 hot path 上无条件更新计数器。
    pub track_ingress_stats: bool,
    /// Sampling rate for marked observability timestamps. Marked pumps default to
    /// 100%; unmarked pumps never read these clocks.
    pub observability_sample_rate: ObservabilitySampleRate,
    /// Record sampled marked data-event stage latencies into per-connection
    /// HdrHistograms for Prometheus export. Default off.
    pub record_observability_histograms: bool,
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
            ws_config: None,
            send_buffer_initial_capacity: None,
            tls_pending_out_initial_capacity: None,
            plain_recv_batch_copy_max_bytes: 0,
            track_ingress_stats: false,
            observability_sample_rate: ObservabilitySampleRate::always(),
            record_observability_histograms: false,
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

    /// 覆盖 proactor 完整配置。适合统一注入 entries / CQ sizing / taskrun flags。
    #[must_use]
    pub const fn with_proactor(mut self, proactor: ProactorConfig) -> Self {
        self.proactor = proactor;
        self
    }

    /// 覆盖 io_uring SQ entries。必须是非零 2 的幂；最终校验由 [`Proactor::new`](crate::proactor::Proactor::new) 完成。
    #[must_use]
    pub const fn with_proactor_entries(mut self, entries: u32) -> Self {
        self.proactor.sq_entries = entries;
        self
    }

    /// 覆盖 io_uring SQ entries。语义同 [`Self::with_proactor_entries`]，
    /// 名字更明确；保留旧方法是为了兼容 bench 和早期调用点。
    #[must_use]
    pub const fn with_sq_entries(mut self, entries: u32) -> Self {
        self.proactor.sq_entries = entries;
        self
    }

    /// 覆盖 io_uring CQ entries。`None` 时使用 kernel 默认（通常为 SQ 的 2 倍）。
    ///
    /// multishot recv 会在一次 SQE 生命周期内产生大量 CQE。行情 burst 场景建议
    /// 从 `max(2 * sq_entries, buf_ring_entries)` 起步做 A/B。
    #[must_use]
    pub const fn with_cq_entries(mut self, entries: u32) -> Self {
        self.proactor.cq_entries = Some(entries);
        self
    }

    /// 覆盖高级 io_uring setup flags。默认关闭；这些 flag 对 event loop 结构有
    /// 约束，建议只在明确 benchmark 假设下开启。
    #[must_use]
    pub const fn with_proactor_setup_flags(mut self, flags: ProactorSetupFlags) -> Self {
        self.proactor.setup_flags = flags;
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

    /// 覆盖底层 WebSocket 配置。`host` / `path` 会在连接建立时被当前
    /// `ConnectionConfig` 的 endpoint 覆盖，只保留 buffer / limit / protocol
    /// 等调优字段。
    #[must_use]
    pub fn with_ws_config(mut self, config: WsConfig) -> Self {
        self.ws_config = Some(config);
        self
    }

    /// 覆盖 WebSocket protocol limits。
    #[must_use]
    pub fn with_ws_limits(mut self, max_message_size: usize, max_frame_payload: u64) -> Self {
        let mut config = self
            .ws_config
            .take()
            .unwrap_or_else(|| WsConfig::new(self.host.clone(), self.path.clone()));
        config.max_message_size = max_message_size;
        config.max_frame_payload = max_frame_payload;
        self.ws_config = Some(config);
        self
    }

    /// 覆盖 WebSocket `recv_buf` 初始容量。
    #[must_use]
    pub fn with_ws_recv_buffer_capacity(mut self, bytes: usize) -> Self {
        let mut config = self
            .ws_config
            .take()
            .unwrap_or_else(|| WsConfig::new(self.host.clone(), self.path.clone()));
        config.initial_recv_buffer_capacity = Some(bytes);
        self.ws_config = Some(config);
        self
    }

    /// 覆盖 WebSocket fragmented message assembly buffer 初始容量。
    #[must_use]
    pub fn with_ws_message_buffer_capacity(mut self, bytes: usize) -> Self {
        let mut config = self
            .ws_config
            .take()
            .unwrap_or_else(|| WsConfig::new(self.host.clone(), self.path.clone()));
        config.initial_message_buffer_capacity = Some(bytes);
        self.ws_config = Some(config);
        self
    }

    /// 覆盖 WebSocket outbound `tx_buf` 初始容量。
    #[must_use]
    pub fn with_ws_tx_buffer_capacity(mut self, bytes: usize) -> Self {
        let mut config = self
            .ws_config
            .take()
            .unwrap_or_else(|| WsConfig::new(self.host.clone(), self.path.clone()));
        config.initial_tx_buffer_capacity = Some(bytes);
        self.ws_config = Some(config);
        self
    }

    /// 一次性覆盖 WebSocket 三个 hot-path heap buffer 的初始容量。
    #[must_use]
    pub fn with_ws_buffer_capacities(
        mut self,
        recv_bytes: usize,
        message_bytes: usize,
        tx_bytes: usize,
    ) -> Self {
        let mut config = self
            .ws_config
            .take()
            .unwrap_or_else(|| WsConfig::new(self.host.clone(), self.path.clone()));
        config.initial_recv_buffer_capacity = Some(recv_bytes);
        config.initial_message_buffer_capacity = Some(message_bytes);
        config.initial_tx_buffer_capacity = Some(tx_bytes);
        self.ws_config = Some(config);
        self
    }

    /// 控制收到 Ping 时是否自动排 Pong。默认开启。
    #[must_use]
    pub fn with_auto_pong(mut self, on: bool) -> Self {
        let mut config = self
            .ws_config
            .take()
            .unwrap_or_else(|| WsConfig::new(self.host.clone(), self.path.clone()));
        config.auto_pong = on;
        self.ws_config = Some(config);
        self
    }

    /// 覆盖 socket/TLS outbound staging buffer 初始容量。
    #[must_use]
    pub const fn with_send_buffer_capacity(mut self, bytes: usize) -> Self {
        self.send_buffer_initial_capacity = Some(bytes);
        self
    }

    /// 覆盖 TLS in-flight 密文 staging buffer 初始容量。
    #[must_use]
    pub const fn with_tls_pending_out_capacity(mut self, bytes: usize) -> Self {
        self.tls_pending_out_initial_capacity = Some(bytes);
        self
    }

    /// 一次性覆盖连接层两个 outbound staging buffer 的初始容量。
    #[must_use]
    pub const fn with_connection_buffer_capacities(
        mut self,
        send_bytes: usize,
        tls_pending_out_bytes: usize,
    ) -> Self {
        self.send_buffer_initial_capacity = Some(send_bytes);
        self.tls_pending_out_initial_capacity = Some(tls_pending_out_bytes);
        self
    }

    /// Enable copy aggregation for consecutive plain recv CQEs in one data pump.
    ///
    /// A value of `0` disables it. This is a throughput-oriented tuning knob:
    /// it can give the WebSocket parser larger contiguous input, at the cost of
    /// copying bytes and delaying the first message in the ready CQE run until
    /// the run has been copied.
    #[must_use]
    pub const fn with_plain_recv_batch_copy_max_bytes(mut self, bytes: usize) -> Self {
        self.plain_recv_batch_copy_max_bytes = bytes;
        self
    }

    /// 启用或关闭 ingress CQE 调优统计。生产连接默认关闭。
    #[must_use]
    pub const fn with_ingress_stats(mut self, on: bool) -> Self {
        self.track_ingress_stats = on;
        self
    }

    /// Configure marked observability sampling in basis points.
    ///
    /// `10_000` means 100%, `1_000` means 10%, and values above `10_000`
    /// saturate to 100%. This only affects marked data-pump APIs.
    #[must_use]
    pub const fn with_observability_sample_rate_bps(mut self, basis_points: u16) -> Self {
        self.observability_sample_rate = ObservabilitySampleRate::from_basis_points(basis_points);
        self
    }

    /// Enable per-connection HdrHistogram recording for marked observability
    /// latency stages. Use [`crate::Pool::write_prometheus_metrics`] or
    /// [`crate::Pool::prometheus_metrics`] to expose the current snapshot.
    #[must_use]
    pub const fn with_observability_histograms(mut self, on: bool) -> Self {
        self.record_observability_histograms = on;
        self
    }
}
