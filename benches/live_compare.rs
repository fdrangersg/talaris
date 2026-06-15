#![allow(
    dead_code,
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::needless_pass_by_value,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

#[cfg(not(target_os = "linux"))]
fn main() {
    common::print_linux_only("live_compare");
}

#[path = "common.rs"]
mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier, OnceLock};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use talaris::connection::IngressStats;
use talaris::observability::DataEventMeta;
use tungstenite::client::{IntoClientRequest, client};

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = run() {
        eprintln!("live_compare: {e}");
        std::process::exit(1);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Transport {
    Talaris,
    Tungstenite,
    Both,
}

impl Transport {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Talaris => "talaris",
            Self::Tungstenite => "tungstenite",
            Self::Both => "both",
        }
    }
}

impl std::str::FromStr for Transport {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "talaris" => Ok(Self::Talaris),
            "tungstenite" => Ok(Self::Tungstenite),
            "both" => Ok(Self::Both),
            other => Err(format!("unknown --transport {other:?}")),
        }
    }
}

#[derive(Debug)]
struct Config {
    transport: Transport,
    host: String,
    port: u16,
    symbols: Vec<String>,
    stream_counts: Vec<usize>,
    redundancy_counts: Vec<usize>,
    seconds: u64,
    sample_bps: u16,
    buf_size: u32,
    buf_entries: u16,
    sq_entries: u32,
    cq_entries: u32,
    completion_batch: usize,
    spin_iters: usize,
    talaris_cpu: Option<usize>,
    tungstenite_cpu: Option<usize>,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let sample_bps = common::arg_or("--sample-bps", common::FULL_SAMPLE_BPS);
        common::validate_sampling_bps(sample_bps)?;

        let buf_entries = common::arg_or("--buf-entries", 512_u16);
        let sq_entries = common::arg_or("--sq-entries", 512_u32);
        let cq_entries = common::arg_or("--cq-entries", 1024_u32);
        common::validate_power_of_two_u16("--buf-entries", buf_entries)?;
        common::validate_power_of_two_u32("--sq-entries", sq_entries)?;
        common::validate_power_of_two_u32("--cq-entries", cq_entries)?;

        let symbols = common::arg_string("--symbols", "btcusdt,ethusdt,bnbusdt,solusdt")
            .split(',')
            .filter_map(|s| {
                let symbol = s.trim().to_ascii_lowercase();
                (!symbol.is_empty()).then_some(symbol)
            })
            .collect::<Vec<_>>();
        if symbols.is_empty() {
            return Err("--symbols must contain at least one symbol".to_owned());
        }

        let stream_counts = common::arg_list("--stream-counts", "2,3,4")?;
        for count in &stream_counts {
            if *count == 0 {
                return Err("--stream-counts values must be positive".to_owned());
            }
            if *count > symbols.len() {
                return Err(format!(
                    "--stream-counts value {count} exceeds symbol count {}",
                    symbols.len()
                ));
            }
        }

        let redundancy_counts = common::arg_list("--redundancy-counts", "1")?;
        for count in &redundancy_counts {
            if *count == 0 {
                return Err("--redundancy-counts values must be positive".to_owned());
            }
        }

        Ok(Self {
            transport: common::arg_or("--transport", Transport::Both),
            host: common::arg_string("--host", "fstream.binance.com"),
            port: common::arg_or("--port", 443_u16),
            symbols,
            stream_counts,
            redundancy_counts,
            seconds: common::arg_or("--seconds", 45_u64).max(1),
            sample_bps,
            buf_size: common::arg_or("--buf-size", 1024_u32),
            buf_entries,
            sq_entries,
            cq_entries,
            completion_batch: common::arg_or("--completion-batch", 64_usize).max(1),
            spin_iters: common::arg_or("--spin-iters", 256_usize),
            talaris_cpu: common::optional_arg("--talaris-cpu"),
            tungstenite_cpu: common::optional_arg("--tungstenite-cpu"),
        })
    }

    fn print(&self) {
        println!(
            "bench_config bench=live_compare transport={} endpoint={}:{} symbols={} stream_counts={:?} redundancy_counts={:?} seconds={} sample_bps={} buf={}x{} sq_entries={} cq_entries={} completion_batch={} spin_iters={} talaris_cpu={} tungstenite_cpu={}",
            self.transport.as_str(),
            self.host,
            self.port,
            self.symbols.join(","),
            self.stream_counts,
            self.redundancy_counts,
            self.seconds,
            self.sample_bps,
            self.buf_entries,
            self.buf_size,
            self.sq_entries,
            self.cq_entries,
            self.completion_batch,
            self.spin_iters,
            self.talaris_cpu
                .map_or_else(|| "-".to_owned(), |cpu| cpu.to_string()),
            self.tungstenite_cpu
                .map_or_else(|| "-".to_owned(), |cpu| cpu.to_string()),
        );
    }
}

#[cfg(target_os = "linux")]
fn run() -> BenchResult<()> {
    if common::flag_present("--help") {
        print_usage();
        return Ok(());
    }

    let cfg = Arc::new(Config::from_args()?);
    cfg.print();

    for &stream_count in &cfg.stream_counts {
        for &redundancy_count in &cfg.redundancy_counts {
            let path = build_combined_path(&cfg.symbols, stream_count);
            println!(
                "bench_live_compare_start streams={stream_count} redundancy={redundancy_count} path={path}"
            );
            let result = run_stream_count(Arc::clone(&cfg), stream_count, redundancy_count, path)?;
            result.print();
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn run_stream_count(
    cfg: Arc<Config>,
    stream_count: usize,
    redundancy_count: usize,
    path: String,
) -> BenchResult<LiveCompareResult> {
    let ready = Arc::new(Barrier::new(match cfg.transport {
        Transport::Both => 3,
        Transport::Talaris | Transport::Tungstenite => 2,
    }));
    let start = Arc::new(OnceLock::new());

    let talaris_thread = matches!(cfg.transport, Transport::Talaris | Transport::Both).then(|| {
        let cfg = Arc::clone(&cfg);
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        let path = path.clone();
        std::thread::spawn(move || {
            run_talaris(cfg, stream_count, redundancy_count, &path, ready, start)
        })
    });

    let tungstenite_thread = matches!(cfg.transport, Transport::Tungstenite | Transport::Both)
        .then(|| {
            let cfg = Arc::clone(&cfg);
            let ready = Arc::clone(&ready);
            let start = Arc::clone(&start);
            let path = path.clone();
            std::thread::spawn(move || {
                run_tungstenite(cfg, stream_count, redundancy_count, &path, ready, start)
            })
        });

    ready.wait();
    let started = Instant::now();
    start.set(started).map_err(|_| "run start already set")?;
    ready.wait();

    let talaris = match talaris_thread {
        Some(handle) => Some(handle.join().map_err(|_| "talaris thread panicked")??),
        None => None,
    };
    let tungstenite = match tungstenite_thread {
        Some(handle) => Some(handle.join().map_err(|_| "tungstenite thread panicked")??),
        None => None,
    };

    Ok(LiveCompareResult {
        stream_count,
        redundancy_count,
        path,
        talaris,
        tungstenite,
    })
}

#[cfg(target_os = "linux")]
fn run_talaris(
    cfg: Arc<Config>,
    stream_count: usize,
    redundancy_count: usize,
    path: &str,
    ready: Arc<Barrier>,
    start: Arc<OnceLock<Instant>>,
) -> BenchResult<TalarisRun> {
    let _pin = cfg
        .talaris_cpu
        .map(|cpu| common::PinGuard::pin("talaris", cpu));

    let conn_cfg = talaris_conn_config(&cfg, path);
    let proactor_cfg = conn_cfg.proactor;
    let mut pool = talaris::Pool::new(
        talaris::PoolConfig::new(proactor_cfg).with_completion_batch_capacity(cfg.completion_batch),
    )?;
    let mut handles = Vec::with_capacity(redundancy_count);
    let mut ingress_before = Vec::with_capacity(redundancy_count);
    for _ in 0..redundancy_count {
        let handle = pool.connect_blocking(talaris_conn_config(&cfg, path))?;
        assert_eq!(pool.state(handle), Some(talaris::connection::State::Open));
        ingress_before.push(pool.ingress_stats(handle).unwrap_or_default());
        handles.push(handle);
    }

    ready.wait();
    ready.wait();
    let started = *start
        .get()
        .expect("start set before second barrier returns");
    let deadline = started + Duration::from_secs(cfg.seconds);

    let mut stats = common::MessageStats::default();
    let mut latency = TalarisLatencyStats::new()?;
    let cpu = common::ThreadCpuTimer::start();
    while Instant::now() < deadline {
        pump_talaris(&mut pool, cfg.spin_iters, &mut stats, &mut latency)?;
    }

    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();
    let ingress = aggregate_ingress_delta(&pool, &handles, &ingress_before);

    Ok(TalarisRun {
        stream_count,
        redundancy_count,
        stats,
        latency,
        ingress,
        elapsed,
        cpu: cpu_elapsed,
    })
}

fn talaris_conn_config(cfg: &Config, path: &str) -> talaris::connection::ConnectionConfig {
    talaris::connection::ConnectionConfig::new(&cfg.host, cfg.port, path)
        .with_sq_entries(cfg.sq_entries)
        .with_cq_entries(cfg.cq_entries)
        .with_buf_ring(cfg.buf_size, cfg.buf_entries)
        .with_ws_limits(8 * 1024 * 1024, 16 * 1024 * 1024)
        .with_ws_buffer_capacities(128 * 1024, 128 * 1024, 16 * 1024)
        .with_ingress_stats(true)
        .with_observability_sample_rate_bps(cfg.sample_bps)
        .with_observability_histograms(false)
}

fn aggregate_ingress_delta(
    pool: &talaris::Pool,
    handles: &[talaris::ConnHandle],
    before: &[IngressStats],
) -> Option<IngressStats> {
    let mut out = IngressStats::default();
    for (handle, before) in handles.iter().copied().zip(before.iter().copied()) {
        let after = pool.ingress_stats(handle)?;
        let delta = common::ingress_stats_delta(before, after);
        out.recv_data_cqes = out.recv_data_cqes.saturating_add(delta.recv_data_cqes);
        out.recv_bytes = out.recv_bytes.saturating_add(delta.recv_bytes);
        out.recv_multishot_rearms = out
            .recv_multishot_rearms
            .saturating_add(delta.recv_multishot_rearms);
        out.recv_ring_exhaustions = out
            .recv_ring_exhaustions
            .saturating_add(delta.recv_ring_exhaustions);
        out.plain_recv_batches = out
            .plain_recv_batches
            .saturating_add(delta.plain_recv_batches);
        out.plain_recv_batch_cqes = out
            .plain_recv_batch_cqes
            .saturating_add(delta.plain_recv_batch_cqes);
        out.plain_recv_copied_batches = out
            .plain_recv_copied_batches
            .saturating_add(delta.plain_recv_copied_batches);
        out.plain_recv_copied_bytes = out
            .plain_recv_copied_bytes
            .saturating_add(delta.plain_recv_copied_bytes);
        out.plaintext_chunks = out.plaintext_chunks.saturating_add(delta.plaintext_chunks);
        out.plaintext_bytes = out.plaintext_bytes.saturating_add(delta.plaintext_bytes);
        out.ws_data_drains = out.ws_data_drains.saturating_add(delta.ws_data_drains);
        out.ws_data_drain_skips = out
            .ws_data_drain_skips
            .saturating_add(delta.ws_data_drain_skips);
        out.ws_data_events = out.ws_data_events.saturating_add(delta.ws_data_events);
        out.ws_text_events = out.ws_text_events.saturating_add(delta.ws_text_events);
        out.ws_binary_events = out.ws_binary_events.saturating_add(delta.ws_binary_events);
    }
    Some(out)
}

#[cfg(target_os = "linux")]
fn pump_talaris(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
    latency: &mut TalarisLatencyStats,
) -> Result<(), talaris::connection::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data_marked(|_, ev| record_talaris_marked_event(stats, latency, &ev))
    } else {
        pool.pump_data_spin_marked(spin_iters, |_, ev| {
            record_talaris_marked_event(stats, latency, &ev);
        })
        .map(|_| ())
    }
}

fn record_talaris_marked_event(
    stats: &mut common::MessageStats,
    latency: &mut TalarisLatencyStats,
    ev: &talaris::ws::MarkedDataEvent<'_>,
) {
    match ev {
        talaris::ws::MarkedDataEvent::Text { payload, meta } => {
            stats.record_meta(*meta);
            stats.record_text(payload);
            latency.record(*meta);
        }
        talaris::ws::MarkedDataEvent::Binary { payload, meta } => {
            stats.record_meta(*meta);
            stats.record_binary(payload);
            latency.record(*meta);
        }
    }
}

#[cfg(target_os = "linux")]
fn run_tungstenite(
    cfg: Arc<Config>,
    stream_count: usize,
    redundancy_count: usize,
    path: &str,
    ready: Arc<Barrier>,
    start: Arc<OnceLock<Instant>>,
) -> BenchResult<TungsteniteRun> {
    let mut sockets = Vec::with_capacity(redundancy_count);
    for _ in 0..redundancy_count {
        sockets.push(connect_tungstenite_socket(&cfg, path)?);
    }

    let worker_start = Arc::new(OnceLock::new());
    let worker_ready = Arc::new(Barrier::new(redundancy_count + 1));
    let mut workers = Vec::with_capacity(redundancy_count);
    for socket in sockets {
        let cfg = Arc::clone(&cfg);
        let worker_start = Arc::clone(&worker_start);
        let worker_ready = Arc::clone(&worker_ready);
        workers.push(std::thread::spawn(move || {
            run_tungstenite_worker(cfg, socket, worker_start, worker_ready)
        }));
    }

    ready.wait();
    ready.wait();
    let started = *start
        .get()
        .expect("start set before second barrier returns");
    worker_start
        .set(started)
        .map_err(|_| "worker start already set")?;
    worker_ready.wait();

    let mut stats = common::MessageStats::default();
    let mut latency = TungsteniteLatencyStats::new()?;
    let mut read_stats = StreamReadStats::default();
    let mut cpu_elapsed = Duration::ZERO;
    for worker in workers {
        let run = worker
            .join()
            .map_err(|_| "tungstenite worker thread panicked")??;
        stats.merge_from(&run.stats);
        latency.merge_from(&run.latency);
        read_stats.merge_from(run.read_stats);
        cpu_elapsed = cpu_elapsed.saturating_add(run.cpu);
    }
    let elapsed = started.elapsed();

    Ok(TungsteniteRun {
        stream_count,
        redundancy_count,
        stats,
        latency,
        read_stats,
        elapsed,
        cpu: cpu_elapsed,
    })
}

fn connect_tungstenite_socket(
    cfg: &Config,
    path: &str,
) -> BenchResult<tungstenite::WebSocket<TlsCountingStream>> {
    let mut tcp = CountingTcpStream::connect((&cfg.host[..], cfg.port))?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(Duration::from_secs(5)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(5)))?;

    let tls_config = Arc::new(talaris::tls::TlsAdapter::client_config(
        talaris::tls::TlsCryptoProvider::default(),
    )?);
    let server_name = rustls::pki_types::ServerName::try_from(cfg.host.clone())
        .map_err(|_| format!("invalid server name {:?}", cfg.host))?;
    let mut tls_conn = rustls::ClientConnection::new(tls_config, server_name)?;
    while tls_conn.is_handshaking() {
        match tls_conn.complete_io(&mut tcp) {
            Ok(_) => {}
            Err(e) if is_timeout(&e) => {}
            Err(e) => return Err(Box::new(e)),
        }
    }
    verify_alpn(&tls_conn)?;

    tcp.set_read_timeout(Some(Duration::from_millis(100)))?;
    tcp.reset_read_stats();
    let stream = TlsCountingStream::new(tls_conn, tcp);
    let request = format!("wss://{}:{}{}", cfg.host, cfg.port, path).into_client_request()?;
    let (mut socket, _) = client(request, stream)?;
    socket.get_mut().reset_read_stats();
    Ok(socket)
}

fn run_tungstenite_worker(
    cfg: Arc<Config>,
    mut socket: tungstenite::WebSocket<TlsCountingStream>,
    start: Arc<OnceLock<Instant>>,
    ready: Arc<Barrier>,
) -> BenchResult<TungsteniteWorkerRun> {
    let _pin = cfg
        .tungstenite_cpu
        .map(|cpu| common::PinGuard::pin("tungstenite", cpu));

    ready.wait();
    let started = *start
        .get()
        .expect("start set before worker barrier returns");
    let deadline = started + Duration::from_secs(cfg.seconds);
    let mut stats = common::MessageStats::default();
    let mut latency = TungsteniteLatencyStats::new()?;
    let cpu = common::ThreadCpuTimer::start();
    while Instant::now() < deadline {
        match socket.read() {
            Ok(message) => {
                let ready_at = Instant::now();
                let marker = socket.get_ref().last_read_marker();
                if record_tungstenite_message(&mut stats, message)? {
                    latency.record_message(marker, ready_at);
                }
            }
            Err(tungstenite::Error::Io(e)) if is_timeout(&e) => {}
            Err(e) => return Err(Box::new(e)),
        }
    }

    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();
    let read_stats = socket.get_ref().read_stats();

    Ok(TungsteniteWorkerRun {
        stats,
        latency,
        read_stats,
        elapsed,
        cpu: cpu_elapsed,
    })
}

fn record_tungstenite_message(
    stats: &mut common::MessageStats,
    message: tungstenite::Message,
) -> Result<bool, tungstenite::Error> {
    match message {
        tungstenite::Message::Text(payload) => {
            stats.record_text(payload.as_str());
            Ok(true)
        }
        tungstenite::Message::Binary(payload) => {
            stats.record_binary(payload.as_ref());
            Ok(true)
        }
        tungstenite::Message::Ping(_)
        | tungstenite::Message::Pong(_)
        | tungstenite::Message::Frame(_) => Ok(false),
        tungstenite::Message::Close(_) => Err(tungstenite::Error::ConnectionClosed),
    }
}

#[derive(Debug)]
struct LiveCompareResult {
    stream_count: usize,
    redundancy_count: usize,
    path: String,
    talaris: Option<TalarisRun>,
    tungstenite: Option<TungsteniteRun>,
}

impl LiveCompareResult {
    fn print(&self) {
        println!(
            "bench_live_compare_done streams={} redundancy={} path={}",
            self.stream_count, self.redundancy_count, self.path
        );
        if let Some(talaris) = &self.talaris {
            talaris.print();
        }
        if let Some(tungstenite) = &self.tungstenite {
            tungstenite.print();
        }
    }
}

#[derive(Debug)]
struct TalarisRun {
    stream_count: usize,
    redundancy_count: usize,
    stats: common::MessageStats,
    latency: TalarisLatencyStats,
    ingress: Option<IngressStats>,
    elapsed: Duration,
    cpu: Duration,
}

impl TalarisRun {
    fn print(&self) {
        print_result(
            self.stream_count,
            self.redundancy_count,
            "talaris",
            &self.stats,
            self.elapsed,
            self.cpu,
        );
        print_marked_summary(self.stream_count, self.redundancy_count, &self.stats);
        self.latency
            .print(self.stream_count, self.redundancy_count, "talaris");
        print_ingress_stats(self.stream_count, self.redundancy_count, self.ingress);
    }
}

#[derive(Debug)]
struct TungsteniteWorkerRun {
    stats: common::MessageStats,
    latency: TungsteniteLatencyStats,
    read_stats: StreamReadStats,
    elapsed: Duration,
    cpu: Duration,
}

#[derive(Debug)]
struct TungsteniteRun {
    stream_count: usize,
    redundancy_count: usize,
    stats: common::MessageStats,
    latency: TungsteniteLatencyStats,
    read_stats: StreamReadStats,
    elapsed: Duration,
    cpu: Duration,
}

impl TungsteniteRun {
    fn print(&self) {
        print_result(
            self.stream_count,
            self.redundancy_count,
            "tungstenite",
            &self.stats,
            self.elapsed,
            self.cpu,
        );
        self.latency
            .print(self.stream_count, self.redundancy_count, "tungstenite");
        print_stream_stats(
            self.stream_count,
            self.redundancy_count,
            "tungstenite",
            &self.stats,
            self.read_stats,
        );
    }
}

#[derive(Debug)]
struct TalarisLatencyStats {
    recv_to_plaintext: StageLatencyStats,
    plaintext_to_ws: StageLatencyStats,
    recv_to_ws: StageLatencyStats,
}

impl TalarisLatencyStats {
    fn new() -> Result<Self, hdrhistogram::CreationError> {
        Ok(Self {
            recv_to_plaintext: StageLatencyStats::new()?,
            plaintext_to_ws: StageLatencyStats::new()?,
            recv_to_ws: StageLatencyStats::new()?,
        })
    }

    fn record(&mut self, meta: DataEventMeta) {
        let position = MessagePosition::from_index(meta.chunk_message_index);
        if let Some(nanos) = meta.recv_to_plaintext_nanos() {
            self.recv_to_plaintext.record(position, nanos);
        }
        if let Some(nanos) = meta.plaintext_to_ws_nanos() {
            self.plaintext_to_ws.record(position, nanos);
        }
        if let Some(nanos) = meta.recv_to_ws_nanos() {
            self.recv_to_ws.record(position, nanos);
        }
    }

    fn print(&self, streams: usize, redundancy: usize, mode: &str) {
        self.recv_to_plaintext.print(
            streams,
            redundancy,
            mode,
            "recv_to_plaintext",
            "chunk_message",
        );
        self.plaintext_to_ws.print(
            streams,
            redundancy,
            mode,
            "plaintext_to_ws",
            "chunk_message",
        );
        self.recv_to_ws
            .print(streams, redundancy, mode, "recv_to_ws", "chunk_message");
    }
}

#[derive(Debug)]
struct TungsteniteLatencyStats {
    read_to_ws: StageLatencyStats,
    last_generation: Option<u64>,
    current_read_message_index: u16,
    missing_markers: u64,
}

impl TungsteniteLatencyStats {
    fn new() -> Result<Self, hdrhistogram::CreationError> {
        Ok(Self {
            read_to_ws: StageLatencyStats::new()?,
            last_generation: None,
            current_read_message_index: 0,
            missing_markers: 0,
        })
    }

    fn record_message(&mut self, marker: Option<ReadMarker>, ready_at: Instant) {
        let Some(marker) = marker else {
            self.missing_markers = self.missing_markers.saturating_add(1);
            return;
        };
        let index = if self.last_generation == Some(marker.generation) {
            self.current_read_message_index = self.current_read_message_index.saturating_add(1);
            self.current_read_message_index
        } else {
            self.last_generation = Some(marker.generation);
            self.current_read_message_index = 0;
            0
        };
        let nanos = duration_nanos(ready_at.saturating_duration_since(marker.read_at));
        self.read_to_ws
            .record(MessagePosition::from_index(index), nanos);
    }

    fn merge_from(&mut self, other: &Self) {
        self.read_to_ws.merge_from(&other.read_to_ws);
        self.missing_markers = self.missing_markers.saturating_add(other.missing_markers);
    }

    fn print(&self, streams: usize, redundancy: usize, mode: &str) {
        self.read_to_ws.print(
            streams,
            redundancy,
            mode,
            "socket_read_to_ws",
            "read_message",
        );
        println!(
            "bench_latency_marker bench=live_compare mode={mode} streams={streams} redundancy={redundancy} missing_markers={}",
            common::fmt_int(self.missing_markers)
        );
    }
}

#[derive(Debug)]
struct StageLatencyStats {
    all: BenchHistogram,
    first: BenchHistogram,
    queued: BenchHistogram,
}

impl StageLatencyStats {
    fn new() -> Result<Self, hdrhistogram::CreationError> {
        Ok(Self {
            all: BenchHistogram::new()?,
            first: BenchHistogram::new()?,
            queued: BenchHistogram::new()?,
        })
    }

    fn record(&mut self, position: MessagePosition, nanos: u64) {
        self.all.record(nanos);
        match position {
            MessagePosition::First => self.first.record(nanos),
            MessagePosition::Queued => self.queued.record(nanos),
        }
    }

    fn merge_from(&mut self, other: &Self) {
        self.all.merge_from(&other.all);
        self.first.merge_from(&other.first);
        self.queued.merge_from(&other.queued);
    }

    fn print(
        &self,
        streams: usize,
        redundancy: usize,
        mode: &str,
        stage: &str,
        position_scope: &str,
    ) {
        self.all
            .print(streams, redundancy, mode, stage, position_scope, "all");
        self.first
            .print(streams, redundancy, mode, stage, position_scope, "first");
        self.queued
            .print(streams, redundancy, mode, stage, position_scope, "queued");
    }
}

#[derive(Clone, Copy, Debug)]
enum MessagePosition {
    First,
    Queued,
}

impl MessagePosition {
    const fn from_index(index: u16) -> Self {
        if index == 0 {
            Self::First
        } else {
            Self::Queued
        }
    }
}

#[derive(Debug)]
struct BenchHistogram {
    hist: Histogram<u64>,
    sum: u64,
}

impl BenchHistogram {
    fn new() -> Result<Self, hdrhistogram::CreationError> {
        Ok(Self {
            hist: Histogram::new_with_bounds(1, 60_000_000_000, 3)?,
            sum: 0,
        })
    }

    fn record(&mut self, nanos: u64) {
        self.hist.saturating_record(nanos.max(1));
        self.sum = self.sum.saturating_add(nanos);
    }

    fn merge_from(&mut self, other: &Self) {
        self.hist
            .add(&other.hist)
            .expect("compatible benchmark histograms");
        self.sum = self.sum.saturating_add(other.sum);
    }

    fn print(
        &self,
        streams: usize,
        redundancy: usize,
        mode: &str,
        stage: &str,
        position_scope: &str,
        position: &str,
    ) {
        let samples = self.hist.len();
        let avg = self.sum.checked_div(samples).unwrap_or(0);
        println!(
            "bench_latency bench=live_compare mode={mode} streams={streams} redundancy={redundancy} stage={stage} position_scope={position_scope} position={position} samples={} avg_ns={} p50_ns={} p90_ns={} p99_ns={} p999_ns={} max_ns={}",
            common::fmt_int(samples),
            avg,
            histogram_quantile(&self.hist, 0.50),
            histogram_quantile(&self.hist, 0.90),
            histogram_quantile(&self.hist, 0.99),
            histogram_quantile(&self.hist, 0.999),
            if self.hist.is_empty() {
                0
            } else {
                self.hist.max()
            }
        );
    }
}

#[derive(Debug)]
struct CountingTcpStream {
    inner: TcpStream,
    read_calls: u64,
    read_bytes: u64,
    read_generation: u64,
    last_read_at: Option<Instant>,
}

impl CountingTcpStream {
    fn connect<A: std::net::ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        Ok(Self::new(TcpStream::connect(addr)?))
    }

    const fn new(inner: TcpStream) -> Self {
        Self {
            inner,
            read_calls: 0,
            read_bytes: 0,
            read_generation: 0,
            last_read_at: None,
        }
    }

    fn set_nodelay(&self, on: bool) -> std::io::Result<()> {
        self.inner.set_nodelay(on)
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.inner.set_read_timeout(timeout)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.inner.set_write_timeout(timeout)
    }

    const fn read_stats(&self) -> StreamReadStats {
        StreamReadStats {
            calls: self.read_calls,
            bytes: self.read_bytes,
        }
    }

    const fn reset_read_stats(&mut self) {
        self.read_calls = 0;
        self.read_bytes = 0;
        self.read_generation = 0;
        self.last_read_at = None;
    }

    const fn last_read_marker(&self) -> Option<ReadMarker> {
        match self.last_read_at {
            Some(read_at) => Some(ReadMarker {
                generation: self.read_generation,
                read_at,
            }),
            None => None,
        }
    }
}

impl Read for CountingTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.read_calls = self.read_calls.saturating_add(1);
            self.read_bytes = self.read_bytes.saturating_add(n as u64);
            self.read_generation = self.read_generation.saturating_add(1);
            self.last_read_at = Some(Instant::now());
        }
        Ok(n)
    }
}

impl Write for CountingTcpStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Debug)]
struct TlsCountingStream {
    conn: rustls::ClientConnection,
    stream: CountingTcpStream,
}

impl TlsCountingStream {
    const fn new(conn: rustls::ClientConnection, stream: CountingTcpStream) -> Self {
        Self { conn, stream }
    }

    const fn read_stats(&self) -> StreamReadStats {
        self.stream.read_stats()
    }

    const fn last_read_marker(&self) -> Option<ReadMarker> {
        self.stream.last_read_marker()
    }

    const fn reset_read_stats(&mut self) {
        self.stream.reset_read_stats();
    }
}

impl Read for TlsCountingStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        rustls::Stream::new(&mut self.conn, &mut self.stream).read(buf)
    }
}

impl Write for TlsCountingStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        rustls::Stream::new(&mut self.conn, &mut self.stream).write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        rustls::Stream::new(&mut self.conn, &mut self.stream).flush()
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct StreamReadStats {
    calls: u64,
    bytes: u64,
}

impl StreamReadStats {
    const fn merge_from(&mut self, other: Self) {
        self.calls = self.calls.saturating_add(other.calls);
        self.bytes = self.bytes.saturating_add(other.bytes);
    }
}

#[derive(Clone, Copy, Debug)]
struct ReadMarker {
    generation: u64,
    read_at: Instant,
}

fn build_combined_path(symbols: &[String], stream_count: usize) -> String {
    let mut path = String::from("/stream?streams=");
    for (index, symbol) in symbols.iter().take(stream_count).enumerate() {
        if index > 0 {
            path.push('/');
        }
        path.push_str(symbol);
        path.push_str("@bookTicker");
    }
    path
}

fn verify_alpn(conn: &rustls::ClientConnection) -> BenchResult<()> {
    match conn.alpn_protocol() {
        None | Some(b"http/1.1") => Ok(()),
        Some(other) => Err(format!("server negotiated unexpected ALPN: {other:?}").into()),
    }
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

fn histogram_quantile(hist: &Histogram<u64>, quantile: f64) -> u64 {
    if hist.is_empty() {
        0
    } else {
        hist.value_at_quantile(quantile)
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn print_result(
    streams: usize,
    redundancy: usize,
    mode: &str,
    stats: &common::MessageStats,
    elapsed: Duration,
    cpu: Duration,
) {
    println!(
        "bench_result bench=live_compare mode={mode} streams={streams} redundancy={redundancy} messages={} text={} binary={} bytes={} elapsed_ms={:.3} cpu_ms={:.3} cpu_pct={:.1} msg_s={:.3} mib_s={:.3} cpu_ns_msg={} checksum={}",
        common::fmt_int(stats.messages),
        common::fmt_int(stats.text_messages),
        common::fmt_int(stats.binary_messages),
        common::fmt_int(stats.bytes),
        common::elapsed_ms(elapsed),
        common::elapsed_ms(cpu),
        common::cpu_pct(cpu, elapsed),
        common::messages_per_sec(stats.messages, elapsed),
        common::mib_per_sec(stats.bytes, elapsed),
        common::ns_per_message(cpu, stats.messages),
        std::hint::black_box(stats.checksum())
    );
}

fn print_marked_summary(streams: usize, redundancy: usize, stats: &common::MessageStats) {
    println!(
        "bench_marked bench=live_compare mode=talaris streams={streams} redundancy={redundancy} messages={} sampled={} chunk_first={} chunk_queued={} max_chunk_message_index={} recv_sequence={}..{}",
        common::fmt_int(stats.messages),
        common::fmt_int(stats.sampled_messages),
        common::fmt_int(stats.chunk_first_messages),
        common::fmt_int(stats.chunk_queued_messages),
        stats.max_chunk_message_index,
        stats
            .first_recv_sequence
            .map_or_else(|| "-".to_owned(), |v| v.to_string()),
        stats
            .last_recv_sequence
            .map_or_else(|| "-".to_owned(), |v| v.to_string())
    );
}

fn print_ingress_stats(streams: usize, redundancy: usize, stats: Option<IngressStats>) {
    let Some(stats) = stats else {
        println!(
            "bench_ingress bench=live_compare streams={streams} redundancy={redundancy} mode=talaris unavailable"
        );
        return;
    };
    let messages_per_recv_cqe = if stats.recv_data_cqes == 0 {
        0.0
    } else {
        stats.ws_data_events as f64 / stats.recv_data_cqes as f64
    };
    let messages_per_plaintext_chunk = if stats.plaintext_chunks == 0 {
        0.0
    } else {
        stats.ws_data_events as f64 / stats.plaintext_chunks as f64
    };
    println!(
        "bench_ingress bench=live_compare mode=talaris streams={streams} redundancy={redundancy} recv_cqes={} recv_bytes={} plaintext_chunks={} plaintext_bytes={} ws_data_events={} text={} binary={} rearm={} ring_exhaustions={} messages_per_recv_cqe={:.3} messages_per_plaintext_chunk={:.3}",
        common::fmt_int(stats.recv_data_cqes),
        common::fmt_int(stats.recv_bytes),
        common::fmt_int(stats.plaintext_chunks),
        common::fmt_int(stats.plaintext_bytes),
        common::fmt_int(stats.ws_data_events),
        common::fmt_int(stats.ws_text_events),
        common::fmt_int(stats.ws_binary_events),
        common::fmt_int(stats.recv_multishot_rearms),
        common::fmt_int(stats.recv_ring_exhaustions),
        messages_per_recv_cqe,
        messages_per_plaintext_chunk
    );
}

fn print_stream_stats(
    streams: usize,
    redundancy: usize,
    mode: &str,
    messages: &common::MessageStats,
    reads: StreamReadStats,
) {
    let messages_per_read = if reads.calls == 0 {
        0.0
    } else {
        messages.messages as f64 / reads.calls as f64
    };
    let bytes_per_read = if reads.calls == 0 {
        0.0
    } else {
        reads.bytes as f64 / reads.calls as f64
    };
    println!(
        "bench_stream bench=live_compare mode={mode} streams={streams} redundancy={redundancy} read_calls={} read_bytes={} messages_per_read={:.3} bytes_per_read={:.1}",
        common::fmt_int(reads.calls),
        common::fmt_int(reads.bytes),
        messages_per_read,
        bytes_per_read
    );
}

fn print_usage() {
    println!(
        "live_compare bench\n\
         \n\
         Runs talaris and tungstenite concurrently against Binance USD-M futures combined BBO streams.\n\
         \n\
         Args:\n\
           --transport talaris|tungstenite|both\n\
           --host HOST                  websocket host\n\
           --port PORT                  websocket TLS port\n\
           --symbols a,b,c,d            symbols used to build combined streams\n\
           --stream-counts A,B,C        number of symbols per scenario\n\
           --redundancy-counts A,B,C    identical connections per client and scenario\n\
           --seconds N                  run duration per scenario\n\
           --sample-bps N               talaris observability sample rate, 0..10000\n\
           --buf-size N                 talaris io_uring provided buffer slot size\n\
           --buf-entries N              provided buffer entries, power of two\n\
           --sq-entries N               io_uring SQ entries, power of two\n\
           --cq-entries N               io_uring CQ entries, power of two\n\
           --completion-batch N         Pool CQE scratch buffer capacity\n\
           --spin-iters N               0 uses blocking talaris pump_data_marked\n\
           --talaris-cpu N              pin talaris thread\n\
           --tungstenite-cpu N          pin tungstenite thread"
    );
}
