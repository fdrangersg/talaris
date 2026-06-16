#![allow(
    dead_code,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

#[cfg(not(target_os = "linux"))]
fn main() {
    common::print_linux_only("live_redundancy");
}

#[path = "common.rs"]
mod common;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = run() {
        eprintln!("live_redundancy: {e}");
        std::process::exit(1);
    }
}

use std::collections::HashMap;
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;

use hdrhistogram::Histogram;
use talaris::connection::IngressStats;
use talaris::ws::{DataEventMeta, MarkedDataEvent};

const HIST_LOWEST_NS: u64 = 1;
const HIST_HIGHEST_NS: u64 = 60_000_000_000;
const HIST_SIGFIG: u8 = 3;
const RECENT_SEQ_RETAIN: u64 = 65_536;
const BINANCE_U_FIELD: &[u8] = b"\"u\"";

#[derive(Debug)]
struct Config {
    host: String,
    port: u16,
    path: String,
    connections: usize,
    seconds: u64,
    sample_bps: u16,
    pool_histograms: bool,
    buf_size: u32,
    buf_entries: u16,
    sq_entries: u32,
    cq_entries: u32,
    completion_batch: usize,
    spin_iters: usize,
    post_progress_spin_iters: usize,
    metrics_interval: Duration,
    prom_out: Option<String>,
    subscribe: Option<String>,
    user_cpu: Option<usize>,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let sample_bps = common::arg_or("--sample-bps", common::FULL_SAMPLE_BPS);
        common::validate_sampling_bps(sample_bps)?;
        let connections = common::arg_or("--connections", 2_usize).max(1);
        let buf_entries = common::arg_or("--buf-entries", 512_u16);
        let sq_entries = common::arg_or("--sq-entries", 512_u32);
        let cq_entries = common::arg_or("--cq-entries", 1024_u32);
        common::validate_power_of_two_u16("--buf-entries", buf_entries)?;
        common::validate_power_of_two_u32("--sq-entries", sq_entries)?;
        common::validate_power_of_two_u32("--cq-entries", cq_entries)?;

        Ok(Self {
            host: common::arg_string("--host", "fstream.binance.com"),
            port: common::arg_or("--port", 443_u16),
            path: common::arg_string("--path", "/ws/btcusdt@bookTicker"),
            connections,
            seconds: common::arg_or("--seconds", 60_u64).max(1),
            sample_bps,
            pool_histograms: !common::flag_present("--no-pool-hist"),
            buf_size: common::arg_or("--buf-size", 1024_u32),
            buf_entries,
            sq_entries,
            cq_entries,
            completion_batch: common::arg_or("--completion-batch", 64_usize).max(1),
            spin_iters: common::arg_or("--spin-iters", 256_usize),
            post_progress_spin_iters: common::arg_or("--post-progress-spin-iters", 0_usize),
            metrics_interval: Duration::from_millis(common::arg_or(
                "--metrics-interval-ms",
                1000_u64,
            )),
            prom_out: common::optional_string("--prom-out"),
            subscribe: common::optional_string("--subscribe"),
            user_cpu: common::optional_arg("--user-cpu"),
        })
    }

    fn print(&self) {
        println!(
            "bench_config bench=live_redundancy endpoint={}:{}{} connections={} seconds={} sample_bps={} pool_histograms={} buf={}x{} sq_entries={} cq_entries={} completion_batch={} spin_iters={} post_progress_spin_iters={} metrics_interval_ms={} subscribe={} prom_out={}",
            self.host,
            self.port,
            self.path,
            self.connections,
            self.seconds,
            self.sample_bps,
            self.pool_histograms,
            self.buf_entries,
            self.buf_size,
            self.sq_entries,
            self.cq_entries,
            self.completion_batch,
            self.spin_iters,
            self.post_progress_spin_iters,
            self.metrics_interval.as_millis(),
            self.subscribe.as_ref().map_or("no", |_| "yes"),
            self.prom_out.as_deref().unwrap_or("-")
        );
    }
}

#[cfg(target_os = "linux")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    if common::flag_present("--help") {
        print_usage();
        return Ok(());
    }

    let cfg = Config::from_args()?;
    cfg.print();

    let _pin = cfg.user_cpu.map(|cpu| common::PinGuard::pin("user", cpu));
    let base_conn_cfg = conn_config(&cfg);
    let proactor_cfg = base_conn_cfg.proactor;
    let mut pool = talaris::Pool::new(
        talaris::PoolConfig::new(proactor_cfg)
            .with_initial_conn_capacity(cfg.connections)
            .with_completion_batch_capacity(cfg.completion_batch)
            .with_post_progress_spin_iters(cfg.post_progress_spin_iters),
    )?;

    let mut handles = Vec::with_capacity(cfg.connections);
    for _ in 0..cfg.connections {
        let handle = pool.connect_blocking(base_conn_cfg.clone())?;
        assert_eq!(pool.state(handle), Some(talaris::connection::State::Open));
        if let Some(subscribe) = cfg.subscribe.as_deref() {
            pool.send_text(handle, subscribe.as_bytes())?;
        }
        handles.push(handle);
    }
    let handle_index = handle_index_map(&handles);

    let ingress_before: Vec<_> = handles.iter().map(|&h| pool.ingress_stats(h)).collect();
    let mut prom = common::PromWriter::from_arg(cfg.prom_out.clone())?;
    let mut dedup = LiveDedup::default();
    let mut stats = LiveRedundancyStats::new(cfg.connections)?;
    let cpu = common::ThreadCpuTimer::start();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(cfg.seconds);
    let mut metrics_schedule = common::MetricsSchedule::new(started, cfg.metrics_interval);

    while Instant::now() < deadline {
        pump_marked(
            &mut pool,
            cfg.spin_iters,
            &handle_index,
            &mut dedup,
            &mut stats,
        )?;
        metrics_schedule.write_due(&mut prom, "live_redundancy", &mut pool, started)?;
    }

    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();
    common::MetricsSchedule::write_final(&mut prom, "live_redundancy", &mut pool, elapsed)?;
    print_result(&stats, elapsed, cpu_elapsed);
    print_latency_summary(&stats);
    print_ingress_summary(
        &pool,
        &handles,
        &ingress_before,
        aggregate_ingress_delta(&pool, &handles, &ingress_before),
    );
    Ok(())
}

fn conn_config(cfg: &Config) -> talaris::connection::ConnectionConfig {
    talaris::connection::ConnectionConfig::new(&cfg.host, cfg.port, &cfg.path)
        .with_sq_entries(cfg.sq_entries)
        .with_cq_entries(cfg.cq_entries)
        .with_buf_ring(cfg.buf_size, cfg.buf_entries)
        .with_ws_limits(8 * 1024 * 1024, 16 * 1024 * 1024)
        .with_ws_buffer_capacities(128 * 1024, 128 * 1024, 16 * 1024)
        .with_ingress_stats(true)
        .with_observability_sample_rate_bps(cfg.sample_bps)
        .with_observability_histograms(cfg.pool_histograms)
}

#[cfg(target_os = "linux")]
fn pump_marked(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    handle_index: &[usize],
    dedup: &mut LiveDedup,
    stats: &mut LiveRedundancyStats,
) -> Result<(), talaris::connection::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data_marked(|handle, ev| {
            let conn_index = conn_index(handle_index, handle);
            record_marked_event(conn_index, &ev, dedup, stats);
        })
    } else {
        pool.pump_data_spin_marked(spin_iters, |handle, ev| {
            let conn_index = conn_index(handle_index, handle);
            record_marked_event(conn_index, &ev, dedup, stats);
        })
        .map(|_| ())
    }
}

fn record_marked_event(
    conn_index: usize,
    ev: &MarkedDataEvent<'_>,
    dedup: &mut LiveDedup,
    stats: &mut LiveRedundancyStats,
) {
    match ev {
        MarkedDataEvent::Text { payload, meta } => {
            stats.record_text(conn_index, payload, *meta, dedup);
        }
        MarkedDataEvent::Binary { payload, meta } => {
            stats.record_binary(conn_index, payload, *meta);
        }
    }
}

#[cfg(target_os = "linux")]
fn handle_index_map(handles: &[talaris::ConnHandle]) -> Vec<usize> {
    let max_handle = handles
        .iter()
        .map(|handle| handle.as_u32() as usize)
        .max()
        .unwrap_or(0);
    let mut out = vec![usize::MAX; max_handle + 1];
    for (index, handle) in handles.iter().enumerate() {
        out[handle.as_u32() as usize] = index;
    }
    out
}

#[cfg(target_os = "linux")]
fn conn_index(handle_index: &[usize], handle: talaris::ConnHandle) -> usize {
    let index = handle_index
        .get(handle.as_u32() as usize)
        .copied()
        .unwrap_or(usize::MAX);
    assert_ne!(index, usize::MAX, "unknown conn handle {handle:?}");
    index
}

#[derive(Clone, Copy, Debug)]
struct FirstSeen {
    conn_index: usize,
    ws_payload_ready_mono_nanos: u64,
}

#[derive(Debug, Default)]
struct LiveDedup {
    high_water: Option<u64>,
    recent: HashMap<u64, FirstSeen>,
}

impl LiveDedup {
    fn classify(&mut self, seq: u64, conn_index: usize, meta: DataEventMeta) -> EventClass {
        let is_winner = self.high_water.is_none_or(|high_water| seq > high_water);
        if is_winner {
            self.high_water = Some(seq);
            self.recent.insert(
                seq,
                FirstSeen {
                    conn_index,
                    ws_payload_ready_mono_nanos: meta.ws_payload_ready_mono_nanos,
                },
            );
            self.purge_old(seq);
            EventClass::Winner
        } else if let Some(first) = self.recent.get(&seq).copied() {
            EventClass::ExactDuplicate {
                first_conn_index: first.conn_index,
                lag_nanos: duplicate_lag_nanos(first, meta),
            }
        } else {
            EventClass::Stale
        }
    }

    fn purge_old(&mut self, high_water: u64) {
        if high_water <= RECENT_SEQ_RETAIN || self.recent.len() < RECENT_SEQ_RETAIN as usize {
            return;
        }
        let min_seq = high_water - RECENT_SEQ_RETAIN;
        self.recent.retain(|&seq, _| seq >= min_seq);
    }
}

const fn duplicate_lag_nanos(first: FirstSeen, meta: DataEventMeta) -> Option<u64> {
    if !meta.sampled || first.ws_payload_ready_mono_nanos == 0 {
        return None;
    }
    meta.ws_payload_ready_mono_nanos
        .checked_sub(first.ws_payload_ready_mono_nanos)
}

#[derive(Clone, Copy, Debug)]
enum EventClass {
    Winner,
    ExactDuplicate {
        first_conn_index: usize,
        lag_nanos: Option<u64>,
    },
    Stale,
}

#[derive(Debug)]
struct LiveRedundancyStats {
    input_events: u64,
    text_events: u64,
    binary_events: u64,
    parse_failures: u64,
    winner_events: u64,
    exact_duplicate_events: u64,
    stale_events: u64,
    bytes: u64,
    first_winner_seq: Option<u64>,
    last_winner_seq: Option<u64>,
    winner_seq_gap_units: u64,
    chunk_first_events: u64,
    chunk_queued_events: u64,
    max_chunk_message_index: u16,
    per_conn_events: Vec<u64>,
    per_conn_winners: Vec<u64>,
    per_conn_exact_duplicates: Vec<u64>,
    per_conn_stale: Vec<u64>,
    first_conn_exact_duplicates: Vec<u64>,
    all_latency: LatencyStages,
    winner_latency: LatencyStages,
    exact_duplicate_latency: LatencyStages,
    stale_latency: LatencyStages,
    duplicate_lag: BenchHistogram,
}

impl LiveRedundancyStats {
    fn new(connections: usize) -> Result<Self, hdrhistogram::CreationError> {
        Ok(Self {
            input_events: 0,
            text_events: 0,
            binary_events: 0,
            parse_failures: 0,
            winner_events: 0,
            exact_duplicate_events: 0,
            stale_events: 0,
            bytes: 0,
            first_winner_seq: None,
            last_winner_seq: None,
            winner_seq_gap_units: 0,
            chunk_first_events: 0,
            chunk_queued_events: 0,
            max_chunk_message_index: 0,
            per_conn_events: vec![0; connections],
            per_conn_winners: vec![0; connections],
            per_conn_exact_duplicates: vec![0; connections],
            per_conn_stale: vec![0; connections],
            first_conn_exact_duplicates: vec![0; connections],
            all_latency: LatencyStages::new()?,
            winner_latency: LatencyStages::new()?,
            exact_duplicate_latency: LatencyStages::new()?,
            stale_latency: LatencyStages::new()?,
            duplicate_lag: BenchHistogram::new()?,
        })
    }

    fn record_text(
        &mut self,
        conn_index: usize,
        payload: &str,
        meta: DataEventMeta,
        dedup: &mut LiveDedup,
    ) {
        self.text_events = self.text_events.saturating_add(1);
        self.record_common(conn_index, payload.as_bytes(), meta);
        let Some(seq) = parse_binance_u(payload.as_bytes()) else {
            self.parse_failures = self.parse_failures.saturating_add(1);
            return;
        };
        let class = dedup.classify(seq, conn_index, meta);
        self.record_class(conn_index, seq, meta, class);
    }

    fn record_binary(&mut self, conn_index: usize, payload: &[u8], meta: DataEventMeta) {
        self.binary_events = self.binary_events.saturating_add(1);
        self.record_common(conn_index, payload, meta);
        self.parse_failures = self.parse_failures.saturating_add(1);
    }

    fn record_common(&mut self, conn_index: usize, payload: &[u8], meta: DataEventMeta) {
        self.input_events = self.input_events.saturating_add(1);
        self.bytes = self.bytes.saturating_add(payload.len() as u64);
        self.per_conn_events[conn_index] = self.per_conn_events[conn_index].saturating_add(1);
        self.max_chunk_message_index = self.max_chunk_message_index.max(meta.chunk_message_index);
        if meta.chunk_message_index == 0 {
            self.chunk_first_events = self.chunk_first_events.saturating_add(1);
        } else {
            self.chunk_queued_events = self.chunk_queued_events.saturating_add(1);
        }
        self.all_latency.record(meta);
    }

    fn record_class(
        &mut self,
        conn_index: usize,
        seq: u64,
        meta: DataEventMeta,
        class: EventClass,
    ) {
        match class {
            EventClass::Winner => {
                self.winner_events = self.winner_events.saturating_add(1);
                self.per_conn_winners[conn_index] =
                    self.per_conn_winners[conn_index].saturating_add(1);
                self.record_winner_seq(seq);
                self.winner_latency.record(meta);
            }
            EventClass::ExactDuplicate {
                first_conn_index,
                lag_nanos,
            } => {
                self.exact_duplicate_events = self.exact_duplicate_events.saturating_add(1);
                self.per_conn_exact_duplicates[conn_index] =
                    self.per_conn_exact_duplicates[conn_index].saturating_add(1);
                self.first_conn_exact_duplicates[first_conn_index] =
                    self.first_conn_exact_duplicates[first_conn_index].saturating_add(1);
                self.exact_duplicate_latency.record(meta);
                if let Some(lag_nanos) = lag_nanos {
                    self.duplicate_lag.record(lag_nanos);
                }
            }
            EventClass::Stale => {
                self.stale_events = self.stale_events.saturating_add(1);
                self.per_conn_stale[conn_index] = self.per_conn_stale[conn_index].saturating_add(1);
                self.stale_latency.record(meta);
            }
        }
    }

    fn record_winner_seq(&mut self, seq: u64) {
        if let Some(prev) = self.last_winner_seq
            && seq > prev.saturating_add(1)
        {
            self.winner_seq_gap_units = self
                .winner_seq_gap_units
                .saturating_add(seq.saturating_sub(prev).saturating_sub(1));
        }
        self.first_winner_seq.get_or_insert(seq);
        self.last_winner_seq = Some(seq);
    }
}

#[derive(Debug)]
struct LatencyStages {
    recv_to_plaintext: BenchHistogram,
    plaintext_to_ws: BenchHistogram,
    plaintext_to_ws_excluding_prior_sink: BenchHistogram,
    recv_to_ws: BenchHistogram,
    recv_to_ws_excluding_prior_sink: BenchHistogram,
    chunk_prior_sink_service: BenchHistogram,
}

impl LatencyStages {
    fn new() -> Result<Self, hdrhistogram::CreationError> {
        Ok(Self {
            recv_to_plaintext: BenchHistogram::new()?,
            plaintext_to_ws: BenchHistogram::new()?,
            plaintext_to_ws_excluding_prior_sink: BenchHistogram::new()?,
            recv_to_ws: BenchHistogram::new()?,
            recv_to_ws_excluding_prior_sink: BenchHistogram::new()?,
            chunk_prior_sink_service: BenchHistogram::new()?,
        })
    }

    fn record(&mut self, meta: DataEventMeta) {
        if let Some(nanos) = meta.recv_to_plaintext_nanos() {
            self.recv_to_plaintext.record(nanos);
        }
        if let Some(nanos) = meta.plaintext_to_ws_nanos() {
            self.plaintext_to_ws.record(nanos);
        }
        if let Some(nanos) = meta.plaintext_to_ws_excluding_prior_sink_nanos() {
            self.plaintext_to_ws_excluding_prior_sink.record(nanos);
        }
        if let Some(nanos) = meta.recv_to_ws_nanos() {
            self.recv_to_ws.record(nanos);
        }
        if let Some(nanos) = meta.recv_to_ws_excluding_prior_sink_nanos() {
            self.recv_to_ws_excluding_prior_sink.record(nanos);
        }
        if meta.chunk_message_index > 0
            && let Some(nanos) = meta.chunk_prior_sink_service_nanos()
        {
            self.chunk_prior_sink_service.record(nanos);
        }
    }

    fn print(&self, outcome: &str) {
        self.recv_to_plaintext
            .print("live_redundancy_latency", outcome, "recv_to_plaintext");
        self.plaintext_to_ws
            .print("live_redundancy_latency", outcome, "plaintext_to_ws");
        self.plaintext_to_ws_excluding_prior_sink.print(
            "live_redundancy_latency",
            outcome,
            "plaintext_to_ws_excluding_prior_sink",
        );
        self.recv_to_ws
            .print("live_redundancy_latency", outcome, "recv_to_ws");
        self.recv_to_ws_excluding_prior_sink.print(
            "live_redundancy_latency",
            outcome,
            "recv_to_ws_excluding_prior_sink",
        );
        self.chunk_prior_sink_service.print(
            "live_redundancy_latency",
            outcome,
            "chunk_prior_sink_service",
        );
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
            hist: Histogram::new_with_bounds(HIST_LOWEST_NS, HIST_HIGHEST_NS, HIST_SIGFIG)?,
            sum: 0,
        })
    }

    fn record(&mut self, nanos: u64) {
        self.hist.saturating_record(nanos.max(1));
        self.sum = self.sum.saturating_add(nanos);
    }

    fn print(&self, metric: &str, outcome: &str, stage: &str) {
        let samples = self.hist.len();
        let p50 = quantile(&self.hist, 0.50);
        let p90 = quantile(&self.hist, 0.90);
        let p99 = quantile(&self.hist, 0.99);
        let p999 = quantile(&self.hist, 0.999);
        let max = if self.hist.is_empty() {
            0
        } else {
            self.hist.max()
        };
        let avg = self.sum.checked_div(samples).unwrap_or(0);
        println!(
            "bench_{metric} outcome={outcome} stage={stage} samples={} avg_ns={} p50_ns={} p90_ns={} p99_ns={} p999_ns={} max_ns={}",
            common::fmt_int(samples),
            avg,
            p50,
            p90,
            p99,
            p999,
            max
        );
    }
}

fn quantile(hist: &Histogram<u64>, quantile: f64) -> u64 {
    if hist.is_empty() {
        0
    } else {
        hist.value_at_quantile(quantile)
    }
}

fn parse_binance_u(payload: &[u8]) -> Option<u64> {
    let key_start = payload
        .windows(BINANCE_U_FIELD.len())
        .position(|window| window == BINANCE_U_FIELD)?;
    let mut index = key_start + BINANCE_U_FIELD.len();
    while matches!(payload.get(index), Some(b' ' | b'\t' | b'\r' | b'\n')) {
        index += 1;
    }
    if payload.get(index) != Some(&b':') {
        return None;
    }
    index += 1;
    while matches!(payload.get(index), Some(b' ' | b'\t' | b'\r' | b'\n')) {
        index += 1;
    }
    let quoted = payload.get(index) == Some(&b'"');
    if quoted {
        index += 1;
    }
    let start = index;
    while payload.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    if start == index {
        return None;
    }
    if quoted && payload.get(index) != Some(&b'"') {
        return None;
    }
    parse_ascii_u64(payload.get(start..index)?)
}

fn parse_ascii_u64(digits: &[u8]) -> Option<u64> {
    let mut out = 0_u64;
    for &digit in digits {
        if !digit.is_ascii_digit() {
            return None;
        }
        out = out.checked_mul(10)?.checked_add(u64::from(digit - b'0'))?;
    }
    Some(out)
}

#[cfg(target_os = "linux")]
fn print_result(stats: &LiveRedundancyStats, elapsed: Duration, cpu: Duration) {
    let duplicate_events = stats
        .exact_duplicate_events
        .saturating_add(stats.stale_events);
    let duplicate_ratio = if stats.input_events == 0 {
        0.0
    } else {
        duplicate_events as f64 / stats.input_events as f64
    };
    println!(
        "bench_live_redundancy_result input_events={} text={} binary={} winner_events={} exact_duplicate_events={} stale_events={} duplicate_ratio={:.4} parse_failures={} bytes={} elapsed_ms={:.3} cpu_ms={:.3} cpu_pct={:.1} input_msg_s={:.3} winner_msg_s={:.3} cpu_ns_input={} cpu_ns_winner={}",
        common::fmt_int(stats.input_events),
        common::fmt_int(stats.text_events),
        common::fmt_int(stats.binary_events),
        common::fmt_int(stats.winner_events),
        common::fmt_int(stats.exact_duplicate_events),
        common::fmt_int(stats.stale_events),
        duplicate_ratio,
        common::fmt_int(stats.parse_failures),
        common::fmt_int(stats.bytes),
        common::elapsed_ms(elapsed),
        common::elapsed_ms(cpu),
        common::cpu_pct(cpu, elapsed),
        common::messages_per_sec(stats.input_events, elapsed),
        common::messages_per_sec(stats.winner_events, elapsed),
        common::ns_per_message(cpu, stats.input_events),
        common::ns_per_message(cpu, stats.winner_events)
    );
    println!(
        "bench_live_redundancy_seq first_winner_seq={} last_winner_seq={} winner_seq_gap_units={} chunk_first={} chunk_queued={} max_chunk_message_index={} per_conn_events={:?} per_conn_winners={:?} per_conn_exact_duplicates={:?} per_conn_stale={:?} first_conn_exact_duplicates={:?}",
        stats
            .first_winner_seq
            .map_or_else(|| "-".to_owned(), |seq| seq.to_string()),
        stats
            .last_winner_seq
            .map_or_else(|| "-".to_owned(), |seq| seq.to_string()),
        common::fmt_int(stats.winner_seq_gap_units),
        common::fmt_int(stats.chunk_first_events),
        common::fmt_int(stats.chunk_queued_events),
        stats.max_chunk_message_index,
        stats.per_conn_events,
        stats.per_conn_winners,
        stats.per_conn_exact_duplicates,
        stats.per_conn_stale,
        stats.first_conn_exact_duplicates
    );
}

fn print_latency_summary(stats: &LiveRedundancyStats) {
    stats.all_latency.print("all");
    stats.winner_latency.print("winner");
    stats.exact_duplicate_latency.print("exact_duplicate");
    stats.stale_latency.print("stale");
    stats.duplicate_lag.print(
        "live_redundancy_latency",
        "exact_duplicate",
        "duplicate_lag",
    );
}

#[cfg(target_os = "linux")]
fn print_ingress_summary(
    pool: &talaris::Pool,
    handles: &[talaris::ConnHandle],
    before: &[Option<IngressStats>],
    aggregate: Option<IngressStats>,
) {
    for (&handle, before) in handles.iter().zip(before) {
        let delta = match (*before, pool.ingress_stats(handle)) {
            (Some(before), Some(after)) => Some(common::ingress_stats_delta(before, after)),
            _ => None,
        };
        common::print_ingress_stats(handle, delta);
    }
    print_aggregate_ingress(aggregate);
}

#[cfg(target_os = "linux")]
fn aggregate_ingress_delta(
    pool: &talaris::Pool,
    handles: &[talaris::ConnHandle],
    before: &[Option<IngressStats>],
) -> Option<IngressStats> {
    let mut out = IngressStats::default();
    for (&handle, before) in handles.iter().zip(before) {
        let (Some(before), Some(after)) = (*before, pool.ingress_stats(handle)) else {
            return None;
        };
        add_ingress_stats(&mut out, common::ingress_stats_delta(before, after));
    }
    Some(out)
}

const fn add_ingress_stats(out: &mut IngressStats, stats: IngressStats) {
    out.recv_data_cqes = out.recv_data_cqes.saturating_add(stats.recv_data_cqes);
    out.recv_bytes = out.recv_bytes.saturating_add(stats.recv_bytes);
    out.recv_multishot_rearms = out
        .recv_multishot_rearms
        .saturating_add(stats.recv_multishot_rearms);
    out.recv_ring_exhaustions = out
        .recv_ring_exhaustions
        .saturating_add(stats.recv_ring_exhaustions);
    out.plain_recv_batches = out
        .plain_recv_batches
        .saturating_add(stats.plain_recv_batches);
    out.plain_recv_batch_cqes = out
        .plain_recv_batch_cqes
        .saturating_add(stats.plain_recv_batch_cqes);
    out.plain_recv_copied_batches = out
        .plain_recv_copied_batches
        .saturating_add(stats.plain_recv_copied_batches);
    out.plain_recv_copied_bytes = out
        .plain_recv_copied_bytes
        .saturating_add(stats.plain_recv_copied_bytes);
    out.plaintext_chunks = out.plaintext_chunks.saturating_add(stats.plaintext_chunks);
    out.plaintext_bytes = out.plaintext_bytes.saturating_add(stats.plaintext_bytes);
    out.ws_data_drains = out.ws_data_drains.saturating_add(stats.ws_data_drains);
    out.ws_data_drain_skips = out
        .ws_data_drain_skips
        .saturating_add(stats.ws_data_drain_skips);
    out.ws_data_events = out.ws_data_events.saturating_add(stats.ws_data_events);
    out.ws_text_events = out.ws_text_events.saturating_add(stats.ws_text_events);
    out.ws_binary_events = out.ws_binary_events.saturating_add(stats.ws_binary_events);
}

fn print_aggregate_ingress(stats: Option<IngressStats>) {
    let Some(stats) = stats else {
        println!("bench_live_redundancy_ingress aggregate=unavailable");
        return;
    };
    let messages_per_recv_cqe = if stats.recv_data_cqes == 0 {
        0.0
    } else {
        stats.ws_data_events as f64 / stats.recv_data_cqes as f64
    };
    println!(
        "bench_live_redundancy_ingress aggregate=true recv_cqes={} recv_bytes={} plaintext_chunks={} ws_data_events={} text={} binary={} rearm={} ring_exhaustions={} messages_per_recv_cqe={:.3}",
        common::fmt_int(stats.recv_data_cqes),
        common::fmt_int(stats.recv_bytes),
        common::fmt_int(stats.plaintext_chunks),
        common::fmt_int(stats.ws_data_events),
        common::fmt_int(stats.ws_text_events),
        common::fmt_int(stats.ws_binary_events),
        common::fmt_int(stats.recv_multishot_rearms),
        common::fmt_int(stats.recv_ring_exhaustions),
        messages_per_recv_cqe
    );
}

fn print_usage() {
    println!(
        "live_redundancy bench\n\
         \n\
         Defaults target Binance USD-M futures raw BBO stream twice:\n\
           --host fstream.binance.com --port 443 --path /ws/btcusdt@bookTicker --connections 2\n\
         \n\
         Args:\n\
           --host HOST                  websocket host\n\
           --port PORT                  websocket TLS port\n\
           --path PATH                  websocket path\n\
           --connections N              redundant connections to the same endpoint\n\
           --subscribe JSON             optional text subscription sent on each connection after open\n\
           --seconds N                  run duration\n\
           --sample-bps N               observability sample rate, 0..10000\n\
           --no-pool-hist               disable Pool-owned HdrHistogram Prometheus metrics\n\
           --buf-size N                 io_uring provided buffer slot size\n\
           --buf-entries N              provided buffer entries, power of two\n\
           --sq-entries N               io_uring SQ entries, power of two\n\
           --cq-entries N               io_uring CQ entries, power of two\n\
           --completion-batch N         Pool CQE scratch buffer capacity\n\
           --spin-iters N               0 uses blocking pump_data_marked\n\
           --post-progress-spin-iters N extra spin/drain budget after first progress\n\
           --metrics-interval-ms N      write interval Prometheus snapshots, 0 disables periodic snapshots\n\
           --prom-out PATH|-            write Pool Prometheus snapshots to file or stdout\n\
           --user-cpu N                 pin benchmark thread"
    );
}
