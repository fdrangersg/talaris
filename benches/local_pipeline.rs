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
    common::print_linux_only("local_pipeline");
}

#[path = "common.rs"]
mod common;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = run() {
        eprintln!("local_pipeline: {e}");
        std::process::exit(1);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Baseline,
    MarkedNoHist0,
    MarkedNoHist100,
    Hist1Pct,
    Hist10Pct,
    Hist100Pct,
}

impl Mode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::MarkedNoHist0 => "marked_0_nohist",
            Self::MarkedNoHist100 => "marked_100_nohist",
            Self::Hist1Pct => "hist_1pct",
            Self::Hist10Pct => "hist_10pct",
            Self::Hist100Pct => "hist_100pct",
        }
    }

    const fn marked(self) -> bool {
        !matches!(self, Self::Baseline)
    }

    const fn histograms(self) -> bool {
        matches!(self, Self::Hist1Pct | Self::Hist10Pct | Self::Hist100Pct)
    }

    const fn sample_bps(self) -> u16 {
        match self {
            Self::Baseline | Self::MarkedNoHist0 => 0,
            Self::Hist1Pct => 100,
            Self::Hist10Pct => 1_000,
            Self::MarkedNoHist100 | Self::Hist100Pct => common::FULL_SAMPLE_BPS,
        }
    }
}

impl std::str::FromStr for Mode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "baseline" => Ok(Self::Baseline),
            "marked_0_nohist" => Ok(Self::MarkedNoHist0),
            "marked_100_nohist" => Ok(Self::MarkedNoHist100),
            "hist_1pct" => Ok(Self::Hist1Pct),
            "hist_10pct" => Ok(Self::Hist10Pct),
            "hist_100pct" => Ok(Self::Hist100Pct),
            other => Err(format!("unknown --mode {other:?}")),
        }
    }
}

#[derive(Debug)]
struct Config {
    mode: Mode,
    seconds: u64,
    messages: u64,
    payload_profile: common::PayloadProfile,
    payload_len: usize,
    actual_payload_len: usize,
    frames_per_write: usize,
    buf_size: u32,
    buf_entries: u16,
    sq_entries: u32,
    cq_entries: u32,
    completion_batch: usize,
    spin_iters: usize,
    batch_sink: bool,
    bbo_chunk_coalesce: bool,
    downstream_spin_ns: u64,
    metrics_interval: std::time::Duration,
    prom_out: Option<String>,
    user_cpu: Option<usize>,
    server_cpu: Option<usize>,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let mode = common::arg_or("--mode", Mode::Hist100Pct);
        let seconds = common::arg_or("--seconds", 10_u64);
        let messages = common::arg_or("--messages", 0_u64);
        if seconds == 0 && messages == 0 {
            return Err("--seconds and --messages cannot both be zero".to_owned());
        }

        let buf_size = common::arg_or("--buf-size", 4096_u32);
        let buf_entries = common::arg_or("--buf-entries", 256_u16);
        let sq_entries = common::arg_or("--sq-entries", 512_u32);
        let cq_entries = common::arg_or("--cq-entries", 1024_u32);
        common::validate_power_of_two_u16("--buf-entries", buf_entries)?;
        common::validate_power_of_two_u32("--sq-entries", sq_entries)?;
        common::validate_power_of_two_u32("--cq-entries", cq_entries)?;

        let payload_profile = common::arg_or("--payload-profile", common::PayloadProfile::Binary);
        let payload_len = common::arg_or("--payload", 256_usize).max(1);
        let actual_payload_len = payload_profile.payload_len(payload_len);

        Ok(Self {
            mode,
            seconds,
            messages,
            payload_profile,
            payload_len,
            actual_payload_len,
            frames_per_write: common::arg_or("--frames-per-write", 16_usize).max(1),
            buf_size,
            buf_entries,
            sq_entries,
            cq_entries,
            completion_batch: common::arg_or("--completion-batch", 64_usize).max(1),
            spin_iters: common::arg_or("--spin-iters", 256_usize),
            batch_sink: common::flag_present("--batch-sink"),
            bbo_chunk_coalesce: common::flag_present("--bbo-chunk-coalesce"),
            downstream_spin_ns: common::arg_or("--downstream-spin-ns", 0_u64),
            metrics_interval: std::time::Duration::from_millis(common::arg_or(
                "--metrics-interval-ms",
                1000_u64,
            )),
            prom_out: common::optional_string("--prom-out"),
            user_cpu: common::optional_arg("--user-cpu"),
            server_cpu: common::optional_arg("--server-cpu"),
        })
    }

    fn print(&self) {
        println!(
            "bench_config bench=local_pipeline mode={} seconds={} messages={} payload_profile={} payload={} actual_payload={} frames_per_write={} buf={}x{} sq_entries={} cq_entries={} completion_batch={} spin_iters={} batch_sink={} bbo_chunk_coalesce={} downstream_spin_ns={} sample_bps={} histograms={} metrics_interval_ms={} prom_out={}",
            self.mode.as_str(),
            self.seconds,
            self.messages,
            self.payload_profile.as_str(),
            self.payload_len,
            self.actual_payload_len,
            self.frames_per_write,
            self.buf_entries,
            self.buf_size,
            self.sq_entries,
            self.cq_entries,
            self.completion_batch,
            self.spin_iters,
            self.batch_sink,
            self.bbo_chunk_coalesce,
            self.downstream_spin_ns,
            self.mode.sample_bps(),
            self.mode.histograms(),
            self.metrics_interval.as_millis(),
            self.prom_out.as_deref().unwrap_or("-")
        );
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    if common::flag_present("--help") {
        print_usage();
        return Ok(());
    }

    let cfg = Config::from_args()?;
    cfg.print();

    let server = common::spawn_local_stream_server_with_profile(
        cfg.payload_profile,
        cfg.payload_len,
        cfg.frames_per_write,
        cfg.server_cpu,
    )?;
    let addr = server.addr();
    let _pin = cfg.user_cpu.map(|cpu| common::PinGuard::pin("user", cpu));

    let conn_cfg = talaris::connection_meta::ConnectionConfig::new("localhost", addr.port(), "/")
        .with_tls(false)
        .with_buf_ring(cfg.buf_size, cfg.buf_entries)
        .with_ws_limits(cfg.actual_payload_len, cfg.actual_payload_len as u64)
        .with_ingress_stats(true)
        .with_observability_sample_rate_bps(cfg.mode.sample_bps())
        .with_observability_histograms(cfg.mode.histograms());
    let proactor_cfg = talaris::proactor::ProactorConfig::default()
        .with_sq_entries(cfg.sq_entries)
        .with_cq_entries(cfg.cq_entries);
    let mut pool = talaris::Pool::new(
        talaris::PoolConfig::new(proactor_cfg).with_completion_batch_capacity(cfg.completion_batch),
    )?;
    let handle = pool.connect_blocking_to(conn_cfg, addr)?;
    assert_eq!(
        pool.state(handle),
        Some(talaris::connection_meta::State::Open)
    );

    let mut prom = common::PromWriter::from_arg(cfg.prom_out.clone())?;
    let mut stats = common::MessageStats::default();
    let mut coalesce = BboChunkCoalescer::default();
    let cpu = common::ThreadCpuTimer::start();
    let started = std::time::Instant::now();
    let mut metrics_schedule = common::MetricsSchedule::new(started, cfg.metrics_interval);

    if cfg.bbo_chunk_coalesce {
        if !cfg.batch_sink {
            return Err("--bbo-chunk-coalesce requires --batch-sink".into());
        }
        if cfg.payload_profile != common::PayloadProfile::BinanceBbo {
            return Err("--bbo-chunk-coalesce requires --payload-profile binance-bbo".into());
        }
    }

    while should_continue(&cfg, &stats, started.elapsed()) {
        if cfg.mode.marked() {
            if cfg.batch_sink && cfg.bbo_chunk_coalesce {
                pump_marked_batches_coalesced(
                    &mut pool,
                    cfg.spin_iters,
                    &mut stats,
                    &mut coalesce,
                    cfg.downstream_spin_ns,
                )?;
            } else if cfg.batch_sink {
                pump_marked_batches(
                    &mut pool,
                    cfg.spin_iters,
                    &mut stats,
                    cfg.downstream_spin_ns,
                )?;
            } else {
                pump_marked(
                    &mut pool,
                    cfg.spin_iters,
                    &mut stats,
                    cfg.downstream_spin_ns,
                )?;
            }
        } else if cfg.batch_sink && cfg.bbo_chunk_coalesce {
            pump_unmarked_batches_coalesced(
                &mut pool,
                cfg.spin_iters,
                &mut stats,
                &mut coalesce,
                cfg.downstream_spin_ns,
            )?;
        } else if cfg.batch_sink {
            pump_unmarked_batches(
                &mut pool,
                cfg.spin_iters,
                &mut stats,
                cfg.downstream_spin_ns,
            )?;
        } else {
            pump_unmarked(
                &mut pool,
                cfg.spin_iters,
                &mut stats,
                cfg.downstream_spin_ns,
            )?;
        }
        metrics_schedule.write_due(&mut prom, "local_pipeline", &mut pool, started)?;
    }

    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();
    common::MetricsSchedule::write_final(&mut prom, "local_pipeline", &mut pool, elapsed)?;
    common::print_result(
        "local_pipeline",
        cfg.mode.as_str(),
        &stats,
        elapsed,
        cpu_elapsed,
    );
    if cfg.mode.marked() {
        common::print_marked_summary(&stats);
    }
    if cfg.bbo_chunk_coalesce {
        coalesce.print();
    }
    common::print_ingress_stats(handle, pool.ingress_stats(handle));

    drop(pool);
    server.join()?;
    Ok(())
}

fn should_continue(
    cfg: &Config,
    stats: &common::MessageStats,
    elapsed: std::time::Duration,
) -> bool {
    let time_ok = cfg.seconds == 0 || elapsed < std::time::Duration::from_secs(cfg.seconds);
    let messages_ok = cfg.messages == 0 || stats.messages < cfg.messages;
    time_ok && messages_ok
}

fn pump_unmarked(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
    downstream_spin_ns: u64,
) -> Result<(), talaris::connection_meta::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data(|_, ev| record_unmarked_event(stats, &ev, downstream_spin_ns))
    } else {
        pool.pump_data_spin(spin_iters, |_, ev| {
            record_unmarked_event(stats, &ev, downstream_spin_ns);
        })
        .map(|_| ())
    }
}

fn pump_marked(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
    downstream_spin_ns: u64,
) -> Result<(), talaris::connection_meta::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data_marked(|_, ev| record_marked_event(stats, &ev, downstream_spin_ns))
    } else {
        pool.pump_data_spin_marked(spin_iters, |_, ev| {
            record_marked_event(stats, &ev, downstream_spin_ns);
        })
        .map(|_| ())
    }
}

fn pump_unmarked_batches(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
    downstream_spin_ns: u64,
) -> Result<(), talaris::connection_meta::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data_batches(|_, batch| record_unmarked_batch(stats, &batch, downstream_spin_ns))
    } else {
        pool.pump_data_spin_batches(spin_iters, |_, batch| {
            record_unmarked_batch(stats, &batch, downstream_spin_ns);
        })
        .map(|_| ())
    }
}

fn pump_marked_batches(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
    downstream_spin_ns: u64,
) -> Result<(), talaris::connection_meta::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data_marked_batches(|_, batch| {
            record_marked_batch(stats, &batch, downstream_spin_ns);
        })
    } else {
        pool.pump_data_spin_marked_batches(spin_iters, |_, batch| {
            record_marked_batch(stats, &batch, downstream_spin_ns);
        })
        .map(|_| ())
    }
}

fn pump_unmarked_batches_coalesced(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
    coalesce: &mut BboChunkCoalescer,
    downstream_spin_ns: u64,
) -> Result<(), talaris::connection_meta::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data_batches(|_, batch| {
            coalesce.record_unmarked_batch(stats, &batch, downstream_spin_ns);
        })
    } else {
        pool.pump_data_spin_batches(spin_iters, |_, batch| {
            coalesce.record_unmarked_batch(stats, &batch, downstream_spin_ns);
        })
        .map(|_| ())
    }
}

fn pump_marked_batches_coalesced(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
    coalesce: &mut BboChunkCoalescer,
    downstream_spin_ns: u64,
) -> Result<(), talaris::connection_meta::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data_marked_batches(|_, batch| {
            coalesce.record_marked_batch(stats, &batch, downstream_spin_ns);
        })
    } else {
        pool.pump_data_spin_marked_batches(spin_iters, |_, batch| {
            coalesce.record_marked_batch(stats, &batch, downstream_spin_ns);
        })
        .map(|_| ())
    }
}

fn record_unmarked_event(
    stats: &mut common::MessageStats,
    ev: &talaris::ws::DataEvent<'_>,
    downstream_spin_ns: u64,
) {
    match ev {
        talaris::ws::DataEvent::Text(payload) => stats.record_text(payload),
        talaris::ws::DataEvent::Binary(payload) => stats.record_binary(payload),
    }
    spin_downstream(downstream_spin_ns);
}

fn record_unmarked_batch(
    stats: &mut common::MessageStats,
    batch: &talaris::ws::DataEventBatch<'_>,
    downstream_spin_ns: u64,
) {
    for ev in batch.iter() {
        record_unmarked_event(stats, &ev, downstream_spin_ns);
    }
}

fn record_marked_event(
    stats: &mut common::MessageStats,
    ev: &talaris::ws::MarkedDataEvent<'_>,
    downstream_spin_ns: u64,
) {
    match ev {
        talaris::ws::MarkedDataEvent::Text { payload, meta } => {
            stats.record_meta(*meta);
            stats.record_text(payload);
        }
        talaris::ws::MarkedDataEvent::Binary { payload, meta } => {
            stats.record_meta(*meta);
            stats.record_binary(payload);
        }
    }
    spin_downstream(downstream_spin_ns);
}

fn record_marked_batch(
    stats: &mut common::MessageStats,
    batch: &talaris::ws::MarkedDataEventBatch<'_>,
    downstream_spin_ns: u64,
) {
    for ev in batch.iter() {
        record_marked_event(stats, &ev, downstream_spin_ns);
    }
}

#[derive(Debug, Default)]
struct BboChunkCoalescer {
    pending: Option<u64>,
    batches: u64,
    chunk_end_batches: u64,
    split_batches: u64,
    raw_messages: u64,
    published_messages: u64,
    replaced_pending: u64,
    dropped_not_newer: u64,
    missing_seq: u64,
    max_batch_len: usize,
}

impl BboChunkCoalescer {
    fn record_unmarked_batch(
        &mut self,
        stats: &mut common::MessageStats,
        batch: &talaris::ws::DataEventBatch<'_>,
        downstream_spin_ns: u64,
    ) {
        self.record_batch_boundary(batch.len(), batch.is_chunk_end());
        for ev in batch.iter() {
            match ev {
                talaris::ws::DataEvent::Text(payload) => {
                    stats.record_text(payload);
                    self.observe_payload(payload.as_bytes());
                }
                talaris::ws::DataEvent::Binary(payload) => {
                    stats.record_binary(payload);
                    self.observe_payload(payload);
                }
            }
        }
        self.flush_chunk_if_needed(batch.is_chunk_end(), downstream_spin_ns);
    }

    fn record_marked_batch(
        &mut self,
        stats: &mut common::MessageStats,
        batch: &talaris::ws::MarkedDataEventBatch<'_>,
        downstream_spin_ns: u64,
    ) {
        self.record_batch_boundary(batch.len(), batch.is_chunk_end());
        for ev in batch.iter() {
            match ev {
                talaris::ws::MarkedDataEvent::Text { payload, meta } => {
                    stats.record_meta(meta);
                    stats.record_text(payload);
                    self.observe_payload(payload.as_bytes());
                }
                talaris::ws::MarkedDataEvent::Binary { payload, meta } => {
                    stats.record_meta(meta);
                    stats.record_binary(payload);
                    self.observe_payload(payload);
                }
            }
        }
        self.flush_chunk_if_needed(batch.is_chunk_end(), downstream_spin_ns);
    }

    fn record_batch_boundary(&mut self, len: usize, chunk_end: bool) {
        self.batches = self.batches.saturating_add(1);
        if chunk_end {
            self.chunk_end_batches = self.chunk_end_batches.saturating_add(1);
        } else {
            self.split_batches = self.split_batches.saturating_add(1);
        }
        self.max_batch_len = self.max_batch_len.max(len);
    }

    fn observe_payload(&mut self, payload: &[u8]) {
        self.raw_messages = self.raw_messages.saturating_add(1);
        let Some(seq) = parse_binance_bbo_update_id(payload) else {
            self.missing_seq = self.missing_seq.saturating_add(1);
            return;
        };

        match self.pending {
            None => self.pending = Some(seq),
            Some(current) if seq > current => {
                self.pending = Some(seq);
                self.replaced_pending = self.replaced_pending.saturating_add(1);
            }
            Some(_) => {
                self.dropped_not_newer = self.dropped_not_newer.saturating_add(1);
            }
        }
    }

    fn flush_chunk_if_needed(&mut self, chunk_end: bool, downstream_spin_ns: u64) {
        if !chunk_end {
            return;
        }
        if self.pending.take().is_some() {
            self.published_messages = self.published_messages.saturating_add(1);
            spin_downstream(downstream_spin_ns);
        }
    }

    fn print(&self) {
        let avoided = self.raw_messages.saturating_sub(self.published_messages);
        let publish_reduction_pct = if self.raw_messages == 0 {
            0.0
        } else {
            100.0 * avoided as f64 / self.raw_messages as f64
        };
        let avg_raw_per_publish = if self.published_messages == 0 {
            0.0
        } else {
            self.raw_messages as f64 / self.published_messages as f64
        };
        println!(
            "bench_bbo_coalesce raw_messages={} published={} avoided={} publish_reduction_pct={:.2} avg_raw_per_publish={:.3} batches={} chunk_end_batches={} split_batches={} max_batch_len={} replaced_pending={} dropped_not_newer={} missing_seq={}",
            common::fmt_int(self.raw_messages),
            common::fmt_int(self.published_messages),
            common::fmt_int(avoided),
            publish_reduction_pct,
            avg_raw_per_publish,
            common::fmt_int(self.batches),
            common::fmt_int(self.chunk_end_batches),
            common::fmt_int(self.split_batches),
            self.max_batch_len,
            common::fmt_int(self.replaced_pending),
            common::fmt_int(self.dropped_not_newer),
            common::fmt_int(self.missing_seq)
        );
    }
}

fn parse_binance_bbo_update_id(payload: &[u8]) -> Option<u64> {
    let key = b"\"u\":";
    let pos = payload
        .windows(key.len())
        .position(|window| window == key)?;
    let mut value = 0_u64;
    let mut saw_digit = false;
    for &byte in &payload[pos + key.len()..] {
        if !byte.is_ascii_digit() {
            break;
        }
        saw_digit = true;
        value = value
            .saturating_mul(10)
            .saturating_add(u64::from(byte - b'0'));
    }
    saw_digit.then_some(value)
}

fn spin_downstream(nanos: u64) {
    if nanos == 0 {
        return;
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_nanos(nanos);
    while std::time::Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

fn print_usage() {
    println!(
        "local_pipeline bench\n\
         \n\
         Modes:\n\
           --mode baseline           unmarked pump_data_spin, no observability metadata\n\
           --mode marked_0_nohist    marked pump, sample 0%, no histograms\n\
           --mode marked_100_nohist  marked pump, sample 100%, no histograms\n\
           --mode hist_1pct          marked pump, sample 1%, HdrHistogram on\n\
           --mode hist_10pct         marked pump, sample 10%, HdrHistogram on\n\
           --mode hist_100pct        marked pump, sample 100%, HdrHistogram on\n\
         \n\
         Args:\n\
           --seconds N               wall-clock run limit, 0 disables time limit\n\
           --messages N              message limit, 0 disables message limit\n\
           --payload-profile binary|binance-bbo\n\
           --payload N               binary payload bytes per WS message\n\
           --frames-per-write N      server-side WS frames per write(2)\n\
           --buf-size N              io_uring provided buffer slot size\n\
           --buf-entries N           provided buffer entries, power of two\n\
           --sq-entries N            io_uring SQ entries, power of two\n\
           --cq-entries N            io_uring CQ entries, power of two\n\
           --completion-batch N      Pool CQE scratch buffer capacity\n\
           --spin-iters N            0 uses blocking pump_data(_marked)\n\
           --batch-sink              use chunk/batch sink API\n\
           --bbo-chunk-coalesce      with --batch-sink, publish only max Binance BBO seq per chunk\n\
           --downstream-spin-ns N    simulated downstream decode/publish cost per published message\n\
           --metrics-interval-ms N   write interval Prometheus snapshots, 0 disables periodic snapshots\n\
           --prom-out PATH|-         write Prometheus snapshots to file or stdout\n\
           --user-cpu N              pin benchmark thread\n\
           --server-cpu N            pin loopback server thread"
    );
}
