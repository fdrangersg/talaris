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
            "bench_config bench=local_pipeline mode={} seconds={} messages={} payload_profile={} payload={} actual_payload={} frames_per_write={} buf={}x{} sq_entries={} cq_entries={} completion_batch={} spin_iters={} sample_bps={} histograms={} metrics_interval_ms={} prom_out={}",
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

    let conn_cfg = talaris::connection::ConnectionConfig::new("localhost", addr.port(), "/")
        .with_tls(false)
        .with_sq_entries(cfg.sq_entries)
        .with_cq_entries(cfg.cq_entries)
        .with_buf_ring(cfg.buf_size, cfg.buf_entries)
        .with_ws_limits(cfg.actual_payload_len, cfg.actual_payload_len as u64)
        .with_ingress_stats(true)
        .with_observability_sample_rate_bps(cfg.mode.sample_bps())
        .with_observability_histograms(cfg.mode.histograms());
    let proactor_cfg = conn_cfg.proactor;
    let mut pool = talaris::Pool::new(
        talaris::PoolConfig::new(proactor_cfg).with_completion_batch_capacity(cfg.completion_batch),
    )?;
    let handle = pool.connect_blocking_to(conn_cfg, addr)?;
    assert_eq!(pool.state(handle), Some(talaris::connection::State::Open));

    let mut prom = common::PromWriter::from_arg(cfg.prom_out.clone())?;
    let mut stats = common::MessageStats::default();
    let cpu = common::ThreadCpuTimer::start();
    let started = std::time::Instant::now();
    let mut metrics_schedule = common::MetricsSchedule::new(started, cfg.metrics_interval);

    while should_continue(&cfg, &stats, started.elapsed()) {
        if cfg.mode.marked() {
            pump_marked(&mut pool, cfg.spin_iters, &mut stats)?;
        } else {
            pump_unmarked(&mut pool, cfg.spin_iters, &mut stats)?;
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
) -> Result<(), talaris::connection::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data(|_, ev| record_unmarked_event(stats, &ev))
    } else {
        pool.pump_data_spin(spin_iters, |_, ev| record_unmarked_event(stats, &ev))
            .map(|_| ())
    }
}

fn pump_marked(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
) -> Result<(), talaris::connection::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data_marked(|_, ev| record_marked_event(stats, &ev))
    } else {
        pool.pump_data_spin_marked(spin_iters, |_, ev| record_marked_event(stats, &ev))
            .map(|_| ())
    }
}

fn record_unmarked_event(stats: &mut common::MessageStats, ev: &talaris::ws::DataEvent<'_>) {
    match ev {
        talaris::ws::DataEvent::Text(payload) => stats.record_text(payload),
        talaris::ws::DataEvent::Binary(payload) => stats.record_binary(payload),
    }
}

fn record_marked_event(stats: &mut common::MessageStats, ev: &talaris::ws::MarkedDataEvent<'_>) {
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
           --metrics-interval-ms N   write interval Prometheus snapshots, 0 disables periodic snapshots\n\
           --prom-out PATH|-         write Prometheus snapshots to file or stdout\n\
           --user-cpu N              pin benchmark thread\n\
           --server-cpu N            pin loopback server thread"
    );
}
