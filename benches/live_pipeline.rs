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
    common::print_linux_only("live_pipeline");
}

#[path = "common.rs"]
mod common;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = run() {
        eprintln!("live_pipeline: {e}");
        std::process::exit(1);
    }
}

#[derive(Debug)]
struct Config {
    host: String,
    port: u16,
    path: String,
    seconds: u64,
    sample_bps: u16,
    histograms: bool,
    buf_size: u32,
    buf_entries: u16,
    sq_entries: u32,
    cq_entries: u32,
    completion_batch: usize,
    spin_iters: usize,
    metrics_interval: std::time::Duration,
    prom_out: Option<String>,
    subscribe: Option<String>,
    user_cpu: Option<usize>,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let sample_bps = common::arg_or("--sample-bps", common::FULL_SAMPLE_BPS);
        common::validate_sampling_bps(sample_bps)?;
        let buf_entries = common::arg_or("--buf-entries", 256_u16);
        let sq_entries = common::arg_or("--sq-entries", 512_u32);
        let cq_entries = common::arg_or("--cq-entries", 1024_u32);
        common::validate_power_of_two_u16("--buf-entries", buf_entries)?;
        common::validate_power_of_two_u32("--sq-entries", sq_entries)?;
        common::validate_power_of_two_u32("--cq-entries", cq_entries)?;

        Ok(Self {
            host: common::arg_string("--host", "fstream.binance.com"),
            port: common::arg_or("--port", 443_u16),
            path: common::arg_string("--path", "/ws/btcusdt@bookTicker"),
            seconds: common::arg_or("--seconds", 60_u64).max(1),
            sample_bps,
            histograms: !common::flag_present("--no-hist"),
            buf_size: common::arg_or("--buf-size", 8192_u32),
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
            subscribe: common::optional_string("--subscribe"),
            user_cpu: common::optional_arg("--user-cpu"),
        })
    }

    fn print(&self) {
        println!(
            "bench_config bench=live_pipeline endpoint={}:{}{} seconds={} sample_bps={} histograms={} buf={}x{} sq_entries={} cq_entries={} completion_batch={} spin_iters={} metrics_interval_ms={} subscribe={} prom_out={}",
            self.host,
            self.port,
            self.path,
            self.seconds,
            self.sample_bps,
            self.histograms,
            self.buf_entries,
            self.buf_size,
            self.sq_entries,
            self.cq_entries,
            self.completion_batch,
            self.spin_iters,
            self.metrics_interval.as_millis(),
            self.subscribe.as_ref().map_or("no", |_| "yes"),
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

    let _pin = cfg.user_cpu.map(|cpu| common::PinGuard::pin("user", cpu));
    let conn_cfg = talaris::connection::ConnectionConfig::new(&cfg.host, cfg.port, &cfg.path)
        .with_sq_entries(cfg.sq_entries)
        .with_cq_entries(cfg.cq_entries)
        .with_buf_ring(cfg.buf_size, cfg.buf_entries)
        .with_ws_limits(8 * 1024 * 1024, 16 * 1024 * 1024)
        .with_ws_buffer_capacities(128 * 1024, 128 * 1024, 16 * 1024)
        .with_ingress_stats(true)
        .with_observability_sample_rate_bps(cfg.sample_bps)
        .with_observability_histograms(cfg.histograms);
    let proactor_cfg = conn_cfg.proactor;
    let mut pool = talaris::Pool::new(
        talaris::PoolConfig::new(proactor_cfg).with_completion_batch_capacity(cfg.completion_batch),
    )?;
    let handle = pool.connect_blocking(conn_cfg)?;
    assert_eq!(pool.state(handle), Some(talaris::connection::State::Open));

    if let Some(subscribe) = cfg.subscribe.as_deref() {
        pool.send_text(handle, subscribe.as_bytes())?;
    }

    let mut prom = common::PromWriter::from_arg(cfg.prom_out.clone())?;
    let mut stats = common::MessageStats::default();
    let cpu = common::ThreadCpuTimer::start();
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(cfg.seconds);
    let mut metrics_schedule = common::MetricsSchedule::new(started, cfg.metrics_interval);

    while std::time::Instant::now() < deadline {
        pump_marked(&mut pool, cfg.spin_iters, &mut stats)?;
        metrics_schedule.write_due(&mut prom, "live_pipeline", &mut pool, started)?;
    }

    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();
    common::MetricsSchedule::write_final(&mut prom, "live_pipeline", &mut pool, elapsed)?;
    common::print_result("live_pipeline", "marked", &stats, elapsed, cpu_elapsed);
    common::print_marked_summary(&stats);
    common::print_ingress_stats(handle, pool.ingress_stats(handle));
    Ok(())
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
        "live_pipeline bench\n\
         \n\
         Defaults target Binance USD-M futures raw stream:\n\
           --host fstream.binance.com --port 443 --path /ws/btcusdt@bookTicker\n\
         \n\
         Args:\n\
           --host HOST               websocket host\n\
           --port PORT               websocket TLS port\n\
           --path PATH               websocket path\n\
           --subscribe JSON          optional text subscription sent after open\n\
           --seconds N               run duration\n\
           --sample-bps N            observability sample rate, 0..10000\n\
           --no-hist                 disable local HdrHistogram recording\n\
           --buf-size N              io_uring provided buffer slot size\n\
           --buf-entries N           provided buffer entries, power of two\n\
           --sq-entries N            io_uring SQ entries, power of two\n\
           --cq-entries N            io_uring CQ entries, power of two\n\
           --completion-batch N      Pool CQE scratch buffer capacity\n\
           --spin-iters N            0 uses blocking pump_data_marked\n\
           --metrics-interval-ms N   write interval Prometheus snapshots, 0 disables periodic snapshots\n\
           --prom-out PATH|-         write Prometheus snapshots to file or stdout\n\
           --user-cpu N              pin benchmark thread"
    );
}
