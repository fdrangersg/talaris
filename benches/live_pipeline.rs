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

use std::sync::Arc;

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
    paths: Vec<String>,
    feed: common::BinanceFeed,
    symbols: Vec<String>,
    stream_count: usize,
    depth_speed: String,
    seconds: u64,
    sample_bps: u16,
    histograms: bool,
    buf_size: u32,
    buf_entries: u16,
    sq_entries: u32,
    cq_entries: u32,
    completion_batch: usize,
    spin_iters: usize,
    batch_sink: bool,
    recv_mode: talaris::connection::RecvMode,
    socket_busy_poll_usecs: Option<u32>,
    setup_flags: talaris::proactor::ProactorSetupFlags,
    tls_provider: talaris::tls::TlsCryptoProvider,
    tls_cipher: talaris::tls::TlsCipherPreference,
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

        let feed = common::arg_or("--feed", common::BinanceFeed::Bbo);
        let symbols = common::parse_symbols(&common::arg_string(
            "--symbols",
            "btcusdt,ethusdt,bnbusdt,solusdt",
        ));
        if symbols.is_empty() {
            return Err("--symbols must contain at least one symbol".to_owned());
        }
        let stream_count = common::arg_or("--stream-count", 1_usize);
        let depth_speed = common::arg_string("--depth-speed", "100ms");
        let paths = match (
            common::optional_string("--paths"),
            common::optional_string("--path"),
        ) {
            (Some(paths), _) => common::split_csv(&paths),
            (None, Some(path)) => vec![path],
            (None, None) => {
                common::build_binance_paths(feed, &symbols, stream_count, &depth_speed)?
            }
        };
        if paths.is_empty() {
            return Err("--paths must contain at least one path".to_owned());
        }

        Ok(Self {
            host: common::arg_string("--host", "fstream.binance.com"),
            port: common::arg_or("--port", 443_u16),
            paths,
            feed,
            symbols,
            stream_count,
            depth_speed,
            seconds: common::arg_or("--seconds", 60_u64).max(1),
            sample_bps,
            histograms: !common::flag_present("--no-hist"),
            buf_size: common::arg_or("--buf-size", 8192_u32),
            buf_entries,
            sq_entries,
            cq_entries,
            completion_batch: common::arg_or("--completion-batch", 64_usize).max(1),
            spin_iters: common::arg_or("--spin-iters", 256_usize),
            batch_sink: common::flag_present("--batch-sink"),
            recv_mode: common::arg_or("--recv-mode", talaris::connection::RecvMode::Multishot),
            socket_busy_poll_usecs: common::optional_arg("--socket-busy-poll-usecs"),
            setup_flags: common::parse_proactor_setup_flags(&common::arg_string(
                "--setup-flags",
                "none",
            ))?,
            tls_provider: common::arg_or(
                "--tls-provider",
                talaris::tls::TlsCryptoProvider::default(),
            ),
            tls_cipher: common::arg_or(
                "--tls-cipher",
                talaris::tls::TlsCipherPreference::ProviderDefault,
            ),
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
            "bench_config bench=live_pipeline endpoint={}:{} paths={} feed={} symbols={} stream_count={} depth_speed={} seconds={} sample_bps={} histograms={} buf={}x{} sq_entries={} cq_entries={} setup_flags={:?} completion_batch={} spin_iters={} batch_sink={} recv_mode={} socket_busy_poll_usecs={} tls_provider={} tls_cipher={} metrics_interval_ms={} subscribe={} prom_out={}",
            self.host,
            self.port,
            self.paths.join(","),
            self.feed.as_str(),
            self.symbols.join(","),
            self.stream_count,
            self.depth_speed,
            self.seconds,
            self.sample_bps,
            self.histograms,
            self.buf_entries,
            self.buf_size,
            self.sq_entries,
            self.cq_entries,
            self.setup_flags,
            self.completion_batch,
            self.spin_iters,
            self.batch_sink,
            self.recv_mode,
            self.socket_busy_poll_usecs
                .map_or_else(|| "-".to_owned(), |usecs| usecs.to_string()),
            self.tls_provider,
            self.tls_cipher,
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
    let tls_config = Arc::new(
        talaris::tls::TlsAdapter::client_config_with_cipher_preference(
            cfg.tls_provider,
            cfg.tls_cipher,
        )?,
    );
    let first_conn_cfg = conn_config(&cfg, &cfg.paths[0], Arc::clone(&tls_config));
    let proactor_cfg = first_conn_cfg.proactor;
    let mut pool = talaris::Pool::new(
        talaris::PoolConfig::new(proactor_cfg).with_completion_batch_capacity(cfg.completion_batch),
    )?;
    let mut handles = Vec::with_capacity(cfg.paths.len());
    for (index, path) in cfg.paths.iter().enumerate() {
        let conn_cfg = if index == 0 {
            first_conn_cfg.clone()
        } else {
            conn_config(&cfg, path, Arc::clone(&tls_config))
        };
        let handle = pool.connect_blocking(conn_cfg)?;
        assert_eq!(pool.state(handle), Some(talaris::connection::State::Open));
        handles.push(handle);
    }

    if let Some(subscribe) = cfg.subscribe.as_deref() {
        for &handle in &handles {
            pool.send_text(handle, subscribe.as_bytes())?;
        }
    }

    let mut prom = common::PromWriter::from_arg(cfg.prom_out.clone())?;
    let mut stats = common::MessageStats::default();
    let mut batch_stats = BatchSinkStats::default();
    let cpu = common::ThreadCpuTimer::start();
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(cfg.seconds);
    let mut metrics_schedule = common::MetricsSchedule::new(started, cfg.metrics_interval);

    while std::time::Instant::now() < deadline {
        if cfg.batch_sink {
            pump_marked_batches(&mut pool, cfg.spin_iters, &mut stats, &mut batch_stats)?;
        } else {
            pump_marked(&mut pool, cfg.spin_iters, &mut stats)?;
        }
        metrics_schedule.write_due(&mut prom, "live_pipeline", &mut pool, started)?;
    }

    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();
    common::MetricsSchedule::write_final(&mut prom, "live_pipeline", &mut pool, elapsed)?;
    let mode = if cfg.batch_sink {
        "marked_batch"
    } else {
        "marked"
    };
    common::print_result("live_pipeline", mode, &stats, elapsed, cpu_elapsed);
    common::print_marked_summary(&stats);
    if cfg.batch_sink {
        batch_stats.print("live_pipeline");
    }
    for &handle in &handles {
        common::print_ingress_stats(handle, pool.ingress_stats(handle));
    }
    Ok(())
}

fn conn_config(
    cfg: &Config,
    path: &str,
    tls_config: Arc<rustls::ClientConfig>,
) -> talaris::connection::ConnectionConfig {
    let mut conn_cfg = talaris::connection::ConnectionConfig::new(&cfg.host, cfg.port, path)
        .with_tls_config(tls_config)
        .with_sq_entries(cfg.sq_entries)
        .with_cq_entries(cfg.cq_entries)
        .with_proactor_setup_flags(cfg.setup_flags)
        .with_recv_mode(cfg.recv_mode)
        .with_buf_ring(cfg.buf_size, cfg.buf_entries)
        .with_ws_limits(8 * 1024 * 1024, 16 * 1024 * 1024)
        .with_ws_buffer_capacities(128 * 1024, 128 * 1024, 16 * 1024)
        .with_ingress_stats(true)
        .with_observability_sample_rate_bps(cfg.sample_bps)
        .with_observability_histograms(cfg.histograms);
    if let Some(usecs) = cfg.socket_busy_poll_usecs {
        conn_cfg = conn_cfg.with_socket_busy_poll_usecs(usecs);
    }
    conn_cfg
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

fn pump_marked_batches(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
    batch_stats: &mut BatchSinkStats,
) -> Result<(), talaris::connection::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data_marked_batches(|_, batch| record_marked_batch(stats, batch_stats, &batch))
    } else {
        pool.pump_data_spin_marked_batches(spin_iters, |_, batch| {
            record_marked_batch(stats, batch_stats, &batch);
        })
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

fn record_marked_batch(
    stats: &mut common::MessageStats,
    batch_stats: &mut BatchSinkStats,
    batch: &talaris::ws::MarkedDataEventBatch<'_>,
) {
    batch_stats.record(batch);
    for ev in batch.iter() {
        record_marked_event(stats, &ev);
    }
}

#[derive(Debug, Default)]
struct BatchSinkStats {
    batches: u64,
    chunk_end_batches: u64,
    split_batches: u64,
    events: u64,
    text_events: u64,
    binary_events: u64,
    max_batch_len: usize,
}

impl BatchSinkStats {
    fn record(&mut self, batch: &talaris::ws::MarkedDataEventBatch<'_>) {
        self.batches = self.batches.saturating_add(1);
        if batch.is_chunk_end() {
            self.chunk_end_batches = self.chunk_end_batches.saturating_add(1);
        } else {
            self.split_batches = self.split_batches.saturating_add(1);
        }
        self.events = self
            .events
            .saturating_add(u64::try_from(batch.len()).unwrap_or(u64::MAX));
        self.text_events = self
            .text_events
            .saturating_add(u64::try_from(batch.text_count()).unwrap_or(u64::MAX));
        self.binary_events = self
            .binary_events
            .saturating_add(u64::try_from(batch.binary_count()).unwrap_or(u64::MAX));
        self.max_batch_len = self.max_batch_len.max(batch.len());
    }

    fn print(&self, bench: &str) {
        let avg_events_per_batch = if self.batches == 0 {
            0.0
        } else {
            self.events as f64 / self.batches as f64
        };
        println!(
            "bench_batch bench={bench} batches={} chunk_end_batches={} split_batches={} events={} text_events={} binary_events={} avg_events_per_batch={avg_events_per_batch:.3} max_batch_len={}",
            self.batches,
            self.chunk_end_batches,
            self.split_batches,
            self.events,
            self.text_events,
            self.binary_events,
            self.max_batch_len
        );
    }
}

fn print_usage() {
    println!(
        "live_pipeline bench\n\
         \n\
         Defaults target Binance USD-M futures routed BBO stream:\n\
           --host fstream.binance.com --port 443 --feed bbo --symbols btcusdt --stream-count 1\n\
         \n\
         Args:\n\
           --host HOST               websocket host\n\
           --port PORT               websocket TLS port\n\
           --feed bbo|depth|trade|depth-trade\n\
           --symbols a,b,c,d         Binance symbols used to build paths\n\
           --stream-count N          number of symbols to use\n\
           --depth-speed default|100ms|250ms|500ms\n\
           --path PATH               websocket path override for one connection\n\
           --paths A,B               websocket path overrides; each path becomes one Pool connection\n\
           --subscribe JSON          optional text subscription sent after open\n\
           --seconds N               run duration\n\
           --sample-bps N            observability sample rate, 0..10000\n\
           --no-hist                 disable local HdrHistogram recording\n\
           --buf-size N              io_uring provided buffer slot size\n\
           --buf-entries N           provided buffer entries, power of two\n\
           --sq-entries N            io_uring SQ entries, power of two\n\
           --cq-entries N            io_uring CQ entries, power of two\n\
           --setup-flags LIST        none|coop|taskrun|single|defer, comma or + separated\n\
           --completion-batch N      Pool CQE scratch buffer capacity\n\
           --spin-iters N            0 uses blocking pump_data_marked\n\
           --batch-sink              use experimental chunk/batch sink API\n\
           --recv-mode MODE          multishot|multishot-bundle\n\
           --socket-busy-poll-usecs N  set Linux SO_BUSY_POLL on talaris sockets\n\
           --tls-provider PROVIDER   ring|aws-lc\n\
           --tls-cipher PREF         default|aes128|aes256|chacha\n\
           --metrics-interval-ms N   write interval Prometheus snapshots, 0 disables periodic snapshots\n\
           --prom-out PATH|-         write Prometheus snapshots to file or stdout\n\
           --user-cpu N              pin benchmark thread"
    );
}
