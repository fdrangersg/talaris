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
    common::print_linux_only("local_tuning");
}

#[path = "common.rs"]
mod common;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = run() {
        eprintln!("local_tuning: {e}");
        std::process::exit(1);
    }
}

#[derive(Clone, Copy, Debug)]
struct Variant {
    payload_len: usize,
    frames_per_write: usize,
    buf_size: u32,
    buf_entries: u16,
    completion_batch: usize,
    spin_iters: usize,
}

#[derive(Debug)]
struct Config {
    seconds: u64,
    messages: u64,
    warmup_messages: u64,
    payload_profile: common::PayloadProfile,
    payloads: Vec<usize>,
    frames_per_writes: Vec<usize>,
    buf_sizes: Vec<u32>,
    buf_entries: Vec<u16>,
    completion_batches: Vec<usize>,
    spin_iters: Vec<usize>,
    post_progress_spin_iters: usize,
    sq_entries: u32,
    cq_entries: u32,
    copy_batch_bytes: usize,
    max_runs: usize,
    top: usize,
    csv_out: Option<String>,
    user_cpu: Option<usize>,
    server_cpu: Option<usize>,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let seconds = common::arg_or("--seconds", 1_u64);
        let messages = common::arg_or("--messages", 0_u64);
        if seconds == 0 && messages == 0 {
            return Err("--seconds and --messages cannot both be zero".to_owned());
        }

        let payloads = nonzero_list("--payloads", common::arg_list("--payloads", "64,256,1024")?)?;
        let payload_profile = common::arg_or("--payload-profile", common::PayloadProfile::Binary);
        let frames_per_writes = nonzero_list(
            "--frames-per-write",
            common::arg_list("--frames-per-write", "1,4,16,32")?,
        )?;
        let buf_sizes = common::arg_list("--buf-sizes", "1024,2048,4096,8192,16384,32768")?;
        let buf_entries = common::arg_list("--buf-entries", "256,512")?;
        let completion_batches = nonzero_list(
            "--completion-batches",
            common::arg_list("--completion-batches", "64,256")?,
        )?;
        let spin_iters = common::arg_list("--spin-iters", "256,1024")?;
        let post_progress_spin_iters = common::arg_or("--post-progress-spin-iters", 0_usize);
        let sq_entries = common::arg_or("--sq-entries", 512_u32);
        let cq_entries = common::arg_or("--cq-entries", 1024_u32);
        common::validate_power_of_two_u32("--sq-entries", sq_entries)?;
        common::validate_power_of_two_u32("--cq-entries", cq_entries)?;
        for value in &buf_sizes {
            common::validate_power_of_two_u32("--buf-sizes", *value)?;
        }
        for value in &buf_entries {
            common::validate_power_of_two_u16("--buf-entries", *value)?;
        }

        Ok(Self {
            seconds,
            messages,
            warmup_messages: common::arg_or("--warmup-messages", 200_000_u64),
            payload_profile,
            payloads,
            frames_per_writes,
            buf_sizes,
            buf_entries,
            completion_batches,
            spin_iters,
            post_progress_spin_iters,
            sq_entries,
            cq_entries,
            copy_batch_bytes: common::arg_or("--copy-batch-bytes", 0_usize),
            max_runs: common::arg_or("--max-runs", usize::MAX),
            top: common::arg_or("--top", 12_usize),
            csv_out: common::optional_string("--csv"),
            user_cpu: common::optional_arg("--user-cpu"),
            server_cpu: common::optional_arg("--server-cpu"),
        })
    }

    fn variants(&self) -> Vec<Variant> {
        let mut out = Vec::new();
        for payload_len in &self.payloads {
            for frames_per_write in &self.frames_per_writes {
                for buf_size in &self.buf_sizes {
                    for buf_entries in &self.buf_entries {
                        for completion_batch in &self.completion_batches {
                            for spin_iters in &self.spin_iters {
                                out.push(Variant {
                                    payload_len: *payload_len,
                                    frames_per_write: *frames_per_write,
                                    buf_size: *buf_size,
                                    buf_entries: *buf_entries,
                                    completion_batch: *completion_batch,
                                    spin_iters: *spin_iters,
                                });
                            }
                        }
                    }
                }
            }
        }
        out
    }

    fn print(&self, variants: usize) {
        println!(
            "bench_config bench=local_tuning variants={} seconds={} messages={} warmup_messages={} payload_profile={} payloads={:?} frames_per_write={:?} buf_sizes={:?} buf_entries={:?} completion_batches={:?} spin_iters={:?} post_progress_spin_iters={} sq_entries={} cq_entries={} copy_batch_bytes={} max_runs={} top={} csv={}",
            variants,
            self.seconds,
            self.messages,
            self.warmup_messages,
            self.payload_profile.as_str(),
            self.payloads,
            self.frames_per_writes,
            self.buf_sizes,
            self.buf_entries,
            self.completion_batches,
            self.spin_iters,
            self.post_progress_spin_iters,
            self.sq_entries,
            self.cq_entries,
            self.copy_batch_bytes,
            self.max_runs,
            self.top,
            self.csv_out.as_deref().unwrap_or("-")
        );
    }
}

#[derive(Clone, Copy, Debug)]
struct ResultRow {
    variant: Variant,
    payload_profile: common::PayloadProfile,
    actual_payload_len: usize,
    sq_entries: u32,
    cq_entries: u32,
    post_progress_spin_iters: usize,
    copy_batch_bytes: usize,
    messages: u64,
    bytes: u64,
    elapsed_ms: f64,
    cpu_ms: f64,
    msg_s: f64,
    mib_s: f64,
    cpu_ns_msg: u64,
    recv_cqes: u64,
    plaintext_chunks: u64,
    ws_events: u64,
    rearms: u64,
    ring_exhaustions: u64,
    plain_batches: u64,
    plain_batch_cqes: u64,
    plain_copied_batches: u64,
    plain_copied_bytes: u64,
    messages_per_recv_cqe: f64,
    messages_per_plaintext_chunk: f64,
    checksum: u64,
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    if common::flag_present("--help") {
        print_usage();
        return Ok(());
    }

    let cfg = Config::from_args()?;
    let mut variants = cfg.variants();
    if variants.len() > cfg.max_runs {
        variants.truncate(cfg.max_runs);
    }
    cfg.print(variants.len());
    let _pin = cfg.user_cpu.map(|cpu| common::PinGuard::pin("user", cpu));
    let mut csv = CsvOutput::from_arg(cfg.csv_out.clone())?;
    csv.write_header()?;

    let mut rows = Vec::with_capacity(variants.len());
    for (index, variant) in variants.iter().copied().enumerate() {
        println!(
            "bench_tuning_start index={} payload={} frames_per_write={} buf={}x{} completion_batch={} spin_iters={}",
            index,
            variant.payload_len,
            variant.frames_per_write,
            variant.buf_entries,
            variant.buf_size,
            variant.completion_batch,
            variant.spin_iters
        );
        match run_variant(&cfg, variant) {
            Ok(row) => {
                print_row(index, &row);
                csv.write_row(&row)?;
                rows.push(row);
            }
            Err(e) => {
                println!(
                    "bench_tuning_error index={} payload={} frames_per_write={} buf={}x{} completion_batch={} spin_iters={} error={:?}",
                    index,
                    variant.payload_len,
                    variant.frames_per_write,
                    variant.buf_entries,
                    variant.buf_size,
                    variant.completion_batch,
                    variant.spin_iters,
                    e
                );
            }
        }
    }
    csv.flush()?;

    rows.sort_by(|a, b| b.msg_s.total_cmp(&a.msg_s));
    println!("bench_tuning_top count={}", cfg.top.min(rows.len()));
    for (rank, row) in rows.iter().take(cfg.top).enumerate() {
        println!(
            "bench_tuning_top_row rank={} payload_profile={} payload={} actual_payload={} frames_per_write={} buf={}x{} completion_batch={} spin_iters={} post_progress_spin_iters={} copy_batch_bytes={} msg_s={:.0} cpu_ns_msg={} mib_s={:.3} messages_per_recv_cqe={:.3} messages_per_plaintext_chunk={:.3} ring_exhaustions={}",
            rank + 1,
            row.payload_profile.as_str(),
            row.variant.payload_len,
            row.actual_payload_len,
            row.variant.frames_per_write,
            row.variant.buf_entries,
            row.variant.buf_size,
            row.variant.completion_batch,
            row.variant.spin_iters,
            row.post_progress_spin_iters,
            row.copy_batch_bytes,
            row.msg_s,
            row.cpu_ns_msg,
            row.mib_s,
            row.messages_per_recv_cqe,
            row.messages_per_plaintext_chunk,
            row.ring_exhaustions
        );
    }

    Ok(())
}

fn run_variant(cfg: &Config, variant: Variant) -> Result<ResultRow, Box<dyn std::error::Error>> {
    let actual_payload_len = cfg.payload_profile.payload_len(variant.payload_len);
    let server = common::spawn_local_stream_server_with_profile(
        cfg.payload_profile,
        variant.payload_len,
        variant.frames_per_write,
        cfg.server_cpu,
    )?;
    let addr = server.addr();

    let conn_cfg = talaris::connection::ConnectionConfig::new("localhost", addr.port(), "/")
        .with_tls(false)
        .with_sq_entries(cfg.sq_entries)
        .with_cq_entries(cfg.cq_entries)
        .with_buf_ring(variant.buf_size, variant.buf_entries)
        .with_ws_limits(actual_payload_len, actual_payload_len as u64)
        .with_plain_recv_batch_copy_max_bytes(cfg.copy_batch_bytes)
        .with_ingress_stats(true)
        .with_observability_sample_rate_bps(0)
        .with_observability_histograms(false);
    let proactor_cfg = conn_cfg.proactor;
    let mut pool = talaris::Pool::new(
        talaris::PoolConfig::new(proactor_cfg)
            .with_completion_batch_capacity(variant.completion_batch)
            .with_post_progress_spin_iters(cfg.post_progress_spin_iters),
    )?;
    let handle = pool.connect_blocking_to(conn_cfg, addr)?;
    assert_eq!(pool.state(handle), Some(talaris::connection::State::Open));

    let mut warmup = common::MessageStats::default();
    while warmup.messages < cfg.warmup_messages {
        pump_talaris(&mut pool, variant.spin_iters, &mut warmup)?;
    }
    let ingress_before = pool.ingress_stats(handle).unwrap_or_default();

    let mut stats = common::MessageStats::default();
    let cpu = common::ThreadCpuTimer::start();
    let started = std::time::Instant::now();
    while should_continue(cfg, &stats, started.elapsed()) {
        pump_talaris(&mut pool, variant.spin_iters, &mut stats)?;
    }
    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();
    let ingress_after = pool.ingress_stats(handle).unwrap_or_default();
    let ingress = diff_ingress(ingress_after, ingress_before);

    drop(pool);
    server.join()?;

    let msg_s = common::messages_per_sec(stats.messages, elapsed);
    let mib_s = common::mib_per_sec(stats.bytes, elapsed);
    Ok(ResultRow {
        variant,
        payload_profile: cfg.payload_profile,
        actual_payload_len,
        sq_entries: cfg.sq_entries,
        cq_entries: cfg.cq_entries,
        post_progress_spin_iters: cfg.post_progress_spin_iters,
        copy_batch_bytes: cfg.copy_batch_bytes,
        messages: stats.messages,
        bytes: stats.bytes,
        elapsed_ms: common::elapsed_ms(elapsed),
        cpu_ms: common::elapsed_ms(cpu_elapsed),
        msg_s,
        mib_s,
        cpu_ns_msg: common::ns_per_message(cpu_elapsed, stats.messages),
        recv_cqes: ingress.recv_data_cqes,
        plaintext_chunks: ingress.plaintext_chunks,
        ws_events: ingress.ws_data_events,
        rearms: ingress.recv_multishot_rearms,
        ring_exhaustions: ingress.recv_ring_exhaustions,
        plain_batches: ingress.plain_recv_batches,
        plain_batch_cqes: ingress.plain_recv_batch_cqes,
        plain_copied_batches: ingress.plain_recv_copied_batches,
        plain_copied_bytes: ingress.plain_recv_copied_bytes,
        messages_per_recv_cqe: ratio(stats.messages, ingress.recv_data_cqes),
        messages_per_plaintext_chunk: ratio(stats.messages, ingress.plaintext_chunks),
        checksum: stats.checksum(),
    })
}

fn pump_talaris(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
) -> Result<(), talaris::connection::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data(|_, ev| record_talaris_event(stats, &ev))
    } else {
        pool.pump_data_spin(spin_iters, |_, ev| record_talaris_event(stats, &ev))
            .map(|_| ())
    }
}

fn record_talaris_event(stats: &mut common::MessageStats, ev: &talaris::ws::DataEvent<'_>) {
    match ev {
        talaris::ws::DataEvent::Text(payload) => stats.record_text(payload),
        talaris::ws::DataEvent::Binary(payload) => stats.record_binary(payload),
    }
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

const fn diff_ingress(
    after: talaris::connection::IngressStats,
    before: talaris::connection::IngressStats,
) -> talaris::connection::IngressStats {
    talaris::connection::IngressStats {
        recv_data_cqes: after.recv_data_cqes.saturating_sub(before.recv_data_cqes),
        recv_bytes: after.recv_bytes.saturating_sub(before.recv_bytes),
        recv_multishot_rearms: after
            .recv_multishot_rearms
            .saturating_sub(before.recv_multishot_rearms),
        recv_ring_exhaustions: after
            .recv_ring_exhaustions
            .saturating_sub(before.recv_ring_exhaustions),
        plain_recv_batches: after
            .plain_recv_batches
            .saturating_sub(before.plain_recv_batches),
        plain_recv_batch_cqes: after
            .plain_recv_batch_cqes
            .saturating_sub(before.plain_recv_batch_cqes),
        plain_recv_copied_batches: after
            .plain_recv_copied_batches
            .saturating_sub(before.plain_recv_copied_batches),
        plain_recv_copied_bytes: after
            .plain_recv_copied_bytes
            .saturating_sub(before.plain_recv_copied_bytes),
        plaintext_chunks: after
            .plaintext_chunks
            .saturating_sub(before.plaintext_chunks),
        plaintext_bytes: after.plaintext_bytes.saturating_sub(before.plaintext_bytes),
        ws_data_drains: after.ws_data_drains.saturating_sub(before.ws_data_drains),
        ws_data_drain_skips: after
            .ws_data_drain_skips
            .saturating_sub(before.ws_data_drain_skips),
        ws_data_events: after.ws_data_events.saturating_sub(before.ws_data_events),
        ws_text_events: after.ws_text_events.saturating_sub(before.ws_text_events),
        ws_binary_events: after
            .ws_binary_events
            .saturating_sub(before.ws_binary_events),
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn nonzero_list(name: &str, values: Vec<usize>) -> Result<Vec<usize>, String> {
    if values.contains(&0) {
        Err(format!("{name} values must be > 0"))
    } else {
        Ok(values)
    }
}

fn print_row(index: usize, row: &ResultRow) {
    println!(
        "bench_tuning_result index={} payload_profile={} payload={} actual_payload={} frames_per_write={} buf={}x{} completion_batch={} spin_iters={} post_progress_spin_iters={} copy_batch_bytes={} messages={} bytes={} elapsed_ms={:.3} cpu_ms={:.3} msg_s={:.0} mib_s={:.3} cpu_ns_msg={} recv_cqes={} plaintext_chunks={} ws_events={} plain_batches={} plain_batch_cqes={} plain_copied_batches={} plain_copied_bytes={} messages_per_recv_cqe={:.3} messages_per_plaintext_chunk={:.3} rearms={} ring_exhaustions={} checksum={}",
        index,
        row.payload_profile.as_str(),
        row.variant.payload_len,
        row.actual_payload_len,
        row.variant.frames_per_write,
        row.variant.buf_entries,
        row.variant.buf_size,
        row.variant.completion_batch,
        row.variant.spin_iters,
        row.post_progress_spin_iters,
        row.copy_batch_bytes,
        row.messages,
        row.bytes,
        row.elapsed_ms,
        row.cpu_ms,
        row.msg_s,
        row.mib_s,
        row.cpu_ns_msg,
        row.recv_cqes,
        row.plaintext_chunks,
        row.ws_events,
        row.plain_batches,
        row.plain_batch_cqes,
        row.plain_copied_batches,
        row.plain_copied_bytes,
        row.messages_per_recv_cqe,
        row.messages_per_plaintext_chunk,
        row.rearms,
        row.ring_exhaustions,
        row.checksum
    );
}

struct CsvOutput {
    out: Option<Box<dyn std::io::Write>>,
}

impl CsvOutput {
    fn from_arg(path: Option<String>) -> std::io::Result<Self> {
        let Some(path) = path else {
            return Ok(Self { out: None });
        };
        let out: Box<dyn std::io::Write> = if path == "-" {
            Box::new(std::io::stdout())
        } else {
            Box::new(std::fs::File::create(path)?)
        };
        Ok(Self { out: Some(out) })
    }

    fn write_header(&mut self) -> std::io::Result<()> {
        let Some(out) = self.out.as_mut() else {
            return Ok(());
        };
        writeln!(
            out,
            "payload_profile,payload,actual_payload,frames_per_write,buf_size,buf_entries,sq_entries,cq_entries,completion_batch,spin_iters,post_progress_spin_iters,copy_batch_bytes,messages,bytes,elapsed_ms,cpu_ms,msg_s,mib_s,cpu_ns_msg,recv_cqes,plaintext_chunks,ws_events,rearms,ring_exhaustions,plain_batches,plain_batch_cqes,plain_copied_batches,plain_copied_bytes,messages_per_recv_cqe,messages_per_plaintext_chunk,checksum"
        )
    }

    fn write_row(&mut self, row: &ResultRow) -> std::io::Result<()> {
        let Some(out) = self.out.as_mut() else {
            return Ok(());
        };
        writeln!(
            out,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.6},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{}",
            row.payload_profile.as_str(),
            row.variant.payload_len,
            row.actual_payload_len,
            row.variant.frames_per_write,
            row.variant.buf_size,
            row.variant.buf_entries,
            row.sq_entries,
            row.cq_entries,
            row.variant.completion_batch,
            row.variant.spin_iters,
            row.post_progress_spin_iters,
            row.copy_batch_bytes,
            row.messages,
            row.bytes,
            row.elapsed_ms,
            row.cpu_ms,
            row.msg_s,
            row.mib_s,
            row.cpu_ns_msg,
            row.recv_cqes,
            row.plaintext_chunks,
            row.ws_events,
            row.rearms,
            row.ring_exhaustions,
            row.plain_batches,
            row.plain_batch_cqes,
            row.plain_copied_batches,
            row.plain_copied_bytes,
            row.messages_per_recv_cqe,
            row.messages_per_plaintext_chunk,
            row.checksum
        )
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let Some(out) = self.out.as_mut() else {
            return Ok(());
        };
        out.flush()
    }
}

fn print_usage() {
    println!(
        "local_tuning bench\n\
         \n\
         Sweeps talaris local plain-WS inbound tuning parameters and emits one\n\
         result row per variant. Use this before comparing tuned talaris against\n\
         tungstenite.\n\
         \n\
         Args:\n\
           --seconds N                  wall-clock seconds per variant, 0 disables time limit\n\
           --messages N                 message limit per variant, 0 disables message limit\n\
           --warmup-messages N          messages discarded before timing each variant\n\
           --payload-profile binary|binance-bbo\n\
           --payloads A,B,C             payload byte sizes\n\
           --frames-per-write A,B,C     server-side WS frames per write(2)\n\
           --buf-sizes A,B,C            talaris provided-buffer slot sizes, powers of two\n\
           --buf-entries A,B,C          talaris provided-buffer entries, powers of two\n\
           --completion-batches A,B,C   Pool CQE scratch capacities\n\
           --spin-iters A,B,C           talaris spin counts; 0 uses blocking pump_data\n\
           --post-progress-spin-iters N extra spin/drain budget after first progress\n\
           --sq-entries N               io_uring SQ entries, power of two\n\
           --cq-entries N               io_uring CQ entries, power of two\n\
           --copy-batch-bytes N         max bytes copied across a plain recv CQE batch; 0 disables\n\
           --max-runs N                 truncate matrix for smoke tests\n\
           --top N                      print top N variants by msg/s\n\
           --csv PATH|-                 write CSV rows to file or stdout\n\
           --user-cpu N                 pin benchmark thread once for the run\n\
           --server-cpu N               pin per-variant loopback server thread"
    );
}
