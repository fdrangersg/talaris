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
    common::print_linux_only("local_redundancy");
}

#[path = "common.rs"]
mod common;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = run() {
        eprintln!("local_redundancy: {e}");
        std::process::exit(1);
    }
}

use std::io::{self, Write};
use std::net::{SocketAddr, TcpListener};
use std::thread::{self, JoinHandle};
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

use talaris::connection::IngressStats;
use talaris::ws::frame::{MAX_HEADER_LEN, OpCode, encode_header};

const SEQ_WIDTH: usize = 20;
const SEQ_MARKER: &[u8] = b"\"u\":\"";
const BBO_SEQ_TEMPLATE: &[u8] = b"{\"e\":\"bookTicker\",\"u\":\"00000000000000000000\",\"s\":\"BNBUSDT\",\"ps\":\"BNBUSDT\",\"E\":1568014460893,\"T\":1568014460891,\"b\":\"25.35190000\",\"B\":\"31.21000000\",\"a\":\"25.36520000\",\"A\":\"40.66000000\",\"st\":1}";

#[derive(Debug)]
struct Config {
    connections: Vec<usize>,
    seconds: u64,
    messages: u64,
    warmup_events: u64,
    frames_per_write: usize,
    buf_size: u32,
    buf_entries: u16,
    sq_entries: u32,
    cq_entries: u32,
    completion_batch: usize,
    spin_iters: usize,
    post_progress_spin_iters: usize,
    copy_batch_bytes: usize,
    user_cpu: Option<usize>,
    server_cpus: Vec<usize>,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let connections =
            nonzero_list("--connections", common::arg_list("--connections", "1,2,4")?)?;
        let seconds = common::arg_or("--seconds", 8_u64);
        let messages = common::arg_or("--messages", 0_u64);
        if seconds == 0 && messages == 0 {
            return Err("--seconds and --messages cannot both be zero".to_owned());
        }

        let buf_size = common::arg_or("--buf-size", 1024_u32);
        let buf_entries = common::arg_or("--buf-entries", 512_u16);
        let sq_entries = common::arg_or("--sq-entries", 512_u32);
        let cq_entries = common::arg_or("--cq-entries", 1024_u32);
        common::validate_power_of_two_u32("--buf-size", buf_size)?;
        common::validate_power_of_two_u16("--buf-entries", buf_entries)?;
        common::validate_power_of_two_u32("--sq-entries", sq_entries)?;
        common::validate_power_of_two_u32("--cq-entries", cq_entries)?;

        Ok(Self {
            connections,
            seconds,
            messages,
            warmup_events: common::arg_or("--warmup-events", 1_000_000_u64),
            frames_per_write: common::arg_or("--frames-per-write", 1_usize).max(1),
            buf_size,
            buf_entries,
            sq_entries,
            cq_entries,
            completion_batch: common::arg_or("--completion-batch", 64_usize).max(1),
            spin_iters: common::arg_or("--spin-iters", 256_usize),
            post_progress_spin_iters: common::arg_or("--post-progress-spin-iters", 0_usize),
            copy_batch_bytes: common::arg_or("--copy-batch-bytes", 0_usize),
            user_cpu: common::optional_arg("--user-cpu"),
            server_cpus: optional_cpu_list("--server-cpus")?,
        })
    }

    fn print(&self) {
        println!(
            "bench_config bench=local_redundancy connections={:?} seconds={} messages={} warmup_events={} payload_profile=binance-bbo-seq actual_payload={} frames_per_write={} buf={}x{} sq_entries={} cq_entries={} completion_batch={} spin_iters={} post_progress_spin_iters={} copy_batch_bytes={} server_cpus={:?}",
            self.connections,
            self.seconds,
            self.messages,
            self.warmup_events,
            BBO_SEQ_TEMPLATE.len(),
            self.frames_per_write,
            self.buf_entries,
            self.buf_size,
            self.sq_entries,
            self.cq_entries,
            self.completion_batch,
            self.spin_iters,
            self.post_progress_spin_iters,
            self.copy_batch_bytes,
            self.server_cpus
        );
    }

    fn server_cpu(&self, index: usize) -> Option<usize> {
        if self.server_cpus.is_empty() {
            None
        } else {
            Some(self.server_cpus[index % self.server_cpus.len()])
        }
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
    for &connections in &cfg.connections {
        run_case(&cfg, connections)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn run_case(cfg: &Config, connections: usize) -> Result<(), Box<dyn std::error::Error>> {
    println!("bench_redundancy_start connections={connections}");
    let mut servers = Vec::with_capacity(connections);
    for index in 0..connections {
        servers.push(spawn_seq_stream_server(
            cfg.frames_per_write,
            cfg.server_cpu(index),
        )?);
    }

    let _pin = cfg.user_cpu.map(|cpu| common::PinGuard::pin("user", cpu));
    let first_addr = servers
        .first()
        .expect("connections list rejects zero")
        .addr();
    let proactor_cfg = conn_config(cfg, first_addr).proactor;
    let mut pool = talaris::Pool::new(
        talaris::PoolConfig::new(proactor_cfg)
            .with_completion_batch_capacity(cfg.completion_batch)
            .with_post_progress_spin_iters(cfg.post_progress_spin_iters),
    )?;

    let mut handles = Vec::with_capacity(connections);
    for server in &servers {
        let addr = server.addr();
        let conn_cfg = conn_config(cfg, addr);
        let handle = pool.connect_blocking_to(conn_cfg, addr)?;
        assert_eq!(pool.state(handle), Some(talaris::connection::State::Open));
        handles.push(handle);
    }
    let handle_index = handle_index_map(&handles);

    let mut dedup = DedupState::default();
    let mut warmup = RedundancyStats::new(connections);
    while warmup.input_events < cfg.warmup_events {
        pump_redundancy(
            &mut pool,
            cfg.spin_iters,
            &handle_index,
            &mut dedup,
            &mut warmup,
        )?;
    }

    let ingress_before: Vec<_> = handles.iter().map(|&h| pool.ingress_stats(h)).collect();
    let mut stats = RedundancyStats::new(connections);
    let cpu = common::ThreadCpuTimer::start();
    let started = Instant::now();
    while should_continue(cfg, &stats, started.elapsed()) {
        pump_redundancy(
            &mut pool,
            cfg.spin_iters,
            &handle_index,
            &mut dedup,
            &mut stats,
        )?;
    }
    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();

    print_redundancy_result(connections, &stats, elapsed, cpu_elapsed);
    print_redundancy_ingress(
        connections,
        aggregate_ingress_delta(&pool, &handles, &ingress_before),
    );

    drop(pool);
    for server in servers {
        server.join()?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn conn_config(cfg: &Config, addr: SocketAddr) -> talaris::connection::ConnectionConfig {
    talaris::connection::ConnectionConfig::new("localhost", addr.port(), "/")
        .with_tls(false)
        .with_sq_entries(cfg.sq_entries)
        .with_cq_entries(cfg.cq_entries)
        .with_buf_ring(cfg.buf_size, cfg.buf_entries)
        .with_ws_limits(BBO_SEQ_TEMPLATE.len(), BBO_SEQ_TEMPLATE.len() as u64)
        .with_plain_recv_batch_copy_max_bytes(cfg.copy_batch_bytes)
        .with_ingress_stats(true)
        .with_observability_sample_rate_bps(0)
        .with_observability_histograms(false)
}

fn nonzero_list<T>(flag: &str, values: Vec<T>) -> Result<Vec<T>, String>
where
    T: Copy + Ord + From<u8> + std::fmt::Display,
{
    let invalid = values.iter().find(|&&value| value < T::from(1)).copied();
    invalid.map_or_else(
        || Ok(values),
        |value| Err(format!("{flag} values must be > 0, got {value}")),
    )
}

fn optional_cpu_list(flag: &str) -> Result<Vec<usize>, String> {
    let Some(raw) = common::optional_string(flag) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        out.push(
            part.parse::<usize>()
                .map_err(|e| format!("{flag} has invalid value {part:?}: {e}"))?,
        );
    }
    Ok(out)
}

#[cfg(target_os = "linux")]
fn should_continue(cfg: &Config, stats: &RedundancyStats, elapsed: Duration) -> bool {
    let time_ok = cfg.seconds == 0 || elapsed < Duration::from_secs(cfg.seconds);
    let messages_ok = cfg.messages == 0 || stats.unique_events < cfg.messages;
    time_ok && messages_ok
}

#[cfg(target_os = "linux")]
fn pump_redundancy(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    handle_index: &[usize],
    dedup: &mut DedupState,
    stats: &mut RedundancyStats,
) -> Result<(), talaris::connection::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data(|handle, ev| {
            let conn_index = conn_index(handle_index, handle);
            record_event(conn_index, &ev, dedup, stats);
        })
    } else {
        pool.pump_data_spin(spin_iters, |handle, ev| {
            let conn_index = conn_index(handle_index, handle);
            record_event(conn_index, &ev, dedup, stats);
        })
        .map(|_| ())
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

#[cfg(target_os = "linux")]
fn record_event(
    conn_index: usize,
    ev: &talaris::ws::DataEvent<'_>,
    dedup: &mut DedupState,
    stats: &mut RedundancyStats,
) {
    match ev {
        talaris::ws::DataEvent::Text(payload) => stats.record_text(conn_index, payload, dedup),
        talaris::ws::DataEvent::Binary(payload) => stats.record_binary(conn_index, payload),
    }
}

#[derive(Debug, Default)]
struct DedupState {
    high_water: Option<u64>,
}

#[derive(Debug)]
struct RedundancyStats {
    input_events: u64,
    text_events: u64,
    binary_events: u64,
    bytes: u64,
    unique_events: u64,
    duplicate_events: u64,
    parse_failures: u64,
    seq_gaps: u64,
    first_unique_seq: Option<u64>,
    last_unique_seq: Option<u64>,
    per_conn_events: Vec<u64>,
    per_conn_unique: Vec<u64>,
    checksum: u64,
}

impl RedundancyStats {
    fn new(connections: usize) -> Self {
        Self {
            input_events: 0,
            text_events: 0,
            binary_events: 0,
            bytes: 0,
            unique_events: 0,
            duplicate_events: 0,
            parse_failures: 0,
            seq_gaps: 0,
            first_unique_seq: None,
            last_unique_seq: None,
            per_conn_events: vec![0; connections],
            per_conn_unique: vec![0; connections],
            checksum: 0,
        }
    }

    fn record_text(&mut self, conn_index: usize, payload: &str, dedup: &mut DedupState) {
        self.text_events = self.text_events.saturating_add(1);
        self.record_payload(conn_index, payload.as_bytes());
        let Some(seq) = parse_seq(payload.as_bytes()) else {
            self.parse_failures = self.parse_failures.saturating_add(1);
            return;
        };
        if is_unique(seq, dedup) {
            self.unique_events = self.unique_events.saturating_add(1);
            self.per_conn_unique[conn_index] = self.per_conn_unique[conn_index].saturating_add(1);
            if let Some(prev) = self.last_unique_seq
                && seq > prev.saturating_add(1)
            {
                self.seq_gaps = self
                    .seq_gaps
                    .saturating_add(seq.saturating_sub(prev).saturating_sub(1));
            }
            self.first_unique_seq.get_or_insert(seq);
            self.last_unique_seq = Some(seq);
            self.checksum = mix_seq_checksum(self.checksum, seq);
        } else {
            self.duplicate_events = self.duplicate_events.saturating_add(1);
        }
    }

    fn record_binary(&mut self, conn_index: usize, payload: &[u8]) {
        self.binary_events = self.binary_events.saturating_add(1);
        self.record_payload(conn_index, payload);
        self.parse_failures = self.parse_failures.saturating_add(1);
    }

    fn record_payload(&mut self, conn_index: usize, payload: &[u8]) {
        self.input_events = self.input_events.saturating_add(1);
        self.bytes = self.bytes.saturating_add(payload.len() as u64);
        self.per_conn_events[conn_index] = self.per_conn_events[conn_index].saturating_add(1);
    }
}

const fn is_unique(seq: u64, dedup: &mut DedupState) -> bool {
    match dedup.high_water {
        Some(high_water) if seq <= high_water => false,
        _ => {
            dedup.high_water = Some(seq);
            true
        }
    }
}

fn parse_seq(payload: &[u8]) -> Option<u64> {
    let marker_start = payload
        .windows(SEQ_MARKER.len())
        .position(|window| window == SEQ_MARKER)?;
    let start = marker_start + SEQ_MARKER.len();
    let digits = payload.get(start..start + SEQ_WIDTH)?;
    let mut out = 0_u64;
    for &digit in digits {
        if !digit.is_ascii_digit() {
            return None;
        }
        out = out
            .saturating_mul(10)
            .saturating_add(u64::from(digit - b'0'));
    }
    Some(out)
}

const fn mix_seq_checksum(mut checksum: u64, seq: u64) -> u64 {
    checksum = checksum.rotate_left(7) ^ seq;
    checksum.wrapping_mul(1_099_511_628_211)
}

#[cfg(target_os = "linux")]
fn print_redundancy_result(
    connections: usize,
    stats: &RedundancyStats,
    elapsed: Duration,
    cpu: Duration,
) {
    let duplicate_ratio = if stats.input_events == 0 {
        0.0
    } else {
        stats.duplicate_events as f64 / stats.input_events as f64
    };
    let input_msg_s = common::messages_per_sec(stats.input_events, elapsed);
    let unique_msg_s = common::messages_per_sec(stats.unique_events, elapsed);
    let cpu_ns_input = common::ns_per_message(cpu, stats.input_events);
    let cpu_ns_unique = common::ns_per_message(cpu, stats.unique_events);
    println!(
        "bench_redundancy_result connections={} input_events={} unique_events={} duplicate_events={} duplicate_ratio={:.4} parse_failures={} bytes={} elapsed_ms={:.3} cpu_ms={:.3} cpu_pct={:.1} input_msg_s={:.0} unique_msg_s={:.0} cpu_ns_input={} cpu_ns_unique={} checksum={}",
        connections,
        common::fmt_int(stats.input_events),
        common::fmt_int(stats.unique_events),
        common::fmt_int(stats.duplicate_events),
        duplicate_ratio,
        common::fmt_int(stats.parse_failures),
        common::fmt_int(stats.bytes),
        common::elapsed_ms(elapsed),
        common::elapsed_ms(cpu),
        common::cpu_pct(cpu, elapsed),
        input_msg_s,
        unique_msg_s,
        cpu_ns_input,
        cpu_ns_unique,
        stats.checksum
    );
    println!(
        "bench_redundancy_seq connections={} first_unique_seq={} last_unique_seq={} seq_gaps={} per_conn_events={:?} per_conn_unique={:?}",
        connections,
        stats
            .first_unique_seq
            .map_or_else(|| "-".to_owned(), |seq| seq.to_string()),
        stats
            .last_unique_seq
            .map_or_else(|| "-".to_owned(), |seq| seq.to_string()),
        common::fmt_int(stats.seq_gaps),
        stats.per_conn_events,
        stats.per_conn_unique
    );
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

#[cfg(target_os = "linux")]
fn print_redundancy_ingress(connections: usize, stats: Option<IngressStats>) {
    let Some(stats) = stats else {
        println!("bench_redundancy_ingress connections={connections} unavailable");
        return;
    };
    let messages_per_recv_cqe = if stats.recv_data_cqes == 0 {
        0.0
    } else {
        stats.ws_data_events as f64 / stats.recv_data_cqes as f64
    };
    let cqes_per_plain_batch = if stats.plain_recv_batches == 0 {
        0.0
    } else {
        stats.plain_recv_batch_cqes as f64 / stats.plain_recv_batches as f64
    };
    println!(
        "bench_redundancy_ingress connections={} recv_cqes={} recv_bytes={} plaintext_chunks={} ws_data_events={} text={} binary={} rearm={} ring_exhaustions={} plain_batches={} plain_batch_cqes={} cqes_per_plain_batch={:.3} messages_per_recv_cqe={:.3}",
        connections,
        common::fmt_int(stats.recv_data_cqes),
        common::fmt_int(stats.recv_bytes),
        common::fmt_int(stats.plaintext_chunks),
        common::fmt_int(stats.ws_data_events),
        common::fmt_int(stats.ws_text_events),
        common::fmt_int(stats.ws_binary_events),
        common::fmt_int(stats.recv_multishot_rearms),
        common::fmt_int(stats.recv_ring_exhaustions),
        common::fmt_int(stats.plain_recv_batches),
        common::fmt_int(stats.plain_recv_batch_cqes),
        cqes_per_plain_batch,
        messages_per_recv_cqe
    );
}

struct SeqStreamServer {
    addr: SocketAddr,
    join: Option<JoinHandle<io::Result<()>>>,
}

impl SeqStreamServer {
    const fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn join(mut self) -> io::Result<()> {
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        join.join()
            .unwrap_or_else(|_| Err(io::Error::other("local redundancy server panicked")))
    }
}

fn spawn_seq_stream_server(
    frames_per_write: usize,
    server_cpu: Option<usize>,
) -> io::Result<SeqStreamServer> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let addr = listener.local_addr()?;
    let join = thread::spawn(move || run_seq_stream_server(listener, frames_per_write, server_cpu));
    Ok(SeqStreamServer {
        addr,
        join: Some(join),
    })
}

#[allow(clippy::needless_pass_by_value)]
fn run_seq_stream_server(
    listener: TcpListener,
    frames_per_write: usize,
    server_cpu: Option<usize>,
) -> io::Result<()> {
    let _pin = server_cpu.map(|cpu| common::PinGuard::pin("server", cpu));
    let (mut stream, _) = listener.accept()?;
    stream.set_nodelay(true)?;
    common::server_upgrade(&mut stream)?;

    let mut chunk = SeqChunk::new(frames_per_write.max(1));
    let mut next_seq = 1_u64;
    loop {
        chunk.set_start_seq(next_seq);
        next_seq = next_seq.saturating_add(chunk.frames as u64);
        match stream.write_all(&chunk.bytes) {
            Ok(()) => {}
            Err(e) if common::is_expected_disconnect(&e) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

struct SeqChunk {
    bytes: Vec<u8>,
    seq_offsets: Vec<usize>,
    frames: usize,
}

impl SeqChunk {
    fn new(frames: usize) -> Self {
        let seq_offset_in_payload = seq_offset_in_payload();
        let mut bytes = Vec::with_capacity(frames * (BBO_SEQ_TEMPLATE.len() + MAX_HEADER_LEN));
        let mut seq_offsets = Vec::with_capacity(frames);
        let mut header = [0_u8; MAX_HEADER_LEN];
        for _ in 0..frames {
            let header_len = encode_header(
                &mut header,
                true,
                OpCode::Text,
                None,
                BBO_SEQ_TEMPLATE.len() as u64,
            );
            let payload_start = bytes.len() + header_len;
            bytes.extend_from_slice(&header[..header_len]);
            bytes.extend_from_slice(BBO_SEQ_TEMPLATE);
            seq_offsets.push(payload_start + seq_offset_in_payload);
        }
        Self {
            bytes,
            seq_offsets,
            frames,
        }
    }

    fn set_start_seq(&mut self, start_seq: u64) {
        for (index, &offset) in self.seq_offsets.iter().enumerate() {
            write_seq_digits(
                &mut self.bytes[offset..offset + SEQ_WIDTH],
                start_seq.saturating_add(index as u64),
            );
        }
    }
}

fn seq_offset_in_payload() -> usize {
    let marker_start = BBO_SEQ_TEMPLATE
        .windows(SEQ_MARKER.len())
        .position(|window| window == SEQ_MARKER)
        .expect("BBO seq template contains u marker");
    marker_start + SEQ_MARKER.len()
}

fn write_seq_digits(out: &mut [u8], mut seq: u64) {
    assert_eq!(out.len(), SEQ_WIDTH);
    for index in (0..SEQ_WIDTH).rev() {
        out[index] = b'0' + u8::try_from(seq % 10).expect("decimal digit fits u8");
        seq /= 10;
    }
}

fn print_usage() {
    println!(
        "local_redundancy bench\n\
         \n\
         BBO redundant-connection race simulation. Each local connection emits\n\
         the same monotonic Binance-BBO-like sequence stream; the client accepts\n\
         only messages whose u sequence advances the global high-water mark.\n\
         \n\
         Args:\n\
           --connections A,B,C        redundant connection counts to run\n\
           --seconds N                wall-clock run limit, 0 disables time limit\n\
           --messages N               unique-message limit, 0 disables message limit\n\
           --warmup-events N          input events discarded before timing\n\
           --frames-per-write N       server-side WS frames per write\n\
           --buf-size N               talaris io_uring provided buffer slot size\n\
           --buf-entries N            talaris provided buffer entries, power of two\n\
           --sq-entries N             talaris io_uring SQ entries, power of two\n\
           --cq-entries N             talaris io_uring CQ entries, power of two\n\
           --completion-batch N       Pool CQE scratch buffer capacity\n\
           --spin-iters N             talaris spin count; 0 uses blocking pump_data\n\
           --post-progress-spin-iters N  extra spin/drain budget after first progress\n\
           --copy-batch-bytes N       max bytes copied across a plain recv CQE batch; 0 disables\n\
           --user-cpu N               pin benchmark thread\n\
           --server-cpus A,B,C        pin server threads round-robin"
    );
}
