#![allow(
    dead_code,
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::unwrap_used
)]

use std::fs::File;
use std::hint::black_box;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use talaris::connection::IngressStats;
use talaris::observability::DataEventMeta;
use talaris::ws::frame::{MAX_HEADER_LEN, OpCode, encode_header};
use talaris::ws::handshake::compute_accept;

pub const FULL_SAMPLE_BPS: u16 = 10_000;
const BINANCE_BBO_TEXT: &str = "{\"e\":\"bookTicker\",\"u\":400900217,\"s\":\"BNBUSDT\",\"ps\":\"BNBUSDT\",\"E\":1568014460893,\"T\":1568014460891,\"b\":\"25.35190000\",\"B\":\"31.21000000\",\"a\":\"25.36520000\",\"A\":\"40.66000000\",\"st\":1}";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadProfile {
    Binary,
    BinanceBbo,
}

impl PayloadProfile {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Binary => "binary",
            Self::BinanceBbo => "binance-bbo",
        }
    }

    #[must_use]
    pub fn payload_len(self, binary_payload_len: usize) -> usize {
        match self {
            Self::Binary => binary_payload_len.max(1),
            Self::BinanceBbo => BINANCE_BBO_TEXT.len(),
        }
    }
}

impl FromStr for PayloadProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "binary" => Ok(Self::Binary),
            "binance-bbo" | "binance_bbo" => Ok(Self::BinanceBbo),
            other => Err(format!("unknown payload profile {other:?}")),
        }
    }
}

pub fn arg_or<T>(flag: &str, default: T) -> T
where
    T: FromStr,
{
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == flag
            && let Some(value) = args.next().and_then(|s| s.parse::<T>().ok())
        {
            return value;
        }
    }
    default
}

pub fn optional_arg<T>(flag: &str) -> Option<T>
where
    T: FromStr,
{
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == flag {
            return args.next().and_then(|s| s.parse::<T>().ok());
        }
    }
    None
}

pub fn arg_string(flag: &str, default: &str) -> String {
    optional_arg(flag).unwrap_or_else(|| default.to_owned())
}

pub fn arg_list<T>(flag: &str, default: &str) -> Result<Vec<T>, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let raw = arg_string(flag, default);
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        out.push(
            part.parse::<T>()
                .map_err(|e| format!("{flag} has invalid value {part:?}: {e}"))?,
        );
    }
    if out.is_empty() {
        Err(format!("{flag} must contain at least one value"))
    } else {
        Ok(out)
    }
}

pub fn flag_present(flag: &str) -> bool {
    std::env::args().skip(1).any(|arg| arg == flag)
}

pub fn optional_string(flag: &str) -> Option<String> {
    optional_arg(flag)
}

pub fn print_linux_only(name: &str) {
    eprintln!("{name}: skipped - talaris pipeline benches only run on Linux");
}

pub fn validate_power_of_two_u16(name: &str, value: u16) -> Result<(), String> {
    if value > 0 && value.is_power_of_two() {
        Ok(())
    } else {
        Err(format!(
            "{name} must be a non-zero power of two, got {value}"
        ))
    }
}

pub fn validate_power_of_two_u32(name: &str, value: u32) -> Result<(), String> {
    if value > 0 && value.is_power_of_two() {
        Ok(())
    } else {
        Err(format!(
            "{name} must be a non-zero power of two, got {value}"
        ))
    }
}

pub fn validate_sampling_bps(value: u16) -> Result<(), String> {
    if value <= FULL_SAMPLE_BPS {
        Ok(())
    } else {
        Err(format!(
            "--sample-bps must be <= {FULL_SAMPLE_BPS}, got {value}"
        ))
    }
}

pub struct PinGuard {
    label: &'static str,
}

impl PinGuard {
    pub fn pin(label: &'static str, cpu: usize) -> Self {
        if let Err(e) = talaris::proactor::pin_current_thread_to(cpu) {
            eprintln!("[{label}] pin_current_thread_to({cpu}) failed: {e}");
        } else {
            eprintln!("[{label}] pinned to CPU {cpu}");
        }
        Self { label }
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        if let Err(e) = talaris::proactor::unpin_current_thread() {
            eprintln!("[{}] unpin failed: {e}", self.label);
        }
    }
}

pub struct ThreadCpuTimer {
    start: libc::timespec,
}

impl ThreadCpuTimer {
    pub fn start() -> Self {
        Self {
            start: thread_cpu_time(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        let end = thread_cpu_time();
        let sec = end.tv_sec - self.start.tv_sec;
        let nsec = end.tv_nsec - self.start.tv_nsec;
        let (sec, nsec) = if nsec >= 0 {
            (sec, nsec)
        } else {
            (sec - 1, nsec + 1_000_000_000)
        };
        Duration::new(
            u64::try_from(sec).expect("thread CPU clock is monotonic"),
            u32::try_from(nsec).expect("normalized nsec fits u32"),
        )
    }
}

fn thread_cpu_time() -> libc::timespec {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts points to valid writable memory.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &raw mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    ts
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromWindow {
    Interval,
    Cumulative,
}

impl PromWindow {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Interval => "interval",
            Self::Cumulative => "cumulative",
        }
    }
}

pub struct PromWriter {
    out: Box<dyn Write>,
}

impl PromWriter {
    pub fn from_arg(path: Option<String>) -> io::Result<Option<Self>> {
        let Some(path) = path else {
            return Ok(None);
        };
        let out: Box<dyn Write> = if path == "-" {
            Box::new(io::stdout())
        } else {
            Box::new(File::create(path)?)
        };
        Ok(Some(Self { out }))
    }

    pub fn write_snapshot(
        &mut self,
        bench: &str,
        window: PromWindow,
        elapsed: Duration,
        metrics: &str,
    ) -> io::Result<()> {
        writeln!(
            self.out,
            "# talaris_bench_snapshot bench=\"{bench}\" window=\"{}\" elapsed_ms=\"{:.3}\"",
            window.as_str(),
            elapsed_ms(elapsed)
        )?;
        self.out.write_all(metrics.as_bytes())?;
        if !metrics.ends_with('\n') {
            writeln!(self.out)?;
        }
        writeln!(self.out)?;
        self.out.flush()
    }
}

#[derive(Debug)]
pub struct MetricsSchedule {
    interval: Duration,
    next: Option<Instant>,
}

impl MetricsSchedule {
    pub fn new(started: Instant, interval: Duration) -> Self {
        let next = if interval.is_zero() {
            None
        } else {
            Some(started + interval)
        };
        Self { interval, next }
    }

    pub fn write_due(
        &mut self,
        writer: &mut Option<PromWriter>,
        bench: &str,
        pool: &mut talaris::Pool,
        started: Instant,
    ) -> io::Result<()> {
        let Some(writer) = writer.as_mut() else {
            return Ok(());
        };
        let Some(next) = self.next else {
            return Ok(());
        };
        let now = Instant::now();
        if now >= next {
            let metrics = pool.prometheus_metrics_and_reset_interval();
            writer.write_snapshot(
                bench,
                PromWindow::Interval,
                now.duration_since(started),
                &metrics,
            )?;
            self.next = Some(now + self.interval);
        }
        Ok(())
    }

    pub fn write_final(
        writer: &mut Option<PromWriter>,
        bench: &str,
        pool: &mut talaris::Pool,
        elapsed: Duration,
    ) -> io::Result<()> {
        let Some(writer) = writer.as_mut() else {
            return Ok(());
        };
        let interval = pool.prometheus_metrics_and_reset_interval();
        writer.write_snapshot(bench, PromWindow::Interval, elapsed, &interval)?;
        let cumulative = pool.prometheus_metrics();
        writer.write_snapshot(bench, PromWindow::Cumulative, elapsed, &cumulative)
    }
}

pub struct LocalStreamServer {
    addr: SocketAddr,
    join: Option<JoinHandle<io::Result<()>>>,
}

impl LocalStreamServer {
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn join(mut self) -> io::Result<()> {
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        join.join()
            .unwrap_or_else(|_| Err(io::Error::other("local websocket server panicked")))
    }
}

pub fn spawn_local_stream_server(
    payload_len: usize,
    frames_per_write: usize,
    server_cpu: Option<usize>,
) -> io::Result<LocalStreamServer> {
    spawn_local_stream_server_with_profile(
        PayloadProfile::Binary,
        payload_len,
        frames_per_write,
        server_cpu,
    )
}

pub fn spawn_local_stream_server_with_profile(
    profile: PayloadProfile,
    payload_len: usize,
    frames_per_write: usize,
    server_cpu: Option<usize>,
) -> io::Result<LocalStreamServer> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let addr = listener.local_addr()?;
    let chunk = encode_profile_frames(profile, payload_len.max(1), frames_per_write.max(1));
    let join = thread::spawn(move || run_stream_server(listener, chunk, server_cpu));
    Ok(LocalStreamServer {
        addr,
        join: Some(join),
    })
}

#[allow(clippy::needless_pass_by_value)]
fn run_stream_server(
    listener: TcpListener,
    chunk: Vec<u8>,
    server_cpu: Option<usize>,
) -> io::Result<()> {
    let _pin = server_cpu.map(|cpu| PinGuard::pin("server", cpu));
    let (mut stream, _) = listener.accept()?;
    stream.set_nodelay(true)?;
    server_upgrade(&mut stream)?;
    loop {
        match stream.write_all(&chunk) {
            Ok(()) => {}
            Err(e) if is_expected_disconnect(&e) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

pub fn is_expected_disconnect(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::NotConnected
    )
}

pub fn payload(payload_len: usize) -> Vec<u8> {
    (0..payload_len)
        .map(|i| {
            u8::try_from(i % 251)
                .expect("modulo result fits u8")
                .wrapping_mul(31)
                .wrapping_add(7)
        })
        .collect()
}

fn encode_binary_frames(payload_len: usize, frames: usize) -> Vec<u8> {
    let payload = payload(payload_len);
    encode_frames(OpCode::Binary, &payload, frames)
}

fn encode_profile_frames(profile: PayloadProfile, payload_len: usize, frames: usize) -> Vec<u8> {
    match profile {
        PayloadProfile::Binary => encode_binary_frames(payload_len, frames),
        PayloadProfile::BinanceBbo => {
            encode_frames(OpCode::Text, BINANCE_BBO_TEXT.as_bytes(), frames)
        }
    }
}

fn encode_frames(opcode: OpCode, payload: &[u8], frames: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(frames * (payload.len() + MAX_HEADER_LEN));
    let mut header = [0_u8; MAX_HEADER_LEN];
    for _ in 0..frames {
        let n = encode_header(&mut header, true, opcode, None, payload.len() as u64);
        out.extend_from_slice(&header[..n]);
        out.extend_from_slice(payload);
    }
    out
}

fn extract_ws_key(request: &[u8]) -> io::Result<String> {
    let request =
        std::str::from_utf8(request).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    for line in request.lines() {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("Sec-WebSocket-Key")
        {
            return Ok(value.trim().to_owned());
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Sec-WebSocket-Key missing from request",
    ))
}

fn upgrade_response_for_request(request: &[u8]) -> io::Result<Vec<u8>> {
    let key = extract_ws_key(request)?;
    let accept = compute_accept(&key);
    Ok(format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    )
    .into_bytes())
}

pub fn server_upgrade(stream: &mut TcpStream) -> io::Result<()> {
    let mut buf = [0_u8; 4096];
    let mut req = Vec::with_capacity(1024);
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed during websocket upgrade",
            ));
        }
        req.extend_from_slice(&buf[..n]);
        if req.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if req.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "websocket upgrade request too large",
            ));
        }
    }
    let response = upgrade_response_for_request(&req)?;
    stream.write_all(&response)
}

#[derive(Debug, Default)]
pub struct MessageStats {
    pub messages: u64,
    pub text_messages: u64,
    pub binary_messages: u64,
    pub bytes: u64,
    pub sampled_messages: u64,
    pub chunk_first_messages: u64,
    pub chunk_queued_messages: u64,
    pub max_chunk_message_index: u16,
    pub first_recv_sequence: Option<u64>,
    pub last_recv_sequence: Option<u64>,
    checksum: u64,
}

impl MessageStats {
    pub fn record_text(&mut self, payload: &str) {
        self.text_messages = self.text_messages.saturating_add(1);
        self.record_payload(payload.as_bytes());
    }

    pub fn record_binary(&mut self, payload: &[u8]) {
        self.binary_messages = self.binary_messages.saturating_add(1);
        self.record_payload(payload);
    }

    pub fn record_meta(&mut self, meta: DataEventMeta) {
        if meta.sampled {
            self.sampled_messages = self.sampled_messages.saturating_add(1);
        }
        if meta.chunk_message_index == 0 {
            self.chunk_first_messages = self.chunk_first_messages.saturating_add(1);
        } else {
            self.chunk_queued_messages = self.chunk_queued_messages.saturating_add(1);
        }
        self.max_chunk_message_index = self.max_chunk_message_index.max(meta.chunk_message_index);
        self.first_recv_sequence.get_or_insert(meta.recv_sequence);
        self.last_recv_sequence = Some(meta.recv_sequence);
    }

    fn record_payload(&mut self, payload: &[u8]) {
        self.messages = self.messages.saturating_add(1);
        self.bytes = self.bytes.saturating_add(payload.len() as u64);
        self.checksum = mix_checksum(self.checksum, payload);
    }

    pub const fn checksum(&self) -> u64 {
        self.checksum
    }

    pub fn merge_from(&mut self, other: &Self) {
        self.messages = self.messages.saturating_add(other.messages);
        self.text_messages = self.text_messages.saturating_add(other.text_messages);
        self.binary_messages = self.binary_messages.saturating_add(other.binary_messages);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.sampled_messages = self.sampled_messages.saturating_add(other.sampled_messages);
        self.chunk_first_messages = self
            .chunk_first_messages
            .saturating_add(other.chunk_first_messages);
        self.chunk_queued_messages = self
            .chunk_queued_messages
            .saturating_add(other.chunk_queued_messages);
        self.max_chunk_message_index = self
            .max_chunk_message_index
            .max(other.max_chunk_message_index);
        self.first_recv_sequence = match (self.first_recv_sequence, other.first_recv_sequence) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        self.last_recv_sequence = match (self.last_recv_sequence, other.last_recv_sequence) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        self.checksum ^= other.checksum.rotate_left(17);
    }
}

fn mix_checksum(mut checksum: u64, payload: &[u8]) -> u64 {
    checksum = checksum.rotate_left(5) ^ payload.len() as u64;
    if let Some(first) = payload.first() {
        checksum = checksum.wrapping_add(u64::from(*first));
    }
    if let Some(last) = payload.last() {
        checksum = checksum.wrapping_mul(16_777_619) ^ u64::from(*last);
    }
    checksum
}

pub fn fmt_int(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

pub fn elapsed_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

pub fn messages_per_sec(messages: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    messages as f64 / elapsed.as_secs_f64()
}

pub fn mib_per_sec(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

pub fn ns_per_message(cpu: Duration, messages: u64) -> u64 {
    if messages == 0 {
        return 0;
    }
    u64::try_from(cpu.as_nanos() / u128::from(messages)).unwrap_or(u64::MAX)
}

pub fn cpu_pct(cpu: Duration, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    100.0 * cpu.as_secs_f64() / elapsed.as_secs_f64()
}

pub fn print_result(
    bench: &str,
    mode: &str,
    stats: &MessageStats,
    elapsed: Duration,
    cpu: Duration,
) {
    println!(
        "bench_result bench={bench} mode={mode} messages={} text={} binary={} bytes={} elapsed_ms={:.3} cpu_ms={:.3} cpu_pct={:.1} msg_s={:.0} mib_s={:.3} cpu_ns_msg={} checksum={}",
        fmt_int(stats.messages),
        fmt_int(stats.text_messages),
        fmt_int(stats.binary_messages),
        fmt_int(stats.bytes),
        elapsed_ms(elapsed),
        elapsed_ms(cpu),
        cpu_pct(cpu, elapsed),
        messages_per_sec(stats.messages, elapsed),
        mib_per_sec(stats.bytes, elapsed),
        ns_per_message(cpu, stats.messages),
        black_box(stats.checksum())
    );
}

pub fn print_marked_summary(stats: &MessageStats) {
    println!(
        "bench_marked messages={} sampled={} chunk_first={} chunk_queued={} max_chunk_message_index={} recv_sequence={}..{}",
        fmt_int(stats.messages),
        fmt_int(stats.sampled_messages),
        fmt_int(stats.chunk_first_messages),
        fmt_int(stats.chunk_queued_messages),
        stats.max_chunk_message_index,
        stats
            .first_recv_sequence
            .map_or_else(|| "-".to_owned(), |v| v.to_string()),
        stats
            .last_recv_sequence
            .map_or_else(|| "-".to_owned(), |v| v.to_string())
    );
}

pub fn print_ingress_stats(handle: talaris::ConnHandle, stats: Option<IngressStats>) {
    let Some(stats) = stats else {
        println!("bench_ingress conn_id={} unavailable", handle.as_u32());
        return;
    };
    let messages_per_recv_cqe = if stats.recv_data_cqes == 0 {
        0.0
    } else {
        stats.ws_data_events as f64 / stats.recv_data_cqes as f64
    };
    let bytes_per_recv_cqe = if stats.recv_data_cqes == 0 {
        0.0
    } else {
        stats.recv_bytes as f64 / stats.recv_data_cqes as f64
    };
    let cqes_per_plain_batch = if stats.plain_recv_batches == 0 {
        0.0
    } else {
        stats.plain_recv_batch_cqes as f64 / stats.plain_recv_batches as f64
    };
    println!(
        "bench_ingress conn_id={} recv_cqes={} recv_bytes={} plaintext_chunks={} plaintext_bytes={} ws_data_drains={} ws_data_drain_skips={} ws_data_events={} text={} binary={} rearm={} ring_exhaustions={} plain_batches={} plain_batch_cqes={} plain_copied_batches={} plain_copied_bytes={} cqes_per_plain_batch={:.3} messages_per_recv_cqe={:.3} bytes_per_recv_cqe={:.1}",
        handle.as_u32(),
        fmt_int(stats.recv_data_cqes),
        fmt_int(stats.recv_bytes),
        fmt_int(stats.plaintext_chunks),
        fmt_int(stats.plaintext_bytes),
        fmt_int(stats.ws_data_drains),
        fmt_int(stats.ws_data_drain_skips),
        fmt_int(stats.ws_data_events),
        fmt_int(stats.ws_text_events),
        fmt_int(stats.ws_binary_events),
        fmt_int(stats.recv_multishot_rearms),
        fmt_int(stats.recv_ring_exhaustions),
        fmt_int(stats.plain_recv_batches),
        fmt_int(stats.plain_recv_batch_cqes),
        fmt_int(stats.plain_recv_copied_batches),
        fmt_int(stats.plain_recv_copied_bytes),
        cqes_per_plain_batch,
        messages_per_recv_cqe,
        bytes_per_recv_cqe
    );
}

#[must_use]
pub const fn ingress_stats_delta(before: IngressStats, after: IngressStats) -> IngressStats {
    IngressStats {
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
