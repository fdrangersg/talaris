#![allow(
    dead_code,
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::unwrap_used
)]

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str::FromStr;
use std::thread;
use std::time::{Duration, Instant};

use talaris::ws::frame::{MAX_HEADER_LEN, OpCode, encode_header, parse_header};
use talaris::ws::handshake::compute_accept;
use talaris::ws::mask::mask_inplace;
use talaris::ws::{Event, WsClient, WsConfig};

pub fn arg_or<T>(flag: &str, default: T) -> T
where
    T: FromStr,
{
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == flag {
            if let Some(value) = args.next().and_then(|s| s.parse::<T>().ok()) {
                return value;
            }
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

pub fn flag_present(flag: &str) -> bool {
    std::env::args().skip(1).any(|arg| arg == flag)
}

pub fn parse_usize_list(flag: &str, default: &str) -> Vec<usize> {
    let raw: String = arg_or(flag, default.to_owned());
    raw.split(',')
        .filter_map(|part| part.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .collect()
}

pub fn print_linux_only(name: &str) {
    eprintln!("{name}: skipped - talaris benches only run on Linux");
}

pub struct PinGuard {
    label: &'static str,
}

impl PinGuard {
    pub fn pin(label: &'static str, cpu: usize) -> Self {
        if let Err(e) = talaris::proactor::pin_current_thread_to(cpu) {
            eprintln!("[{label}] pin_current_thread_to({cpu}) failed: {e}");
        } else {
            eprintln!("[{label}] user thread -> CPU {cpu}");
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
        if nsec >= 0 {
            Duration::new(sec as u64, nsec as u32)
        } else {
            Duration::new((sec - 1) as u64, (nsec + 1_000_000_000) as u32)
        }
    }
}

fn thread_cpu_time() -> libc::timespec {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts points to valid writable memory.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    ts
}

pub fn ns_per_frame(cpu: Duration, frames: u64) -> u64 {
    if frames == 0 {
        return 0;
    }
    (cpu.as_nanos() / u128::from(frames)) as u64
}

pub fn cpu_pct(cpu: Duration, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    100.0 * cpu.as_secs_f64() / elapsed.as_secs_f64()
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

pub fn frames_per_sec(frames: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    frames as f64 / elapsed.as_secs_f64()
}

pub fn mib_per_sec(bytes: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

pub fn payload(payload_len: usize) -> Vec<u8> {
    (0..payload_len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
        .collect()
}

pub fn encode_binary_frames(payload_len: usize, frames: usize) -> Vec<u8> {
    let payload = payload(payload_len);
    let mut out = Vec::with_capacity(frames * (payload_len + MAX_HEADER_LEN));
    let mut header = [0_u8; MAX_HEADER_LEN];
    for _ in 0..frames {
        let n = encode_header(&mut header, true, OpCode::Binary, None, payload_len as u64);
        out.extend_from_slice(&header[..n]);
        out.extend_from_slice(&payload);
    }
    out
}

pub fn encode_text_frames(payload: &str, frames: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(frames * (payload.len() + MAX_HEADER_LEN));
    let mut header = [0_u8; MAX_HEADER_LEN];
    for _ in 0..frames {
        let n = encode_header(&mut header, true, OpCode::Text, None, payload.len() as u64);
        out.extend_from_slice(&header[..n]);
        out.extend_from_slice(payload.as_bytes());
    }
    out
}

pub fn parse_wire_frames(mut wire: &[u8]) -> (u64, u64) {
    let mut frames = 0_u64;
    let mut bytes = 0_u64;
    while !wire.is_empty() {
        let (header, header_len) = parse_header(wire)
            .expect("valid frame")
            .expect("full header");
        let frame_len = header_len + header.payload_len as usize;
        bytes += header.payload_len;
        frames += 1;
        wire = &wire[frame_len..];
    }
    (frames, bytes)
}

pub fn extract_ws_key(request: &[u8]) -> String {
    let request = std::str::from_utf8(request).expect("upgrade request is utf-8");
    for line in request.lines() {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("Sec-WebSocket-Key")
        {
            return value.trim().to_owned();
        }
    }
    panic!("Sec-WebSocket-Key missing from request");
}

pub fn upgrade_response_for_request(request: &[u8]) -> Vec<u8> {
    let key = extract_ws_key(request);
    let accept = compute_accept(&key);
    format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    )
    .into_bytes()
}

pub fn server_upgrade(stream: &mut TcpStream) -> io::Result<()> {
    let mut buf = [0_u8; 4096];
    let mut req = Vec::new();
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
    }
    let response = upgrade_response_for_request(&req);
    stream.write_all(&response)?;
    Ok(())
}

pub fn open_ws_client(recv_capacity: usize, max_payload_len: usize) -> WsClient {
    let max_payload_len = max_payload_len.max(1);
    let mut ws = WsClient::new_client(
        WsConfig::new("localhost", "/")
            .with_max_message_size(max_payload_len)
            .with_max_frame_payload(max_payload_len as u64)
            .with_initial_buffer_capacities(recv_capacity.max(1), max_payload_len, 4096),
    )
    .expect("ws client");
    ws.begin_handshake().expect("begin handshake");
    let request = ws.pending_tx().to_vec();
    ws.ack_tx(request.len());
    let response = upgrade_response_for_request(&request);
    ws.feed_recv(&response);
    match ws.poll_event() {
        Some(Ok(Event::HandshakeComplete)) => ws,
        other => panic!("expected HandshakeComplete, got {other:?}"),
    }
}

pub fn spawn_stream_server(
    listener: TcpListener,
    chunk: Vec<u8>,
    server_cpu: Option<usize>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let _pin = server_cpu.map(|cpu| PinGuard::pin("server", cpu));
        let (mut stream, _) = listener.accept().expect("accept");
        stream.set_nodelay(true).expect("nodelay");
        server_upgrade(&mut stream).expect("upgrade");
        loop {
            if stream.write_all(&chunk).is_err() {
                return;
            }
        }
    })
}

pub fn spawn_echo_server(
    listener: TcpListener,
    messages: usize,
    server_cpu: Option<usize>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let _pin = server_cpu.map(|cpu| PinGuard::pin("server", cpu));
        let (mut stream, _) = listener.accept().expect("accept");
        stream.set_nodelay(true).expect("nodelay");
        server_upgrade(&mut stream).expect("upgrade");
        for _ in 0..messages {
            let Some((opcode, payload)) = read_client_frame(&mut stream).expect("read frame")
            else {
                return;
            };
            let mut header = [0_u8; MAX_HEADER_LEN];
            let n = encode_header(&mut header, true, opcode, None, payload.len() as u64);
            stream.write_all(&header[..n]).expect("write header");
            stream.write_all(&payload).expect("write payload");
        }
    })
}

fn read_client_frame(stream: &mut TcpStream) -> io::Result<Option<(OpCode, Vec<u8>)>> {
    let mut header = [0_u8; MAX_HEADER_LEN];
    let mut filled = 0_usize;
    let (frame, header_len) = loop {
        if filled == header.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "websocket header too long",
            ));
        }
        let n = stream.read(&mut header[filled..filled + 1])?;
        if n == 0 {
            return Ok(None);
        }
        filled += n;
        if let Some(parsed) = parse_header(&header[..filled]).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("bad frame header: {e}"))
        })? {
            break parsed;
        }
    };
    let mut payload = vec![0_u8; frame.payload_len as usize];
    stream.read_exact(&mut payload)?;
    if let Some(mask) = frame.mask {
        mask_inplace(&mut payload, mask);
    }
    if frame.opcode == OpCode::Close {
        return Ok(None);
    }
    let _ = header_len;
    Ok(Some((frame.opcode, payload)))
}

pub fn sampled_hist() -> hdrhistogram::Histogram<u64> {
    hdrhistogram::Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("histogram")
}

pub fn maybe_record_arrival(
    hist: &mut hdrhistogram::Histogram<u64>,
    last: &mut Option<Instant>,
    sample_every: u64,
    frame: u64,
) {
    if sample_every == 0 || frame % sample_every != 0 {
        return;
    }
    let now = Instant::now();
    if let Some(prev) = last.replace(now) {
        hist.record(now.duration_since(prev).as_nanos().max(1) as u64)
            .ok();
    }
}

pub fn print_hist(label: &str, hist: &hdrhistogram::Histogram<u64>) {
    if hist.len() == 0 {
        println!("{label}: no samples");
        return;
    }
    println!(
        "{label:<14} p50={}ns p99={}ns p999={}ns max={}ns samples={}",
        fmt_int(hist.value_at_quantile(0.50)),
        fmt_int(hist.value_at_quantile(0.99)),
        fmt_int(hist.value_at_quantile(0.999)),
        fmt_int(hist.max()),
        fmt_int(hist.len())
    );
}
