// 多个 bench 共享代码 —— histogram、CLI、StopMode、PinGuard、单 OS 线程
// tokio WS stream server、tokio 侧手卷 WS recv loop（共用 talaris parse_header）。
//
// 不是 Cargo bench target（住在子目录里）；每个 bench 文件用
// `#[path = "common/mod.rs"] mod common;` 引进来。

#![allow(
    dead_code, // 不同 bench 用到的子集不同；让所有 helper 都共存
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::unwrap_used,
    clippy::doc_markdown,
    clippy::module_name_repetitions,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::semicolon_if_nothing_returned,
    clippy::needless_pass_by_value
)]

use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

// ─── HdrHistogram ──────────────────────────────────────────────────────

type Metric = (&'static str, fn(&Histogram<u64>) -> u64);

pub fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("hist")
}

pub fn record_ns(hist: &mut Histogram<u64>, dt: std::time::Duration) {
    let ns = u64::try_from(dt.as_nanos().min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
    hist.record(ns.max(1)).ok();
}

pub fn print_hist(label: &str, h: &Histogram<u64>) {
    println!(
        "{:<24}  mean={:>10}  p50={:>10}  p99={:>10}  p99.9={:>10}  max={:>10}  n={}",
        label,
        ns(h.mean() as u64),
        ns(h.value_at_quantile(0.50)),
        ns(h.value_at_quantile(0.99)),
        ns(h.value_at_quantile(0.999)),
        ns(h.max()),
        h.len(),
    );
}

pub fn print_comparison(rows: &[(&str, &Histogram<u64>)]) {
    if rows.is_empty() {
        return;
    }
    let cols: [Metric; 5] = [
        ("mean", |h| h.mean() as u64),
        ("p50", |h| h.value_at_quantile(0.50)),
        ("p99", |h| h.value_at_quantile(0.99)),
        ("p99.9", |h| h.value_at_quantile(0.999)),
        ("max", |h| h.max()),
    ];
    print!("{:<10}", "metric");
    for (label, _) in rows {
        print!(" │ {label:>18}");
    }
    println!();
    print!("{}", "─".repeat(10));
    for _ in rows {
        print!("─┼─{}", "─".repeat(18));
    }
    println!();
    for (col_name, extract) in cols {
        print!("{col_name:<10}");
        for (_, h) in rows {
            print!(" │ {:>18}", ns(extract(h)));
        }
        println!();
    }
}

pub fn fmt_int(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

pub fn ns(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3 + 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out.push_str(" ns");
    out
}

pub fn ns_per_frame(cpu: Duration, frames: u64) -> u64 {
    if frames == 0 {
        return 0;
    }
    let per = cpu.as_nanos() / u128::from(frames);
    u64::try_from(per.min(u128::from(u64::MAX))).unwrap_or(u64::MAX)
}

pub fn cpu_pct(cpu: Duration, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    100.0 * cpu.as_secs_f64() / elapsed.as_secs_f64()
}

#[cfg(target_os = "linux")]
pub fn thread_cpu_time() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &raw mut ts) };
    if rc != 0 {
        return Duration::ZERO;
    }
    let secs = u64::try_from(ts.tv_sec).unwrap_or(0);
    let nanos = u32::try_from(ts.tv_nsec).unwrap_or(0);
    Duration::new(secs, nanos)
}

#[cfg(not(target_os = "linux"))]
pub const fn thread_cpu_time() -> Duration {
    Duration::ZERO
}

#[derive(Debug, Clone, Copy)]
pub struct ThreadCpuTimer {
    start: Duration,
}

impl ThreadCpuTimer {
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn start() -> Self {
        Self {
            start: thread_cpu_time(),
        }
    }

    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn elapsed(self) -> Duration {
        thread_cpu_time().saturating_sub(self.start)
    }
}

/// 把每帧的 Instant 序列折算成 inter-arrival histogram。第 0 帧没有"上一帧"
/// 所以不进 hist；最少要 2 帧才有 1 个间隔。
pub fn inter_arrival_hist(arrivals: &[Instant]) -> Histogram<u64> {
    let mut h = new_hist();
    for w in arrivals.windows(2) {
        record_ns(&mut h, w[1] - w[0]);
    }
    h
}

pub fn sampled_arrivals(stop: StopMode, sample_every: u64) -> Vec<Instant> {
    if sample_every == 0 {
        return Vec::new();
    }
    let divisor = usize::try_from(sample_every).unwrap_or(usize::MAX);
    Vec::with_capacity(stop.cap_hint().div_ceil(divisor))
}

pub fn record_sampled_arrival(arrivals: &mut Vec<Instant>, frame_count: u64, sample_every: u64) {
    if sample_every > 0 && frame_count.is_multiple_of(sample_every) {
        arrivals.push(Instant::now());
    }
}

// ─── CLI ─────────────────────────────────────────────────────────────────

pub fn arg_opt<T: std::str::FromStr>(key: &str) -> Option<T> {
    let mut it = std::env::args().skip(1);
    let mut found: Option<T> = None;
    while let Some(a) = it.next() {
        if a == key
            && let Some(v) = it.next().and_then(|s| s.parse().ok())
        {
            found = Some(v);
        }
    }
    found
}

pub fn arg_or<T: std::str::FromStr + Clone>(key: &str, default: T) -> T {
    arg_opt(key).unwrap_or(default)
}

// ─── Stop condition ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub enum StopMode {
    Frames(u64),
    Seconds(f64),
}

impl StopMode {
    pub fn from_args(default_frames: u64) -> Self {
        let secs: Option<f64> = arg_opt("--seconds");
        let frames: Option<u64> = arg_opt("--frames");
        match (secs, frames) {
            (Some(_), Some(_)) => {
                eprintln!("[bench] --seconds and --frames both given; using --seconds");
                Self::Seconds(secs.unwrap())
            }
            (Some(s), None) => Self::Seconds(s),
            (None, Some(n)) => Self::Frames(n),
            (None, None) => Self::Frames(default_frames),
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Self::Frames(n) => format!("frames={n}"),
            Self::Seconds(s) => format!("seconds={s}"),
        }
    }

    /// Predicate for the recv loop. `count` = frames seen so far, `started` =
    /// instant the measure phase began.
    pub fn keep_going(&self, count: u64, started: Instant) -> bool {
        match *self {
            Self::Frames(n) => count < n,
            Self::Seconds(s) => started.elapsed().as_secs_f64() < s,
        }
    }

    /// 给 Vec::with_capacity 用的 sensible upper bound。
    pub fn cap_hint(&self) -> usize {
        match *self {
            Self::Frames(n) => (n as usize).min(20_000_000),
            // seconds mode: 估个上限避免一直 grow；2M 大概够 talaris 跑 2 秒 1M/s
            Self::Seconds(_) => 2_000_000,
        }
    }
}

// ─── PinGuard ───────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct PinGuard {
    saved: libc::cpu_set_t,
}

#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
pub struct PinGuard;

#[cfg(target_os = "linux")]
impl PinGuard {
    #[must_use]
    pub fn pin(label: &str, cpu: usize) -> Self {
        let mut saved: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &raw mut saved)
        };
        if rc != 0 {
            eprintln!(
                "[{label}] sched_getaffinity failed (errno {}); will not restore on drop",
                std::io::Error::last_os_error()
            );
        }
        if let Err(e) = talaris::proactor::pin_current_thread_to(cpu) {
            eprintln!("[{label}] pin to CPU {cpu} failed: {e}; continuing unpinned");
        }
        Self { saved }
    }
}

#[cfg(target_os = "linux")]
impl Drop for PinGuard {
    fn drop(&mut self) {
        let rc = unsafe {
            libc::sched_setaffinity(
                0,
                std::mem::size_of::<libc::cpu_set_t>(),
                &raw const self.saved,
            )
        };
        if rc != 0 {
            eprintln!(
                "[PinGuard] restore affinity failed: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
impl PinGuard {
    #[must_use]
    pub const fn pin(_label: &str, _cpu: usize) -> Self {
        Self
    }
}

// ─── Pre-encoded WS Binary stream chunk ────────────────────────────────
//
// Server hot loop 只 write_all 这块 buf 一遍又一遍。掏空 server-side framing
// 成本（已经一次性编完 chunk），server 永远不会成为 client 测量的瓶颈。

/// 编码 `n_frames` 个 WS Binary 帧（server→client 不 mask）成一块连续 byte
/// buffer。payload 用 0..255 循环字节填，方便 client 端校验。
pub fn pre_encode_ws_binary_chunk(payload_size: usize, n_frames: usize) -> Vec<u8> {
    use talaris::ws::OpCode;
    let mut payload = vec![0_u8; payload_size];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 256) as u8;
    }
    pre_encode_ws_chunk(OpCode::Binary, payload.as_slice(), n_frames)
}

pub fn pre_encode_ws_text_chunk(payload: &[u8], n_frames: usize) -> Vec<u8> {
    use talaris::ws::OpCode;
    debug_assert!(std::str::from_utf8(payload).is_ok());
    pre_encode_ws_chunk(OpCode::Text, payload, n_frames)
}

fn pre_encode_ws_chunk(opcode: talaris::ws::OpCode, payload: &[u8], n_frames: usize) -> Vec<u8> {
    use talaris::ws::frame::{MAX_HEADER_LEN, encode_header};

    // 每帧 header 大小：payload ≤125 是 2B，≤65535 是 4B，否则 10B。
    let payload_size = payload.len();
    let est_hdr = if payload_size <= 125 {
        2
    } else if payload_size <= 0xFFFF {
        4
    } else {
        10
    };
    let mut buf = Vec::with_capacity(n_frames * (est_hdr + payload_size));
    let mut hdr = [0_u8; MAX_HEADER_LEN];
    for _ in 0..n_frames {
        let hn = encode_header(&mut hdr, true, opcode, None, payload_size as u64);
        buf.extend_from_slice(&hdr[..hn]);
        buf.extend_from_slice(payload);
    }
    buf
}

pub fn json_quote_payload(target_size: usize) -> Vec<u8> {
    let target_size = target_size.max(96);
    let mut s = String::from(
        r#"{"stream":"book","symbol":"BTC-PERP","ts":1780000000123456789,"bid":68123.5,"ask":68124.0,"bid_sz":12.5,"ask_sz":9.25,"seq":123456789"#,
    );
    if s.len() + 2 < target_size {
        s.push_str(r#","pad":""#);
        let pad_len = target_size.saturating_sub(s.len() + 1);
        for i in 0..pad_len {
            let c = b'a' + u8::try_from(i % 26).unwrap_or(0);
            s.push(char::from(c));
        }
        s.push('"');
    }
    s.push('}');
    s.into_bytes()
}

pub fn decode_json_value(payload: &str) -> u64 {
    let value: serde_json::Value = serde_json::from_str(payload).expect("bench JSON payload");
    let seq = value
        .get("seq")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let ts = value
        .get("ts")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    seq ^ ts
}

/// 给 server 推荐的 chunk size：~64 KiB（一次 write_all 大致填满 TCP send buffer
/// 但不溢出，使 server 端 syscall 次数最少）。换算到对应 payload 帧数。
pub fn frames_per_chunk(payload_size: usize) -> usize {
    const TARGET: usize = 64 * 1024;
    let per_frame = payload_size + if payload_size <= 125 { 2 } else { 4 };
    (TARGET / per_frame).max(1)
}

// ─── Loopback TLS fixture ──────────────────────────────────────────────

#[cfg(target_os = "linux")]
const LOCALHOST_CERT_DER_B64: &str = "MIIBtzCCAV2gAwIBAgIULMXcdAUoSgffJOqrkNy8ys85OpMwCgYIKoZIzj0EAwIwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDYwMTEyMTUzN1oXDTM2MDUyOTEyMTUzN1owFDESMBAGA1UEAwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEtzVTB0Xmwq2F+UtvdKK0+RCzUlKs6fltm+4RpWStXe+LrAqKTDE4WhbgA0MHaAZb1MhLiQWZZz8a05MSJV2ENaOBjDCBiTAdBgNVHQ4EFgQU+CPA+u+43y2DhURcdumGYZl1Z68wHwYDVR0jBBgwFoAU+CPA+u+43y2DhURcdumGYZl1Z68wFAYDVR0RBA0wC4IJbG9jYWxob3N0MAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMBMAoGCCqGSM49BAMCA0gAMEUCIHLdUaBOlNSKe/D7jAhqJrpZS7BUp3vGE0OkKTquKUvjAiEAzYFtBs9Ja2MZRwJ61W398M0zloqd7PxIPxvgaNDJu3k=";

#[cfg(target_os = "linux")]
const LOCALHOST_KEY_DER_B64: &str = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgqDeYiIpjp/JVnlTcD7dCux2AXD753/yHKLA1ACHhgCChRANCAAS3NVMHRebCrYX5S290orT5ELNSUqzp+W2b7hGlZK1d74usCopMMThaFuADQwdoBlvUyEuJBZlnPxrTkxIlXYQ1";

#[cfg(target_os = "linux")]
fn local_tls_cert() -> rustls::pki_types::CertificateDer<'static> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(LOCALHOST_CERT_DER_B64)
        .expect("decode localhost cert")
        .into()
}

#[cfg(target_os = "linux")]
pub fn local_tls_client_config() -> std::sync::Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(local_tls_cert()).expect("add localhost cert");
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    std::sync::Arc::new(config)
}

#[cfg(target_os = "linux")]
fn local_tls_server_config() -> std::sync::Arc<rustls::ServerConfig> {
    use base64::Engine as _;
    let key = base64::engine::general_purpose::STANDARD
        .decode(LOCALHOST_KEY_DER_B64)
        .expect("decode localhost key");
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key.into());
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![local_tls_cert()], key)
        .expect("localhost server cert");
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    std::sync::Arc::new(config)
}

// ─── Server：tokio current_thread 单 OS 线程 N 个 stream session ─────────
//
// 起一个 OS 线程跑 tokio current_thread runtime。pin 到 isolated CPU。N 条 WS
// stream session 共用这一条线程的 epoll/io_uring。server 不是 bench 的测量对
// 象，但必须是 client 之外的稳定推流源——单线程 + pin 让 server 永远只占 1 个
// CPU，跨 variant 一致。
//
// 每个 session：accept → WS upgrade → loop write_all(chunk_buf) → client 关
// 连接时 write_all 返 EPIPE/ECONNRESET → session 退。

#[cfg(target_os = "linux")]
pub fn spawn_ws_stream_server(
    std_listener: std::net::TcpListener,
    n_conns: u32,
    chunk_buf: std::sync::Arc<Vec<u8>>,
    cpu: Option<usize>,
) -> std::thread::JoinHandle<()> {
    std_listener.set_nonblocking(true).expect("set_nonblocking");

    std::thread::Builder::new()
        .name("ws-stream-srv".into())
        .spawn(move || {
            let _g = cpu.map(|c| PinGuard::pin("ws-stream-srv", c));
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .expect("rt");
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(std_listener)
                    .expect("tokio listener from std");
                let mut sessions = Vec::with_capacity(n_conns as usize);
                for _ in 0..n_conns {
                    let (stream, _) = listener.accept().await.expect("accept");
                    let buf = chunk_buf.clone();
                    sessions.push(tokio::spawn(stream_session(stream, buf)));
                }
                for s in sessions {
                    let _ = s.await;
                }
            });
        })
        .expect("spawn ws-stream-srv")
}

#[cfg(target_os = "linux")]
async fn stream_session(mut s: tokio::net::TcpStream, chunk_buf: std::sync::Arc<Vec<u8>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let _ = s.set_nodelay(true);

    // ── HTTP upgrade ────────────────────────────────────────────────
    let mut buf = [0_u8; 4096];
    let mut req = Vec::<u8>::new();
    loop {
        let n = match s.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        req.extend_from_slice(&buf[..n]);
        if req.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let Ok(req_str) = std::str::from_utf8(&req) else {
        return;
    };
    let key = match req_str
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("sec-websocket-key:"))
        .and_then(|l| l.split(':').nth(1))
    {
        Some(k) => k.trim().to_owned(),
        None => return,
    };
    let accept = talaris::ws::handshake::compute_accept(&key);
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    if s.write_all(resp.as_bytes()).await.is_err() {
        return;
    }

    // ── Stream loop ──
    // write_all(chunk) → block 在 TCP send buffer 满；client drain → 解除。
    // client 关连接后下一次 write 拿到 EPIPE，session 退。
    loop {
        if s.write_all(&chunk_buf).await.is_err() {
            return;
        }
    }
}

// ─── Server：blocking rustls loopback stream session ───────────────────

#[cfg(target_os = "linux")]
pub fn spawn_tls_ws_stream_server(
    std_listener: std::net::TcpListener,
    chunk_buf: std::sync::Arc<Vec<u8>>,
    cpu: Option<usize>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("tls-ws-stream-srv".into())
        .spawn(move || {
            let _g = cpu.map(|c| PinGuard::pin("tls-ws-stream-srv", c));
            let Ok((stream, _)) = std_listener.accept() else {
                return;
            };
            let _ = stream.set_nodelay(true);
            let Ok(conn) = rustls::ServerConnection::new(local_tls_server_config()) else {
                return;
            };
            let mut stream = rustls::StreamOwned::new(conn, stream);
            tls_stream_session(&mut stream, &chunk_buf);
        })
        .expect("spawn tls-ws-stream-srv")
}

#[cfg(target_os = "linux")]
fn tls_stream_session(
    s: &mut rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>,
    chunk_buf: &[u8],
) {
    use std::io::{Read as _, Write as _};

    let mut buf = [0_u8; 4096];
    let mut req = Vec::<u8>::new();
    loop {
        let Ok(n) = s.read(&mut buf) else {
            return;
        };
        if n == 0 {
            return;
        }
        req.extend_from_slice(&buf[..n]);
        if req.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let Ok(req_str) = std::str::from_utf8(&req) else {
        return;
    };
    let Some(key) = websocket_key(req_str) else {
        return;
    };
    let accept = talaris::ws::handshake::compute_accept(key);
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    if s.write_all(resp.as_bytes()).is_err() || s.flush().is_err() {
        return;
    }

    loop {
        if s.write_all(chunk_buf).is_err() || s.flush().is_err() {
            return;
        }
    }
}

#[cfg(target_os = "linux")]
fn websocket_key(req: &str) -> Option<&str> {
    req.lines()
        .find(|line| {
            line.get(.."sec-websocket-key:".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("sec-websocket-key:"))
        })
        .and_then(|line| line.split_once(':'))
        .map(|(_, value)| value.trim())
}

// ─── Tokio 侧 client：WS upgrade + 手卷 recv loop（用 talaris parse_header）

/// 给 bench tokio 端用 —— 用 talaris 自己的 `generate_key` / `parse_header`
/// 保证两侧 framing 实现完全一致。bench 量的是 IO model（io_uring multishot
/// vs epoll readiness），不是 tokio-tungstenite 跟 talaris 的 framing 谁快。
///
/// 返回 `Ok(leftover)` —— `\r\n\r\n` 之后已经从 socket 拿到的字节。loopback
/// 上 server 几乎一定把 upgrade response 和第一批 WS 帧合在同一个 TCP packet
/// 发过来，这块必须传给 recv loop 当 `leftover` 初值，否则前面几千帧凭空消失。
#[cfg(target_os = "linux")]
pub async fn tokio_ws_upgrade_client(
    s: &mut tokio::net::TcpStream,
    host: &str,
    path: &str,
) -> std::io::Result<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let key = talaris::ws::handshake::generate_key()
        .map_err(|e| std::io::Error::other(format!("generate_key: {e}")))?;
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await?;

    let mut resp = Vec::<u8>::new();
    let mut buf = [0_u8; 4096];
    loop {
        let n = s.read(&mut buf).await?;
        if n == 0 {
            return Err(std::io::Error::other("conn closed mid-handshake"));
        }
        resp.extend_from_slice(&buf[..n]);
        if let Some(idx) = resp.windows(4).position(|w| w == b"\r\n\r\n") {
            // header end = `\r\n\r\n` 后第一字节；之后全是 WS payload
            let header_end = idx + 4;
            let leftover = resp[header_end..].to_vec();
            return Ok(leftover);
        }
    }
}

/// 在 tokio 端收 WS Binary 帧的 recv loop。每收一帧 push 一个 Instant 到
/// `arrivals`，给 inter-arrival histogram 用。
///
/// `initial_leftover` 是 upgrade 阶段顺手读到的 WS payload bytes（loopback 上
/// 这里能有几千字节），必须从这里开始 parse 才不丢帧。
///
/// 退出条件：`stop` 触发，或者 socket EOF / 解析错。返回 (arrivals, count)。
#[cfg(target_os = "linux")]
pub async fn tokio_recv_ws_binary_frames(
    s: &mut tokio::net::TcpStream,
    initial_leftover: Vec<u8>,
    stop: StopMode,
    expected_payload: usize,
    bench_start: Instant,
) -> (Vec<Instant>, u64) {
    use talaris::ws::frame::parse_header;
    use tokio::io::AsyncReadExt;

    let mut arrivals: Vec<Instant> = Vec::with_capacity(stop.cap_hint());
    let mut frame_count = 0_u64;

    let mut recv_buf = vec![0_u8; 256 * 1024];
    let mut leftover: Vec<u8> = initial_leftover;
    leftover.reserve(64 * 1024);

    // 在进 await 之前先把 initial_leftover 里能解的帧全干掉，免得后面一个空
    // read 就把这部分扔了。
    let mut had_initial = !leftover.is_empty();

    'outer: loop {
        // 解 leftover 里现有帧
        let mut pos = 0_usize;
        while pos < leftover.len() {
            match parse_header(&leftover[pos..]) {
                Ok(Some((hdr, consumed))) => {
                    let total = consumed + hdr.payload_len as usize;
                    if leftover.len() - pos < total {
                        break;
                    }
                    debug_assert_eq!(hdr.payload_len as usize, expected_payload);
                    arrivals.push(Instant::now());
                    frame_count += 1;
                    pos += total;
                    if !stop.keep_going(frame_count, bench_start) {
                        leftover.drain(..pos);
                        break 'outer;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("[tokio_recv] parse_header err after {frame_count}: {e}");
                    leftover.drain(..pos);
                    break 'outer;
                }
            }
        }
        leftover.drain(..pos);

        if had_initial {
            had_initial = false;
            // 第一轮可能解掉了 initial_leftover 的全部帧；继续 read 流量。
        }

        if !stop.keep_going(frame_count, bench_start) {
            break;
        }

        let n = match s.read(&mut recv_buf).await {
            Ok(0) => {
                eprintln!(
                    "[tokio_recv] EOF after {frame_count} frames, leftover={} bytes",
                    leftover.len()
                );
                break;
            }
            Ok(n) => n,
            Err(e) => {
                eprintln!("[tokio_recv] read error after {frame_count}: {e}");
                break;
            }
        };
        leftover.extend_from_slice(&recv_buf[..n]);
    }

    (arrivals, frame_count)
}

#[cfg(target_os = "linux")]
pub async fn tokio_recv_ktls_ws_binary_frames_sampled(
    s: &tokio::net::TcpStream,
    initial_leftover: Vec<u8>,
    stop: StopMode,
    expected_payload: usize,
    sample_every: u64,
    bench_start: Instant,
) -> (Vec<Instant>, u64) {
    use talaris::ws::frame::parse_header;
    let mut arrivals = sampled_arrivals(stop, sample_every);
    let mut frame_count = 0_u64;
    let mut recv_buf = vec![0_u8; 256 * 1024];
    let mut leftover = initial_leftover;
    leftover.reserve(64 * 1024);

    'outer: loop {
        let mut pos = 0_usize;
        while pos < leftover.len() {
            match parse_header(&leftover[pos..]) {
                Ok(Some((hdr, consumed))) => {
                    let total = consumed + hdr.payload_len as usize;
                    if leftover.len() - pos < total {
                        break;
                    }
                    debug_assert_eq!(hdr.payload_len as usize, expected_payload);
                    frame_count += 1;
                    record_sampled_arrival(&mut arrivals, frame_count, sample_every);
                    pos += total;
                    if !stop.keep_going(frame_count, bench_start) {
                        leftover.drain(..pos);
                        break 'outer;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("[tokio_ws_sampled] parse_header err after {frame_count}: {e}");
                    leftover.drain(..pos);
                    break 'outer;
                }
            }
        }
        leftover.drain(..pos);

        if !stop.keep_going(frame_count, bench_start) {
            break;
        }
        let n = match tokio_ktls_read_application_data(s, &mut recv_buf).await {
            Ok(0) => {
                eprintln!("[tokio_ws_sampled] EOF after {frame_count} frames");
                break;
            }
            Ok(n) => n,
            Err(e) => {
                eprintln!("[tokio_ws_sampled] read error after {frame_count}: {e}");
                break;
            }
        };
        leftover.extend_from_slice(&recv_buf[..n]);
    }

    (arrivals, frame_count)
}

// ─── Tokio 侧 client：同一 rustls 的 TLS + WS recv loop ───────────────

#[cfg(target_os = "linux")]
pub fn local_tls_client_connection() -> rustls::ClientConnection {
    local_tls_client_connection_with_config(local_tls_client_config())
}

#[cfg(target_os = "linux")]
pub fn local_ktls_client_connection() -> rustls::ClientConnection {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(local_tls_cert()).expect("add localhost cert");
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config.enable_secret_extraction = true;
    local_tls_client_connection_with_config(std::sync::Arc::new(config))
}

#[cfg(target_os = "linux")]
fn local_tls_client_connection_with_config(
    config: std::sync::Arc<rustls::ClientConfig>,
) -> rustls::ClientConnection {
    let server_name = rustls::pki_types::ServerName::try_from("localhost")
        .expect("localhost is valid server name")
        .to_owned();
    rustls::ClientConnection::new(config, server_name).expect("localhost tls client")
}

#[cfg(target_os = "linux")]
pub async fn tokio_ktls_ws_upgrade_client(
    s: &mut tokio::net::TcpStream,
    mut tls: rustls::ClientConnection,
    host: &str,
    path: &str,
) -> std::io::Result<Vec<u8>> {
    let mut network_buf = vec![0_u8; 256 * 1024];
    let mut plaintext = Vec::<u8>::new();
    while tls.is_handshaking() {
        flush_tokio_tls(s, &mut tls).await?;
        read_tokio_tls(s, &mut tls, &mut network_buf, &mut plaintext).await?;
    }
    flush_tokio_tls(s, &mut tls).await?;
    if !plaintext.is_empty() {
        return Err(std::io::Error::other(
            "received application plaintext before kTLS install",
        ));
    }

    let version = tls
        .protocol_version()
        .ok_or_else(|| std::io::Error::other("TLS handshake completed without protocol version"))?;
    let secrets = tls
        .dangerous_extract_secrets()
        .map_err(std::io::Error::other)?;
    install_ktls(s, version, secrets)?;
    tokio_ktls_ws_upgrade_after_install(s, host, path).await
}

#[cfg(target_os = "linux")]
pub async fn tokio_tls_ws_upgrade_client(
    s: &mut tokio::net::TcpStream,
    tls: &mut rustls::ClientConnection,
    host: &str,
    path: &str,
) -> std::io::Result<Vec<u8>> {
    use std::io::Write as _;

    let mut network_buf = vec![0_u8; 256 * 1024];
    let mut plaintext = Vec::<u8>::new();
    while tls.is_handshaking() {
        flush_tokio_tls(s, tls).await?;
        read_tokio_tls(s, tls, &mut network_buf, &mut plaintext).await?;
    }

    let key = talaris::ws::handshake::generate_key()
        .map_err(|e| std::io::Error::other(format!("generate_key: {e}")))?;
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    tls.writer().write_all(req.as_bytes())?;
    flush_tokio_tls(s, tls).await?;

    loop {
        read_tokio_tls(s, tls, &mut network_buf, &mut plaintext).await?;
        if let Some(idx) = plaintext.windows(4).position(|w| w == b"\r\n\r\n") {
            let header_end = idx + 4;
            return Ok(plaintext[header_end..].to_vec());
        }
    }
}

#[cfg(target_os = "linux")]
pub async fn tokio_tls_ws_client_connect(
    s: &mut tokio::net::TcpStream,
    tls: &mut rustls::ClientConnection,
    host: &str,
    path: &str,
) -> std::io::Result<talaris::ws::WsClient> {
    let mut network_buf = vec![0_u8; 256 * 1024];
    let mut plaintext = Vec::<u8>::new();
    while tls.is_handshaking() {
        flush_tokio_tls(s, tls).await?;
        read_tokio_tls(s, tls, &mut network_buf, &mut plaintext).await?;
    }
    if !plaintext.is_empty() {
        return Err(std::io::Error::other(
            "received application plaintext before websocket request",
        ));
    }

    let mut ws = talaris::ws::WsClient::new_client(talaris::ws::WsConfig::new(host, path))
        .map_err(std::io::Error::other)?;
    ws.begin_handshake().map_err(std::io::Error::other)?;
    flush_tokio_ws_tx(s, tls, &mut ws).await?;

    loop {
        read_tokio_tls(s, tls, &mut network_buf, &mut plaintext).await?;
        ws.feed_recv(&plaintext);
        plaintext.clear();
        while let Some(event) = ws.poll_event() {
            if matches!(
                event.map_err(std::io::Error::other)?,
                talaris::ws::Event::HandshakeComplete
            ) {
                return Ok(ws);
            }
        }
    }
}

#[cfg(target_os = "linux")]
pub async fn tokio_recv_tls_ws_binary_frames(
    s: &mut tokio::net::TcpStream,
    tls: &mut rustls::ClientConnection,
    initial_leftover: Vec<u8>,
    stop: StopMode,
    expected_payload: usize,
    sample_every: u64,
    bench_start: Instant,
) -> (Vec<Instant>, u64) {
    use talaris::ws::frame::parse_header;

    let mut arrivals = sampled_arrivals(stop, sample_every);
    let mut frame_count = 0_u64;
    let mut network_buf = vec![0_u8; 256 * 1024];
    let mut plaintext = initial_leftover;
    plaintext.reserve(64 * 1024);

    'outer: loop {
        let mut pos = 0_usize;
        while pos < plaintext.len() {
            match parse_header(&plaintext[pos..]) {
                Ok(Some((hdr, consumed))) => {
                    let total = consumed + hdr.payload_len as usize;
                    if plaintext.len() - pos < total {
                        break;
                    }
                    debug_assert_eq!(hdr.payload_len as usize, expected_payload);
                    frame_count += 1;
                    record_sampled_arrival(&mut arrivals, frame_count, sample_every);
                    pos += total;
                    if !stop.keep_going(frame_count, bench_start) {
                        plaintext.drain(..pos);
                        break 'outer;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("[tokio_tls_recv] parse_header err after {frame_count}: {e}");
                    plaintext.drain(..pos);
                    break 'outer;
                }
            }
        }
        plaintext.drain(..pos);

        if !stop.keep_going(frame_count, bench_start) {
            break;
        }
        if let Err(e) = read_tokio_tls(s, tls, &mut network_buf, &mut plaintext).await {
            eprintln!("[tokio_tls_recv] read error after {frame_count}: {e}");
            break;
        }
    }

    (arrivals, frame_count)
}

#[cfg(target_os = "linux")]
pub async fn tokio_recv_tls_ws_data_events(
    s: &mut tokio::net::TcpStream,
    tls: &mut rustls::ClientConnection,
    mut ws: talaris::ws::WsClient,
    stop: StopMode,
    expected_payload: usize,
    sample_every: u64,
    bench_start: Instant,
) -> (Vec<Instant>, u64) {
    let mut arrivals = sampled_arrivals(stop, sample_every);
    let mut frame_count = 0_u64;
    let mut network_buf = vec![0_u8; 256 * 1024];
    let mut plaintext = Vec::with_capacity(64 * 1024);

    loop {
        if let Err(e) = ws.drain_data_events(|event| {
            if let talaris::ws::DataEvent::Binary(data) = event {
                debug_assert_eq!(data.len(), expected_payload);
                frame_count += 1;
                record_sampled_arrival(&mut arrivals, frame_count, sample_every);
            }
        }) {
            eprintln!("[tokio_tls_ws] websocket error after {frame_count}: {e}");
            break;
        }
        if !stop.keep_going(frame_count, bench_start) {
            break;
        }
        if let Err(e) = flush_tokio_ws_tx(s, tls, &mut ws).await {
            eprintln!("[tokio_tls_ws] write error after {frame_count}: {e}");
            break;
        }
        if let Err(e) = read_tokio_tls(s, tls, &mut network_buf, &mut plaintext).await {
            eprintln!("[tokio_tls_ws] read error after {frame_count}: {e}");
            break;
        }
        ws.feed_recv(&plaintext);
        plaintext.clear();
    }

    (arrivals, frame_count)
}

#[cfg(target_os = "linux")]
async fn read_tokio_tls(
    s: &mut tokio::net::TcpStream,
    tls: &mut rustls::ClientConnection,
    network_buf: &mut [u8],
    plaintext: &mut Vec<u8>,
) -> std::io::Result<()> {
    use tokio::io::AsyncReadExt as _;

    let n = s.read(network_buf).await?;
    if n == 0 {
        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
    }
    let mut src = &network_buf[..n];
    while !src.is_empty() {
        let consumed = tls.read_tls(&mut src)?;
        if consumed == 0 {
            break;
        }
        tls.process_new_packets().map_err(std::io::Error::other)?;
        drain_tls_plaintext(tls, plaintext)?;
    }
    flush_tokio_tls(s, tls).await
}

#[cfg(target_os = "linux")]
async fn flush_tokio_tls(
    s: &mut tokio::net::TcpStream,
    tls: &mut rustls::ClientConnection,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt as _;

    let mut ciphertext = Vec::new();
    while tls.wants_write() {
        tls.write_tls(&mut ciphertext)?;
    }
    if !ciphertext.is_empty() {
        s.write_all(&ciphertext).await?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn flush_tokio_ws_tx(
    s: &mut tokio::net::TcpStream,
    tls: &mut rustls::ClientConnection,
    ws: &mut talaris::ws::WsClient,
) -> std::io::Result<()> {
    use std::io::Write as _;

    let n = ws.pending_tx().len();
    if n > 0 {
        tls.writer().write_all(ws.pending_tx())?;
        ws.ack_tx(n);
    }
    flush_tokio_tls(s, tls).await
}

#[cfg(target_os = "linux")]
fn drain_tls_plaintext(
    tls: &mut rustls::ClientConnection,
    plaintext: &mut Vec<u8>,
) -> std::io::Result<()> {
    use std::io::BufRead as _;

    loop {
        let mut reader = tls.reader();
        match reader.fill_buf() {
            Ok([]) => return Ok(()),
            Ok(chunk) => {
                let n = chunk.len();
                plaintext.extend_from_slice(chunk);
                reader.consume(n);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

#[cfg(target_os = "linux")]
pub struct TokioUnbufferedTls {
    conn: rustls::client::UnbufferedClientConnection,
    incoming: Vec<u8>,
    incoming_head: usize,
    outgoing: Vec<u8>,
}

#[cfg(target_os = "linux")]
impl TokioUnbufferedTls {
    fn new() -> Self {
        let server_name = rustls::pki_types::ServerName::try_from("localhost")
            .expect("localhost is valid server name")
            .to_owned();
        let conn =
            rustls::client::UnbufferedClientConnection::new(local_tls_client_config(), server_name)
                .expect("localhost unbuffered tls client");
        Self {
            conn,
            incoming: Vec::with_capacity(256 * 1024),
            incoming_head: 0,
            outgoing: Vec::with_capacity(128 * 1024),
        }
    }

    async fn handshake(&mut self, s: &mut tokio::net::TcpStream) -> std::io::Result<()> {
        loop {
            match self.drive_available(s, None, |_| {}).await? {
                TokioUnbufferedDrive::NeedRead => self.read_more(s).await?,
                TokioUnbufferedDrive::WriteTraffic => return Ok(()),
                TokioUnbufferedDrive::Delivered => {}
                TokioUnbufferedDrive::Encrypted => unreachable!("handshake has no app plaintext"),
            }
        }
    }

    async fn write_plaintext(
        &mut self,
        s: &mut tokio::net::TcpStream,
        plaintext: &[u8],
    ) -> std::io::Result<()> {
        loop {
            match self.drive_available(s, Some(plaintext), |_| {}).await? {
                TokioUnbufferedDrive::Encrypted => return Ok(()),
                TokioUnbufferedDrive::NeedRead | TokioUnbufferedDrive::WriteTraffic => {
                    self.read_more(s).await?;
                }
                TokioUnbufferedDrive::Delivered => {}
            }
        }
    }

    async fn read_plaintext(
        &mut self,
        s: &mut tokio::net::TcpStream,
        mut on_plaintext: impl FnMut(&[u8]),
    ) -> std::io::Result<()> {
        loop {
            match self.drive_available(s, None, &mut on_plaintext).await? {
                TokioUnbufferedDrive::Delivered => return Ok(()),
                TokioUnbufferedDrive::NeedRead | TokioUnbufferedDrive::WriteTraffic => {
                    self.read_more(s).await?;
                }
                TokioUnbufferedDrive::Encrypted => unreachable!("read has no app plaintext"),
            }
        }
    }

    async fn drive_available(
        &mut self,
        s: &mut tokio::net::TcpStream,
        mut plaintext_to_encrypt: Option<&[u8]>,
        mut on_plaintext: impl FnMut(&[u8]),
    ) -> std::io::Result<TokioUnbufferedDrive> {
        use rustls::unbuffered::ConnectionState;
        use tokio::io::AsyncWriteExt as _;

        let mut delivered = false;
        loop {
            let status = self
                .conn
                .process_tls_records(&mut self.incoming[self.incoming_head..]);
            let mut discard = status.discard;
            let next = match status.state.map_err(std::io::Error::other)? {
                ConnectionState::ReadTraffic(mut traffic) => {
                    while let Some(record) = traffic.next_record() {
                        let record = record.map_err(std::io::Error::other)?;
                        discard += record.discard;
                        on_plaintext(record.payload);
                        delivered = true;
                    }
                    None
                }
                ConnectionState::EncodeTlsData(mut data) => {
                    const HANDSHAKE_SLACK: usize = 128 * 1024;
                    let start = self.outgoing.len();
                    self.outgoing.resize(start + HANDSHAKE_SLACK, 0);
                    let n = data
                        .encode(&mut self.outgoing[start..])
                        .map_err(std::io::Error::other)?;
                    self.outgoing.truncate(start + n);
                    None
                }
                ConnectionState::TransmitTlsData(data) => {
                    s.write_all(&self.outgoing).await?;
                    self.outgoing.clear();
                    data.done();
                    None
                }
                ConnectionState::BlockedHandshake => Some(TokioUnbufferedDrive::NeedRead),
                ConnectionState::WriteTraffic(mut traffic) => {
                    if let Some(plaintext) = plaintext_to_encrypt.take() {
                        let required = plaintext.len() + 4096;
                        self.outgoing.resize(required, 0);
                        let n = traffic
                            .encrypt(plaintext, &mut self.outgoing)
                            .map_err(std::io::Error::other)?;
                        s.write_all(&self.outgoing[..n]).await?;
                        self.outgoing.clear();
                        Some(TokioUnbufferedDrive::Encrypted)
                    } else {
                        Some(TokioUnbufferedDrive::WriteTraffic)
                    }
                }
                ConnectionState::PeerClosed | ConnectionState::Closed => {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                }
                _ => {
                    return Err(std::io::Error::other(
                        "unexpected rustls unbuffered connection state",
                    ));
                }
            };
            self.incoming_head += discard;
            if let Some(next) = next {
                return Ok(match next {
                    TokioUnbufferedDrive::Encrypted => TokioUnbufferedDrive::Encrypted,
                    _ if delivered => TokioUnbufferedDrive::Delivered,
                    _ => next,
                });
            }
        }
    }

    async fn read_more(&mut self, s: &mut tokio::net::TcpStream) -> std::io::Result<()> {
        use tokio::io::AsyncReadExt as _;

        if self.incoming_head == self.incoming.len() {
            self.incoming.clear();
            self.incoming_head = 0;
        } else if self.incoming_head > 0 {
            self.incoming.copy_within(self.incoming_head.., 0);
            self.incoming
                .truncate(self.incoming.len() - self.incoming_head);
            self.incoming_head = 0;
        }
        let n = s.read_buf(&mut self.incoming).await?;
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
enum TokioUnbufferedDrive {
    NeedRead,
    WriteTraffic,
    Delivered,
    Encrypted,
}

#[cfg(target_os = "linux")]
pub async fn tokio_unbuffered_tls_ws_upgrade_client(
    s: &mut tokio::net::TcpStream,
    host: &str,
    path: &str,
) -> std::io::Result<(TokioUnbufferedTls, Vec<u8>)> {
    let mut tls = TokioUnbufferedTls::new();
    tls.handshake(s).await?;

    let key = talaris::ws::handshake::generate_key()
        .map_err(|e| std::io::Error::other(format!("generate_key: {e}")))?;
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    tls.write_plaintext(s, req.as_bytes()).await?;

    let mut plaintext = Vec::<u8>::new();
    loop {
        tls.read_plaintext(s, |chunk| plaintext.extend_from_slice(chunk))
            .await?;
        if let Some(idx) = plaintext.windows(4).position(|w| w == b"\r\n\r\n") {
            let header_end = idx + 4;
            return Ok((tls, plaintext[header_end..].to_vec()));
        }
    }
}

#[cfg(target_os = "linux")]
pub async fn tokio_recv_unbuffered_tls_ws_binary_frames(
    s: &mut tokio::net::TcpStream,
    tls: &mut TokioUnbufferedTls,
    initial_leftover: Vec<u8>,
    stop: StopMode,
    expected_payload: usize,
    sample_every: u64,
    bench_start: Instant,
) -> (Vec<Instant>, u64) {
    use talaris::ws::frame::parse_header;

    let mut arrivals = sampled_arrivals(stop, sample_every);
    let mut frame_count = 0_u64;
    let mut leftover = initial_leftover;
    leftover.reserve(256 * 1024);

    'outer: loop {
        let mut pos = 0_usize;
        while pos < leftover.len() {
            match parse_header(&leftover[pos..]) {
                Ok(Some((hdr, consumed))) => {
                    let total = consumed + hdr.payload_len as usize;
                    if leftover.len() - pos < total {
                        break;
                    }
                    debug_assert_eq!(hdr.payload_len as usize, expected_payload);
                    frame_count += 1;
                    record_sampled_arrival(&mut arrivals, frame_count, sample_every);
                    pos += total;
                    if !stop.keep_going(frame_count, bench_start) {
                        leftover.drain(..pos);
                        break 'outer;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("[tokio-unbuffered] parse_header err after {frame_count}: {e}");
                    leftover.drain(..pos);
                    break 'outer;
                }
            }
        }
        leftover.drain(..pos);

        if !stop.keep_going(frame_count, bench_start) {
            break;
        }
        if let Err(e) = tls
            .read_plaintext(s, |chunk| leftover.extend_from_slice(chunk))
            .await
        {
            eprintln!("[tokio-unbuffered] read error after {frame_count}: {e}");
            break;
        }
    }

    (arrivals, frame_count)
}

#[cfg(target_os = "linux")]
async fn tokio_ktls_ws_upgrade_after_install(
    s: &mut tokio::net::TcpStream,
    host: &str,
    path: &str,
) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncWriteExt as _;

    let key = talaris::ws::handshake::generate_key()
        .map_err(|e| std::io::Error::other(format!("generate_key: {e}")))?;
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await?;

    let mut resp = Vec::<u8>::new();
    let mut buf = [0_u8; 4096];
    loop {
        let n = tokio_ktls_read_application_data(s, &mut buf).await?;
        if n == 0 {
            return Err(std::io::Error::other("conn closed mid-handshake"));
        }
        resp.extend_from_slice(&buf[..n]);
        if let Some(idx) = resp.windows(4).position(|w| w == b"\r\n\r\n") {
            let header_end = idx + 4;
            return Ok(resp[header_end..].to_vec());
        }
    }
}

#[cfg(target_os = "linux")]
async fn tokio_ktls_read_application_data(
    s: &tokio::net::TcpStream,
    buf: &mut [u8],
) -> std::io::Result<usize> {
    use tokio::io::Interest;

    loop {
        s.readable().await?;
        match s.try_io(Interest::READABLE, || recv_ktls_record(s, buf)) {
            Ok((n, None | Some(23))) => return Ok(n),
            Ok((_n, Some(_control_record))) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e),
        }
    }
}

#[cfg(target_os = "linux")]
fn recv_ktls_record(
    s: &tokio::net::TcpStream,
    buf: &mut [u8],
) -> std::io::Result<(usize, Option<u8>)> {
    use std::os::fd::AsRawFd as _;

    // SAFETY: one byte is a valid ancillary-data payload size.
    const CONTROL_SPACE: usize = unsafe { libc::CMSG_SPACE(1) as usize };

    let mut control = [0_u8; CONTROL_SPACE];
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    // SAFETY: zero is a valid empty msghdr initialization.
    let mut msg = unsafe { std::mem::zeroed::<libc::msghdr>() };
    msg.msg_iov = &raw mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = control.len();

    // SAFETY: msg points to initialized iovec and control buffers for this synchronous call.
    let n = unsafe { libc::recvmsg(s.as_raw_fd(), &raw mut msg, libc::MSG_DONTWAIT) };
    if n < 0 {
        return Err(std::io::Error::last_os_error());
    }

    // SAFETY: libc validates msg_controllen and returns null if no complete cmsghdr exists.
    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&raw const msg) };
    let record_type = if cmsg.is_null() {
        None
    } else {
        // SAFETY: CMSG_FIRSTHDR returned a header inside `control`; kTLS record type is one byte.
        unsafe {
            ((*cmsg).cmsg_level == libc::SOL_TLS && (*cmsg).cmsg_type == libc::TLS_GET_RECORD_TYPE)
                .then(|| *libc::CMSG_DATA(cmsg))
        }
    };
    Ok((
        usize::try_from(n).expect("positive recvmsg length fits usize"),
        record_type,
    ))
}

#[cfg(target_os = "linux")]
fn install_ktls(
    s: &tokio::net::TcpStream,
    version: rustls::ProtocolVersion,
    secrets: rustls::ExtractedSecrets,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd as _;

    let fd = s.as_raw_fd();
    set_socket_option(fd, libc::SOL_TCP, libc::TCP_ULP, b"tls")?;
    set_ktls_direction(fd, libc::TLS_TX, version, secrets.tx)?;
    set_ktls_direction(fd, libc::TLS_RX, version, secrets.rx)
}

#[cfg(target_os = "linux")]
fn set_ktls_direction(
    fd: std::os::fd::RawFd,
    direction: libc::c_int,
    version: rustls::ProtocolVersion,
    (seq, secrets): (u64, rustls::ConnectionTrafficSecrets),
) -> std::io::Result<()> {
    let version = match version {
        rustls::ProtocolVersion::TLSv1_2 => libc::TLS_1_2_VERSION,
        rustls::ProtocolVersion::TLSv1_3 => libc::TLS_1_3_VERSION,
        _ => {
            return Err(std::io::Error::other(
                "kTLS probe only supports TLS 1.2 and TLS 1.3",
            ));
        }
    };
    let rec_seq = seq.to_be_bytes();
    match secrets {
        rustls::ConnectionTrafficSecrets::Aes128Gcm { key, iv } => {
            let iv = iv.as_ref();
            let crypto = libc::tls12_crypto_info_aes_gcm_128 {
                info: libc::tls_crypto_info {
                    version,
                    cipher_type: libc::TLS_CIPHER_AES_GCM_128,
                },
                iv: iv[4..].try_into().expect("AES-GCM explicit IV is 8 bytes"),
                key: key.as_ref().try_into().expect("AES-128 key is 16 bytes"),
                salt: iv[..4].try_into().expect("AES-GCM salt is 4 bytes"),
                rec_seq,
            };
            set_socket_option(fd, libc::SOL_TLS, direction, as_bytes(&crypto))
        }
        rustls::ConnectionTrafficSecrets::Aes256Gcm { key, iv } => {
            let iv = iv.as_ref();
            let crypto = libc::tls12_crypto_info_aes_gcm_256 {
                info: libc::tls_crypto_info {
                    version,
                    cipher_type: libc::TLS_CIPHER_AES_GCM_256,
                },
                iv: iv[4..].try_into().expect("AES-GCM explicit IV is 8 bytes"),
                key: key.as_ref().try_into().expect("AES-256 key is 32 bytes"),
                salt: iv[..4].try_into().expect("AES-GCM salt is 4 bytes"),
                rec_seq,
            };
            set_socket_option(fd, libc::SOL_TLS, direction, as_bytes(&crypto))
        }
        rustls::ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv } => {
            let crypto = libc::tls12_crypto_info_chacha20_poly1305 {
                info: libc::tls_crypto_info {
                    version,
                    cipher_type: libc::TLS_CIPHER_CHACHA20_POLY1305,
                },
                iv: iv.as_ref().try_into().expect("ChaCha20 IV is 12 bytes"),
                key: key.as_ref().try_into().expect("ChaCha20 key is 32 bytes"),
                salt: [],
                rec_seq,
            };
            set_socket_option(fd, libc::SOL_TLS, direction, as_bytes(&crypto))
        }
        _ => Err(std::io::Error::other(
            "kTLS probe does not support negotiated cipher suite",
        )),
    }
}

#[cfg(target_os = "linux")]
fn set_socket_option(
    fd: std::os::fd::RawFd,
    level: libc::c_int,
    name: libc::c_int,
    value: &[u8],
) -> std::io::Result<()> {
    let len = libc::socklen_t::try_from(value.len()).expect("socket option length fits socklen_t");
    // SAFETY: `value` points to `len` initialized bytes for the duration of setsockopt.
    let rc = unsafe { libc::setsockopt(fd, level, name, value.as_ptr().cast(), len) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
const fn as_bytes<T>(value: &T) -> &[u8] {
    // SAFETY: Linux tls_crypto_info structs are plain C structs with initialized fields.
    unsafe {
        std::slice::from_raw_parts(
            std::ptr::from_ref(value).cast::<u8>(),
            std::mem::size_of::<T>(),
        )
    }
}
