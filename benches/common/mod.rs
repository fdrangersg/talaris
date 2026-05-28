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

use std::time::Instant;

use hdrhistogram::Histogram;

// ─── HdrHistogram ──────────────────────────────────────────────────────

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
    let cols: [(&str, fn(&Histogram<u64>) -> u64); 5] = [
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

fn ns(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3 + 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(b as char);
    }
    out.push_str(" ns");
    out
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

// ─── CLI ─────────────────────────────────────────────────────────────────

pub fn arg_opt<T: std::str::FromStr>(key: &str) -> Option<T> {
    let mut it = std::env::args().skip(1);
    let mut found: Option<T> = None;
    while let Some(a) = it.next() {
        if a == key {
            if let Some(v) = it.next().and_then(|s| s.parse().ok()) {
                found = Some(v);
            }
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
                eprintln!(
                    "[bench] --seconds and --frames both given; using --seconds"
                );
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
            libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut saved)
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
                &self.saved,
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
    pub fn pin(_label: &str, _cpu: usize) -> Self {
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
    use talaris::ws::frame::{MAX_HEADER_LEN, encode_header};

    let mut payload = vec![0_u8; payload_size];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 256) as u8;
    }

    // 每帧 header 大小：payload ≤125 是 2B，≤65535 是 4B，否则 10B。
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
        let hn = encode_header(&mut hdr, true, OpCode::Binary, None, payload_size as u64);
        buf.extend_from_slice(&hdr[..hn]);
        buf.extend_from_slice(&payload);
    }
    buf
}

/// 给 server 推荐的 chunk size：~64 KiB（一次 write_all 大致填满 TCP send buffer
/// 但不溢出，使 server 端 syscall 次数最少）。换算到对应 payload 帧数。
pub fn frames_per_chunk(payload_size: usize) -> usize {
    const TARGET: usize = 64 * 1024;
    let per_frame = payload_size + if payload_size <= 125 { 2 } else { 4 };
    (TARGET / per_frame).max(1)
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
    std_listener
        .set_nonblocking(true)
        .expect("set_nonblocking");

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
async fn stream_session(
    mut s: tokio::net::TcpStream,
    chunk_buf: std::sync::Arc<Vec<u8>>,
) {
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
    let req_str = match std::str::from_utf8(&req) {
        Ok(s) => s,
        Err(_) => return,
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
    if let Err(e) = s.write_all(resp.as_bytes()).await {
        eprintln!("[stream_session] write upgrade resp failed: {e}");
        return;
    }
    eprintln!("[stream_session] upgrade OK, entering stream loop");

    // ── Stream loop ──
    // write_all(chunk) → block 在 TCP send buffer 满；client drain → 解除。
    // client 关连接后下一次 write 拿到 EPIPE，session 退。
    let mut total_written: u64 = 0;
    loop {
        match s.write_all(&chunk_buf).await {
            Ok(()) => {
                total_written += chunk_buf.len() as u64;
            }
            Err(e) => {
                eprintln!(
                    "[stream_session] write_all failed after {total_written} bytes: {e}"
                );
                return;
            }
        }
    }
}

// ─── Tokio 侧 client：WS upgrade + 手卷 recv loop（用 talaris parse_header）

/// 给 bench tokio 端用 —— 用 talaris 自己的 `generate_key` / `parse_header`
/// 保证两侧 framing 实现完全一致。bench 量的是 IO model（io_uring multishot
/// vs epoll readiness），不是 tokio-tungstenite 跟 talaris 的 framing 谁快。
#[cfg(target_os = "linux")]
pub async fn tokio_ws_upgrade_client(
    s: &mut tokio::net::TcpStream,
    host: &str,
    path: &str,
) -> std::io::Result<()> {
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
        if resp.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    // bench 不校验 accept hash —— server 是自己起的，trusted
    Ok(())
}

/// 在 tokio 端收 WS Binary 帧的 recv loop。每收一帧 push 一个 Instant 到
/// `arrivals`，给 inter-arrival histogram 用。
///
/// 退出条件：`stop` 触发，或者 socket EOF / 解析错。返回 (arrivals, count)。
#[cfg(target_os = "linux")]
pub async fn tokio_recv_ws_binary_frames(
    s: &mut tokio::net::TcpStream,
    stop: StopMode,
    expected_payload: usize,
    bench_start: Instant,
) -> (Vec<Instant>, u64) {
    use talaris::ws::frame::parse_header;
    use tokio::io::AsyncReadExt;

    let mut arrivals: Vec<Instant> = Vec::with_capacity(stop.cap_hint());
    let mut frame_count = 0_u64;

    // 256 KiB recv buffer：跟 talaris 的 BUF_RING entry 大小同量级（4KiB × 256
    // = 1 MiB pool），但 tokio 不能 multi-buffer，单 buffer 就给到 256 KiB 一
    // 次 read 尽可能多吸。
    let mut recv_buf = vec![0_u8; 256 * 1024];
    let mut leftover = Vec::<u8>::with_capacity(64 * 1024);

    'outer: loop {
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
                eprintln!("[tokio_recv] read error after {frame_count} frames: {e}");
                break;
            }
        };
        leftover.extend_from_slice(&recv_buf[..n]);

        let mut pos = 0_usize;
        while pos < leftover.len() {
            match parse_header(&leftover[pos..]) {
                Ok(Some((hdr, consumed))) => {
                    let total = consumed + hdr.payload_len as usize;
                    if leftover.len() - pos < total {
                        break; // 帧不完整，等下一轮 read
                    }
                    debug_assert_eq!(hdr.payload_len as usize, expected_payload);
                    arrivals.push(Instant::now());
                    frame_count += 1;
                    pos += total;
                    if !stop.keep_going(frame_count, bench_start) {
                        break 'outer;
                    }
                }
                Ok(None) => break, // 头不完整
                Err(_) => break 'outer, // 协议错
            }
        }
        leftover.drain(..pos);
    }

    (arrivals, frame_count)
}
