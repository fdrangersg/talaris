// 多个 bench 共享代码 —— histogram 打印、CLI 解析、in-process echo server、
// PinGuard、StopCondition。
//
// 不是 Cargo bench target（住在子目录里，cargo 不会自动收作 target）；每个 bench
// 文件用 `#[path = "common/mod.rs"] mod common;` 引进来。
//
// 整文件 bench-only：unwrap / expect / panic 都是设计选择，不复用本 crate lib
// 的 HFT 守门 lint。

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
    clippy::semicolon_if_nothing_returned
)]

use std::time::Instant;

use hdrhistogram::Histogram;

// ─── HdrHistogram helper ────────────────────────────────────────────────

/// 一个延迟 bench 通用的 histogram bound：1 ns … 60 s, 3 位有效数字。
pub fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("hist")
}

/// 把 `Duration` 安全转 ns 喂给 hist.record。
pub fn record_ns(hist: &mut Histogram<u64>, dt: std::time::Duration) {
    let ns = u64::try_from(dt.as_nanos().min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
    hist.record(ns.max(1)).ok();
}

/// 单 histogram 单行打印。
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

/// 多 variant 并列。
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
    println!(
        "samples : {}",
        rows.iter()
            .map(|(l, h)| format!("{l}={}", h.len()))
            .collect::<Vec<_>>()
            .join("  ")
    );
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

// ─── CLI 解析 ───────────────────────────────────────────────────────────

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

// ─── Stop condition：iter 数或 wall-clock 二选一 ─────────────────────────
//
// 严格控制变量的核心：所有 variant 在同一个 stop condition 下跑。
//
// - `--iters N`（默认）：每 variant 跑 N 次 round（数据量对齐）。延迟比较的标准
//   做法。注意：更快的 variant 用时更短，wall-clock 不齐。
// - `--seconds T`：每 variant 跑 T 秒（wall-clock 对齐）。throughput 比较用，
//   慢的 variant 完成的 iter 数变少，但每条 sample 仍是同一段时钟下的"系统
//   稳态"，比 iter-aligned 抗 thermal / IRQ drift。
//
// 不允许同时给，互相矛盾。

#[derive(Debug, Clone, Copy)]
pub enum StopMode {
    Iters(u64),
    Seconds(f64),
}

impl StopMode {
    /// Default iters 由 caller bench 给（不同 bench 量级差很多）。
    pub fn from_args(default_iters: u64) -> Self {
        let secs: Option<f64> = arg_opt("--seconds");
        let iters: Option<u64> = arg_opt("--iters");
        match (secs, iters) {
            (Some(_), Some(_)) => {
                eprintln!(
                    "[bench] --seconds and --iters both given; using --seconds (wall-clock aligned)"
                );
                Self::Seconds(secs.unwrap())
            }
            (Some(s), None) => Self::Seconds(s),
            (None, Some(i)) => Self::Iters(i),
            (None, None) => Self::Iters(default_iters),
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Self::Iters(n) => format!("iters={n}"),
            Self::Seconds(s) => format!("seconds={s}"),
        }
    }

    /// 主循环 predicate：iter 数和起始时间，决定要不要继续下一轮。
    pub fn keep_going(&self, iter: u64, started: Instant) -> bool {
        match *self {
            Self::Iters(n) => iter < n,
            Self::Seconds(s) => started.elapsed().as_secs_f64() < s,
        }
    }
}

// ─── PinGuard：作用域期间钉 CPU，drop 时**精确还原**到 pin 之前的 mask ───
//
// 严格控制变量的关键：跨 variant 串行跑时，前一个 variant 的 affinity 不能
// 影响下一个。
//
// 早期实现用 `talaris::proactor::unpin_current_thread()` 还原，但它内部用
// `core_affinity::get_core_ids()` 拿可见 CPU 数 —— pin 后该 API 只返回当前
// 单核，于是 unpin 后线程其实仍被夹在那个核上，下一 variant pin 到别的 index
// 直接 OutOfRange。
//
// 现在 pin 前用 `sched_getaffinity` 抓一份 `cpu_set_t`，drop 时
// `sched_setaffinity` 写回，跟"先 pin 再 unpin"无关，纯 syscall restore。
// 进一步 `pin_current_thread_to(N)` 的 N 是 visible-cores 的 index 而不是
// 真 CPU 号 —— 在 mask 完整时（默认 taskset -c 0-7 后），index 0..7 ≡
// CPU 0..7；mask 被压窄过的话两者就分裂。restore 让 mask 永远停在初始完整态。

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
    /// 钉当前线程到 `cpu`（visible-cores 的 index）。返回的 guard drop 时
    /// 精确还原到 pin 之前的 affinity mask。失败 warn 但不 panic。
    #[must_use]
    pub fn pin(label: &str, cpu: usize) -> Self {
        // SAFETY: cpu_set_t POD，0-init 合法
        let mut saved: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        // SAFETY: saved 是有效栈对象；size 用 size_of 取
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
        // SAFETY: self.saved 仍存活；size_of 跟 saved 类型匹配
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

// ─── Sync TCP echo server（单 conn / 单 OS 线程，bench 用一次性 session）

#[cfg(target_os = "linux")]
pub fn run_tcp_echo_once(listener: std::net::TcpListener, cpu: Option<usize>) {
    use std::io::{Read, Write};
    let _g = cpu.map(|c| PinGuard::pin("tcp-echo", c));
    let (mut s, _) = listener.accept().expect("accept");
    s.set_nodelay(true).expect("nodelay");
    let mut buf = vec![0_u8; 256 * 1024];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if s.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

// ─── Sync WS echo server（单 conn / 单 OS 线程）────────────────────────

#[cfg(target_os = "linux")]
pub fn run_ws_echo_session(mut s: std::net::TcpStream) {
    use std::io::Write;
    use talaris::ws::OpCode;
    use talaris::ws::frame::{MAX_HEADER_LEN, encode_header};
    use talaris::ws::handshake::compute_accept;

    s.set_nodelay(true).expect("nodelay");

    // 1. HTTP upgrade
    upgrade_ws(&mut s);

    // 2. echo loop
    let mut header_buf = vec![0_u8; MAX_HEADER_LEN];
    loop {
        let (opcode, payload) = match read_frame(&mut s) {
            Some(x) => x,
            None => return,
        };
        match opcode {
            OpCode::Text | OpCode::Binary | OpCode::Ping => {
                let echo_op = if opcode == OpCode::Ping {
                    OpCode::Pong
                } else {
                    opcode
                };
                let hn = encode_header(&mut header_buf, true, echo_op, None, payload.len() as u64);
                if s.write_all(&header_buf[..hn]).is_err() {
                    return;
                }
                if s.write_all(&payload).is_err() {
                    return;
                }
            }
            OpCode::Close => {
                let hn = encode_header(&mut header_buf, true, OpCode::Close, None, 2);
                let _ = s.write_all(&header_buf[..hn]);
                let _ = s.write_all(&1000_u16.to_be_bytes());
                return;
            }
            OpCode::Pong | OpCode::Continuation => {}
        }
    }

    #[inline]
    fn upgrade_ws(s: &mut std::net::TcpStream) {
        use std::io::Read;
        let mut buf = [0_u8; 4096];
        let mut req = Vec::<u8>::new();
        loop {
            let n = s.read(&mut buf).expect("read upgrade");
            assert!(n > 0, "client closed mid-upgrade");
            req.extend_from_slice(&buf[..n]);
            if req.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let req_str = std::str::from_utf8(&req).expect("utf8");
        let key = req_str
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("sec-websocket-key:"))
            .and_then(|l| l.split(':').nth(1))
            .expect("Sec-WebSocket-Key")
            .trim();
        let accept = compute_accept(key);
        let resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        s.write_all(resp.as_bytes()).expect("write upgrade resp");
    }
}

#[cfg(target_os = "linux")]
pub fn read_frame(stream: &mut std::net::TcpStream) -> Option<(talaris::ws::OpCode, Vec<u8>)> {
    use std::io::Read;
    use talaris::ws::OpCode;
    use talaris::ws::mask::mask_inplace;

    let mut hdr = [0_u8; 2];
    if stream.read_exact(&mut hdr).is_err() {
        return None;
    }
    let fin = (hdr[0] & 0x80) != 0;
    if !fin {
        panic!("fragmented frames not supported in bench echo server");
    }
    let opcode = match hdr[0] & 0x0F {
        0x0 => OpCode::Continuation,
        0x1 => OpCode::Text,
        0x2 => OpCode::Binary,
        0x8 => OpCode::Close,
        0x9 => OpCode::Ping,
        0xA => OpCode::Pong,
        other => panic!("unsupported opcode 0x{other:x}"),
    };
    let masked = (hdr[1] & 0x80) != 0;
    let len_field = hdr[1] & 0x7F;
    let len: usize = if len_field < 126 {
        usize::from(len_field)
    } else if len_field == 126 {
        let mut b = [0_u8; 2];
        stream.read_exact(&mut b).ok()?;
        usize::from(u16::from_be_bytes(b))
    } else {
        let mut b = [0_u8; 8];
        stream.read_exact(&mut b).ok()?;
        usize::try_from(u64::from_be_bytes(b)).ok()?
    };
    let mut mask = [0_u8; 4];
    if masked {
        stream.read_exact(&mut mask).ok()?;
    }
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).ok()?;
    if masked {
        mask_inplace(&mut payload, mask);
    }
    Some((opcode, payload))
}

// ─── Async WS echo server（单 OS 线程驱动 N 条 conn，tokio current_thread）
//
// 给 pool_fanout 用。N 大（64+）时也只占用 1 个 CPU，client 端测量不被
// server-side 调度抖动污染。

#[cfg(target_os = "linux")]
pub fn spawn_ws_echo_server_multiplexed(
    std_listener: std::net::TcpListener,
    n_conns: u32,
    cpu: Option<usize>,
) -> std::thread::JoinHandle<()> {
    // tokio::net::TcpListener::from_std 要求 std listener 是 non-blocking
    std_listener
        .set_nonblocking(true)
        .expect("set_nonblocking on listener");

    std::thread::Builder::new()
        .name("ws-echo-mplex".into())
        .spawn(move || {
            let _g = cpu.map(|c| PinGuard::pin("ws-echo-mplex", c));
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
                    sessions.push(tokio::spawn(ws_echo_session_async(stream)));
                }
                for j in sessions {
                    let _ = j.await;
                }
            });
        })
        .expect("spawn ws-echo-mplex")
}

#[cfg(target_os = "linux")]
async fn ws_echo_session_async(mut s: tokio::net::TcpStream) {
    use talaris::ws::OpCode;
    use talaris::ws::frame::{MAX_HEADER_LEN, encode_header};
    use talaris::ws::handshake::compute_accept;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let _ = s.set_nodelay(true);

    // 1. HTTP upgrade
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
    let accept = compute_accept(&key);
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    if s.write_all(resp.as_bytes()).await.is_err() {
        return;
    }

    // 2. echo loop
    let mut header_buf = vec![0_u8; MAX_HEADER_LEN];
    loop {
        let mut hdr = [0_u8; 2];
        if s.read_exact(&mut hdr).await.is_err() {
            return;
        }
        let fin = (hdr[0] & 0x80) != 0;
        if !fin {
            return; // fragments not supported
        }
        let opcode_raw = hdr[0] & 0x0F;
        let masked = (hdr[1] & 0x80) != 0;
        let len_field = hdr[1] & 0x7F;
        let payload_len: usize = if len_field < 126 {
            usize::from(len_field)
        } else if len_field == 126 {
            let mut b = [0_u8; 2];
            if s.read_exact(&mut b).await.is_err() {
                return;
            }
            usize::from(u16::from_be_bytes(b))
        } else {
            let mut b = [0_u8; 8];
            if s.read_exact(&mut b).await.is_err() {
                return;
            }
            match usize::try_from(u64::from_be_bytes(b)) {
                Ok(v) => v,
                Err(_) => return,
            }
        };
        let mut mask = [0_u8; 4];
        if masked && s.read_exact(&mut mask).await.is_err() {
            return;
        }
        let mut payload = vec![0_u8; payload_len];
        if s.read_exact(&mut payload).await.is_err() {
            return;
        }
        if masked {
            talaris::ws::mask::mask_inplace(&mut payload, mask);
        }

        let echo_op = match opcode_raw {
            0x1 => OpCode::Text,
            0x2 => OpCode::Binary,
            0x9 => OpCode::Pong, // Ping → Pong
            0x8 => {
                let hn = encode_header(&mut header_buf, true, OpCode::Close, None, 2);
                let _ = s.write_all(&header_buf[..hn]).await;
                let _ = s.write_all(&1000_u16.to_be_bytes()).await;
                return;
            }
            _ => return,
        };
        let hn = encode_header(&mut header_buf, true, echo_op, None, payload.len() as u64);
        if s.write_all(&header_buf[..hn]).await.is_err() {
            return;
        }
        if s.write_all(&payload).await.is_err() {
            return;
        }
    }
}
