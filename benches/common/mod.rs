// 多个 bench 共享代码 —— histogram 打印、CLI 解析、in-process WS echo server。
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

use hdrhistogram::Histogram;

// ─── HdrHistogram helper ────────────────────────────────────────────────

/// 一个延迟 bench 通用的 histogram bound：1 ns … 60 s, 3 位有效数字。
///
/// 60 s 上界是为了"假死"的 outlier 也不丢；HdrHistogram 实际存储是 bucket，
/// 上界开得高也不消耗多少内存（log 增长）。
pub fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("hist")
}

/// 把一个 `std::time::Duration` 安全转 ns 喂给 hist.record。负数（不可能）/
/// 0（hist 拒收）做防御性 clamp 到 1。
pub fn record_ns(hist: &mut Histogram<u64>, dt: std::time::Duration) {
    let ns = u64::try_from(dt.as_nanos().min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
    hist.record(ns.max(1)).ok();
}

/// 单 histogram 单行打印。给单 variant bench 用。
pub fn print_hist(label: &str, h: &Histogram<u64>) {
    println!(
        "{:<20} mean={:>10}  p50={:>10}  p99={:>10}  p99.9={:>10}  max={:>10}  n={}",
        label,
        ns(h.mean() as u64),
        ns(h.value_at_quantile(0.50)),
        ns(h.value_at_quantile(0.99)),
        ns(h.value_at_quantile(0.999)),
        ns(h.max()),
        h.len(),
    );
}

/// 多 variant 并列：用于 talaris vs tokio / SQ_POLL on/off 等对照。
/// 第一个 entry 当作 baseline，其它列出 ratio = baseline / self。
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
        print!(" │ {label:>16}");
    }
    if rows.len() > 1 {
        print!(" │ ratio vs first");
    }
    println!();

    print!("{:<10}", "─".repeat(10));
    for _ in rows {
        print!("─┼─{:>16}", "─".repeat(16));
    }
    if rows.len() > 1 {
        print!("─┼─{:>14}", "─".repeat(14));
    }
    println!();

    for (col_name, extract) in cols {
        print!("{col_name:<10}");
        let mut first: Option<u64> = None;
        for (_, h) in rows {
            let v = extract(h);
            print!(" │ {:>16}", ns(v));
            if first.is_none() {
                first = Some(v);
            }
        }
        if rows.len() > 1 {
            // ratio: 后面 variant 相对第一个的倍率（>1 = 慢；<1 = 快）。这里
            // 只展示最后一个 variant 的 ratio，多个 variant 时按需扩展。
            let (_, last_h) = rows[rows.len() - 1];
            let last = extract(last_h);
            let first = first.unwrap_or(1).max(1);
            print!(" │ {:>13.2}×", last as f64 / first as f64);
        }
        println!();
    }
    println!("samples: {:?}", rows.iter().map(|(_, h)| h.len()).collect::<Vec<_>>());
}

/// 把 ns 加千分位逗号格式化成 "1,234,567 ns"
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
//
// 不引入 clap —— bench 是给开发者跑的，5 个 flag 一个 while-loop 搞定。

/// 提取 `--key value` 形式的 flag。多次出现取最后一个。返回 None 表示用户没传。
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

// ─── 线程 pin（带 warn fallback）───────────────────────────────────────

#[cfg(target_os = "linux")]
pub fn pin_or_warn(label: &str, cpu: usize) {
    if let Err(e) = talaris::proactor::pin_current_thread_to(cpu) {
        eprintln!("[{label}] pin to CPU {cpu} failed: {e}; continuing unpinned");
    }
}

// ─── 通用 TCP echo server（bytes-in → bytes-out 循环到 peer 关）───────

#[cfg(target_os = "linux")]
pub fn run_tcp_echo_once(listener: std::net::TcpListener, cpu: Option<usize>) {
    use std::io::{Read, Write};
    if let Some(cpu) = cpu {
        pin_or_warn("tcp-echo", cpu);
    }
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

/// 连续 echo `sessions` 个 client；用于一个 server 顺序服务多个 variant。
#[cfg(target_os = "linux")]
pub fn run_tcp_echo_sessions(
    listener: std::net::TcpListener,
    cpu: Option<usize>,
    sessions: u32,
) {
    use std::io::{Read, Write};
    if let Some(cpu) = cpu {
        pin_or_warn("tcp-echo", cpu);
    }
    for _ in 0..sessions {
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
}

// ─── 通用 WS echo server（loop echo 直到 peer 发 Close 或 EOF）─────────
//
// 直接用 talaris 自己暴露的 frame / handshake / mask helper。pool_ws_echo 和
// pool_fanout 都用这套。
//
// 协议覆盖：HTTP upgrade → 循环 (read masked Text/Binary/Ping → echo same opcode
// 不 mask；read Close → echo Close → return)；fragmented frame 不支持。

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
            None => return, // peer EOF
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
                // echo Close (code 1000) then bail
                let hn = encode_header(&mut header_buf, true, OpCode::Close, None, 2);
                let _ = s.write_all(&header_buf[..hn]);
                let _ = s.write_all(&1000_u16.to_be_bytes());
                return;
            }
            OpCode::Pong | OpCode::Continuation => {
                // 不期望 client 发这两个；忽略
            }
        }
    }
    // helper 直接 inline 在 fn 内，避免外露
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

/// 读 1 个 client→server 帧（masked）。EOF / 任何 IO 错误返回 None。
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

/// 起一个 WS echo server 线程，accept `sessions` 个 client 后退出。
#[cfg(target_os = "linux")]
pub fn spawn_ws_echo_server(
    listener: std::net::TcpListener,
    cpu: Option<usize>,
    sessions: u32,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("bench-ws-echo".into())
        .spawn(move || {
            if let Some(cpu) = cpu {
                pin_or_warn("ws-echo", cpu);
            }
            for _ in 0..sessions {
                let (s, _) = listener.accept().expect("accept");
                run_ws_echo_session(s);
            }
        })
        .expect("spawn ws-echo")
}
