#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::panic
)]
//! Quickstart —— talaris Pool 从 0 到 1 走通的最小例子。
//!
//! 展示 3 件事：
//!   1. 怎样把 talaris 加进 Cargo.toml 并 `use` 起来；
//!   2. 怎样组装 [`talaris::Pool`] / [`talaris::connection::ConnectionConfig`]，
//!      跑通一条 plain WebSocket 连接；
//!   3. 怎样把 user 线程钉到指定 CPU，降低 scheduler migration 抖动。
//!
//! 运行：
//!   ```bash
//!   # 假设 CPU 1 被 isolcpus 隔离
//!   taskset -c 0-7 cargo run --release --example quickstart -- \
//!       --user-cpu 1
//!   ```
//!
//! 为了让例子离线、零依赖地跑通，这里在同进程内起一个最小 plain-WS echo
//! server（基于 std::net::TcpListener + talaris 自己暴露的 ws helper：
//! `compute_accept` / `encode_header` / `mask_inplace`）。生产里就把 host /
//! port / use_tls 换成真实的交易所 wss endpoint。
//!
//! ⚠️ talaris 是 Linux-only 的（io_uring）。macOS / Windows 上 stub 模块只让
//! crate 能 type-check，example 的 `main()` 会直接打印 "skipped" 退出。

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("talaris quickstart: skipped — io_uring 只在 Linux 上可用");
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    use talaris::connection::{ConnectionConfig, State};
    use talaris::proactor::pin_current_thread_to;
    use talaris::ws::Event as WsEvent;
    use talaris::{Pool, PoolConfig};

    // ── 0. 解析 user CPU 编号 ────────────────────────────────────────
    // 极简 CLI：`--user-cpu N`。失败时回退到 1。
    let user_cpu = parse_cpu_args();
    eprintln!("[quickstart] user thread → CPU {user_cpu}");
    eprintln!(
        "[quickstart] 提示：进程父 affinity 必须覆盖目标 CPU，建议外面套 \
         `taskset -c 0-N`"
    );

    // ── 1. 起一个 in-process plain-WS echo server，监听 127.0.0.1:<random> ──
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?;
    let addr = listener.local_addr()?;
    let (_shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
    let server = thread::Builder::new()
        .name("quickstart-echo".into())
        .spawn(move || ws_echo_server(&listener, &shutdown_rx))?;
    eprintln!("[quickstart] in-process WS echo server up on {addr}");

    // ── 2. 把 user 线程钉死在 user_cpu ───────────────────────────────
    // io_uring 的 user 端代码（Pool::pump、CQE drain）跑在这条线程上。在 hot
    // loop 启动前 pin 一次，避开 scheduler migration 抖动。
    pin_current_thread_to(user_cpu)?;

    // ── 3. 组装 ConnectionConfig ─────────────────────────────────────
    // 本地 echo 没 TLS，所以 with_tls(false)。生产对接交易所改回 true、port=443。
    let cfg = ConnectionConfig::new("localhost", addr.port(), "/echo").with_tls(false);

    // ── 4. 起 Pool 并阻塞 connect ───────────────────────────────────
    // PoolConfig 透传 proactor 配置（entries / CQ sizing / taskrun flags）。
    let mut pool = Pool::new(PoolConfig::new(cfg.proactor))?;
    let handle = pool.connect_blocking_to(cfg, addr)?;
    assert_eq!(pool.state(handle), Some(State::Open));
    eprintln!("[quickstart] WS upgrade OK, handle = {handle:?}");

    // ── 5. 发一条 text frame，pump 直到收到 echo ─────────────────────
    let payload = br#"{"ping":"talaris"}"#;
    let sent_at = Instant::now();
    pool.send_text(handle, payload)?;

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got_echo = false;
    while Instant::now() < deadline && !got_echo {
        pool.pump(|_h, ev| {
            if let WsEvent::Text(s) = ev {
                eprintln!("[quickstart] ← {s}");
                if s.contains("talaris") {
                    got_echo = true;
                }
            }
        })?;
    }
    let rtt = sent_at.elapsed();
    if got_echo {
        eprintln!("[quickstart] round-trip RTT (loopback) = {rtt:?}");
    } else {
        eprintln!("[quickstart] timed out waiting for echo");
    }

    // ── 6. 干净退出 —— 主动 Close handshake ─────────────────────────
    pool.initiate_close(handle, 1000, "bye")?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && !matches!(pool.state(handle), Some(State::Closed)) {
        let _ = pool.pump_nowait(|_, _| {});
    }
    server.join().expect("server thread");
    Ok(())
}

#[cfg(target_os = "linux")]
fn parse_cpu_args() -> usize {
    // 极简 --user-cpu 解析。缺省 1。
    let mut user_cpu = 1_usize;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a.as_str() == "--user-cpu" {
            if let Some(v) = args.next().and_then(|s| s.parse().ok()) {
                user_cpu = v;
            }
        }
    }
    user_cpu
}

// ───────────────────────────────────────────────────────────────────────
// 最小 plain-WS echo server：直接用 talaris 自己暴露的 ws helper。
// 单 client、单帧 echo 后退出。生产里你不会这么写——这只是给 quickstart 留
// 个离线 round-trip 对端。
// ───────────────────────────────────────────────────────────────────────
#[cfg(target_os = "linux")]
fn ws_echo_server(listener: &std::net::TcpListener, _shutdown: &std::sync::mpsc::Receiver<()>) {
    use std::io::{Read, Write};
    use talaris::ws::OpCode;
    use talaris::ws::frame::{MAX_HEADER_LEN, encode_header};
    use talaris::ws::handshake::compute_accept;
    use talaris::ws::mask::mask_inplace;

    let (mut s, _) = listener.accept().expect("accept");
    s.set_nodelay(true).expect("nodelay");

    // 1. HTTP upgrade
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

    // 2. 接 1 个 masked text frame
    let (opcode, payload) = read_one_frame(&mut s);
    assert_eq!(opcode, OpCode::Text);

    // 3. 同 payload echo（server → client 不 mask）
    let mut frame = vec![0_u8; MAX_HEADER_LEN];
    let hn = encode_header(&mut frame, true, OpCode::Text, None, payload.len() as u64);
    frame.truncate(hn);
    frame.extend_from_slice(&payload);
    s.write_all(&frame).expect("write echo");

    // 4. 等 client 主动 Close，echo Close 帧后退出
    let (close_op, _) = read_one_frame(&mut s);
    assert_eq!(close_op, OpCode::Close);
    let mut close_frame = vec![0_u8; MAX_HEADER_LEN + 2];
    let hn = encode_header(&mut close_frame, true, OpCode::Close, None, 2);
    close_frame.truncate(hn);
    close_frame.extend_from_slice(&1000_u16.to_be_bytes());
    let _ = s.write_all(&close_frame);
    // 同步 mask 引用使 helper not dead
    let _ = mask_inplace;
}

#[cfg(target_os = "linux")]
fn read_one_frame(stream: &mut std::net::TcpStream) -> (talaris::ws::OpCode, Vec<u8>) {
    use std::io::Read;
    use talaris::ws::OpCode;
    use talaris::ws::mask::mask_inplace;

    let mut hdr = [0_u8; 2];
    stream.read_exact(&mut hdr).expect("hdr");
    let fin = (hdr[0] & 0x80) != 0;
    assert!(fin, "fragmented frame not supported in quickstart server");
    let opcode = match hdr[0] & 0x0F {
        0x1 => OpCode::Text,
        0x2 => OpCode::Binary,
        0x8 => OpCode::Close,
        0x9 => OpCode::Ping,
        0xA => OpCode::Pong,
        other => panic!("unsupported opcode 0x{other:x}"),
    };
    let masked = (hdr[1] & 0x80) != 0;
    let len_field = hdr[1] & 0x7F;
    let len: usize = match len_field {
        0..=125 => usize::from(len_field),
        126 => {
            let mut b = [0_u8; 2];
            stream.read_exact(&mut b).expect("len16");
            usize::from(u16::from_be_bytes(b))
        }
        127 => {
            let mut b = [0_u8; 8];
            stream.read_exact(&mut b).expect("len64");
            usize::try_from(u64::from_be_bytes(b)).expect("u64→usize")
        }
        _ => unreachable!("len_field is masked to 7 bits"),
    };
    let mut mask = [0_u8; 4];
    if masked {
        stream.read_exact(&mut mask).expect("mask key");
    }
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).expect("payload");
    if masked {
        mask_inplace(&mut payload, mask);
    }
    (opcode, payload)
}
