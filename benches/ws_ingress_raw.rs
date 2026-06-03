// ws_ingress_raw —— 绕开 Pool + WsClient，直接走 Proactor + BufferRing 收 WS
// Binary 帧。和 ws_ingress_single (Pool) / tokio 对照，定位 single-conn high-rate
// inbound 慢 2× 是哪一层的锅。
//
// ## 三个怀疑点（自顶向下）
//
// 1. **Pool routing layer**: `pump_impl` 每轮 iterate conn slots，submit/rearm，
//    drain CQE 到中间 Vec，再 dispatch。即使 N=1 也走整套。
// 2. **WsClient 全状态机**: fragmentation / control auto-pong / close handshake
//    每帧 +20-30 ns 开销，bench 只跑 Binary 流用不上。
// 3. **BufferRing entry size = 4 KiB**：server 一次 push 63 KiB chunk → kernel
//    切 16 个 buffer entry → posts 16 个 CQE → user 走 16 轮 drain。
//
// 本 bench 直接用 `Proactor` + `BufferRing` + `parse_header`，**跳过 1 和 2**：
//
// - `--buf-size 4096` → 跟 Pool 同 buf_ring 配置（lib 内是 const）。如果跟 Pool
//   benchmark 跑出同样的 13M f/s → 慢的不是 1+2 → 单看 3。
// - `--buf-size 65536` → 一次 chunk 一个 CQE。如果跳到 tokio 量级 → 3 是元凶。
// - `--buf-size 4096` 仍接近 tokio → 1+2 才是元凶。
//
// 三种结果都给我们指向不同的优化方向。
//
// ## 运行
//
// ```bash
// # 4 KiB（同 Pool default）：
// taskset -c 0-7 cargo bench --bench ws_ingress_raw -- --frames 1000000 --payload 64 --buf-size 4096
//
// # 16 KiB / 64 KiB 看 chunk-per-CQE 拉满后的效果：
// taskset -c 0-7 cargo bench --bench ws_ingress_raw -- --frames 1000000 --payload 64 --buf-size 16384
// taskset -c 0-7 cargo bench --bench ws_ingress_raw -- --frames 1000000 --payload 64 --buf-size 65536
// ```

#![allow(
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
    clippy::similar_names,
    clippy::too_many_lines
)]

#[path = "common/mod.rs"]
mod common;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("ws_ingress_raw: skipped — io_uring 只在 Linux 上可用");
}

#[cfg(target_os = "linux")]
fn main() {
    linux_impl::run();
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use talaris::proactor::{
        BufferRing, Domain, OpKind, Proactor, ProactorConfig, SockAddr, SqeFlags, TcpSocket,
        UserData,
    };
    use talaris::ws::frame::parse_header;

    use super::common;
    use super::common::{PinGuard, StopMode};

    const BGID: u16 = 7;

    pub fn run() {
        let stop = StopMode::from_args(1_000_000);
        let payload: usize = common::arg_or("--payload", 64);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let user_cpu: usize = common::arg_or("--user-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
        let buf_size: u32 = common::arg_or("--buf-size", 4096);
        let buf_entries: u16 = common::arg_or("--buf-entries", 256);
        let staging = common::BenchStagingConfig::from_args();

        if !buf_entries.is_power_of_two() || buf_entries == 0 {
            eprintln!("[bench] --buf-entries must be a non-zero power of 2; got {buf_entries}");
            std::process::exit(2);
        }
        if buf_size == 0 {
            eprintln!("[bench] --buf-size must be > 0");
            std::process::exit(2);
        }

        eprintln!("=========================================================");
        eprintln!(" ws_ingress_raw — Proactor + BufferRing direct (no Pool/WsClient)");
        eprintln!("=========================================================");
        eprintln!(" stop      : {}", stop.describe());
        eprintln!(" payload   : {payload}B");
        eprintln!(
            " buf_ring  : {buf_entries} × {buf_size}B = {} KiB pool",
            (u32::from(buf_entries) * buf_size) / 1024
        );
        eprintln!(" server-cpu: {server_cpu}");
        eprintln!(" user      : CPU {user_cpu}, SQ_POLL→CPU {sq_poll_cpu}");
        staging.print_stderr(" ");
        eprintln!();

        let frames_per_chunk =
            common::frames_per_chunk_for_bytes(payload, staging.server_chunk_bytes);
        let chunk_buf = Arc::new(common::pre_encode_ws_binary_chunk(
            payload,
            frames_per_chunk,
        ));
        eprintln!(
            "[bench] server chunk: {frames_per_chunk} frames × {payload}B = {} KiB",
            chunk_buf.len() / 1024
        );

        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server = common::spawn_ws_stream_server(listener, 1, chunk_buf, Some(server_cpu));
        eprintln!("[bench] fresh stream server on {addr}");

        let outcome = run_raw(
            addr,
            stop,
            payload,
            user_cpu,
            sq_poll_cpu,
            buf_size,
            buf_entries,
            staging,
        );
        server.join().expect("server panic");

        println!();
        println!("=== ws_ingress_raw (payload={payload}B, buf_size={buf_size}B) ===");
        println!();
        println!(
            "{:<14} │ {:>14} │ {:>10} │ {:>14} │ {:>11}",
            "variant", "frames", "elapsed", "frames/s", "MiB/s"
        );
        println!("{}", "─".repeat(72));
        println!(
            "{:<14} │ {:>14} │ {:>9.3}s │ {:>14} │ {:>11.2}",
            format!("raw (buf={buf_size})"),
            fmt_int(outcome.frames),
            outcome.elapsed.as_secs_f64(),
            fmt_int((outcome.frames as f64 / outcome.elapsed.as_secs_f64()) as u64),
            (outcome.frames as f64 * payload as f64)
                / outcome.elapsed.as_secs_f64()
                / (1024.0 * 1024.0),
        );

        println!();
        println!("=== inter-arrival latency ===");
        common::print_hist("raw", &outcome.inter_arrival);
        println!();
        println!("for comparison reference, run ws_ingress_single and ws_framing too.");
    }

    struct Outcome {
        frames: u64,
        elapsed: Duration,
        inter_arrival: hdrhistogram::Histogram<u64>,
    }

    fn run_raw(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
        buf_size: u32,
        buf_entries: u16,
        staging: common::BenchStagingConfig,
    ) -> Outcome {
        let _guard = PinGuard::pin("raw", user_cpu);
        eprintln!("[raw] user→CPU {user_cpu}, SQ_POLL kthread→CPU {sq_poll_cpu}");

        let cfg = ProactorConfig {
            entries: 256,
            sq_poll_idle_ms: Some(10_000),
            sq_poll_cpu: Some(sq_poll_cpu),
        };
        let mut proactor = Proactor::new(cfg).expect("proactor");

        let mut buf_ring =
            BufferRing::new(&mut proactor, BGID, buf_entries, buf_size).expect("buf_ring");

        // ── socket + connect via uring ───────────────────────────────────
        let sock = TcpSocket::new(Domain::V4).expect("socket");
        sock.set_nodelay(true).expect("nodelay");
        let fd = sock.as_raw_fd();
        let sock_addr = SockAddr::from_std(addr);
        let ud_connect = UserData::new(OpKind::Connect, 0);
        // SAFETY: sock_addr / sock 都活到 fn 末尾
        unsafe {
            proactor
                .submit_connect(fd, &sock_addr, ud_connect, SqeFlags::NONE)
                .expect("submit_connect");
        }
        proactor.submit_and_wait(1).expect("connect wait");
        let mut connect_result: Option<i32> = None;
        proactor.drain_completions(|c| connect_result = Some(c.result));
        assert_eq!(connect_result, Some(0), "connect failed");

        // ── WS upgrade (manual) ──────────────────────────────────────────
        let key = talaris::ws::handshake::generate_key().expect("ws key");
        let req = format!(
            "GET / HTTP/1.1\r\n\
             Host: localhost\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\n\
             Sec-WebSocket-Version: 13\r\n\r\n"
        );
        let req_bytes = req.into_bytes();
        let ud_send = UserData::new(OpKind::Send, 0);
        // SAFETY: req_bytes 活到 send CQE 之后
        unsafe {
            proactor
                .submit_send(
                    fd,
                    req_bytes.as_ptr(),
                    req_bytes.len() as u32,
                    ud_send,
                    SqeFlags::NONE,
                )
                .expect("send upgrade");
        }
        proactor.submit_and_wait(1).expect("send wait");
        proactor.drain_completions(|_| {});

        // Recv upgrade response 走 regular recv（不是 multishot）—— 这里就一次性
        // I/O，没必要走 multishot；而且我们要把 `\r\n\r\n` 后顺手读到的 WS payload
        // 字节捞出来当 leftover 起始。
        let mut tmp = vec![0_u8; 4096];
        let mut resp = Vec::<u8>::new();
        let leftover_bytes: Vec<u8>;
        loop {
            let ud_recv = UserData::new(OpKind::Recv, 99);
            // SAFETY: tmp 活在 fn 内
            unsafe {
                proactor
                    .submit_recv(
                        fd,
                        tmp.as_mut_ptr(),
                        tmp.len() as u32,
                        ud_recv,
                        SqeFlags::NONE,
                    )
                    .expect("recv upgrade");
            }
            proactor.submit_and_wait(1).expect("recv wait");
            let mut n_read = 0_usize;
            proactor.drain_completions(|c| {
                n_read = c.to_result().expect("recv ok");
            });
            assert!(n_read > 0, "EOF mid-handshake");
            resp.extend_from_slice(&tmp[..n_read]);
            if let Some(idx) = resp.windows(4).position(|w| w == b"\r\n\r\n") {
                let header_end = idx + 4;
                leftover_bytes = resp[header_end..].to_vec();
                break;
            }
        }
        eprintln!(
            "[raw] upgrade OK; leftover after header = {} bytes",
            leftover_bytes.len()
        );

        // ── Arm multishot recv on the buffer ring ────────────────────────
        let ud_ms = UserData::new(OpKind::Recv, 0);
        // SAFETY: fd 已 connected，buf_ring 已 registered
        unsafe {
            proactor
                .submit_recv_multishot(fd, BGID, ud_ms)
                .expect("arm multishot");
        }
        proactor.submit().expect("submit");

        // ── Recv loop ────────────────────────────────────────────────────
        let mut arrivals: Vec<Instant> = Vec::with_capacity(stop.cap_hint());
        let mut frame_count = 0_u64;
        let mut leftover = leftover_bytes;
        leftover.reserve(staging.leftover_capacity);
        let mut multishot_armed = true;
        let mut chunks: Vec<(u16, usize)> =
            Vec::with_capacity(staging.raw_completion_batch_capacity);

        let bench_start = Instant::now();

        'outer: loop {
            // Parse all complete frames in leftover
            let mut pos = 0_usize;
            while pos < leftover.len() {
                match parse_header(&leftover[pos..]) {
                    Ok(Some((hdr, consumed))) => {
                        let total = consumed + hdr.payload_len as usize;
                        if leftover.len() - pos < total {
                            break;
                        }
                        debug_assert_eq!(hdr.payload_len as usize, payload);
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
                        eprintln!("[raw] parse_header err after {frame_count}: {e}");
                        leftover.drain(..pos);
                        break 'outer;
                    }
                }
            }
            leftover.drain(..pos);

            if !stop.keep_going(frame_count, bench_start) {
                break;
            }

            if !multishot_armed {
                let ud_rearm = UserData::new(OpKind::Recv, 0);
                // SAFETY: fd 仍有效
                unsafe {
                    proactor
                        .submit_recv_multishot(fd, BGID, ud_rearm)
                        .expect("rearm multishot");
                }
                proactor.submit().expect("submit");
                multishot_armed = true;
            }

            proactor.wait_for_cqe(1).expect("wait cqe");

            // 把所有 ready CQE 一口气吃干净
            let mut rearm_needed = false;
            chunks.clear();
            proactor.drain_completions(|c| {
                let Ok(res) = c.to_result() else {
                    // -ENOBUFS：multishot 在 buffer ring 空时退出，has_more=false，
                    // 主循环检测到后 re-arm。loopback 高速场景下偶尔会跑出来。
                    if !c.has_more() {
                        rearm_needed = true;
                    }
                    return;
                };
                if res == 0 {
                    if !c.has_more() {
                        rearm_needed = true;
                    }
                    return;
                }
                if let Some(bid) = c.buffer_id() {
                    chunks.push((bid, res));
                }
                if !c.has_more() {
                    rearm_needed = true;
                }
            });

            // 复制每个 chunk 到 leftover，然后 recycle 对应 bid
            for &(bid, n) in &chunks {
                let slice = &buf_ring.buffer(bid)[..n];
                leftover.extend_from_slice(slice);
                buf_ring.recycle(bid);
            }
            if rearm_needed {
                multishot_armed = false;
            }
        }

        let elapsed = bench_start.elapsed();
        eprintln!(
            "[raw] {} frames in {:.3}s ({:.0} f/s)",
            frame_count,
            elapsed.as_secs_f64(),
            frame_count as f64 / elapsed.as_secs_f64()
        );

        // ── 清理：unregister buf_ring 后同步 close ───────────────────────
        // 已离开测量窗口，不需要为了 cleanup 再提交异步 Close SQE。
        let _ = buf_ring.unregister(&mut proactor);
        drop(sock);

        let inter_arrival = common::inter_arrival_hist(&arrivals);
        Outcome {
            frames: frame_count,
            elapsed,
            inter_arrival,
        }
    }

    fn fmt_int(n: u64) -> String {
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
}
