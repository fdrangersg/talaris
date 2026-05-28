// example / bench 二进制 —— 整文件用 unwrap / expect / 强制断言 / 简单 cast 是
// 设计选择（不进 lib，崩了重启没事）。crate 顶层 lint 是 lib 的 HFT 守门规则，
// 不该用同一把尺子量这里。
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
    clippy::module_name_repetitions,
    clippy::too_many_lines,
    clippy::struct_excessive_bools,
    clippy::similar_names,
    clippy::semicolon_if_nothing_returned
)]
//! Benchmark —— talaris (io_uring proactor + SQ_POLL + pinned) vs tokio (epoll
//! reactor + current_thread runtime + pinned)，TCP echo round-trip。
//!
//! ## 范围
//!
//! 公平起见两侧都跑 raw TCP echo（不带 WS framing）：
//! - 测的是 **IO model**：io_uring proactor vs tokio epoll reactor；
//! - 协议层做完全相同的事——`write_all(N B)` → `read_exact(N B)` 一来回；
//! - 服务端是同一个 std::net::TcpListener thread，对两侧 client 完全对称。
//!
//! 因为 talaris 的 lib API 只暴露 WS 层 `Pool`，没有 raw TCP 客户端，这里把
//! `Proactor` + `TcpSocket` 直接拼了一条 "TCP echo client"。这部分是 bench
//! 临时造的轮子，**不会进 lib**；不代表 talaris 的对外 API 风格。
//!
//! ## 拓扑（默认值匹配 ripple-testnet-tokyo `isolcpus=1-5`，8 vCPU SMT pairs
//! (0,4) (1,5) (2,6) (3,7)）：
//!
//! ```text
//!   CPU 0  6  7  ← OS noise (非 isolated)
//!   CPU 1  ← talaris user thread (isolated)
//!   CPU 5  ← talaris SQ_POLL kthread (sibling of 1, isolated) —— L1/L2 共享
//!   CPU 2  ← tokio worker thread (isolated, sibling 6 没 isolated → 安静)
//!   CPU 4  ← echo server (isolated)
//!   CPU 3  ← spare
//! ```
//!
//! 两侧 client 跑在不同物理核，server 跑在第三个物理核，互不抢 cache。
//!
//! ## 运行
//!
//! ```bash
//! cargo run --release --example bench_vs_tokio
//!
//! # 自定义：
//! cargo run --release --example bench_vs_tokio -- \
//!     --iters 200000 --payload 64 \
//!     --server-cpu 4 --talaris-cpu 1 --sq-poll-cpu 5 --tokio-cpu 2
//! ```
//!
//! ## 输出
//!
//! 单边 RTT 直方图（HdrHistogram，3 位有效数字）：
//! - mean / p50 / p99 / p99.9 / max
//! - 两侧并列打印，方便人眼对比

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("bench_vs_tokio: skipped — io_uring 只在 Linux 上可用");
}

#[cfg(target_os = "linux")]
fn main() {
    linux_impl::run();
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
    use std::thread;
    use std::time::Instant;

    use hdrhistogram::Histogram;
    use talaris::proactor::{
        Completion, Domain, OpKind, Proactor, ProactorConfig, SockAddr, SqeFlags, TcpSocket,
        UserData, pin_current_thread_to, unpin_current_thread,
    };

    struct Args {
        iters: u64,
        warmup: u64,
        payload: usize,
        server_cpu: usize,
        talaris_cpu: usize,
        sq_poll_cpu: u32,
        tokio_cpu: usize,
    }

    impl Default for Args {
        fn default() -> Self {
            // 默认对 ripple-testnet-tokyo (isolcpus=1-5) 调好。换机器请覆盖。
            Self {
                iters: 100_000,
                warmup: 5_000,
                payload: 64,
                server_cpu: 4,
                talaris_cpu: 1,
                sq_poll_cpu: 5,
                tokio_cpu: 2,
            }
        }
    }

    fn parse_args() -> Args {
        let mut a = Args::default();
        let mut it = std::env::args().skip(1);
        while let Some(k) = it.next() {
            let v: Option<String> = it.next();
            let parse_usize = |v: &Option<String>| v.as_deref().and_then(|s| s.parse().ok());
            let parse_u64 = |v: &Option<String>| v.as_deref().and_then(|s| s.parse().ok());
            let parse_u32 = |v: &Option<String>| v.as_deref().and_then(|s| s.parse().ok());
            match k.as_str() {
                "--iters" => a.iters = parse_u64(&v).unwrap_or(a.iters),
                "--warmup" => a.warmup = parse_u64(&v).unwrap_or(a.warmup),
                "--payload" => a.payload = parse_usize(&v).unwrap_or(a.payload),
                "--server-cpu" => a.server_cpu = parse_usize(&v).unwrap_or(a.server_cpu),
                "--talaris-cpu" => a.talaris_cpu = parse_usize(&v).unwrap_or(a.talaris_cpu),
                "--sq-poll-cpu" => a.sq_poll_cpu = parse_u32(&v).unwrap_or(a.sq_poll_cpu),
                "--tokio-cpu" => a.tokio_cpu = parse_usize(&v).unwrap_or(a.tokio_cpu),
                _ => {}
            }
        }
        a
    }

    pub fn run() {
        let args = parse_args();
        eprintln!(
            "[bench] iters={} warmup={} payload={}B \n\
             [bench] server-cpu={} talaris-cpu={} sq-poll-cpu={} tokio-cpu={}",
            args.iters,
            args.warmup,
            args.payload,
            args.server_cpu,
            args.talaris_cpu,
            args.sq_poll_cpu,
            args.tokio_cpu,
        );

        // ── 起 echo server：accept 两次，按顺序服务 talaris 然后 tokio ───
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .expect("bind echo server");
        let addr = listener.local_addr().expect("local_addr");
        let server_cpu = args.server_cpu;
        let server_handle = thread::Builder::new()
            .name("bench-echo-srv".into())
            .spawn(move || run_echo_server(listener, server_cpu, /* sessions */ 2))
            .expect("spawn server");
        eprintln!("[bench] echo server listening on {addr}");

        // ── talaris (proactor) variant ──────────────────────────────────
        let talaris_cpu = args.talaris_cpu;
        let sq_cpu = args.sq_poll_cpu;
        let iters = args.iters;
        let warmup = args.warmup;
        let payload = args.payload;
        let talaris_hist = thread::Builder::new()
            .name("bench-talaris".into())
            .spawn(move || run_talaris(addr, iters, warmup, payload, talaris_cpu, sq_cpu))
            .expect("spawn talaris")
            .join()
            .expect("talaris panic");

        // ── tokio variant ────────────────────────────────────────────────
        let tokio_cpu = args.tokio_cpu;
        let tokio_hist = thread::Builder::new()
            .name("bench-tokio".into())
            .spawn(move || run_tokio(addr, iters, warmup, payload, tokio_cpu))
            .expect("spawn tokio")
            .join()
            .expect("tokio panic");

        server_handle.join().expect("server panic");

        print_comparison(&talaris_hist, &tokio_hist);
    }

    /// 简陋 echo server：接 `sessions` 个连接，每个 read→write loop 直到 peer
    /// EOF。pinned 到 `cpu` 让 server 抖动不影响 client 测量。
    fn run_echo_server(listener: TcpListener, cpu: usize, sessions: u32) {
        if let Err(e) = pin_current_thread_to(cpu) {
            eprintln!("[server] pin failed: {e}; continuing unpinned");
        }
        for sid in 0..sessions {
            let (mut s, _) = listener.accept().expect("accept");
            s.set_nodelay(true).expect("nodelay");
            // 256 KiB buffer 覆盖任何 payload；server 不假设 payload size。
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
            eprintln!("[server] session {sid} done");
        }
    }

    // ─── talaris (io_uring + SQ_POLL + pinned) ─────────────────────────────

    fn run_talaris(
        addr: SocketAddr,
        iters: u64,
        warmup: u64,
        payload_size: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
    ) -> Histogram<u64> {
        if let Err(e) = pin_current_thread_to(user_cpu) {
            eprintln!("[talaris] pin failed: {e}");
        }
        eprintln!("[talaris] user thread pinned to CPU {user_cpu}, SQ_POLL kthread → CPU {sq_poll_cpu}");

        let proactor_cfg = ProactorConfig {
            entries: 64,
            sq_poll_idle_ms: Some(10_000),
            sq_poll_cpu: Some(sq_poll_cpu),
        };
        let mut proactor = Proactor::new(proactor_cfg).expect("proactor");

        // socket + connect via uring
        let sock = TcpSocket::new(Domain::V4).expect("socket");
        sock.set_nodelay(true).expect("nodelay");
        let fd = sock.as_raw_fd();
        let sock_addr = SockAddr::from_std(addr);

        let connect_ud = UserData::new(OpKind::Connect, 0);
        // SAFETY: sock_addr / sock 都活到 connect CQE 拿到之后 (在 fn 结尾才 drop)
        unsafe {
            proactor
                .submit_connect(fd, &sock_addr, connect_ud, SqeFlags::NONE)
                .expect("submit_connect");
        }
        proactor.submit_and_wait(1).expect("submit connect");
        let mut got: Option<Completion> = None;
        proactor.drain_completions(|c| got = Some(c));
        let c = got.expect("connect CQE");
        c.to_result().expect("connect ok");

        // 单 send + 单 recv buffer 全程复用，避免每轮分配。
        let mut send_buf = vec![0_u8; payload_size];
        let mut recv_buf = vec![0_u8; payload_size];
        // 初始 payload：4 字节递增计数器 + 0 padding。每轮把计数器写进去。
        for (i, b) in send_buf.iter_mut().enumerate() {
            *b = i as u8;
        }

        // 一次 RTT = (submit_send + submit_recv) 推到 SQ → submit + wait(2 CQE)
        //     → drain 两个 CQE 验证成功。
        //
        // SQ_POLL 模式下 `submit()` 多数是 cacheline-store（kthread 还在 spin），
        // 真正进 syscall 的只有 wait_for_cqe(2)。对照 tokio 那边的
        // write_all().await + read_exact().await（两个 syscall + epoll wait），
        // 这就是 IO model 的核心差异点。
        let mut hist: Histogram<u64> = Histogram::new_with_bounds(1, 60_000_000, 3).expect("hist");

        let mut iter = 0_u64;
        let total = iters + warmup;
        while iter < total {
            // 把计数器序号 patch 进 payload 头 8 字节，避免 echo 被路由错也能 detect
            let token = iter.to_le_bytes();
            send_buf[..token.len()].copy_from_slice(&token);

            let t0 = Instant::now();

            let send_ud = UserData::new(OpKind::Send, iter);
            let recv_ud = UserData::new(OpKind::Recv, iter);
            // 两个 SQE 同时 batch 进 SQ ring；不加 IO_LINK，让 kernel 可以并行
            // 推 send 完成同时 recv 已经在等数据回来——loopback 上少一次切换。
            // SAFETY: send_buf/recv_buf 整轮存活；fd 已 connected。
            unsafe {
                proactor
                    .submit_send(
                        fd,
                        send_buf.as_ptr(),
                        payload_size as u32,
                        send_ud,
                        SqeFlags::NONE,
                    )
                    .expect("submit_send");
                proactor
                    .submit_recv(
                        fd,
                        recv_buf.as_mut_ptr(),
                        payload_size as u32,
                        recv_ud,
                        SqeFlags::NONE,
                    )
                    .expect("submit_recv");
            }

            // SQ_POLL 关键点：submit() 不进 syscall（多数情况），wait_for_cqe 才进。
            proactor.submit().expect("submit");
            // 等两个 CQE（send + recv）
            let mut got = 0_u32;
            while got < 2 {
                proactor.wait_for_cqe(1).expect("wait cqe");
                proactor.drain_completions(|c| {
                    got += 1;
                    let res = c.to_result().expect("op ok");
                    match c.user_data.kind() {
                        Some(OpKind::Send) => assert_eq!(res, payload_size, "short send"),
                        Some(OpKind::Recv) => assert_eq!(res, payload_size, "short recv"),
                        other => panic!("unexpected CQE kind {other:?}"),
                    }
                });
            }
            // 验证 echo
            debug_assert_eq!(&recv_buf[..token.len()], &token);

            let dt = t0.elapsed();
            if iter >= warmup {
                let ns = dt.as_nanos().min(u128::from(u64::MAX)) as u64;
                hist.record(ns.max(1)).ok();
            }
            iter += 1;
        }

        // 关闭连接（让 server 退出 read loop）
        // SAFETY: 没有 OwnedFd wrapper 再持有 fd —— TcpSocket 还活着，但我们要先
        // submit_close 走 io_uring 关掉它。这里直接 forget 后再 submit_close_raw
        // 避免双 close。
        let raw_fd = sock.as_raw_fd();
        std::mem::forget(sock);
        let close_ud = UserData::new(OpKind::Close, 0);
        unsafe {
            proactor
                .submit_close_raw(raw_fd, close_ud)
                .expect("submit_close");
        }
        proactor.submit_and_wait(1).expect("close");
        proactor.drain_completions(|_| {});

        // 还原 affinity，让后续 thread 不继承本线程残留 mask（虽然新 thread 用
        // Builder spawn 一般会从父继承，pin 是按 thread 的；这里防御性写一下）
        let _ = unpin_current_thread();
        hist
    }

    // ─── tokio (epoll + current_thread + pinned) ─────────────────────────

    fn run_tokio(
        addr: SocketAddr,
        iters: u64,
        warmup: u64,
        payload_size: usize,
        user_cpu: usize,
    ) -> Histogram<u64> {
        if let Err(e) = pin_current_thread_to(user_cpu) {
            eprintln!("[tokio] pin failed: {e}");
        }
        eprintln!("[tokio] worker thread pinned to CPU {user_cpu}");

        // 单线程 runtime——和 talaris 一样单线程，避免 multi-thread runtime 引入
        // work-stealing 调度噪声。
        // 只开 IO；不开 time / signal —— bench loop 用不到，少一份 reactor 噪声。
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("rt build");

        let hist = rt.block_on(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio::net::TcpStream;

            let mut stream = TcpStream::connect(addr).await.expect("connect");
            stream.set_nodelay(true).expect("nodelay");

            let mut send_buf = vec![0_u8; payload_size];
            for (i, b) in send_buf.iter_mut().enumerate() {
                *b = i as u8;
            }
            let mut recv_buf = vec![0_u8; payload_size];

            let mut hist: Histogram<u64> =
                Histogram::new_with_bounds(1, 60_000_000, 3).expect("hist");

            let total = iters + warmup;
            for iter in 0..total {
                let token = iter.to_le_bytes();
                send_buf[..token.len()].copy_from_slice(&token);

                let t0 = Instant::now();
                stream.write_all(&send_buf).await.expect("write");
                stream.read_exact(&mut recv_buf).await.expect("read");
                let dt = t0.elapsed();

                debug_assert_eq!(&recv_buf[..token.len()], &token);

                if iter >= warmup {
                    let ns = dt.as_nanos().min(u128::from(u64::MAX)) as u64;
                    hist.record(ns.max(1)).ok();
                }
            }

            // 显式 shutdown，让 server 这条 session 干净结束
            stream.shutdown().await.ok();
            hist
        });

        let _ = unpin_current_thread();
        hist
    }

    // ─── 直方图对比输出 ──────────────────────────────────────────────────

    fn print_comparison(talaris: &Histogram<u64>, tokio: &Histogram<u64>) {
        println!();
        println!("┌─────────────┬───────────────────┬───────────────────┬─────────┐");
        println!("│ percentile  │ talaris (io_uring)│ tokio (epoll)     │ ratio   │");
        println!("├─────────────┼───────────────────┼───────────────────┼─────────┤");
        let rows: [(&str, u64); 5] = [
            ("mean", 0),
            ("p50", 50),
            ("p99", 99),
            ("p99.9", 999),
            ("max", 100),
        ];
        for (label, pct) in rows {
            let (t_ns, k_ns) = if label == "mean" {
                (talaris.mean() as u64, tokio.mean() as u64)
            } else if label == "max" {
                (talaris.max(), tokio.max())
            } else if pct == 999 {
                (
                    talaris.value_at_quantile(0.999),
                    tokio.value_at_quantile(0.999),
                )
            } else {
                let q = pct as f64 / 100.0;
                (talaris.value_at_quantile(q), tokio.value_at_quantile(q))
            };
            let ratio = if t_ns == 0 {
                0.0
            } else {
                k_ns as f64 / t_ns as f64
            };
            println!(
                "│ {:<11} │ {:>13} ns │ {:>13} ns │ {:>6.2}× │",
                label,
                fmt_with_commas(t_ns),
                fmt_with_commas(k_ns),
                ratio
            );
        }
        println!("└─────────────┴───────────────────┴───────────────────┴─────────┘");
        println!(
            "samples: talaris={} tokio={}",
            talaris.len(),
            tokio.len(),
        );
        println!("ratio = tokio_ns / talaris_ns ; >1 表 talaris 更快");
    }

    fn fmt_with_commas(n: u64) -> String {
        let s = n.to_string();
        let bytes = s.as_bytes();
        let mut out = String::with_capacity(s.len() + s.len() / 3);
        for (i, &b) in bytes.iter().enumerate() {
            if i > 0 && (bytes.len() - i) % 3 == 0 {
                out.push(',');
            }
            out.push(b as char);
        }
        out
    }

}
