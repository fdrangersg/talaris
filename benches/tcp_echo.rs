// TCP echo round-trip bench：talaris (io_uring + SQ_POLL + pinned) vs
// tokio (epoll + current_thread + pinned)。
//
// ## 这层 bench 在测什么
//
// 比 proactor_overhead 多了：完整的 TCP 协议栈 + loopback 设备 + 一次 server
// echo。还没有 WS framing。所以两侧 client 走的是 raw TCP，**协议层完全对称**，
// 差异只来自 IO model。
//
// 比 pool_ws_echo 少了：WS 帧编解码、mask、Pool state machine。
//
// 加这一层是为了把 "io_uring vs epoll" 的 RTT 提升和 "WS framing 开销" 解耦。
// 任何 pool_ws_echo 提升必须先在这里看到，否则可疑。
//
// ## 拓扑（默认匹配 ripple-testnet-tokyo `isolcpus=1-5`，8 vCPU SMT pairs
// (0,4) (1,5) (2,6) (3,7)）：
//
// ```text
//   CPU 0  6  7  ← OS noise (非 isolated)
//   CPU 1  ← talaris user thread (isolated)
//   CPU 5  ← talaris SQ_POLL kthread (sibling of 1, isolated)
//   CPU 2  ← tokio worker thread (isolated, sibling 6 没 isolated → 安静)
//   CPU 4  ← echo server (isolated)
//   CPU 3  ← spare
// ```
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench tcp_echo -- \
//     --iters 200000 --warmup 20000 --payload 64
// ```
//
// 进程父 affinity 必须覆盖目标 CPU，外面建议套 `taskset -c 0-N`。
//
// talaris 这条 client 是 bench 内临时拼的 raw-TCP wheel（直接拿 `Proactor` +
// `TcpSocket`）。**不进 lib**，不代表 talaris 对外 API。

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
    eprintln!("tcp_echo: skipped — io_uring 只在 Linux 上可用");
}

#[cfg(target_os = "linux")]
fn main() {
    linux_impl::run();
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
    use std::thread;
    use std::time::Instant;

    use hdrhistogram::Histogram;
    use talaris::proactor::{
        Completion, Domain, OpKind, Proactor, ProactorConfig, SockAddr, SqeFlags, TcpSocket,
        UserData, unpin_current_thread,
    };

    use super::common;

    pub fn run() {
        let iters: u64 = common::arg_or("--iters", 200_000);
        let warmup: u64 = common::arg_or("--warmup", 20_000);
        let payload: usize = common::arg_or("--payload", 64);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let talaris_cpu: usize = common::arg_or("--talaris-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
        let tokio_cpu: usize = common::arg_or("--tokio-cpu", 2);

        eprintln!(
            "[tcp_echo] iters={iters} warmup={warmup} payload={payload}B"
        );
        eprintln!(
            "[tcp_echo] server-cpu={server_cpu} talaris-cpu={talaris_cpu} \
             sq-poll-cpu={sq_poll_cpu} tokio-cpu={tokio_cpu}"
        );

        // 一个 TCP echo server 顺序服务两侧 client（一条 session 一条 client）
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .expect("bind echo server");
        let addr = listener.local_addr().expect("local_addr");
        let server = thread::Builder::new()
            .name("tcp-echo-srv".into())
            .spawn(move || common::run_tcp_echo_sessions(listener, Some(server_cpu), 2))
            .expect("spawn srv");
        eprintln!("[tcp_echo] echo server on {addr}");

        // ── talaris variant ─────────────────────────────────────────────
        let talaris_hist = thread::Builder::new()
            .name("bench-talaris".into())
            .spawn(move || {
                run_talaris(addr, iters, warmup, payload, talaris_cpu, sq_poll_cpu)
            })
            .expect("spawn talaris")
            .join()
            .expect("talaris panic");

        // ── tokio variant ───────────────────────────────────────────────
        let tokio_hist = thread::Builder::new()
            .name("bench-tokio".into())
            .spawn(move || run_tokio(addr, iters, warmup, payload, tokio_cpu))
            .expect("spawn tokio")
            .join()
            .expect("tokio panic");

        server.join().expect("server panic");

        println!();
        println!("=== TCP echo RTT (payload={payload}B, iters={iters}) ===");
        common::print_comparison(&[
            ("talaris (io_uring)", &talaris_hist),
            ("tokio (epoll)", &tokio_hist),
        ]);
    }

    fn run_talaris(
        addr: SocketAddr,
        iters: u64,
        warmup: u64,
        payload_size: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
    ) -> Histogram<u64> {
        common::pin_or_warn("talaris", user_cpu);
        eprintln!(
            "[talaris] user→CPU {user_cpu}, SQ_POLL kthread→CPU {sq_poll_cpu}"
        );

        let proactor_cfg = ProactorConfig {
            entries: 64,
            sq_poll_idle_ms: Some(10_000),
            sq_poll_cpu: Some(sq_poll_cpu),
        };
        let mut proactor = Proactor::new(proactor_cfg).expect("proactor");

        let sock = TcpSocket::new(Domain::V4).expect("socket");
        sock.set_nodelay(true).expect("nodelay");
        let fd = sock.as_raw_fd();
        let sock_addr = SockAddr::from_std(addr);

        let connect_ud = UserData::new(OpKind::Connect, 0);
        // SAFETY: sock_addr / sock 都活到 connect CQE 之后
        unsafe {
            proactor
                .submit_connect(fd, &sock_addr, connect_ud, SqeFlags::NONE)
                .expect("submit_connect");
        }
        proactor.submit_and_wait(1).expect("connect");
        let mut got: Option<Completion> = None;
        proactor.drain_completions(|c| got = Some(c));
        got.expect("connect CQE").to_result().expect("connect ok");

        let mut send_buf = vec![0_u8; payload_size];
        let mut recv_buf = vec![0_u8; payload_size];
        for (i, b) in send_buf.iter_mut().enumerate() {
            *b = i as u8;
        }

        let mut hist = common::new_hist();
        let total = iters + warmup;
        for iter in 0..total {
            send_buf[..8].copy_from_slice(&iter.to_le_bytes());

            let t0 = Instant::now();
            let send_ud = UserData::new(OpKind::Send, iter);
            let recv_ud = UserData::new(OpKind::Recv, iter);
            // 两个 SQE 同时 batch（不加 IO_LINK，让 send / recv 并发推进 ——
            // recv 在 send 完成前已经在 kernel 等数据回来，loopback 上少一次切换）。
            // SAFETY: send_buf / recv_buf 整轮存活；fd 已 connected。
            unsafe {
                proactor
                    .submit_send(
                        fd,
                        send_buf.as_ptr(),
                        payload_size as u32,
                        send_ud,
                        SqeFlags::NONE,
                    )
                    .expect("send");
                proactor
                    .submit_recv(
                        fd,
                        recv_buf.as_mut_ptr(),
                        payload_size as u32,
                        recv_ud,
                        SqeFlags::NONE,
                    )
                    .expect("recv");
            }
            // SQ_POLL 关键：submit() 不进 syscall，wait_for_cqe 才进
            proactor.submit().expect("submit");
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
            let dt = t0.elapsed();
            debug_assert_eq!(&recv_buf[..8], &iter.to_le_bytes());
            if iter >= warmup {
                common::record_ns(&mut hist, dt);
            }
        }

        // 关连接
        let raw_fd = sock.as_raw_fd();
        std::mem::forget(sock);
        let close_ud = UserData::new(OpKind::Close, 0);
        // SAFETY: TcpSocket 已 forget，没有别的 RAII 还在追踪 fd
        unsafe { proactor.submit_close_raw(raw_fd, close_ud).expect("close") };
        proactor.submit_and_wait(1).expect("close wait");
        proactor.drain_completions(|_| {});

        let _ = unpin_current_thread();
        hist
    }

    fn run_tokio(
        addr: SocketAddr,
        iters: u64,
        warmup: u64,
        payload_size: usize,
        user_cpu: usize,
    ) -> Histogram<u64> {
        common::pin_or_warn("tokio", user_cpu);
        eprintln!("[tokio] worker→CPU {user_cpu}");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("rt");

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

            let mut hist = common::new_hist();
            let total = iters + warmup;
            for iter in 0..total {
                send_buf[..8].copy_from_slice(&iter.to_le_bytes());

                let t0 = Instant::now();
                stream.write_all(&send_buf).await.expect("write");
                stream.read_exact(&mut recv_buf).await.expect("read");
                let dt = t0.elapsed();
                debug_assert_eq!(&recv_buf[..8], &iter.to_le_bytes());
                if iter >= warmup {
                    common::record_ns(&mut hist, dt);
                }
            }
            stream.shutdown().await.ok();
            hist
        });

        let _ = unpin_current_thread();
        hist
    }
}
