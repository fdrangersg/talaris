// ws_ingress_single —— 1 条 WS conn，server 用尽全力 push，量 client 端
// max sustained ingress rate + client-thread CPU/frame。
//
// ## 这层 bench 在测什么
//
// **inbound-only workload**：server (tokio current_thread 单 OS 线程) 把一块
// 预编码 chunk_buf 在 hot loop 里反复 `write_all`，直到 client 关连接。
// client 只 drain 不发数据（订阅类客户端的稳态）。
//
// 差异完全来自 IO model：
//
// - **talaris**：multishot recv + provided buffer ring + io_uring CQE 直发。
//   kernel 一次 recv syscall 起的 op 一直活，每次有数据来就 post CQE，user
//   `pump()` 把已就绪的 CQE 一次性 drain 走，无需 user-space syscall 去取。
//
// - **tokio**：epoll readiness + 每次 `read()` 一个 syscall。kernel 通知
//   readable → user syscall 把数据 copy 到 user buffer → user 解帧 → 重复。
//
// 两侧 framing 都走 talaris 的 `parse_header`（fairness），只比 IO 路径。
//
// ## 严格控制变量
//
// - **server 行为对两 variant 完全一致**：每 variant 起一个新 listener +
//   新 server thread，pre-encoded chunk_buf 内容、size 相同，写循环逻辑相同。
// - **client 顺序串行**：talaris → unpin → tokio，inline on main thread。
// - **client side framing 同源**：两侧都用 `talaris::ws::frame::parse_header`。
// - **stop 对齐**：默认 `--frames N`；可选 `--seconds T`。
//
// ## 拓扑（默认匹配 ripple-testnet-tokyo `isolcpus=1-5`，SMT pairs (0,4) (1,5)
// (2,6) (3,7)）：
//
// ```text
//   CPU 4  ← server (tokio current_thread, isolated)
//   CPU 1  ← talaris client user thread (isolated)
//   CPU 5  ← talaris SQ_POLL kthread (sibling of 1, isolated)
//   CPU 2  ← tokio client (isolated)
// ```
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench ws_ingress_single -- \
//     --frames 2000000 --payload 64
//
// # wall-clock 对齐：
// taskset -c 0-7 cargo bench --bench ws_ingress_single -- \
//     --seconds 3 --payload 256
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
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

#[path = "common/mod.rs"]
mod common;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("ws_ingress_single: skipped — io_uring 只在 Linux 上可用");
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

    use hdrhistogram::Histogram;
    use talaris::Pool;
    use talaris::connection::{ConnectionConfig, State};
    use talaris::ws::{DataEvent as WsDataEvent, Event as WsEvent};

    use super::common;
    use super::common::{PinGuard, StopMode};

    struct Outcome {
        frames: u64,
        elapsed: Duration,
        client_cpu: Duration,
        inter_arrival: Histogram<u64>,
    }

    impl Outcome {
        fn frames_per_sec(&self) -> f64 {
            self.frames as f64 / self.elapsed.as_secs_f64()
        }
        fn mib_per_sec(&self, payload: usize) -> f64 {
            (self.frames as f64 * payload as f64) / self.elapsed.as_secs_f64() / (1024.0 * 1024.0)
        }
    }

    pub fn run() {
        let stop = StopMode::from_args(2_000_000);
        let payload: usize = common::arg_or("--payload", 64);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let talaris_cpu: usize = common::arg_or("--talaris-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
        let tokio_cpu: usize = common::arg_or("--tokio-cpu", 2);
        let spin_iters: usize = common::arg_or("--spin-iters", 256);
        let sample_every: u64 = common::arg_or("--sample-every", 0);
        let tune = common::TalarisTuneConfig::from_args(4096, 256);
        let staging = common::BenchStagingConfig::from_args();

        eprintln!("=========================================================");
        eprintln!(" ws_ingress_single — 1 conn server push → client drain");
        eprintln!("=========================================================");
        eprintln!(" stop      : {}", stop.describe());
        eprintln!(" payload   : {payload}B");
        eprintln!(" server-cpu: {server_cpu}  (fresh tokio runtime per variant)");
        eprintln!(" talaris   : user→CPU {talaris_cpu}, SQ_POLL→CPU {sq_poll_cpu}");
        eprintln!(" tokio     : worker→CPU {tokio_cpu}");
        eprintln!(" spin_iters: {spin_iters}");
        eprintln!(" samples   : every {sample_every} frame(s), 0 disables diagnostic jitter hist");
        tune.print_stderr(" ");
        staging.print_stderr(" ");
        eprintln!(" execution : 串行，inline on main thread，每 variant 之间 unpin");
        eprintln!();

        // 预编码 chunk_buf：server 写循环就一遍遍 write_all 这块。Arc 让两次
        // variant 共享同一个内容（fresh server thread 各拿一份 clone）。
        let frames_per_chunk =
            common::frames_per_chunk_for_bytes(payload, staging.server_chunk_bytes);
        let chunk_buf = Arc::new(common::pre_encode_ws_binary_chunk(
            payload,
            frames_per_chunk,
        ));
        eprintln!(
            "[bench] pre-encoded chunk: {} frames × {}B = {} KiB total",
            frames_per_chunk,
            payload,
            chunk_buf.len() / 1024
        );
        eprintln!();

        // ── variant 1/4: talaris pool.pump (general path) ────────────────
        eprintln!("─── variant 1/4: talaris Pool.pump (general path, Event enum) ───");
        let talaris = with_fresh_stream_server(server_cpu, chunk_buf.clone(), |addr| {
            run_talaris(
                addr,
                stop,
                payload,
                talaris_cpu,
                sq_poll_cpu,
                tune,
                sample_every,
            )
        });
        eprintln!();

        // ── variant 2/4: talaris pool.pump_data (data-only dispatch) ──────
        eprintln!("─── variant 2/4: talaris Pool.pump_data (data-only dispatch) ───");
        let talaris_data = with_fresh_stream_server(server_cpu, chunk_buf.clone(), |addr| {
            run_talaris_data(
                addr,
                stop,
                payload,
                talaris_cpu,
                sq_poll_cpu,
                tune,
                sample_every,
            )
        });
        eprintln!();

        // ── variant 3/4: talaris pool.pump_data_spin ─────────────────────
        eprintln!("─── variant 3/4: talaris Pool.pump_data_spin (busy-poll CQ) ───");
        let talaris_data_spin = with_fresh_stream_server(server_cpu, chunk_buf.clone(), |addr| {
            run_talaris_data_spin(
                addr,
                stop,
                payload,
                talaris_cpu,
                sq_poll_cpu,
                spin_iters,
                tune,
                sample_every,
            )
        });
        eprintln!();

        // ── variant 4/4: tokio ───────────────────────────────────────────
        eprintln!("─── variant 4/4: tokio (epoll + current_thread + pin) ───");
        let tokio = with_fresh_stream_server(server_cpu, chunk_buf, |addr| {
            run_tokio(addr, stop, payload, tokio_cpu, sample_every, staging)
        });

        println!();
        println!("=== ws_ingress_single (payload={payload}B) ===");
        println!();
        println!(
            "{:<22} │ {:>14} │ {:>10} │ {:>14} │ {:>11} │ {:>12} │ {:>9}",
            "variant", "frames", "elapsed", "frames/s", "MiB/s", "cpu ns/frame", "cpu%"
        );
        println!("{}", "─".repeat(112));
        for (label, o) in [
            ("talaris Pool.pump", &talaris),
            ("talaris pump_data", &talaris_data),
            ("talaris data spin", &talaris_data_spin),
            ("tokio", &tokio),
        ] {
            println!(
                "{:<22} │ {:>14} │ {:>9.3}s │ {:>14} │ {:>11.2} │ {:>12} │ {:>8.1}%",
                label,
                common::fmt_int(o.frames),
                o.elapsed.as_secs_f64(),
                common::fmt_int(o.frames_per_sec() as u64),
                o.mib_per_sec(payload),
                common::fmt_int(common::ns_per_frame(o.client_cpu, o.frames)),
                common::cpu_pct(o.client_cpu, o.elapsed),
            );
        }
        let r_data = talaris_data.frames_per_sec() / talaris.frames_per_sec();
        let r_vs_tokio = talaris_data.frames_per_sec() / tokio.frames_per_sec();
        let r_spin_vs_tokio = talaris_data_spin.frames_per_sec() / tokio.frames_per_sec();
        println!();
        println!(
            "data-only dispatch vs general path: {:.2}× ({:.0} → {:.0} f/s)",
            r_data,
            talaris.frames_per_sec(),
            talaris_data.frames_per_sec()
        );
        println!("pump_data vs tokio: {r_vs_tokio:.2}× (1.0 = parity)");
        println!("data spin vs tokio: {r_spin_vs_tokio:.2}× (1.0 = parity)");
        println!(
            "cpu ns/frame is client-thread CPU only; SQ_POLL kernel thread CPU is not included."
        );

        if sample_every > 0 {
            println!();
            println!("=== diagnostic inter-arrival latency ===");
            common::print_comparison(&[
                ("talaris Pool.pump", &talaris.inter_arrival),
                ("talaris pump_data", &talaris_data.inter_arrival),
                ("talaris data spin", &talaris_data_spin.inter_arrival),
                ("tokio", &tokio.inter_arrival),
            ]);
            println!();
            println!("inter-arrival is diagnostic only; it is not used for IO-model ROI.");
        }
    }

    /// 一个 variant 一个 fresh tokio stream server。
    fn with_fresh_stream_server<R>(
        server_cpu: usize,
        chunk_buf: Arc<Vec<u8>>,
        body: impl FnOnce(SocketAddr) -> R,
    ) -> R {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server = common::spawn_ws_stream_server(listener, 1, chunk_buf, Some(server_cpu));
        eprintln!("[bench] fresh stream server on {addr}, cpu={server_cpu}");
        let result = body(addr);
        server.join().expect("server thread panic");
        result
    }

    fn run_talaris(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
        tune: common::TalarisTuneConfig,
        sample_every: u64,
    ) -> Outcome {
        let _guard = PinGuard::pin("talaris", user_cpu);
        eprintln!("[talaris] user→CPU {user_cpu}, SQ_POLL kthread→CPU {sq_poll_cpu}");

        let cfg = tune.apply_connection(
            ConnectionConfig::new("localhost", addr.port(), "/")
                .with_tls(false)
                .with_sq_poll(10_000, Some(sq_poll_cpu)),
        );
        let mut pool = Pool::new(tune.pool_config(cfg.proactor)).expect("pool");
        let h = pool.connect_blocking_to(cfg, addr).expect("connect");
        assert_eq!(pool.state(h), Some(State::Open));

        let mut arrivals = common::sampled_arrivals(stop, sample_every);
        let mut frame_count = 0_u64;
        let cpu_timer = common::ThreadCpuTimer::start();
        let bench_start = Instant::now();

        while stop.keep_going(frame_count, bench_start) {
            pool.pump(|_h, ev| {
                if let WsEvent::Binary(data) = ev {
                    debug_assert_eq!(data.len(), payload);
                    frame_count += 1;
                    common::record_sampled_arrival(&mut arrivals, frame_count, sample_every);
                }
            })
            .expect("pump");
        }
        let elapsed = bench_start.elapsed();
        let client_cpu = cpu_timer.elapsed();
        eprintln!(
            "[talaris] {} frames in {:.3}s ({:.0} f/s)",
            frame_count,
            elapsed.as_secs_f64(),
            frame_count as f64 / elapsed.as_secs_f64()
        );

        // 干净关：发 Close，pump 短时间消化 server 端 EPIPE 后的退出
        pool.initiate_close(h, 1000, "bye").ok();
        let close_start = Instant::now();
        while close_start.elapsed() < Duration::from_secs(2) {
            let _ = pool.pump_nowait(|_, _| {});
            if matches!(pool.state(h), Some(State::Closed)) {
                break;
            }
        }

        let inter_arrival = common::inter_arrival_hist(&arrivals);
        Outcome {
            frames: frame_count,
            elapsed,
            client_cpu,
            inter_arrival,
        }
    }

    /// Same client setup as `run_talaris`, but drain via `pump_data` (data-only dispatch).
    fn run_talaris_data(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
        tune: common::TalarisTuneConfig,
        sample_every: u64,
    ) -> Outcome {
        let _guard = PinGuard::pin("talaris-data", user_cpu);
        eprintln!("[talaris-data] user→CPU {user_cpu}, SQ_POLL kthread→CPU {sq_poll_cpu}");

        let cfg = tune.apply_connection(
            ConnectionConfig::new("localhost", addr.port(), "/")
                .with_tls(false)
                .with_sq_poll(10_000, Some(sq_poll_cpu)),
        );
        let mut pool = Pool::new(tune.pool_config(cfg.proactor)).expect("pool");
        let h = pool.connect_blocking_to(cfg, addr).expect("connect");
        assert_eq!(pool.state(h), Some(State::Open));

        let mut arrivals = common::sampled_arrivals(stop, sample_every);
        let mut frame_count = 0_u64;
        let cpu_timer = common::ThreadCpuTimer::start();
        let bench_start = Instant::now();

        while stop.keep_going(frame_count, bench_start) {
            pool.pump_data(|_h, ev| {
                if let WsDataEvent::Binary(data) = ev {
                    debug_assert_eq!(data.len(), payload);
                    frame_count += 1;
                    common::record_sampled_arrival(&mut arrivals, frame_count, sample_every);
                }
            })
            .expect("pump_data");
        }
        let elapsed = bench_start.elapsed();
        let client_cpu = cpu_timer.elapsed();
        eprintln!(
            "[talaris-data] {} frames in {:.3}s ({:.0} f/s)",
            frame_count,
            elapsed.as_secs_f64(),
            frame_count as f64 / elapsed.as_secs_f64()
        );

        // 干净关：pump_data 仍走完整 WS control path，可以正常 close handshake。
        pool.initiate_close(h, 1000, "bye").ok();
        let close_start = Instant::now();
        while close_start.elapsed() < Duration::from_secs(2) {
            let _ = pool.pump_data_nowait(|_, _| {});
            if matches!(pool.state(h), Some(State::Closed)) {
                break;
            }
        }

        let inter_arrival = common::inter_arrival_hist(&arrivals);
        Outcome {
            frames: frame_count,
            elapsed,
            client_cpu,
            inter_arrival,
        }
    }

    /// Same data-only path as `run_talaris_data`, but busy-poll the CQ ring instead
    /// of entering `io_uring_enter(GETEVENTS)` while waiting for each batch.
    #[allow(clippy::too_many_arguments)]
    fn run_talaris_data_spin(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
        spin_iters: usize,
        tune: common::TalarisTuneConfig,
        sample_every: u64,
    ) -> Outcome {
        let _guard = PinGuard::pin("talaris-data-spin", user_cpu);
        eprintln!(
            "[talaris-data-spin] user→CPU {user_cpu}, SQ_POLL kthread→CPU {sq_poll_cpu}, spin_iters={spin_iters}"
        );

        let cfg = tune.apply_connection(
            ConnectionConfig::new("localhost", addr.port(), "/")
                .with_tls(false)
                .with_sq_poll(10_000, Some(sq_poll_cpu)),
        );
        let mut pool = Pool::new(tune.pool_config(cfg.proactor)).expect("pool");
        let h = pool.connect_blocking_to(cfg, addr).expect("connect");
        assert_eq!(pool.state(h), Some(State::Open));

        let mut arrivals = common::sampled_arrivals(stop, sample_every);
        let mut frame_count = 0_u64;
        let cpu_timer = common::ThreadCpuTimer::start();
        let bench_start = Instant::now();

        while stop.keep_going(frame_count, bench_start) {
            pool.pump_data_spin(spin_iters, |_h, ev| {
                if let WsDataEvent::Binary(data) = ev {
                    debug_assert_eq!(data.len(), payload);
                    frame_count += 1;
                    common::record_sampled_arrival(&mut arrivals, frame_count, sample_every);
                }
            })
            .expect("pump_data_spin");
        }
        let elapsed = bench_start.elapsed();
        let client_cpu = cpu_timer.elapsed();
        eprintln!(
            "[talaris-data-spin] {} frames in {:.3}s ({:.0} f/s)",
            frame_count,
            elapsed.as_secs_f64(),
            frame_count as f64 / elapsed.as_secs_f64()
        );

        pool.initiate_close(h, 1000, "bye").ok();
        let close_start = Instant::now();
        while close_start.elapsed() < Duration::from_secs(2) {
            let _ = pool.pump_data_nowait(|_, _| {});
            if matches!(pool.state(h), Some(State::Closed)) {
                break;
            }
        }

        let inter_arrival = common::inter_arrival_hist(&arrivals);
        Outcome {
            frames: frame_count,
            elapsed,
            client_cpu,
            inter_arrival,
        }
    }

    fn run_tokio(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sample_every: u64,
        staging: common::BenchStagingConfig,
    ) -> Outcome {
        let _guard = PinGuard::pin("tokio", user_cpu);
        eprintln!("[tokio] worker→CPU {user_cpu}");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("rt");

        rt.block_on(async move {
            use tokio::io::AsyncWriteExt;
            use tokio::net::TcpStream;
            let mut s = TcpStream::connect(addr).await.expect("connect");
            s.set_nodelay(true).expect("nodelay");
            let leftover = common::tokio_ws_upgrade_client(&mut s, "localhost", "/")
                .await
                .expect("ws upgrade");

            let cpu_timer = common::ThreadCpuTimer::start();
            let bench_start = Instant::now();
            let (arrivals, frame_count) = tokio_recv_ws_binary_frames_sampled(
                &mut s,
                leftover,
                stop,
                payload,
                sample_every,
                bench_start,
                staging,
            )
            .await;
            let elapsed = bench_start.elapsed();
            let client_cpu = cpu_timer.elapsed();
            eprintln!(
                "[tokio] {} frames in {:.3}s ({:.0} f/s)",
                frame_count,
                elapsed.as_secs_f64(),
                frame_count as f64 / elapsed.as_secs_f64()
            );

            let _ = s.shutdown().await;

            let inter_arrival = common::inter_arrival_hist(&arrivals);
            Outcome {
                frames: frame_count,
                elapsed,
                client_cpu,
                inter_arrival,
            }
        })
    }

    async fn tokio_recv_ws_binary_frames_sampled(
        s: &mut tokio::net::TcpStream,
        initial_leftover: Vec<u8>,
        stop: StopMode,
        expected_payload: usize,
        sample_every: u64,
        bench_start: Instant,
        staging: common::BenchStagingConfig,
    ) -> (Vec<Instant>, u64) {
        use talaris::ws::frame::parse_header;
        use tokio::io::AsyncReadExt;

        let mut arrivals = common::sampled_arrivals(stop, sample_every);
        let mut frame_count = 0_u64;
        let mut recv_buf = vec![0_u8; staging.tokio_read_buffer_capacity];
        let mut leftover = initial_leftover;
        leftover.reserve(staging.leftover_capacity);

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
                        common::record_sampled_arrival(&mut arrivals, frame_count, sample_every);
                        pos += total;
                        if !stop.keep_going(frame_count, bench_start) {
                            leftover.drain(..pos);
                            break 'outer;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("[tokio_recv_sampled] parse_header err after {frame_count}: {e}");
                        leftover.drain(..pos);
                        break 'outer;
                    }
                }
            }
            leftover.drain(..pos);

            if !stop.keep_going(frame_count, bench_start) {
                break;
            }
            let n = match s.read(&mut recv_buf).await {
                Ok(0) => {
                    eprintln!("[tokio_recv_sampled] EOF after {frame_count} frames");
                    break;
                }
                Ok(n) => n,
                Err(e) => {
                    eprintln!("[tokio_recv_sampled] read error after {frame_count}: {e}");
                    break;
                }
            };
            leftover.extend_from_slice(&recv_buf[..n]);
        }

        (arrivals, frame_count)
    }
}
