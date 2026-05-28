// ws_ingress_single —— 1 条 WS conn，server 用尽全力 push，量 client 端
// max sustained ingress rate + 帧间投递 jitter。
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
    use talaris::connection::{ConnectionConfig, State};
    use talaris::ws::Event as WsEvent;
    use talaris::{Pool, PoolConfig};

    use super::common;
    use super::common::{PinGuard, StopMode};

    struct Outcome {
        frames: u64,
        elapsed: Duration,
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

        eprintln!("=========================================================");
        eprintln!(" ws_ingress_single — 1 conn server push → client drain");
        eprintln!("=========================================================");
        eprintln!(" stop      : {}", stop.describe());
        eprintln!(" payload   : {payload}B");
        eprintln!(" server-cpu: {server_cpu}  (fresh tokio runtime per variant)");
        eprintln!(" talaris   : user→CPU {talaris_cpu}, SQ_POLL→CPU {sq_poll_cpu}");
        eprintln!(" tokio     : worker→CPU {tokio_cpu}");
        eprintln!(" execution : 串行，inline on main thread，每 variant 之间 unpin");
        eprintln!();

        // 预编码 chunk_buf：server 写循环就一遍遍 write_all 这块。Arc 让两次
        // variant 共享同一个内容（fresh server thread 各拿一份 clone）。
        let frames_per_chunk = common::frames_per_chunk(payload);
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

        // ── variant 1/2: talaris ─────────────────────────────────────────
        eprintln!("─── variant 1/2: talaris (Pool, multishot recv, SQ_POLL + pin) ───");
        let talaris = with_fresh_stream_server(server_cpu, chunk_buf.clone(), |addr| {
            run_talaris(addr, stop, payload, talaris_cpu, sq_poll_cpu)
        });
        eprintln!();

        // ── variant 2/2: tokio ──────────────────────────────────────────
        eprintln!("─── variant 2/2: tokio (epoll + current_thread + pin) ───");
        let tokio = with_fresh_stream_server(server_cpu, chunk_buf.clone(), |addr| {
            run_tokio(addr, stop, payload, tokio_cpu)
        });

        println!();
        println!("=== ws_ingress_single (payload={payload}B) ===");
        println!();
        println!(
            "{:<14} │ {:>14} │ {:>10} │ {:>14} │ {:>11}",
            "variant", "frames", "elapsed", "frames/s", "MiB/s"
        );
        println!("{}", "─".repeat(72));
        println!(
            "{:<14} │ {:>14} │ {:>9.3}s │ {:>14} │ {:>11.2}",
            "talaris",
            fmt_int(talaris.frames),
            talaris.elapsed.as_secs_f64(),
            fmt_int(talaris.frames_per_sec() as u64),
            talaris.mib_per_sec(payload),
        );
        println!(
            "{:<14} │ {:>14} │ {:>9.3}s │ {:>14} │ {:>11.2}",
            "tokio",
            fmt_int(tokio.frames),
            tokio.elapsed.as_secs_f64(),
            fmt_int(tokio.frames_per_sec() as u64),
            tokio.mib_per_sec(payload),
        );
        let ratio = talaris.frames_per_sec() / tokio.frames_per_sec();
        println!("ratio (talaris / tokio) = {ratio:.2}×");

        println!();
        println!("=== inter-arrival latency (delivery jitter) ===");
        common::print_comparison(&[
            ("talaris", &talaris.inter_arrival),
            ("tokio", &tokio.inter_arrival),
        ]);
        println!();
        println!("(inter-arrival = 用户回调拿到相邻两帧之间的 ns 间隔；")
        ;
        println!(" loopback 上自然双峰：chunk 内 ~ns 级，chunk 间 ~µs 级)");
    }

    /// 一个 variant 一个 fresh tokio stream server。
    fn with_fresh_stream_server<R>(
        server_cpu: usize,
        chunk_buf: Arc<Vec<u8>>,
        body: impl FnOnce(SocketAddr) -> R,
    ) -> R {
        let listener =
            TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server =
            common::spawn_ws_stream_server(listener, 1, chunk_buf, Some(server_cpu));
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
    ) -> Outcome {
        let _guard = PinGuard::pin("talaris", user_cpu);
        eprintln!(
            "[talaris] user→CPU {user_cpu}, SQ_POLL kthread→CPU {sq_poll_cpu}"
        );

        let cfg = ConnectionConfig::new("localhost", addr.port(), "/")
            .with_tls(false)
            .with_sq_poll(10_000, Some(sq_poll_cpu));
        let mut pool = Pool::new(PoolConfig::new(cfg.proactor)).expect("pool");
        let h = pool.connect_blocking_to(cfg, addr).expect("connect");
        assert_eq!(pool.state(h), Some(State::Open));

        let mut arrivals: Vec<Instant> = Vec::with_capacity(stop.cap_hint());
        let mut frame_count = 0_u64;
        let bench_start = Instant::now();

        while stop.keep_going(frame_count, bench_start) {
            pool.pump(|_h, ev| {
                if let WsEvent::Binary(data) = ev {
                    debug_assert_eq!(data.len(), payload);
                    arrivals.push(Instant::now());
                    frame_count += 1;
                }
            })
            .expect("pump");
        }
        let elapsed = bench_start.elapsed();
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
            inter_arrival,
        }
    }

    fn run_tokio(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
    ) -> Outcome {
        let _guard = PinGuard::pin("tokio", user_cpu);
        eprintln!("[tokio] worker→CPU {user_cpu}");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("rt");

        rt.block_on(async move {
            use tokio::net::TcpStream;
            let mut s = TcpStream::connect(addr).await.expect("connect");
            s.set_nodelay(true).expect("nodelay");
            let leftover = common::tokio_ws_upgrade_client(&mut s, "localhost", "/")
                .await
                .expect("ws upgrade");

            let bench_start = Instant::now();
            let (arrivals, frame_count) = common::tokio_recv_ws_binary_frames(
                &mut s, leftover, stop, payload, bench_start,
            )
            .await;
            let elapsed = bench_start.elapsed();
            eprintln!(
                "[tokio] {} frames in {:.3}s ({:.0} f/s)",
                frame_count,
                elapsed.as_secs_f64(),
                frame_count as f64 / elapsed.as_secs_f64()
            );

            use tokio::io::AsyncWriteExt;
            let _ = s.shutdown().await;

            let inter_arrival = common::inter_arrival_hist(&arrivals);
            Outcome {
                frames: frame_count,
                elapsed,
                inter_arrival,
            }
        })
    }

    fn fmt_int(n: u64) -> String {
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
