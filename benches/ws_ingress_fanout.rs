// ws_ingress_fanout —— N 条 WS conn 同时被 server push（模拟订多家 venue），
// 量 talaris Pool 路由 vs tokio N 个 async task 的总吞吐 scaling。
//
// ## 这层 bench 在测什么
//
// N ∈ {1, 4, 16, 64} 条 conn，每条 conn 一个独立 server session 在 tokio
// current_thread runtime 里推 chunk_buf。client 端：
//
// - **talaris**：1 个 `Pool` 持 N 条 `ConnHandle`，单 `pump()` 循环 drain N
//   条 multishot recv 流。CQE token 解出 conn_id 是 O(1) slot-table 查。
//
// - **tokio**：1 个 current_thread runtime，N 个 `tokio::spawn` task，每个 task
//   一条 `TcpStream` recv loop。epoll readiness 多路复用 N 个 fd。
//
// 两侧都跑在 1 条 OS 线程上（pinned），apples-to-apples 比较"1 个 IO worker
// 能撑住 N 条 conn 的最大持续 inbound rate 是多少"。
//
// ## 严格控制变量
//
// - 每个 N 跑两 variant：talaris → unpin → tokio。每 variant 起 fresh server
//   thread + fresh listener bind，server 端永远 fresh 状态。
// - 两 variant 共用同一份 pre-encoded chunk_buf 内容（每个 server 拿一份
//   Arc clone）。
// - 两侧都用 talaris 的 `parse_header`。
//
// ## 运行
//
// ```bash
// taskset -c 0-7 cargo bench --bench ws_ingress_fanout -- \
//     --frames 500000 --payload 64 --n-list 1,4,16,64
//
// # wall-clock 对齐 throughput scaling：
// taskset -c 0-7 cargo bench --bench ws_ingress_fanout -- \
//     --seconds 3 --payload 64 --n-list 1,4,16,64
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
    eprintln!("ws_ingress_fanout: skipped — io_uring 只在 Linux 上可用");
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
    use talaris::{ConnHandle, Pool, PoolConfig};

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

    struct Row {
        n: u32,
        talaris: Outcome,
        tokio: Outcome,
    }

    pub fn run() {
        let stop = StopMode::from_args(500_000);
        let payload: usize = common::arg_or("--payload", 64);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let talaris_cpu: usize = common::arg_or("--talaris-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
        let tokio_cpu: usize = common::arg_or("--tokio-cpu", 2);
        let n_list: String = common::arg_or("--n-list", "1,4,16,64".to_string());
        let ns: Vec<u32> = n_list
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        eprintln!("=========================================================");
        eprintln!(" ws_ingress_fanout — N conn server push → client drain");
        eprintln!("=========================================================");
        eprintln!(" stop      : {} (per N per variant)", stop.describe());
        eprintln!(" payload   : {payload}B");
        eprintln!(" n-list    : {ns:?}");
        eprintln!(" server-cpu: {server_cpu}");
        eprintln!(" talaris   : user→CPU {talaris_cpu}, SQ_POLL→CPU {sq_poll_cpu}");
        eprintln!(" tokio     : worker→CPU {tokio_cpu}");
        eprintln!();

        let frames_per_chunk = common::frames_per_chunk(payload);
        let chunk_buf = Arc::new(common::pre_encode_ws_binary_chunk(
            payload,
            frames_per_chunk,
        ));
        eprintln!(
            "[bench] pre-encoded chunk: {} frames × {}B = {} KiB",
            frames_per_chunk,
            payload,
            chunk_buf.len() / 1024
        );

        let mut rows: Vec<Row> = Vec::new();
        let total = ns.len();
        for (idx, &n) in ns.iter().enumerate() {
            eprintln!();
            eprintln!("─── N={n} ({}/{}) ───", idx + 1, total);

            eprintln!("[N={n}] variant 1/2: talaris");
            let talaris = with_fresh_server(server_cpu, n, chunk_buf.clone(), |addr| {
                run_talaris(addr, n, stop, payload, talaris_cpu, sq_poll_cpu)
            });

            eprintln!("[N={n}] variant 2/2: tokio");
            let tokio = with_fresh_server(server_cpu, n, chunk_buf.clone(), |addr| {
                run_tokio(addr, n, stop, payload, tokio_cpu)
            });

            rows.push(Row {
                n,
                talaris,
                tokio,
            });
        }

        println!();
        println!("=== ws_ingress_fanout (payload={payload}B) ===");
        println!();
        println!(
            "{:<6} │ {:<8} │ {:>14} │ {:>10} │ {:>14} │ {:>10}",
            "N", "variant", "frames", "elapsed", "frames/s", "MiB/s"
        );
        println!("{}", "─".repeat(80));
        for r in &rows {
            println!(
                "{:<6} │ {:<8} │ {:>14} │ {:>9.3}s │ {:>14} │ {:>10.2}",
                format!("N={}", r.n),
                "talaris",
                fmt_int(r.talaris.frames),
                r.talaris.elapsed.as_secs_f64(),
                fmt_int(r.talaris.frames_per_sec() as u64),
                r.talaris.mib_per_sec(payload),
            );
            let ratio = r.talaris.frames_per_sec() / r.tokio.frames_per_sec();
            println!(
                "{:<6} │ {:<8} │ {:>14} │ {:>9.3}s │ {:>14} │ {:>10.2}    (ratio {:.2}×)",
                "",
                "tokio",
                fmt_int(r.tokio.frames),
                r.tokio.elapsed.as_secs_f64(),
                fmt_int(r.tokio.frames_per_sec() as u64),
                r.tokio.mib_per_sec(payload),
                ratio,
            );
        }

        println!();
        println!("=== inter-arrival latency (delivery jitter, aggregate over N conns) ===");
        for r in &rows {
            println!();
            println!("[N={}]", r.n);
            common::print_comparison(&[
                ("talaris", &r.talaris.inter_arrival),
                ("tokio", &r.tokio.inter_arrival),
            ]);
        }
    }

    fn with_fresh_server<R>(
        server_cpu: usize,
        n_conns: u32,
        chunk_buf: Arc<Vec<u8>>,
        body: impl FnOnce(SocketAddr) -> R,
    ) -> R {
        let listener =
            TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server = common::spawn_ws_stream_server(
            listener,
            n_conns,
            chunk_buf,
            Some(server_cpu),
        );
        let result = body(addr);
        server.join().expect("server panic");
        result
    }

    fn run_talaris(
        addr: SocketAddr,
        n_conns: u32,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
    ) -> Outcome {
        let _guard = PinGuard::pin("talaris", user_cpu);

        let cfg_template = ConnectionConfig::new("localhost", addr.port(), "/")
            .with_tls(false)
            .with_sq_poll(10_000, Some(sq_poll_cpu));
        let mut pool = Pool::new(PoolConfig::new(cfg_template.proactor)).expect("pool");

        // 顺序 connect N 条（Pool::pump 不 sync_ws_open_state → 非阻塞并发
        // handshake 走不通；等 lib 修）。
        let mut handles: Vec<ConnHandle> = Vec::with_capacity(n_conns as usize);
        let connect_start = Instant::now();
        for _ in 0..n_conns {
            let h = pool
                .connect_blocking_to(cfg_template.clone(), addr)
                .expect("connect");
            assert_eq!(pool.state(h), Some(State::Open));
            handles.push(h);
        }
        eprintln!(
            "[talaris N={n_conns}] handshakes in {:?}",
            connect_start.elapsed()
        );

        let mut arrivals: Vec<Instant> = Vec::with_capacity(stop.cap_hint());
        let mut frame_count = 0_u64;
        let bench_start = Instant::now();

        // 注意：stop 在 pump 之间检测；pump 一轮可能 deliver 多帧，会略 overshoot
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
            "[talaris N={n_conns}] {} frames in {:.3}s ({:.0} f/s)",
            frame_count,
            elapsed.as_secs_f64(),
            frame_count as f64 / elapsed.as_secs_f64()
        );

        // 关所有 conn
        for &h in &handles {
            pool.initiate_close(h, 1000, "bye").ok();
        }
        let close_start = Instant::now();
        while close_start.elapsed() < Duration::from_secs(2) {
            let _ = pool.pump_nowait(|_, _| {});
            let all_closed = handles
                .iter()
                .all(|h| matches!(pool.state(*h), Some(State::Closed)));
            if all_closed {
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
        n_conns: u32,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
    ) -> Outcome {
        let _guard = PinGuard::pin("tokio", user_cpu);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("rt");

        rt.block_on(async move {
            use tokio::net::TcpStream;

            // 起 N 个 task，每个 task 拿自己的 TcpStream + recv loop。N 个 task
            // 全跑在这条 current_thread runtime（=本 OS 线程）上。
            let bench_start = Instant::now();
            let mut handles = Vec::with_capacity(n_conns as usize);
            for _ in 0..n_conns {
                let h = tokio::spawn(async move {
                    let mut s = TcpStream::connect(addr).await.expect("connect");
                    s.set_nodelay(true).expect("nodelay");
                    let leftover = common::tokio_ws_upgrade_client(&mut s, "localhost", "/")
                        .await
                        .expect("upgrade");
                    let (arr, cnt) = common::tokio_recv_ws_binary_frames(
                        &mut s,
                        leftover,
                        stop,
                        payload,
                        bench_start,
                    )
                    .await;
                    use tokio::io::AsyncWriteExt;
                    let _ = s.shutdown().await;
                    (arr, cnt)
                });
                handles.push(h);
            }

            let mut all_arrivals: Vec<Instant> = Vec::new();
            let mut total_count = 0_u64;
            for h in handles {
                let (arr, cnt) = h.await.expect("task");
                all_arrivals.extend(arr);
                total_count += cnt;
            }
            let elapsed = bench_start.elapsed();
            eprintln!(
                "[tokio N={n_conns}] {} frames in {:.3}s ({:.0} f/s)",
                total_count,
                elapsed.as_secs_f64(),
                total_count as f64 / elapsed.as_secs_f64()
            );

            // merge N 条 conn 的 arrivals 后按时间排序，inter-arrival 才是"用户
            // 应用层"角度的真实 delivery jitter（不是单条 conn 内的）。
            all_arrivals.sort_unstable();
            let inter_arrival = common::inter_arrival_hist(&all_arrivals);
            Outcome {
                frames: total_count,
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
