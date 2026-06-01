// ws_ingress_tls - 1 条 loopback TLS WebSocket conn，server 全力 push，
// 对比 talaris rustls + io_uring 与 tokio rustls + epoll 的 steady-state ingress。
//
// 两侧使用同一 rustls 版本、同一自签 localhost CA、同一 WsClient 和同一
// pre-encoded payload chunk。额外保留 bare tokio parse_header 作为理论下限。
// 这个 bench 才是实盘 WSS transport 的可控对照；`ws_ingress_single` 保留为
// plain TCP 拆层诊断。

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

#[path = "common/mod.rs"]
mod common;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("ws_ingress_tls: skipped - io_uring hot path only runs on Linux");
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
    use talaris::ws::DataEvent as WsDataEvent;
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
        let payload: usize = common::arg_or("--payload", 256);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let talaris_cpu: usize = common::arg_or("--talaris-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
        let sq_poll_idle_ms: u32 = common::arg_or("--sq-poll-idle-ms", 10_000);
        let tokio_cpu: usize = common::arg_or("--tokio-cpu", 2);
        let spin_iters: usize = common::arg_or("--spin-iters", 256);
        let sample_every: u64 = common::arg_or("--sample-every", 1);
        let buf_size: u32 = common::arg_or("--buf-size", 8192);
        let buf_entries: u16 = common::arg_or("--buf-entries", 256);

        eprintln!("=========================================================");
        eprintln!(" ws_ingress_tls - loopback WSS TLS ingress");
        eprintln!("=========================================================");
        eprintln!(" stop      : {}", stop.describe());
        eprintln!(" payload   : {payload}B");
        eprintln!(" server-cpu: {server_cpu}");
        eprintln!(
            " talaris   : user->CPU {talaris_cpu}, SQ_POLL->CPU {sq_poll_cpu}, idle={sq_poll_idle_ms}ms"
        );
        eprintln!(" tokio     : worker->CPU {tokio_cpu}");
        eprintln!(" spin_iters: {spin_iters}");
        eprintln!(" samples   : every {sample_every} frame(s), 0 disables");
        eprintln!(
            " buf_ring  : {buf_entries} x {buf_size}B = {} KiB pool",
            (u32::from(buf_entries) * buf_size) / 1024
        );
        eprintln!();

        let frames_per_chunk = common::frames_per_chunk(payload);
        let chunk_buf = Arc::new(common::pre_encode_ws_binary_chunk(
            payload,
            frames_per_chunk,
        ));
        eprintln!(
            "[bench] pre-encoded chunk: {} frames x {}B = {} KiB total",
            frames_per_chunk,
            payload,
            chunk_buf.len() / 1024
        );
        eprintln!();

        eprintln!("--- variant 1/4: talaris Pool.pump_data ---");
        let talaris = with_fresh_tls_server(server_cpu, chunk_buf.clone(), |addr| {
            run_talaris(
                addr,
                stop,
                payload,
                talaris_cpu,
                sq_poll_cpu,
                sq_poll_idle_ms,
                buf_size,
                buf_entries,
                sample_every,
                None,
            )
        });
        eprintln!();

        eprintln!("--- variant 2/4: talaris Pool.pump_data_spin ---");
        let talaris_spin = with_fresh_tls_server(server_cpu, chunk_buf.clone(), |addr| {
            run_talaris(
                addr,
                stop,
                payload,
                talaris_cpu,
                sq_poll_cpu,
                sq_poll_idle_ms,
                buf_size,
                buf_entries,
                sample_every,
                Some(spin_iters),
            )
        });
        eprintln!();

        eprintln!("--- variant 3/4: tokio + rustls + WsClient ---");
        let tokio_ws = with_fresh_tls_server(server_cpu, chunk_buf.clone(), |addr| {
            run_tokio_ws_client(addr, stop, payload, tokio_cpu, sample_every)
        });
        eprintln!();

        eprintln!("--- variant 4/4: tokio + rustls + bare parse_header lower bound ---");
        let tokio_bare = with_fresh_tls_server(server_cpu, chunk_buf, |addr| {
            run_tokio_bare(addr, stop, payload, tokio_cpu, sample_every)
        });

        println!();
        println!("=== ws_ingress_tls (payload={payload}B) ===");
        println!();
        println!(
            "{:<22} | {:>14} | {:>10} | {:>14} | {:>11}",
            "variant", "frames", "elapsed", "frames/s", "MiB/s"
        );
        println!("{}", "-".repeat(82));
        for (label, outcome) in [
            ("talaris pump_data", &talaris),
            ("talaris data spin", &talaris_spin),
            ("tokio + rustls + WS", &tokio_ws),
            ("tokio bare lower bound", &tokio_bare),
        ] {
            println!(
                "{:<22} | {:>14} | {:>9.3}s | {:>14} | {:>11.2}",
                label,
                fmt_int(outcome.frames),
                outcome.elapsed.as_secs_f64(),
                fmt_int(outcome.frames_per_sec() as u64),
                outcome.mib_per_sec(payload),
            );
        }
        println!();
        println!(
            "pump_data vs tokio same WS: {:.2}x (1.0 = parity)",
            talaris.frames_per_sec() / tokio_ws.frames_per_sec()
        );
        println!(
            "data spin vs tokio same WS: {:.2}x (1.0 = parity)",
            talaris_spin.frames_per_sec() / tokio_ws.frames_per_sec()
        );

        println!();
        println!("=== inter-arrival latency (delivery jitter) ===");
        common::print_comparison(&[
            ("talaris pump_data", &talaris.inter_arrival),
            ("talaris data spin", &talaris_spin.inter_arrival),
            ("tokio + rustls + WS", &tokio_ws.inter_arrival),
            ("tokio bare lower bound", &tokio_bare.inter_arrival),
        ]);
        if sample_every != 1 {
            println!(
                "sampled intervals are diagnostic only; use --sample-every 1 for adjacent-frame jitter."
            );
        }
    }

    fn with_fresh_tls_server<R>(
        server_cpu: usize,
        chunk_buf: Arc<Vec<u8>>,
        body: impl FnOnce(SocketAddr) -> R,
    ) -> R {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server = common::spawn_tls_ws_stream_server(listener, chunk_buf, Some(server_cpu));
        eprintln!("[bench] fresh TLS stream server on {addr}, cpu={server_cpu}");
        let result = body(addr);
        server.join().expect("TLS server thread panic");
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn run_talaris(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sq_poll_cpu: u32,
        sq_poll_idle_ms: u32,
        buf_size: u32,
        buf_entries: u16,
        sample_every: u64,
        spin_iters: Option<usize>,
    ) -> Outcome {
        let label = if spin_iters.is_some() {
            "talaris-data-spin"
        } else {
            "talaris-data"
        };
        let _guard = PinGuard::pin(label, user_cpu);

        let cfg = ConnectionConfig::new("localhost", addr.port(), "/")
            .with_tls_config(common::local_tls_client_config())
            .with_buf_ring(buf_size, buf_entries);
        let cfg = if sq_poll_idle_ms == 0 {
            cfg
        } else {
            cfg.with_sq_poll(sq_poll_idle_ms, Some(sq_poll_cpu))
        };
        let mut pool = Pool::new(PoolConfig::new(cfg.proactor)).expect("pool");
        let h = pool
            .connect_blocking_to(cfg, addr)
            .expect("TLS + WS connect");
        assert_eq!(pool.state(h), Some(State::Open));

        let mut arrivals = common::sampled_arrivals(stop, sample_every);
        let mut frame_count = 0_u64;
        let bench_start = Instant::now();
        if let Some(spin_iters) = spin_iters {
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
        } else {
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
        }
        let elapsed = bench_start.elapsed();
        eprintln!(
            "[{label}] {} frames in {:.3}s ({:.0} f/s)",
            frame_count,
            elapsed.as_secs_f64(),
            frame_count as f64 / elapsed.as_secs_f64()
        );

        pool.initiate_close(h, 1000, "bye").ok();
        let inter_arrival = common::inter_arrival_hist(&arrivals);
        Outcome {
            frames: frame_count,
            elapsed,
            inter_arrival,
        }
    }

    fn run_tokio_ws_client(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sample_every: u64,
    ) -> Outcome {
        let _guard = PinGuard::pin("tokio-tls-ws", user_cpu);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("rt");

        rt.block_on(async move {
            use tokio::io::AsyncWriteExt as _;

            let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
            stream.set_nodelay(true).expect("nodelay");
            let mut tls = common::local_tls_client_connection();
            let ws = common::tokio_tls_ws_client_connect(&mut stream, &mut tls, "localhost", "/")
                .await
                .expect("TLS + WsClient upgrade");

            let bench_start = Instant::now();
            let (arrivals, frame_count) = common::tokio_recv_tls_ws_data_events(
                &mut stream,
                &mut tls,
                ws,
                stop,
                payload,
                sample_every,
                bench_start,
            )
            .await;
            let elapsed = bench_start.elapsed();
            eprintln!(
                "[tokio-tls-ws] {} frames in {:.3}s ({:.0} f/s)",
                frame_count,
                elapsed.as_secs_f64(),
                frame_count as f64 / elapsed.as_secs_f64()
            );

            let _ = stream.shutdown().await;
            Outcome {
                frames: frame_count,
                elapsed,
                inter_arrival: common::inter_arrival_hist(&arrivals),
            }
        })
    }

    fn run_tokio_bare(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sample_every: u64,
    ) -> Outcome {
        let _guard = PinGuard::pin("tokio-tls-bare", user_cpu);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("rt");

        rt.block_on(async move {
            use tokio::io::AsyncWriteExt as _;

            let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
            stream.set_nodelay(true).expect("nodelay");
            let mut tls = common::local_tls_client_connection();
            let leftover =
                common::tokio_tls_ws_upgrade_client(&mut stream, &mut tls, "localhost", "/")
                    .await
                    .expect("TLS + WS upgrade");

            let bench_start = Instant::now();
            let (arrivals, frame_count) = common::tokio_recv_tls_ws_binary_frames(
                &mut stream,
                &mut tls,
                leftover,
                stop,
                payload,
                sample_every,
                bench_start,
            )
            .await;
            let elapsed = bench_start.elapsed();
            eprintln!(
                "[tokio-tls-bare] {} frames in {:.3}s ({:.0} f/s)",
                frame_count,
                elapsed.as_secs_f64(),
                frame_count as f64 / elapsed.as_secs_f64()
            );

            let _ = stream.shutdown().await;
            Outcome {
                frames: frame_count,
                elapsed,
                inter_arrival: common::inter_arrival_hist(&arrivals),
            }
        })
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
