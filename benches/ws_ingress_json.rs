// ws_ingress_json - Text WebSocket ingress with optional serde_json decode.
//
// 目的：验证 io_uring 在真实 JSON 行情 payload 下还有没有 ROI。Binary framing
// bench 会把 I/O dispatch 的差异放大；JSON 解码一旦成为主成本，io_uring 的
// 收益可能被 serde_json、UTF-8 和业务字段提取吃掉。

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

#[path = "common/mod.rs"]
mod common;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("ws_ingress_json: skipped - io_uring hot path only runs on Linux");
}

#[cfg(target_os = "linux")]
fn main() {
    linux_impl::run();
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::hint::black_box;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use hdrhistogram::Histogram;
    use talaris::Pool;
    use talaris::connection::{ConnectionConfig, State};
    use talaris::ws::DataEvent as WsDataEvent;

    use super::common;
    use super::common::{PinGuard, StopMode, ThreadCpuTimer};

    struct Outcome {
        frames: u64,
        elapsed: Duration,
        client_cpu: Duration,
        inter_arrival: Histogram<u64>,
        checksum: u64,
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
        let stop = StopMode::from_args(500_000);
        let target_payload: usize = common::arg_or("--payload", 256);
        let server_cpu: usize = common::arg_or("--server-cpu", 4);
        let talaris_cpu: usize = common::arg_or("--talaris-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
        let sq_poll_idle_ms: u32 = common::arg_or("--sq-poll-idle-ms", 10_000);
        let tokio_cpu: usize = common::arg_or("--tokio-cpu", 2);
        let sample_every: u64 = common::arg_or("--sample-every", 0);
        let tune = common::TalarisTuneConfig::from_args(8192, 256);
        let staging = common::BenchStagingConfig::from_args();

        let json_payload = common::json_quote_payload(target_payload);
        let payload = json_payload.len();
        let frames_per_chunk =
            common::frames_per_chunk_for_bytes(payload, staging.server_chunk_bytes);
        let chunk_buf = Arc::new(common::pre_encode_ws_text_chunk(
            &json_payload,
            frames_per_chunk,
        ));

        eprintln!("=========================================================");
        eprintln!(" ws_ingress_json - loopback Text WS ingress + JSON decode");
        eprintln!("=========================================================");
        eprintln!(" stop      : {}", stop.describe());
        eprintln!(" payload   : target={target_payload}B actual={payload}B");
        eprintln!(" server-cpu: {server_cpu}");
        eprintln!(
            " talaris   : user->CPU {talaris_cpu}, SQ_POLL->CPU {sq_poll_cpu}, idle={sq_poll_idle_ms}ms"
        );
        eprintln!(" tokio     : worker->CPU {tokio_cpu}");
        eprintln!(" samples   : every {sample_every} frame(s), 0 disables diagnostic jitter hist");
        tune.print_stderr(" ");
        staging.print_stderr(" ");
        eprintln!(
            "[bench] pre-encoded text chunk: {} frames x {}B = {} KiB total",
            frames_per_chunk,
            payload,
            chunk_buf.len() / 1024
        );
        eprintln!();

        eprintln!("--- variant 1/4: talaris Text, no JSON decode ---");
        let talaris_text = with_fresh_server(server_cpu, chunk_buf.clone(), |addr| {
            run_talaris(
                addr,
                stop,
                payload,
                talaris_cpu,
                sq_poll_cpu,
                sq_poll_idle_ms,
                tune,
                sample_every,
                false,
            )
        });
        eprintln!();

        eprintln!("--- variant 2/4: talaris Text + serde_json::Value decode ---");
        let talaris_json = with_fresh_server(server_cpu, chunk_buf.clone(), |addr| {
            run_talaris(
                addr,
                stop,
                payload,
                talaris_cpu,
                sq_poll_cpu,
                sq_poll_idle_ms,
                tune,
                sample_every,
                true,
            )
        });
        eprintln!();

        eprintln!("--- variant 3/4: tokio Text, no JSON decode ---");
        let tokio_text = with_fresh_server(server_cpu, chunk_buf.clone(), |addr| {
            run_tokio(addr, stop, payload, tokio_cpu, sample_every, false, staging)
        });
        eprintln!();

        eprintln!("--- variant 4/4: tokio Text + serde_json::Value decode ---");
        let tokio_json = with_fresh_server(server_cpu, chunk_buf, |addr| {
            run_tokio(addr, stop, payload, tokio_cpu, sample_every, true, staging)
        });

        println!();
        println!("=== ws_ingress_json (payload={payload}B) ===");
        println!();
        println!(
            "{:<22} | {:>14} | {:>10} | {:>14} | {:>11} | {:>12} | {:>9}",
            "variant", "frames", "elapsed", "frames/s", "MiB/s", "cpu ns/frame", "cpu%"
        );
        println!("{}", "-".repeat(112));
        for (label, outcome) in [
            ("talaris text", &talaris_text),
            ("talaris + json", &talaris_json),
            ("tokio text", &tokio_text),
            ("tokio + json", &tokio_json),
        ] {
            println!(
                "{label:<22} | {:>14} | {:>9.3}s | {:>14} | {:>11.2} | {:>12} | {:>8.1}%",
                common::fmt_int(outcome.frames),
                outcome.elapsed.as_secs_f64(),
                common::fmt_int(outcome.frames_per_sec() as u64),
                outcome.mib_per_sec(payload),
                common::fmt_int(common::ns_per_frame(outcome.client_cpu, outcome.frames)),
                common::cpu_pct(outcome.client_cpu, outcome.elapsed),
            );
        }
        println!();
        println!(
            "text dispatch ROI     : {:.2}x talaris/tokio",
            talaris_text.frames_per_sec() / tokio_text.frames_per_sec()
        );
        println!(
            "with JSON decode ROI  : {:.2}x talaris/tokio",
            talaris_json.frames_per_sec() / tokio_json.frames_per_sec()
        );
        println!(
            "JSON tax on talaris   : {:.2}x slower",
            talaris_text.frames_per_sec() / talaris_json.frames_per_sec()
        );
        println!(
            "JSON tax on tokio     : {:.2}x slower",
            tokio_text.frames_per_sec() / tokio_json.frames_per_sec()
        );
        println!(
            "checksum guard        : {}",
            talaris_json.checksum ^ tokio_json.checksum
        );
        if sample_every > 0 {
            println!();
            println!("=== diagnostic inter-arrival latency ===");
            common::print_comparison(&[
                ("talaris text", &talaris_text.inter_arrival),
                ("talaris + json", &talaris_json.inter_arrival),
                ("tokio text", &tokio_text.inter_arrival),
                ("tokio + json", &tokio_json.inter_arrival),
            ]);
            println!("inter-arrival is diagnostic only; it is not used for IO-model ROI.");
        }
        println!();
        println!(
            "cpu ns/frame is client-thread CPU only; SQ_POLL kernel thread CPU is not included."
        );
    }

    fn with_fresh_server<R>(
        server_cpu: usize,
        chunk_buf: Arc<Vec<u8>>,
        body: impl FnOnce(SocketAddr) -> R,
    ) -> R {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server = common::spawn_ws_stream_server(listener, 1, chunk_buf, Some(server_cpu));
        eprintln!("[bench] fresh text stream server on {addr}, cpu={server_cpu}");
        let result = body(addr);
        server.join().expect("server thread panic");
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
        tune: common::TalarisTuneConfig,
        sample_every: u64,
        decode_json: bool,
    ) -> Outcome {
        let label = if decode_json {
            "talaris-json"
        } else {
            "talaris-text"
        };
        let _guard = PinGuard::pin(label, user_cpu);

        let cfg = tune
            .apply_connection(ConnectionConfig::new("localhost", addr.port(), "/").with_tls(false));
        let cfg = if sq_poll_idle_ms == 0 {
            cfg
        } else {
            cfg.with_sq_poll(sq_poll_idle_ms, Some(sq_poll_cpu))
        };
        let mut pool = Pool::new(tune.pool_config(cfg.proactor)).expect("pool");
        let h = pool.connect_blocking_to(cfg, addr).expect("connect");
        assert_eq!(pool.state(h), Some(State::Open));

        let mut arrivals = common::sampled_arrivals(stop, sample_every);
        let mut frame_count = 0_u64;
        let mut checksum = 0_u64;
        let cpu_timer = ThreadCpuTimer::start();
        let bench_start = Instant::now();
        while stop.keep_going(frame_count, bench_start) {
            pool.pump_data(|_h, ev| {
                if let WsDataEvent::Text(text) = ev {
                    debug_assert_eq!(text.len(), payload);
                    frame_count += 1;
                    common::record_sampled_arrival(&mut arrivals, frame_count, sample_every);
                    if decode_json {
                        checksum ^= common::decode_json_value(text);
                    } else {
                        checksum ^= text.len() as u64;
                    }
                }
            })
            .expect("pump_data");
        }
        let elapsed = bench_start.elapsed();
        let client_cpu = cpu_timer.elapsed();
        black_box(checksum);
        eprintln!(
            "[{label}] {} frames in {:.3}s ({:.0} f/s)",
            frame_count,
            elapsed.as_secs_f64(),
            frame_count as f64 / elapsed.as_secs_f64()
        );

        pool.initiate_close(h, 1000, "bye").ok();
        Outcome {
            frames: frame_count,
            elapsed,
            client_cpu,
            inter_arrival: common::inter_arrival_hist(&arrivals),
            checksum,
        }
    }

    fn run_tokio(
        addr: SocketAddr,
        stop: StopMode,
        payload: usize,
        user_cpu: usize,
        sample_every: u64,
        decode_json: bool,
        staging: common::BenchStagingConfig,
    ) -> Outcome {
        let label = if decode_json {
            "tokio-json"
        } else {
            "tokio-text"
        };
        let _guard = PinGuard::pin(label, user_cpu);
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

            let cpu_timer = ThreadCpuTimer::start();
            let bench_start = Instant::now();
            let (arrivals, frame_count, checksum) = tokio_recv_ws_text_frames(
                &mut s,
                leftover,
                stop,
                payload,
                sample_every,
                bench_start,
                decode_json,
                staging,
            )
            .await;
            let elapsed = bench_start.elapsed();
            let client_cpu = cpu_timer.elapsed();
            black_box(checksum);
            eprintln!(
                "[{label}] {} frames in {:.3}s ({:.0} f/s)",
                frame_count,
                elapsed.as_secs_f64(),
                frame_count as f64 / elapsed.as_secs_f64()
            );

            let _ = s.shutdown().await;
            Outcome {
                frames: frame_count,
                elapsed,
                client_cpu,
                inter_arrival: common::inter_arrival_hist(&arrivals),
                checksum,
            }
        })
    }

    async fn tokio_recv_ws_text_frames(
        s: &mut tokio::net::TcpStream,
        initial_leftover: Vec<u8>,
        stop: StopMode,
        expected_payload: usize,
        sample_every: u64,
        bench_start: Instant,
        decode_json: bool,
        staging: common::BenchStagingConfig,
    ) -> (Vec<Instant>, u64, u64) {
        use talaris::ws::OpCode;
        use talaris::ws::frame::parse_header;
        use tokio::io::AsyncReadExt;

        let mut arrivals = common::sampled_arrivals(stop, sample_every);
        let mut frame_count = 0_u64;
        let mut checksum = 0_u64;
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
                        debug_assert_eq!(hdr.opcode, OpCode::Text);
                        debug_assert_eq!(hdr.payload_len as usize, expected_payload);
                        let payload = &leftover[pos + consumed..pos + total];
                        let text = std::str::from_utf8(payload).expect("valid JSON text");
                        frame_count += 1;
                        common::record_sampled_arrival(&mut arrivals, frame_count, sample_every);
                        if decode_json {
                            checksum ^= common::decode_json_value(text);
                        } else {
                            checksum ^= text.len() as u64;
                        }
                        pos += total;
                        if !stop.keep_going(frame_count, bench_start) {
                            leftover.drain(..pos);
                            break 'outer;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("[tokio_text_recv] parse_header err after {frame_count}: {e}");
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
                    eprintln!("[tokio_text_recv] EOF after {frame_count} frames");
                    break;
                }
                Ok(n) => n,
                Err(e) => {
                    eprintln!("[tokio_text_recv] read error after {frame_count}: {e}");
                    break;
                }
            };
            leftover.extend_from_slice(&recv_buf[..n]);
        }

        (arrivals, frame_count, checksum)
    }
}
