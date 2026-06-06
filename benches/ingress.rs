#![allow(
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::unwrap_used
)]

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("ingress: skipped - talaris benches only run on Linux");
}

#[cfg(target_os = "linux")]
#[path = "common.rs"]
mod common;

#[cfg(target_os = "linux")]
fn main() {
    linux::run();
}

#[cfg(target_os = "linux")]
mod linux {
    use std::hint::black_box;
    use std::net::TcpListener;
    use std::time::Instant;

    use talaris::connection::{ConnectionConfig, State};
    use talaris::ws::DataEvent;
    use talaris::{Pool, PoolConfig};

    use super::common;

    pub fn run() {
        let target_frames = common::arg_or("--frames", 1_000_000_u64);
        let payload_len = common::arg_or("--payload", 64_usize);
        let buf_size = common::arg_or("--buf-size", 4096_u32);
        let buf_entries = common::arg_or("--buf-entries", 256_u16);
        let spin_iters = common::arg_or("--spin-iters", 0_usize);
        let sample_every = common::arg_or("--sample-every", 0_u64);
        let user_cpu = common::optional_arg("--user-cpu");
        let server_cpu = common::optional_arg("--server-cpu");

        let frames_per_chunk = frames_per_chunk(payload_len);
        let chunk = common::encode_binary_frames(payload_len, frames_per_chunk);
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let server = common::spawn_stream_server(listener, chunk, server_cpu);

        let _pin = user_cpu.map(|cpu| common::PinGuard::pin("user", cpu));
        let cfg = ConnectionConfig::new("localhost", addr.port(), "/")
            .with_tls(false)
            .with_buf_ring(buf_size, buf_entries)
            .with_ws_limits(payload_len.max(1), payload_len.max(1) as u64)
            .with_ingress_stats(true);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let handle = pool
            .connect_blocking_to(cfg, addr)
            .expect("connect loopback websocket");
        assert_eq!(pool.state(handle), Some(State::Open));

        println!(
            "ingress bench: io_uring multishot recv + Pool::pump_data, target_frames={}, payload={}B, buf={}x{}, spin_iters={}, sample_every={}",
            common::fmt_int(target_frames),
            payload_len,
            buf_entries,
            buf_size,
            spin_iters,
            sample_every
        );

        let mut frames = 0_u64;
        let mut bytes = 0_u64;
        let mut checksum = 0_u64;
        let mut hist = common::sampled_hist();
        let mut last_sample = None;

        let cpu = common::ThreadCpuTimer::start();
        let wall = Instant::now();
        while frames < target_frames {
            let result = if spin_iters == 0 {
                pool.pump_data(|_, ev| {
                    count_event(
                        ev,
                        &mut frames,
                        &mut bytes,
                        &mut checksum,
                        &mut hist,
                        &mut last_sample,
                        sample_every,
                    );
                })
                .map(|()| true)
            } else {
                pool.pump_data_spin(spin_iters, |_, ev| {
                    count_event(
                        ev,
                        &mut frames,
                        &mut bytes,
                        &mut checksum,
                        &mut hist,
                        &mut last_sample,
                        sample_every,
                    );
                })
            };
            result.expect("pump data");
        }
        let elapsed = wall.elapsed();
        let cpu_elapsed = cpu.elapsed();
        black_box(checksum);

        println!(
            "{:<12} {:>14} {:>11.3} {:>11.3} {:>8.1}% {:>11.0} {:>12.1} {:>9}",
            "ingress",
            common::fmt_int(frames),
            elapsed.as_secs_f64() * 1000.0,
            cpu_elapsed.as_secs_f64() * 1000.0,
            common::cpu_pct(cpu_elapsed, elapsed),
            common::frames_per_sec(frames, elapsed),
            common::mib_per_sec(bytes, elapsed),
            common::ns_per_frame(cpu_elapsed, frames)
        );
        if let Some(stats) = pool.ingress_stats(handle) {
            println!("ingress stats: {stats:?}");
        }
        common::print_hist("arrival-gap", &hist);

        drop(pool);
        server.join().expect("stream server join");
    }

    fn frames_per_chunk(payload_len: usize) -> usize {
        let approximate_frame_len = payload_len.saturating_add(14).max(1);
        (64 * 1024 / approximate_frame_len).max(1)
    }

    fn count_event(
        ev: DataEvent<'_>,
        frames: &mut u64,
        bytes: &mut u64,
        checksum: &mut u64,
        hist: &mut hdrhistogram::Histogram<u64>,
        last_sample: &mut Option<Instant>,
        sample_every: u64,
    ) {
        let payload = match ev {
            DataEvent::Binary(payload) => payload,
            DataEvent::Text(text) => text.as_bytes(),
        };
        *frames += 1;
        *bytes += payload.len() as u64;
        if let Some(first) = payload.first() {
            *checksum = (*checksum).wrapping_add(u64::from(*first));
        }
        common::maybe_record_arrival(hist, last_sample, sample_every, *frames);
    }
}
