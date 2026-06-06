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
    eprintln!("e2e: skipped - talaris benches only run on Linux");
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

    use talaris::connection::{ConnectionConfig, ConnectionError, State};
    use talaris::ws::DataEvent;
    use talaris::{Pool, PoolConfig};

    use super::common;

    pub fn run() {
        let messages = common::arg_or("--messages", 10_000_usize);
        let payload_len = common::arg_or("--payload", 64_usize);
        let buf_size = common::arg_or("--buf-size", 4096_u32);
        let buf_entries = common::arg_or("--buf-entries", 256_u16);
        let user_cpu = common::optional_arg("--user-cpu");
        let server_cpu = common::optional_arg("--server-cpu");
        let payload = common::payload(payload_len);

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let server = common::spawn_echo_server(listener, messages, server_cpu);

        let _pin = user_cpu.map(|cpu| common::PinGuard::pin("user", cpu));
        let cfg = ConnectionConfig::new("localhost", addr.port(), "/")
            .with_tls(false)
            .with_buf_ring(buf_size, buf_entries)
            .with_ws_limits(payload_len.max(1), payload_len.max(1) as u64);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let handle = pool
            .connect_blocking_to(cfg, addr)
            .expect("connect loopback websocket");
        assert_eq!(pool.state(handle), Some(State::Open));

        println!(
            "e2e bench: one outstanding binary message, loopback echo, messages={}, payload={}B, buf={}x{}",
            common::fmt_int(messages as u64),
            payload_len,
            buf_entries,
            buf_size
        );

        let mut hist = common::sampled_hist();
        let mut frames = 0_u64;
        let mut bytes = 0_u64;
        let mut checksum = 0_u64;

        let cpu = common::ThreadCpuTimer::start();
        let wall = Instant::now();
        for i in 0..messages {
            let sent_at = Instant::now();
            pool.send_binary(handle, &payload).expect("send binary");
            let mut got_echo = false;
            while !got_echo {
                let result = pool.pump_data(|_, ev| {
                    let echoed = match ev {
                        DataEvent::Binary(payload) => payload,
                        DataEvent::Text(text) => text.as_bytes(),
                    };
                    frames += 1;
                    bytes += echoed.len() as u64;
                    if let Some(first) = echoed.first() {
                        checksum = checksum.wrapping_add(u64::from(*first));
                    }
                    got_echo = true;
                });
                match result {
                    Ok(()) => {}
                    Err(ConnectionError::PeerClosed) if got_echo && i + 1 == messages => {}
                    Err(e) => panic!("pump data: {e}"),
                }
            }
            hist.record(common::duration_ns_u64(sent_at.elapsed()))
                .expect("record rtt");
        }
        let elapsed = wall.elapsed();
        let cpu_elapsed = cpu.elapsed();
        black_box(checksum);

        println!(
            "{:<12} {:>14} {:>11.3} {:>11.3} {:>8.1}% {:>11.0} {:>12.1} {:>9}",
            "e2e",
            common::fmt_int(frames),
            elapsed.as_secs_f64() * 1000.0,
            cpu_elapsed.as_secs_f64() * 1000.0,
            common::cpu_pct(cpu_elapsed, elapsed),
            common::frames_per_sec(frames, elapsed),
            common::mib_per_sec(bytes, elapsed),
            common::ns_per_frame(cpu_elapsed, frames)
        );
        common::print_hist("rtt", &hist);

        drop(pool);
        server.join().expect("echo server join");
    }
}
