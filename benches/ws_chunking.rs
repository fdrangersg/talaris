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
    eprintln!("ws_chunking: skipped - talaris benches only run on Linux");
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
    use std::time::Instant;

    use talaris::ws::DataEvent;
    use talaris::ws::MAX_HEADER_LEN;

    use super::common;

    pub fn run() {
        let frames = common::arg_or("--frames", 1_000_000_usize);
        let payload_len = common::arg_or("--payload", 256_usize);
        let chunk_sizes = common::parse_usize_list("--chunk-sizes", "128,512,4096,65536");
        let wire = common::encode_binary_frames(payload_len, frames);

        println!(
            "ws_chunking bench: WsClient feed_recv + drain_data_events, frames={}, payload={}B, wire={}B",
            common::fmt_int(frames as u64),
            payload_len,
            common::fmt_int(wire.len() as u64)
        );
        println!(
            "{:<12} {:>14} {:>11} {:>11} {:>11} {:>12} {:>9}",
            "chunk", "frames", "wall_ms", "user_ms", "frames/s", "MiB/s", "ns/frame"
        );

        for chunk_size in chunk_sizes {
            run_chunk(chunk_size, payload_len, frames as u64, black_box(&wire));
        }
    }

    fn run_chunk(chunk_size: usize, payload_len: usize, expected_frames: u64, wire: &[u8]) {
        let recv_capacity = chunk_size.max(payload_len + MAX_HEADER_LEN);
        let mut ws = common::open_ws_client(recv_capacity, payload_len);
        let mut frames = 0_u64;
        let mut bytes = 0_u64;
        let mut checksum = 0_u64;

        let cpu = common::ThreadCpuTimer::start();
        let wall = Instant::now();
        for chunk in wire.chunks(chunk_size) {
            ws.feed_recv(chunk);
            drain(&mut ws, &mut frames, &mut bytes, &mut checksum);
        }
        drain(&mut ws, &mut frames, &mut bytes, &mut checksum);
        let elapsed = wall.elapsed();
        let cpu_elapsed = cpu.elapsed();

        assert_eq!(frames, expected_frames);
        black_box(checksum);
        println!(
            "{chunk_size:<12} {:>14} {:>11.3} {:>11.3} {:>11.0} {:>12.1} {:>9}",
            common::fmt_int(frames),
            elapsed.as_secs_f64() * 1000.0,
            cpu_elapsed.as_secs_f64() * 1000.0,
            common::frames_per_sec(frames, elapsed),
            common::mib_per_sec(bytes, elapsed),
            common::ns_per_frame(cpu_elapsed, frames)
        );
    }

    fn drain(
        ws: &mut talaris::ws::WsClient,
        frames: &mut u64,
        bytes: &mut u64,
        checksum: &mut u64,
    ) {
        ws.drain_data_events(|ev| match ev {
            DataEvent::Binary(payload) => {
                *frames += 1;
                *bytes += payload.len() as u64;
                if let Some(first) = payload.first() {
                    *checksum = (*checksum).wrapping_add(u64::from(*first));
                }
            }
            DataEvent::Text(text) => {
                *frames += 1;
                *bytes += text.len() as u64;
                if let Some(first) = text.as_bytes().first() {
                    *checksum = (*checksum).wrapping_add(u64::from(*first));
                }
            }
        })
        .expect("drain ws data");
    }
}
