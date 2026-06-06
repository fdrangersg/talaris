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
    eprintln!("framing: skipped - talaris benches only run on Linux");
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
    use std::time::{Duration, Instant};

    use talaris::ws::DataEvent;
    use talaris::ws::frame::parse_header;
    use talaris::ws::parser::{FeedOutcome, FrameEvent, FrameParser};

    use super::common;

    #[derive(Clone, Copy)]
    struct Counts {
        frames: u64,
        bytes: u64,
        checksum: u64,
    }

    pub fn run() {
        let frames = common::arg_or("--frames", 1_000_000_usize);
        let payloads = common::parse_usize_list("--payloads", "64,256,1024");

        println!(
            "framing bench: pure inbound CPU path, frames={}, payloads={:?}",
            common::fmt_int(frames as u64),
            payloads
        );
        println!(
            "{:<16} {:>8} {:>14} {:>11} {:>11} {:>11} {:>12} {:>9}",
            "variant", "payload", "frames", "wall_ms", "user_ms", "frames/s", "MiB/s", "ns/frame"
        );

        for payload_len in payloads {
            let wire = common::encode_binary_frames(payload_len, frames);
            run_variant("header", payload_len, &wire, parse_header_only);
            run_variant("parser", payload_len, &wire, parse_streaming);
            run_variant("ws-client", payload_len, &wire, |bytes| {
                parse_ws_client(bytes, payload_len)
            });
        }
    }

    fn run_variant<F>(name: &str, payload_len: usize, wire: &[u8], mut f: F)
    where
        F: FnMut(&[u8]) -> Counts,
    {
        let cpu = common::ThreadCpuTimer::start();
        let wall = Instant::now();
        let counts = f(black_box(wire));
        let elapsed = wall.elapsed();
        let cpu_elapsed = cpu.elapsed();
        black_box(counts.checksum);
        print_row(name, payload_len, counts, elapsed, cpu_elapsed);
    }

    fn print_row(
        name: &str,
        payload_len: usize,
        counts: Counts,
        elapsed: Duration,
        cpu_elapsed: Duration,
    ) {
        println!(
            "{name:<16} {payload_len:>8} {:>14} {:>11.3} {:>11.3} {:>11.0} {:>12.1} {:>9}",
            common::fmt_int(counts.frames),
            elapsed.as_secs_f64() * 1000.0,
            cpu_elapsed.as_secs_f64() * 1000.0,
            common::frames_per_sec(counts.frames, elapsed),
            common::mib_per_sec(counts.bytes, elapsed),
            common::ns_per_frame(cpu_elapsed, counts.frames)
        );
    }

    fn parse_header_only(mut wire: &[u8]) -> Counts {
        let mut frames = 0_u64;
        let mut bytes = 0_u64;
        let mut checksum = 0_u64;
        while !wire.is_empty() {
            let (header, header_len) = parse_header(wire).expect("valid frame").expect("header");
            let payload_len = usize::try_from(header.payload_len).expect("payload fits usize");
            let frame_len = header_len.checked_add(payload_len).expect("frame len");
            if payload_len > 0 {
                checksum = checksum.wrapping_add(u64::from(wire[header_len]));
            }
            frames += 1;
            bytes += header.payload_len;
            wire = &wire[frame_len..];
        }
        Counts {
            frames,
            bytes,
            checksum,
        }
    }

    fn parse_streaming(wire: &[u8]) -> Counts {
        let mut parser = FrameParser::new();
        let mut rest = wire;
        let mut frames = 0_u64;
        let mut bytes = 0_u64;
        let mut checksum = 0_u64;

        loop {
            match parser.feed_one(rest).expect("valid frame") {
                FeedOutcome::NeedMore { consumed } => {
                    rest = &rest[consumed..];
                    assert!(rest.is_empty(), "complete wire should not need more input");
                    break;
                }
                FeedOutcome::Event { consumed, event } => {
                    match event {
                        FrameEvent::FrameStart(_) => {}
                        FrameEvent::PayloadChunk(chunk) => {
                            bytes += chunk.len() as u64;
                            if let Some(first) = chunk.first() {
                                checksum = checksum.wrapping_add(u64::from(*first));
                            }
                        }
                        FrameEvent::FrameEnd => frames += 1,
                    }
                    rest = &rest[consumed..];
                    if rest.is_empty() && parser.is_idle() {
                        break;
                    }
                }
            }
        }

        Counts {
            frames,
            bytes,
            checksum,
        }
    }

    fn parse_ws_client(wire: &[u8], payload_len: usize) -> Counts {
        let mut ws = common::open_ws_client(wire.len(), payload_len);
        let mut frames = 0_u64;
        let mut bytes = 0_u64;
        let mut checksum = 0_u64;
        ws.feed_recv(wire);
        ws.drain_data_events(|ev| match ev {
            DataEvent::Binary(payload) => {
                frames += 1;
                bytes += payload.len() as u64;
                if let Some(first) = payload.first() {
                    checksum = checksum.wrapping_add(u64::from(*first));
                }
            }
            DataEvent::Text(text) => {
                frames += 1;
                bytes += text.len() as u64;
                if let Some(first) = text.as_bytes().first() {
                    checksum = checksum.wrapping_add(u64::from(*first));
                }
            }
        })
        .expect("drain ws data");
        Counts {
            frames,
            bytes,
            checksum,
        }
    }
}
