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
    eprintln!("read: skipped - talaris benches only run on Linux");
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
    use talaris::ws::frame::{MAX_HEADER_LEN, OpCode, encode_header};

    use super::common;

    struct MixedWire {
        wire: Vec<u8>,
        expected_sum: u64,
        messages: u64,
        payload_bytes: u64,
        max_payload_len: usize,
    }

    pub fn run() {
        let messages = common::arg_or("--messages", 100_000_usize);
        let mixed = build_mixed_wire(messages);

        println!(
            "read bench: talaris client inbound, tungstenite-style mixed small messages, messages={}, wire={}B",
            common::fmt_int(mixed.messages),
            common::fmt_int(mixed.wire.len() as u64)
        );
        println!(
            "{:<34} {:>14} {:>11} {:>11} {:>11} {:>12} {:>9}",
            "variant", "messages", "wall_ms", "user_ms", "msg/s", "MiB/s", "ns/msg"
        );

        let counts = run_read(&mixed);
        assert_eq!(counts.sum, mixed.expected_sum);
        assert_eq!(counts.messages, mixed.messages);
        print_row("read 100k mixed messages", &counts, mixed.payload_bytes);
    }

    fn build_mixed_wire(messages: usize) -> MixedWire {
        let mut wire = Vec::with_capacity(messages * 16);
        let mut header = [0_u8; MAX_HEADER_LEN];
        let mut expected_sum = 0_u64;
        let mut payload_bytes = 0_u64;
        let mut max_payload_len = 8_usize;

        for i in 0..messages {
            let id = u64::try_from(i).expect("message index fits u64");
            expected_sum = expected_sum.wrapping_add(id);
            if id.is_multiple_of(3) {
                let payload = id.to_le_bytes();
                let n = encode_header(
                    &mut header,
                    true,
                    OpCode::Binary,
                    None,
                    payload.len() as u64,
                );
                wire.extend_from_slice(&header[..n]);
                wire.extend_from_slice(&payload);
                payload_bytes += payload.len() as u64;
            } else {
                let payload = format!("{{\"id\":{id}}}");
                max_payload_len = max_payload_len.max(payload.len());
                let n = encode_header(&mut header, true, OpCode::Text, None, payload.len() as u64);
                wire.extend_from_slice(&header[..n]);
                wire.extend_from_slice(payload.as_bytes());
                payload_bytes += payload.len() as u64;
            }
        }

        MixedWire {
            wire,
            expected_sum,
            messages: messages as u64,
            payload_bytes,
            max_payload_len,
        }
    }

    fn run_read(mixed: &MixedWire) -> Counts {
        let mut ws = common::open_ws_client(mixed.wire.len(), mixed.max_payload_len);
        let mut messages = 0_u64;
        let mut sum = 0_u64;

        let cpu = common::ThreadCpuTimer::start();
        let wall = Instant::now();
        ws.feed_recv(black_box(&mixed.wire));
        ws.drain_data_events(|ev| {
            messages += 1;
            match ev {
                DataEvent::Binary(payload) => {
                    let bytes: [u8; 8] = payload.try_into().expect("binary id");
                    let id = u64::from_le_bytes(bytes);
                    sum = sum.wrapping_add(id);
                }
                DataEvent::Text(text) => {
                    let id = parse_id(text);
                    sum = sum.wrapping_add(id);
                }
            }
        })
        .expect("drain ws data");

        Counts {
            messages,
            sum,
            elapsed: wall.elapsed(),
            cpu: cpu.elapsed(),
        }
    }

    fn parse_id(text: &str) -> u64 {
        let id = text
            .strip_prefix("{\"id\":")
            .and_then(|s| s.strip_suffix('}'))
            .expect("json id envelope");
        id.parse().expect("id parses")
    }

    fn print_row(name: &str, counts: &Counts, payload_bytes: u64) {
        println!(
            "{name:<34} {:>14} {:>11.3} {:>11.3} {:>11.0} {:>12.1} {:>9}",
            common::fmt_int(counts.messages),
            counts.elapsed.as_secs_f64() * 1000.0,
            counts.cpu.as_secs_f64() * 1000.0,
            common::frames_per_sec(counts.messages, counts.elapsed),
            common::mib_per_sec(payload_bytes, counts.elapsed),
            common::ns_per_frame(counts.cpu, counts.messages)
        );
    }

    struct Counts {
        messages: u64,
        sum: u64,
        elapsed: Duration,
        cpu: Duration,
    }
}
