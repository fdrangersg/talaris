// binance_live —— 通过 Binance Spot 公共 WSS 订阅高频行情，量 TLS ingress
// steady-state 的 sink 投递分布。
//
// 这不是交易所撮合延迟 benchmark：公网 RTT、Binance 推送节奏和东京机房网络都会
// 进入结果。它用于确认真实 TLS + Text JSON + Ping/Pong 路径，并观察生产形态下的
// callback inter-arrival、payload size 和持续吞吐。传输层极限吞吐仍用 loopback
// `ws_ingress_single` / `ws_ingress_fanout` 测。
//
// 默认订阅 15 个热门 USDT symbol 的：
// - `<symbol>@trade`：逐笔成交，real-time
// - `<symbol>@bookTicker`：最优买卖价变化，real-time
// - `<symbol>@depth@100ms`：增量深度，100ms
//
// Binance 官方限制每条连接最多 1024 streams；默认只有 45 条。订阅请求仅发送一次，
// 也不会撞每秒 5 条 incoming message 限制。
//
// 运行：
//
// ```bash
// taskset -c 0-7 cargo bench --bench binance_live -- \
//     --seconds 30 --warmup-seconds 5 \
//     --user-cpu 1 --sq-poll-cpu 5 \
//     --buf-size 8192 --buf-entries 256
// ```

#![allow(
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

#[path = "common/mod.rs"]
mod common;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("binance_live: skipped - io_uring hot path only runs on Linux");
}

#[cfg(target_os = "linux")]
fn main() {
    linux_impl::run();
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::time::{Duration, Instant};

    use hdrhistogram::Histogram;
    use talaris::connection::{ConnectionConfig, State};
    use talaris::ws::DataEvent as WsDataEvent;
    use talaris::{Pool, PoolConfig};

    use super::common;
    use super::common::PinGuard;

    const DEFAULT_SYMBOLS: &str = "btcusdt,ethusdt,solusdt,xrpusdt,bnbusdt,dogeusdt,adausdt,trxusdt,linkusdt,\
         suiusdt,avaxusdt,ltcusdt,bchusdt,nearusdt,pepeusdt";

    struct Stats {
        text_frames: u64,
        binary_frames: u64,
        payload_bytes: u64,
        checksum: u64,
        last_arrival: Option<Instant>,
        inter_arrival: Histogram<u64>,
        payload_size: Histogram<u64>,
    }

    impl Stats {
        fn new() -> Self {
            Self {
                text_frames: 0,
                binary_frames: 0,
                payload_bytes: 0,
                checksum: 0,
                last_arrival: None,
                inter_arrival: common::new_hist(),
                payload_size: common::new_hist(),
            }
        }

        fn record(&mut self, ev: WsDataEvent<'_>) {
            let now = Instant::now();
            if let Some(previous) = self.last_arrival.replace(now) {
                common::record_ns(&mut self.inter_arrival, now - previous);
            }

            let payload = match ev {
                WsDataEvent::Text(text) => {
                    self.text_frames += 1;
                    text.as_bytes()
                }
                WsDataEvent::Binary(payload) => {
                    self.binary_frames += 1;
                    payload
                }
            };
            self.payload_bytes += payload.len() as u64;
            self.checksum ^= u64::from(payload.first().copied().unwrap_or_default());
            self.payload_size.record(payload.len().max(1) as u64).ok();
        }

        fn frames(&self) -> u64 {
            self.text_frames + self.binary_frames
        }
    }

    pub fn run() {
        let seconds: f64 = common::arg_or("--seconds", 30.0);
        let warmup_seconds: f64 = common::arg_or("--warmup-seconds", 5.0);
        let user_cpu: usize = common::arg_or("--user-cpu", 1);
        let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
        let spin_iters: usize = common::arg_or("--spin-iters", 256);
        let buf_size: u32 = common::arg_or("--buf-size", 8192);
        let buf_entries: u16 = common::arg_or("--buf-entries", 256);
        let symbols_csv: String = common::arg_or("--symbols", DEFAULT_SYMBOLS.to_owned());

        assert!(seconds > 0.0, "--seconds must be > 0");
        assert!(warmup_seconds >= 0.0, "--warmup-seconds must be >= 0");

        let symbols = parse_symbols(&symbols_csv);
        let streams = build_streams(&symbols);
        let subscribe = subscribe_request(&streams);

        eprintln!("=========================================================");
        eprintln!(" binance_live - Binance Spot public WSS TLS ingress");
        eprintln!("=========================================================");
        eprintln!(" endpoint    : wss://stream.binance.com:443/ws");
        eprintln!(" symbols     : {}", symbols.join(","));
        eprintln!(" streams     : {}", streams.len());
        eprintln!(" warmup      : {warmup_seconds:.3}s");
        eprintln!(" measure     : {seconds:.3}s");
        eprintln!(" talaris     : user->CPU {user_cpu}, SQ_POLL->CPU {sq_poll_cpu}");
        eprintln!(" spin_iters  : {spin_iters}");
        eprintln!(
            " buf_ring    : {buf_entries} x {buf_size}B = {} KiB pool",
            (u32::from(buf_entries) * buf_size) / 1024
        );
        eprintln!();

        let _guard = PinGuard::pin("binance-live", user_cpu);
        let cfg = ConnectionConfig::new("stream.binance.com", 443, "/ws")
            .with_sq_poll(10_000, Some(sq_poll_cpu))
            .with_buf_ring(buf_size, buf_entries);
        let mut pool = Pool::new(PoolConfig::new(cfg.proactor)).expect("pool");
        let handle = pool.connect_blocking(cfg).expect("connect Binance WSS");
        assert_eq!(pool.state(handle), Some(State::Open));
        eprintln!("[binance] connected; sending one SUBSCRIBE request");

        pool.send_text(handle, subscribe.as_bytes())
            .expect("send SUBSCRIBE");
        wait_for_subscribe_ack(&mut pool, spin_iters, Duration::from_secs(10));

        let warmup = run_for(
            &mut pool,
            spin_iters,
            Duration::from_secs_f64(warmup_seconds),
        );
        eprintln!(
            "[binance] warmup: {} frames in {:.3}s",
            warmup.frames(),
            warmup_seconds
        );

        let measure_for = Duration::from_secs_f64(seconds);
        let started = Instant::now();
        let stats = run_for(&mut pool, spin_iters, measure_for);
        let elapsed = started.elapsed();

        pool.initiate_close(handle, 1000, "benchmark complete").ok();
        let close_started = Instant::now();
        while close_started.elapsed() < Duration::from_secs(1) {
            let _ = pool.pump_data_nowait(|_, _| {});
            if matches!(pool.state(handle), Some(State::Closed)) {
                break;
            }
        }

        let frames = stats.frames();
        let frames_per_sec = frames as f64 / elapsed.as_secs_f64();
        let mib_per_sec = stats.payload_bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);

        println!();
        println!("=== binance_live ===");
        println!("elapsed       : {:.3}s", elapsed.as_secs_f64());
        println!("frames        : {frames}");
        println!("text frames   : {}", stats.text_frames);
        println!("binary frames : {}", stats.binary_frames);
        println!("payload bytes : {}", stats.payload_bytes);
        println!("frames/s      : {frames_per_sec:.2}");
        println!("payload MiB/s : {mib_per_sec:.4}");
        println!("checksum      : {}", stats.checksum);
        println!();
        common::print_hist("sink inter-arrival", &stats.inter_arrival);
        common::print_hist("payload size", &stats.payload_size);
        println!();
        println!("inter-arrival is callback-to-callback delivery spacing, not exchange latency.");
    }

    fn run_for(pool: &mut Pool, spin_iters: usize, duration: Duration) -> Stats {
        let mut stats = Stats::new();
        let started = Instant::now();
        while started.elapsed() < duration {
            pool.pump_data_spin(spin_iters, |_handle, ev| stats.record(ev))
                .expect("pump Binance data");
        }
        stats
    }

    fn wait_for_subscribe_ack(pool: &mut Pool, spin_iters: usize, timeout: Duration) {
        let started = Instant::now();
        let mut ack = false;
        let mut last_control_message: Option<String> = None;
        while !ack && started.elapsed() < timeout {
            pool.pump_data_spin(spin_iters, |_handle, ev| {
                let WsDataEvent::Text(text) = ev else {
                    return;
                };
                if is_subscribe_ack(text) {
                    ack = true;
                } else if text.contains("\"code\"") || text.contains("\"result\"") {
                    last_control_message = Some(text.to_owned());
                }
            })
            .expect("pump SUBSCRIBE ack");
        }
        assert!(
            ack,
            "Binance SUBSCRIBE ack timeout; last control message: {last_control_message:?}"
        );
        eprintln!("[binance] SUBSCRIBE acknowledged");
    }

    fn is_subscribe_ack(text: &str) -> bool {
        let compact: String = text.chars().filter(|c| !c.is_ascii_whitespace()).collect();
        compact.contains("\"result\":null") && compact.contains("\"id\":1")
    }

    fn parse_symbols(csv: &str) -> Vec<&str> {
        let symbols: Vec<_> = csv
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        assert!(!symbols.is_empty(), "--symbols must not be empty");
        for symbol in &symbols {
            assert!(
                symbol
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()),
                "invalid symbol {symbol:?}: use lowercase ASCII letters and digits"
            );
        }
        symbols
    }

    fn build_streams(symbols: &[&str]) -> Vec<String> {
        let mut streams = Vec::with_capacity(symbols.len() * 3);
        for symbol in symbols {
            streams.push(format!("{symbol}@trade"));
            streams.push(format!("{symbol}@bookTicker"));
            streams.push(format!("{symbol}@depth@100ms"));
        }
        streams
    }

    fn subscribe_request(streams: &[String]) -> String {
        let params = streams
            .iter()
            .map(|stream| format!("\"{stream}\""))
            .collect::<Vec<_>>()
            .join(",");
        format!(r#"{{"method":"SUBSCRIBE","params":[{params}],"id":1}}"#)
    }
}
