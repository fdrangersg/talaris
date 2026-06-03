// binance_futures_live —— Binance USD-M perpetual live market-data bench.
//
// 同机并发跑 talaris 与 tokio-tungstenite，两边都订阅 BTC/ETH perpetual 的：
// - `<symbol>@bookTicker`：BBO，/public
// - `<symbol>@depth@100ms`：L2 diff depth，/public
// - `<symbol>@aggTrade`：public trade aggregation，/market
//
// Binance USD-M 新 routed endpoint 下，bookTicker/depth 属于 /public，aggTrade
// 属于 /market；因此每个实现都同时维护 2 条 WSS 连接，并把两条连接的 framing
// 与 JSON 分类成本合并计入一个结果。
//
// 输出的 event lag = 本机收到帧时的 wall-clock - payload.data.E。它包含公网、
// 交易所推送排队、TLS/WS/framing、以及本机/交易所时钟偏差；只能作为同机
// 同窗口对照分布，不能解释成撮合延迟或纯 framing latency。
//
// 注意：inter-arrival 主要是交易所推送节奏，JSON classify cost 主要是业务解析
// 路径；二者不再作为 I/O 模型 bench 的默认输出。
//
// 运行：
//
// ```bash
// taskset -c 0-7 cargo bench --bench binance_futures_live -- \
//     --seconds 30 --warmup-seconds 5 \
//     --symbols btcusdt,ethusdt \
//     --talaris-pump spin \
//     --talaris-cpu 1 --tokio-cpu 2 --sq-poll-cpu 5 \
//     --buf-size 8192 --buf-entries 256
// ```

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value,
    clippy::panic,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

#[path = "common/mod.rs"]
mod common;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("binance_futures_live: skipped - io_uring hot path only runs on Linux");
}

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = linux_impl::run() {
        panic!("binance_futures_live failed: {e}");
    }
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::error::Error;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use futures_util::StreamExt;
    use hdrhistogram::Histogram;
    use serde_json::Value;
    use talaris::Pool;
    use talaris::connection::{ConnectionConfig, State};
    use talaris::ws::DataEvent as WsDataEvent;
    use tokio_tungstenite::tungstenite::protocol::Message;
    use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

    use super::common;
    use super::common::PinGuard;

    type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
    type TokioWs = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

    const DEFAULT_HOST: &str = "fstream.binance.com";
    const DEFAULT_SYMBOLS: &str = "btcusdt,ethusdt";

    #[derive(Debug, Clone)]
    struct BenchConfig {
        host: String,
        public_path: String,
        market_path: String,
        symbols: Vec<String>,
        public_streams: Vec<String>,
        market_streams: Vec<String>,
        seconds: f64,
        warmup_seconds: f64,
        start_timeout_seconds: f64,
        talaris_cpu: usize,
        tokio_cpu: usize,
        sq_poll_cpu: u32,
        spin_iters: usize,
        talaris_pump: TalarisPumpMode,
        tune: common::TalarisTuneConfig,
        staging: common::BenchStagingConfig,
    }

    #[derive(Debug)]
    enum WorkerEvent {
        Ready(&'static str),
        Failed(&'static str, String),
    }

    #[derive(Debug)]
    struct Outcome {
        label: &'static str,
        elapsed: Duration,
        cpu: Duration,
        stats: Stats,
    }

    #[derive(Debug)]
    struct Stats {
        text_frames: u64,
        binary_frames: u64,
        control_frames: u64,
        payload_bytes: u64,
        checksum: u64,
        bbo_frames: u64,
        depth_frames: u64,
        trade_frames: u64,
        unknown_frames: u64,
        json_errors: u64,
        missing_event_time: u64,
        negative_event_lag: u64,
        payload_size: Histogram<u64>,
        event_lag_ns: Histogram<u64>,
        bbo_event_lag_ns: Histogram<u64>,
        depth_event_lag_ns: Histogram<u64>,
        trade_event_lag_ns: Histogram<u64>,
    }

    impl Stats {
        fn new() -> Self {
            Self {
                text_frames: 0,
                binary_frames: 0,
                control_frames: 0,
                payload_bytes: 0,
                checksum: 0,
                bbo_frames: 0,
                depth_frames: 0,
                trade_frames: 0,
                unknown_frames: 0,
                json_errors: 0,
                missing_event_time: 0,
                negative_event_lag: 0,
                payload_size: common::new_hist(),
                event_lag_ns: common::new_hist(),
                bbo_event_lag_ns: common::new_hist(),
                depth_event_lag_ns: common::new_hist(),
                trade_event_lag_ns: common::new_hist(),
            }
        }

        const fn frames(&self) -> u64 {
            self.text_frames + self.binary_frames
        }

        fn record_text(&mut self, text: &str) {
            self.text_frames += 1;
            self.record_payload(text.as_bytes());
        }

        fn record_binary(&mut self, payload: &[u8]) {
            self.binary_frames += 1;
            self.record_payload(payload);
        }

        const fn record_control(&mut self) {
            self.control_frames += 1;
        }

        fn record_payload(&mut self, payload: &[u8]) {
            let recv_epoch_ms = unix_epoch_ms_now();

            self.payload_bytes += payload.len() as u64;
            self.checksum = self.checksum.rotate_left(5)
                ^ payload.len() as u64
                ^ u64::from(payload.first().copied().unwrap_or_default());
            self.payload_size.record(payload.len().max(1) as u64).ok();

            if let Ok((kind, event_epoch_ms)) = decode_market_payload(payload) {
                self.record_market_kind(kind);
                self.record_event_lag(kind, recv_epoch_ms, event_epoch_ms);
            } else {
                self.json_errors += 1;
                self.unknown_frames += 1;
            }
        }

        const fn record_market_kind(&mut self, kind: MarketKind) {
            match kind {
                MarketKind::Bbo => self.bbo_frames += 1,
                MarketKind::Depth => self.depth_frames += 1,
                MarketKind::Trade => self.trade_frames += 1,
                MarketKind::Unknown => self.unknown_frames += 1,
            }
        }

        fn record_event_lag(
            &mut self,
            kind: MarketKind,
            recv_epoch_ms: u128,
            event_epoch_ms: Option<u64>,
        ) {
            let Some(event_epoch_ms) = event_epoch_ms else {
                self.missing_event_time += 1;
                return;
            };
            let event_epoch_ms = u128::from(event_epoch_ms);
            if recv_epoch_ms < event_epoch_ms {
                self.negative_event_lag += 1;
                return;
            }
            let lag_ns =
                ((recv_epoch_ms - event_epoch_ms) * 1_000_000).min(u128::from(u64::MAX)) as u64;
            let lag_ns = lag_ns.max(1);
            self.event_lag_ns.record(lag_ns).ok();
            match kind {
                MarketKind::Bbo => self.bbo_event_lag_ns.record(lag_ns).ok(),
                MarketKind::Depth => self.depth_event_lag_ns.record(lag_ns).ok(),
                MarketKind::Trade => self.trade_event_lag_ns.record(lag_ns).ok(),
                MarketKind::Unknown => None,
            };
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum MarketKind {
        Bbo,
        Depth,
        Trade,
        Unknown,
    }

    #[derive(Debug, Clone, Copy)]
    enum TalarisPumpMode {
        Spin,
        Block,
    }

    impl TalarisPumpMode {
        fn from_arg(raw: &str) -> Self {
            match raw {
                "spin" => Self::Spin,
                "block" | "blocking" => Self::Block,
                _ => panic!("--talaris-pump must be spin or block"),
            }
        }

        const fn label(self) -> &'static str {
            match self {
                Self::Spin => "spin",
                Self::Block => "block",
            }
        }
    }

    pub fn run() -> BenchResult<()> {
        let cfg = BenchConfig::from_args();
        cfg.validate();
        print_config(&cfg);

        let start_gate = Arc::new(AtomicBool::new(false));
        let cancel_gate = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = mpsc::channel();

        let talaris_handle = spawn_worker(
            "talaris",
            cfg.clone(),
            start_gate.clone(),
            cancel_gate.clone(),
            ready_tx.clone(),
            run_talaris_worker,
        );
        let tokio_handle = spawn_worker(
            "tokio-tungstenite",
            cfg.clone(),
            start_gate.clone(),
            cancel_gate.clone(),
            ready_tx,
            run_tokio_worker,
        );

        wait_for_workers_ready(
            &ready_rx,
            Duration::from_secs_f64(cfg.start_timeout_seconds),
            &start_gate,
            &cancel_gate,
        )?;
        eprintln!("[bench] both workers are warm; starting measure phase");
        start_gate.store(true, Ordering::Release);

        let talaris = join_worker("talaris", talaris_handle)?;
        let tokio = join_worker("tokio-tungstenite", tokio_handle)?;
        print_results(&talaris, &tokio);
        Ok(())
    }

    impl BenchConfig {
        fn from_args() -> Self {
            let seconds: f64 = common::arg_or("--seconds", 30.0);
            let warmup_seconds: f64 = common::arg_or("--warmup-seconds", 5.0);
            let start_timeout_seconds: f64 = common::arg_or("--start-timeout-seconds", 45.0);
            let talaris_cpu: usize = common::arg_or("--talaris-cpu", 1);
            let tokio_cpu: usize = common::arg_or("--tokio-cpu", 2);
            let sq_poll_cpu: u32 = common::arg_or("--sq-poll-cpu", 5);
            let spin_iters: usize = common::arg_or("--spin-iters", 256);
            let talaris_pump =
                TalarisPumpMode::from_arg(&common::arg_or("--talaris-pump", "spin".to_owned()));
            let tune = common::TalarisTuneConfig::from_args(8192, 256);
            let staging = common::BenchStagingConfig::from_args();
            let host: String = common::arg_or("--host", DEFAULT_HOST.to_owned());
            let symbols_csv: String = common::arg_or("--symbols", DEFAULT_SYMBOLS.to_owned());
            let depth_speed: String = common::arg_or("--depth-speed", "100ms".to_owned());

            let symbols = parse_symbols(&symbols_csv);
            let public_streams = build_public_streams(&symbols, &depth_speed);
            let market_streams = build_market_streams(&symbols);
            let public_path = combined_stream_path("/public", &public_streams);
            let market_path = combined_stream_path("/market", &market_streams);

            Self {
                host,
                public_path,
                market_path,
                symbols,
                public_streams,
                market_streams,
                seconds,
                warmup_seconds,
                start_timeout_seconds,
                talaris_cpu,
                tokio_cpu,
                sq_poll_cpu,
                spin_iters,
                talaris_pump,
                tune,
                staging,
            }
        }

        fn validate(&self) {
            assert!(self.seconds > 0.0, "--seconds must be > 0");
            assert!(self.warmup_seconds >= 0.0, "--warmup-seconds must be >= 0");
            assert!(
                self.start_timeout_seconds > 0.0,
                "--start-timeout-seconds must be > 0"
            );
        }

        fn public_url(&self) -> String {
            format!("wss://{}{}", self.host, self.public_path)
        }

        fn market_url(&self) -> String {
            format!("wss://{}{}", self.host, self.market_path)
        }
    }

    fn spawn_worker<F>(
        label: &'static str,
        cfg: BenchConfig,
        start_gate: Arc<AtomicBool>,
        cancel_gate: Arc<AtomicBool>,
        ready_tx: mpsc::Sender<WorkerEvent>,
        run_worker: F,
    ) -> thread::JoinHandle<BenchResult<Outcome>>
    where
        F: FnOnce(
                BenchConfig,
                Arc<AtomicBool>,
                Arc<AtomicBool>,
                mpsc::Sender<WorkerEvent>,
            ) -> BenchResult<Outcome>
            + Send
            + 'static,
    {
        thread::spawn(move || {
            let result = run_worker(cfg, start_gate, cancel_gate, ready_tx.clone());
            if let Err(e) = &result {
                let _ = ready_tx.send(WorkerEvent::Failed(label, e.to_string()));
            }
            result
        })
    }

    fn wait_for_workers_ready(
        ready_rx: &mpsc::Receiver<WorkerEvent>,
        timeout: Duration,
        start_gate: &AtomicBool,
        cancel_gate: &AtomicBool,
    ) -> BenchResult<()> {
        let deadline = Instant::now() + timeout;
        let mut ready = 0_u8;
        while ready < 2 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                cancel_gate.store(true, Ordering::Release);
                start_gate.store(true, Ordering::Release);
                return Err(boxed_error(format!(
                    "workers were not ready within {:.3}s",
                    timeout.as_secs_f64()
                )));
            }

            match ready_rx.recv_timeout(remaining) {
                Ok(WorkerEvent::Ready(label)) => {
                    ready += 1;
                    eprintln!("[bench] {label} ready ({ready}/2)");
                }
                Ok(WorkerEvent::Failed(label, error)) => {
                    cancel_gate.store(true, Ordering::Release);
                    start_gate.store(true, Ordering::Release);
                    return Err(boxed_error(format!("{label} failed before start: {error}")));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    cancel_gate.store(true, Ordering::Release);
                    start_gate.store(true, Ordering::Release);
                    return Err(boxed_error(format!(
                        "workers were not ready within {:.3}s",
                        timeout.as_secs_f64()
                    )));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    cancel_gate.store(true, Ordering::Release);
                    start_gate.store(true, Ordering::Release);
                    return Err(boxed_error("worker ready channel disconnected"));
                }
            }
        }
        Ok(())
    }

    fn join_worker(
        label: &'static str,
        handle: thread::JoinHandle<BenchResult<Outcome>>,
    ) -> BenchResult<Outcome> {
        handle
            .join()
            .map_err(|_| boxed_error(format!("{label} thread panicked")))?
    }

    fn run_talaris_worker(
        cfg: BenchConfig,
        start_gate: Arc<AtomicBool>,
        cancel_gate: Arc<AtomicBool>,
        ready_tx: mpsc::Sender<WorkerEvent>,
    ) -> BenchResult<Outcome> {
        let _guard = PinGuard::pin("binance-futures-talaris", cfg.talaris_cpu);
        let base = cfg.tune.apply_connection(
            ConnectionConfig::new(&cfg.host, 443, cfg.public_path.clone())
                .with_sq_poll(10_000, Some(cfg.sq_poll_cpu)),
        );
        let mut pool = Pool::new(cfg.tune.pool_config(base.proactor))?;
        let public = pool.connect_blocking(base)?;
        let market_cfg = cfg.tune.apply_connection(
            ConnectionConfig::new(&cfg.host, 443, cfg.market_path.clone())
                .with_sq_poll(10_000, Some(cfg.sq_poll_cpu)),
        );
        let market = pool.connect_blocking(market_cfg)?;
        assert_eq!(pool.state(public), Some(State::Open));
        assert_eq!(pool.state(market), Some(State::Open));
        eprintln!("[talaris] connected public+market WSS");

        let warmup = run_talaris_for(
            &mut pool,
            cfg.talaris_pump,
            cfg.spin_iters,
            Duration::from_secs_f64(cfg.warmup_seconds),
        )?;
        eprintln!(
            "[talaris] warmup: {} frames in {:.3}s",
            common::fmt_int(warmup.frames()),
            cfg.warmup_seconds
        );

        ready_tx
            .send(WorkerEvent::Ready("talaris"))
            .map_err(|e| boxed_error(format!("send talaris ready: {e}")))?;
        wait_for_start("talaris", &start_gate, &cancel_gate)?;

        let measure_for = Duration::from_secs_f64(cfg.seconds);
        let cpu = common::ThreadCpuTimer::start();
        let started = Instant::now();
        let stats = run_talaris_for(&mut pool, cfg.talaris_pump, cfg.spin_iters, measure_for)?;
        let elapsed = started.elapsed();
        let cpu = cpu.elapsed();

        pool.initiate_close(public, 1000, "benchmark complete").ok();
        pool.initiate_close(market, 1000, "benchmark complete").ok();
        drain_talaris_close(&mut pool, Duration::from_millis(500));

        Ok(Outcome {
            label: "talaris",
            elapsed,
            cpu,
            stats,
        })
    }

    fn run_talaris_for(
        pool: &mut Pool,
        pump_mode: TalarisPumpMode,
        spin_iters: usize,
        duration: Duration,
    ) -> BenchResult<Stats> {
        let mut stats = Stats::new();
        let started = Instant::now();
        while started.elapsed() < duration {
            match pump_mode {
                TalarisPumpMode::Spin => {
                    pool.pump_data_spin(spin_iters, |_handle, ev| match ev {
                        WsDataEvent::Text(text) => stats.record_text(text),
                        WsDataEvent::Binary(payload) => stats.record_binary(payload),
                    })?;
                }
                TalarisPumpMode::Block => {
                    pool.pump_data(|_handle, ev| match ev {
                        WsDataEvent::Text(text) => stats.record_text(text),
                        WsDataEvent::Binary(payload) => stats.record_binary(payload),
                    })?;
                }
            }
        }
        Ok(stats)
    }

    fn drain_talaris_close(pool: &mut Pool, duration: Duration) {
        let started = Instant::now();
        while started.elapsed() < duration {
            let _ = pool.pump_data_nowait(|_, _| {});
        }
    }

    fn run_tokio_worker(
        cfg: BenchConfig,
        start_gate: Arc<AtomicBool>,
        cancel_gate: Arc<AtomicBool>,
        ready_tx: mpsc::Sender<WorkerEvent>,
    ) -> BenchResult<Outcome> {
        let _guard = PinGuard::pin("binance-futures-tokio", cfg.tokio_cpu);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()?;
        rt.block_on(run_tokio_async(cfg, start_gate, cancel_gate, ready_tx))
    }

    async fn run_tokio_async(
        cfg: BenchConfig,
        start_gate: Arc<AtomicBool>,
        cancel_gate: Arc<AtomicBool>,
        ready_tx: mpsc::Sender<WorkerEvent>,
    ) -> BenchResult<Outcome> {
        let public_url = cfg.public_url();
        let market_url = cfg.market_url();
        let (mut public_ws, _) = connect_async_with_config(
            public_url.as_str(),
            Some(tungstenite_config(cfg.staging)),
            false,
        )
        .await?;
        let (mut market_ws, _) = connect_async_with_config(
            market_url.as_str(),
            Some(tungstenite_config(cfg.staging)),
            false,
        )
        .await?;
        eprintln!("[tokio-tungstenite] connected public+market WSS");

        let warmup = run_tokio_for(
            &mut public_ws,
            &mut market_ws,
            Duration::from_secs_f64(cfg.warmup_seconds),
        )
        .await?;
        eprintln!(
            "[tokio-tungstenite] warmup: {} frames in {:.3}s",
            common::fmt_int(warmup.frames()),
            cfg.warmup_seconds
        );

        ready_tx
            .send(WorkerEvent::Ready("tokio-tungstenite"))
            .map_err(|e| boxed_error(format!("send tokio ready: {e}")))?;
        wait_for_start("tokio-tungstenite", &start_gate, &cancel_gate)?;

        let measure_for = Duration::from_secs_f64(cfg.seconds);
        let cpu = common::ThreadCpuTimer::start();
        let started = Instant::now();
        let stats = run_tokio_for(&mut public_ws, &mut market_ws, measure_for).await?;
        let elapsed = started.elapsed();
        let cpu = cpu.elapsed();

        Ok(Outcome {
            label: "tokio-tungstenite",
            elapsed,
            cpu,
            stats,
        })
    }

    async fn run_tokio_for(
        public_ws: &mut TokioWs,
        market_ws: &mut TokioWs,
        duration: Duration,
    ) -> BenchResult<Stats> {
        let mut stats = Stats::new();
        let started = Instant::now();
        while started.elapsed() < duration {
            tokio::select! {
                msg = public_ws.next() => handle_tokio_message("public", msg, &mut stats)?,
                msg = market_ws.next() => handle_tokio_message("market", msg, &mut stats)?,
            }
        }
        Ok(stats)
    }

    fn tungstenite_config(staging: common::BenchStagingConfig) -> WebSocketConfig {
        WebSocketConfig::default()
            .read_buffer_size(staging.tungstenite_read_buffer_capacity)
            .write_buffer_size(staging.tungstenite_write_buffer_capacity)
            .max_write_buffer_size(staging.tungstenite_max_write_buffer_capacity)
    }

    fn handle_tokio_message(
        route: &str,
        msg: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
        stats: &mut Stats,
    ) -> BenchResult<()> {
        let Some(msg) = msg else {
            return Err(boxed_error(format!("{route} websocket stream ended")));
        };
        match msg? {
            Message::Text(text) => stats.record_text(text.as_str()),
            Message::Binary(payload) => stats.record_binary(payload.as_ref()),
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => stats.record_control(),
            Message::Close(frame) => {
                return Err(boxed_error(format!(
                    "{route} websocket closed during benchmark: {frame:?}"
                )));
            }
        }
        Ok(())
    }

    fn wait_for_start(
        label: &str,
        start_gate: &AtomicBool,
        cancel_gate: &AtomicBool,
    ) -> BenchResult<()> {
        while !start_gate.load(Ordering::Acquire) {
            if cancel_gate.load(Ordering::Acquire) {
                return Err(boxed_error(format!("{label} canceled before measure")));
            }
            thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }

    fn decode_market_payload(
        payload: &[u8],
    ) -> Result<(MarketKind, Option<u64>), serde_json::Error> {
        let value: Value = serde_json::from_slice(payload)?;
        let stream = value.get("stream").and_then(Value::as_str);
        let data = value.get("data").unwrap_or(&value);
        let kind = stream
            .and_then(classify_stream)
            .or_else(|| {
                data.get("e")
                    .and_then(Value::as_str)
                    .and_then(classify_event_type)
            })
            .unwrap_or(MarketKind::Unknown);
        let event_epoch_ms = data.get("E").and_then(Value::as_u64);
        Ok((kind, event_epoch_ms))
    }

    fn classify_stream(stream: &str) -> Option<MarketKind> {
        if stream.contains("@bookTicker") {
            Some(MarketKind::Bbo)
        } else if stream.contains("@depth") {
            Some(MarketKind::Depth)
        } else if stream.contains("@aggTrade") {
            Some(MarketKind::Trade)
        } else {
            None
        }
    }

    fn classify_event_type(event: &str) -> Option<MarketKind> {
        match event {
            "bookTicker" => Some(MarketKind::Bbo),
            "depthUpdate" => Some(MarketKind::Depth),
            "aggTrade" => Some(MarketKind::Trade),
            _ => None,
        }
    }

    fn unix_epoch_ms_now() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    }

    fn parse_symbols(csv: &str) -> Vec<String> {
        let symbols: Vec<_> = csv
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase)
            .collect();
        assert!(!symbols.is_empty(), "--symbols must not be empty");
        for symbol in &symbols {
            assert!(
                symbol
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()),
                "invalid symbol {symbol:?}: use ASCII letters and digits"
            );
        }
        symbols
    }

    fn build_public_streams(symbols: &[String], depth_speed: &str) -> Vec<String> {
        let mut streams = Vec::with_capacity(symbols.len() * 2);
        for symbol in symbols {
            streams.push(format!("{symbol}@bookTicker"));
            streams.push(depth_stream_name(symbol, depth_speed));
        }
        streams
    }

    fn build_market_streams(symbols: &[String]) -> Vec<String> {
        symbols
            .iter()
            .map(|symbol| format!("{symbol}@aggTrade"))
            .collect()
    }

    fn depth_stream_name(symbol: &str, depth_speed: &str) -> String {
        match depth_speed {
            "" | "250ms" => format!("{symbol}@depth"),
            "100ms" | "500ms" => format!("{symbol}@depth@{depth_speed}"),
            _ => panic!(
                "--depth-speed must be one of 100ms, 250ms, 500ms, or empty string for default"
            ),
        }
    }

    fn combined_stream_path(route: &str, streams: &[String]) -> String {
        assert!(!streams.is_empty(), "streams must not be empty");
        let route = route.trim_end_matches('/');
        format!("{route}/stream?streams={}", streams.join("/"))
    }

    fn print_config(cfg: &BenchConfig) {
        eprintln!("=========================================================");
        eprintln!(" binance_futures_live - Binance USD-M perpetual WSS");
        eprintln!("=========================================================");
        eprintln!(" public url  : {}", cfg.public_url());
        eprintln!(" market url  : {}", cfg.market_url());
        eprintln!(" symbols     : {}", cfg.symbols.join(","));
        eprintln!(" public      : {} streams", cfg.public_streams.len());
        eprintln!(" market      : {} streams", cfg.market_streams.len());
        eprintln!(" warmup      : {:.3}s", cfg.warmup_seconds);
        eprintln!(" measure     : {:.3}s", cfg.seconds);
        eprintln!(
            " talaris     : user->CPU {}, SQ_POLL->CPU {}",
            cfg.talaris_cpu, cfg.sq_poll_cpu
        );
        eprintln!(" tokio       : user->CPU {}", cfg.tokio_cpu);
        eprintln!(" talaris pump: {}", cfg.talaris_pump.label());
        eprintln!(" spin_iters  : {}", cfg.spin_iters);
        cfg.tune.print_stderr(" ");
        cfg.staging.print_stderr(" ");
        eprintln!();
    }

    fn print_results(talaris: &Outcome, tokio: &Outcome) {
        println!();
        println!("=== binance_futures_live ===");
        println!(
            "{:<20} {:>12} {:>10} {:>14} {:>12} {:>14} {:>9} {:>14}",
            "impl", "frames", "seconds", "frames/s", "MiB/s", "cpu ns/frame", "cpu%", "checksum"
        );
        print_outcome_row(talaris);
        print_outcome_row(tokio);

        let talaris_fps = frames_per_second(talaris);
        let tokio_fps = frames_per_second(tokio);
        if tokio_fps > 0.0 {
            println!(
                "framing throughput ratio talaris/tokio-tungstenite: {:.3}x",
                talaris_fps / tokio_fps
            );
        }
        println!();

        print_kind_counts(talaris, tokio);
        print_kind_sample_counts(talaris, tokio);
        println!();

        print_payload_size_comparison(talaris, tokio);
        println!();
        println!("--- E-to-app event lag distributions ---");
        common::print_comparison(&[
            (talaris.label, &talaris.stats.event_lag_ns),
            (tokio.label, &tokio.stats.event_lag_ns),
        ]);
        println!("  above: local receive wall-clock - exchange event E");
        println!();
        println!("--- E-to-app event lag by stream kind ---");
        print_kind_lag("BBO/bookTicker", talaris, tokio, |s| &s.bbo_event_lag_ns);
        print_kind_lag("L2/depth", talaris, tokio, |s| &s.depth_event_lag_ns);
        print_kind_lag("trade/aggTrade", talaris, tokio, |s| &s.trade_event_lag_ns);
        println!();
        println!(
            "event lag is not exchange/matching latency; it includes WAN, server push cadence, TLS/WS/framing and clock skew."
        );
    }

    fn print_outcome_row(outcome: &Outcome) {
        let frames = outcome.stats.frames();
        let seconds = outcome.elapsed.as_secs_f64();
        let fps = frames_per_second(outcome);
        let mib_per_sec = if seconds > 0.0 {
            outcome.stats.payload_bytes as f64 / seconds / (1024.0 * 1024.0)
        } else {
            0.0
        };
        println!(
            "{:<20} {:>12} {:>10.3} {:>14.2} {:>12.4} {:>14} {:>8.2}% {:>14}",
            outcome.label,
            common::fmt_int(frames),
            seconds,
            fps,
            mib_per_sec,
            common::ns_per_frame(outcome.cpu, frames),
            common::cpu_pct(outcome.cpu, outcome.elapsed),
            outcome.stats.checksum
        );
    }

    fn frames_per_second(outcome: &Outcome) -> f64 {
        if outcome.elapsed.is_zero() {
            return 0.0;
        }
        outcome.stats.frames() as f64 / outcome.elapsed.as_secs_f64()
    }

    fn print_kind_counts(talaris: &Outcome, tokio: &Outcome) {
        println!(
            "{:<20} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "impl", "text", "binary", "control", "BBO", "depth", "trade", "json_err"
        );
        for outcome in [talaris, tokio] {
            println!(
                "{:<20} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
                outcome.label,
                common::fmt_int(outcome.stats.text_frames),
                common::fmt_int(outcome.stats.binary_frames),
                common::fmt_int(outcome.stats.control_frames),
                common::fmt_int(outcome.stats.bbo_frames),
                common::fmt_int(outcome.stats.depth_frames),
                common::fmt_int(outcome.stats.trade_frames),
                common::fmt_int(outcome.stats.json_errors),
            );
        }
        println!(
            "{:<20} {:>10} {:>10} {:>10}",
            "event time gaps", "missing E", "negative", "unknown"
        );
        for outcome in [talaris, tokio] {
            println!(
                "{:<20} {:>10} {:>10} {:>10}",
                outcome.label,
                common::fmt_int(outcome.stats.missing_event_time),
                common::fmt_int(outcome.stats.negative_event_lag),
                common::fmt_int(outcome.stats.unknown_frames),
            );
        }
    }

    fn print_kind_sample_counts(talaris: &Outcome, tokio: &Outcome) {
        println!();
        println!(
            "{:<20} {:<16} {:>12} {:>12} {:>12} {:>10}",
            "impl", "kind", "frames", "lag n", "no lag", "share"
        );
        for outcome in [talaris, tokio] {
            let total = outcome.stats.frames();
            print_kind_sample_row(
                outcome.label,
                "BBO/bookTicker",
                outcome.stats.bbo_frames,
                outcome.stats.bbo_event_lag_ns.len(),
                total,
            );
            print_kind_sample_row(
                outcome.label,
                "L2/depth",
                outcome.stats.depth_frames,
                outcome.stats.depth_event_lag_ns.len(),
                total,
            );
            print_kind_sample_row(
                outcome.label,
                "trade/aggTrade",
                outcome.stats.trade_frames,
                outcome.stats.trade_event_lag_ns.len(),
                total,
            );
        }
    }

    fn print_kind_sample_row(
        impl_label: &str,
        kind: &str,
        frames: u64,
        lag_samples: u64,
        total_frames: u64,
    ) {
        let no_lag = frames.saturating_sub(lag_samples);
        let share = if total_frames == 0 {
            0.0
        } else {
            100.0 * frames as f64 / total_frames as f64
        };
        println!(
            "{:<20} {:<16} {:>12} {:>12} {:>12} {:>9.2}%",
            impl_label,
            kind,
            common::fmt_int(frames),
            common::fmt_int(lag_samples),
            common::fmt_int(no_lag),
            share
        );
    }

    fn print_payload_size_comparison(talaris: &Outcome, tokio: &Outcome) {
        println!(
            "{:<20} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "payload size", "mean B", "p50 B", "p99 B", "p99.9 B", "max B", "n"
        );
        for outcome in [talaris, tokio] {
            let hist = &outcome.stats.payload_size;
            println!(
                "{:<20} {:>10.1} {:>10} {:>10} {:>10} {:>10} {:>10}",
                outcome.label,
                hist.mean(),
                hist.value_at_quantile(0.50),
                hist.value_at_quantile(0.99),
                hist.value_at_quantile(0.999),
                hist.max(),
                common::fmt_int(hist.len()),
            );
        }
    }

    fn print_kind_lag(
        label: &str,
        talaris: &Outcome,
        tokio: &Outcome,
        hist: fn(&Stats) -> &Histogram<u64>,
    ) {
        println!("{label}");
        common::print_comparison(&[
            (talaris.label, hist(&talaris.stats)),
            (tokio.label, hist(&tokio.stats)),
        ]);
    }

    fn boxed_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
        Box::new(std::io::Error::other(message.into()))
    }
}
