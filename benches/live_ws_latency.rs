#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("live_ws_latency: skipped - talaris live transport benchmark requires Linux");
}

#[cfg(target_os = "linux")]
#[path = "common.rs"]
mod common;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = linux::run() {
        eprintln!("live_ws_latency: {e}");
        std::process::exit(1);
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::cell::Cell;
    use std::fmt;
    use std::io::{self, Read, Write};
    use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
    use std::rc::Rc;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use talaris::proactor::{
        BufferRing, Domain, OpKind, Proactor, ProactorConfig, ProactorSetupFlags, SockAddr,
        SqeFlags, TcpSocket, UserData,
    };
    use talaris::tls::{TlsAdapter, TlsCipherPreference, TlsCryptoProvider};
    use talaris::ws::{DataEvent as TalarisDataEvent, Event as TalarisEvent, WsClient, WsConfig};
    use tungstenite::client::IntoClientRequest;
    use tungstenite::{Message, client as tungstenite_client};

    use super::common;

    const DEFAULT_SECONDS: u64 = 60;
    const DEFAULT_HOST: &str = "fstream.binance.com";
    const DEFAULT_PORT: u16 = 443;
    const DEFAULT_PATH: &str = "/public/stream";
    const DEFAULT_SUBSCRIBE: &str =
        r#"{"id":1,"method":"SUBSCRIBE","params":["btcusdt@bookTicker"]}"#;
    const DEFAULT_BUF_SIZE: u32 = 8 * 1024;
    const DEFAULT_BUF_ENTRIES: u16 = 256;
    const DEFAULT_SQ_ENTRIES: u32 = 512;
    const DEFAULT_CQ_ENTRIES: u32 = 1024;
    const DEFAULT_MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024;
    const DEFAULT_MAX_FRAME_PAYLOAD: u64 = 16 * 1024 * 1024;
    const DEFAULT_WS_RECV_CAPACITY: usize = 128 * 1024;
    const DEFAULT_WS_MESSAGE_CAPACITY: usize = 128 * 1024;
    const DEFAULT_WS_TX_CAPACITY: usize = 16 * 1024;
    const DEFAULT_READ_TIMEOUT_MS: u64 = 100;
    const RECV_TOKEN: u64 = 1;

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        if common::flag_present("--help") {
            print_usage();
            return Ok(());
        }

        let config = BenchConfig::from_args();
        println!("{config}");

        match config.transport {
            Transport::Talaris => run_talaris(&config)?.print(),
            Transport::Tungstenite => run_tungstenite(&config)?.print(),
            Transport::Both => {
                run_talaris(&config)?.print();
                run_tungstenite(&config)?.print();
            }
        }

        Ok(())
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Transport {
        Talaris,
        Tungstenite,
        Both,
    }

    impl FromStr for Transport {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match s {
                "talaris" => Ok(Self::Talaris),
                "tungstenite" => Ok(Self::Tungstenite),
                "both" => Ok(Self::Both),
                _ => Err(format!(
                    "invalid transport {s:?}; expected talaris, tungstenite, or both"
                )),
            }
        }
    }

    impl fmt::Display for Transport {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Talaris => f.write_str("talaris"),
                Self::Tungstenite => f.write_str("tungstenite"),
                Self::Both => f.write_str("both"),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct BenchConfig {
        transport: Transport,
        seconds: u64,
        host: String,
        port: u16,
        path: String,
        subscribe: String,
        user_cpu: Option<usize>,
        buf_size: u32,
        buf_entries: u16,
        sq_entries: u32,
        cq_entries: u32,
        read_timeout_ms: u64,
        assume_text_utf8: bool,
        tls_provider: TlsCryptoProvider,
        tls_cipher_preference: TlsCipherPreference,
        record_every: u64,
    }

    impl BenchConfig {
        fn from_args() -> Self {
            Self {
                transport: common::arg_or("--transport", Transport::Talaris),
                seconds: common::arg_or("--seconds", DEFAULT_SECONDS),
                host: common::arg_or("--host", DEFAULT_HOST.to_owned()),
                port: common::arg_or("--port", DEFAULT_PORT),
                path: common::arg_or("--path", DEFAULT_PATH.to_owned()),
                subscribe: common::arg_or("--subscribe", DEFAULT_SUBSCRIBE.to_owned()),
                user_cpu: common::optional_arg("--user-cpu"),
                buf_size: common::arg_or("--buf-size", DEFAULT_BUF_SIZE),
                buf_entries: common::arg_or("--buf-entries", DEFAULT_BUF_ENTRIES),
                sq_entries: common::arg_or("--sq-entries", DEFAULT_SQ_ENTRIES),
                cq_entries: common::arg_or("--cq-entries", DEFAULT_CQ_ENTRIES),
                read_timeout_ms: common::arg_or("--read-timeout-ms", DEFAULT_READ_TIMEOUT_MS),
                assume_text_utf8: common::flag_present("--assume-text-utf8"),
                tls_provider: common::arg_or("--tls-provider", TlsCryptoProvider::default()),
                tls_cipher_preference: common::arg_or(
                    "--tls-cipher-preference",
                    TlsCipherPreference::default(),
                ),
                record_every: common::arg_or("--record-every", 1_u64),
            }
        }

        fn duration(&self) -> Duration {
            Duration::from_secs(self.seconds)
        }

        fn endpoint_url(&self) -> String {
            format!("wss://{}{}", self.host, self.path)
        }
    }

    impl fmt::Display for BenchConfig {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "live_ws_latency transport={} seconds={} endpoint=wss://{}:{}{} user_cpu={:?} buf={}x{} sq_entries={} cq_entries={} read_timeout_ms={} assume_text_utf8={} tls_provider={} tls_cipher_preference={} record_every={}",
                self.transport,
                self.seconds,
                self.host,
                self.port,
                self.path,
                self.user_cpu,
                self.buf_entries,
                self.buf_size,
                self.sq_entries,
                self.cq_entries,
                self.read_timeout_ms,
                self.assume_text_utf8,
                self.tls_provider,
                self.tls_cipher_preference,
                self.record_every
            )
        }
    }

    #[derive(Clone)]
    struct BenchClock {
        base: Instant,
    }

    impl BenchClock {
        fn start() -> Self {
            Self {
                base: Instant::now(),
            }
        }

        fn now_ns(&self) -> u64 {
            self.base.elapsed().as_nanos() as u64
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct FrameMarks {
        transport_recv_ns: u64,
        tls_decode_start_ns: u64,
        tls_plaintext_ready_ns: u64,
        ws_payload_ready_ns: u64,
        sink_ready_ns: u64,
    }

    impl FrameMarks {
        fn transport_tls_ws_ns(self) -> u64 {
            self.ws_payload_ready_ns
                .saturating_sub(self.transport_recv_ns)
        }

        fn tls_ws_ns(self) -> u64 {
            self.ws_payload_ready_ns
                .saturating_sub(self.tls_decode_start_ns)
        }

        fn recv_to_tls_plaintext_ns(self) -> u64 {
            self.tls_plaintext_ready_ns
                .saturating_sub(self.transport_recv_ns)
        }

        fn plaintext_to_ws_payload_ns(self) -> u64 {
            self.ws_payload_ready_ns
                .saturating_sub(self.tls_plaintext_ready_ns)
        }

        fn payload_sink_ns(self) -> u64 {
            self.sink_ready_ns.saturating_sub(self.ws_payload_ready_ns)
        }

        fn total_to_sink_ns(self) -> u64 {
            self.sink_ready_ns.saturating_sub(self.transport_recv_ns)
        }
    }

    #[derive(Debug)]
    struct BenchReport {
        transport: &'static str,
        elapsed: Duration,
        frames: u64,
        text_frames: u64,
        binary_frames: u64,
        payload_bytes: u64,
        checksum: u64,
        record_every: u64,
        transport_tls_ws_ns: hdrhistogram::Histogram<u64>,
        tls_ws_ns: hdrhistogram::Histogram<u64>,
        recv_to_tls_plaintext_ns: hdrhistogram::Histogram<u64>,
        plaintext_to_ws_payload_ns: hdrhistogram::Histogram<u64>,
        plaintext_to_ws_payload_first_in_chunk_ns: hdrhistogram::Histogram<u64>,
        plaintext_to_ws_payload_rest_in_chunk_ns: hdrhistogram::Histogram<u64>,
        payload_sink_ns: hdrhistogram::Histogram<u64>,
        total_to_sink_ns: hdrhistogram::Histogram<u64>,
        total_to_sink_first_in_chunk_ns: hdrhistogram::Histogram<u64>,
        total_to_sink_rest_in_chunk_ns: hdrhistogram::Histogram<u64>,
        messages_per_recv_cqe: BatchStats,
        messages_per_plaintext_chunk: BatchStats,
        position_in_plaintext_chunk: BatchStats,
    }

    impl BenchReport {
        fn new(transport: &'static str, record_every: u64) -> Self {
            Self {
                transport,
                elapsed: Duration::ZERO,
                frames: 0,
                text_frames: 0,
                binary_frames: 0,
                payload_bytes: 0,
                checksum: 0,
                record_every: record_every.max(1),
                transport_tls_ws_ns: common::sampled_hist(),
                tls_ws_ns: common::sampled_hist(),
                recv_to_tls_plaintext_ns: common::sampled_hist(),
                plaintext_to_ws_payload_ns: common::sampled_hist(),
                plaintext_to_ws_payload_first_in_chunk_ns: common::sampled_hist(),
                plaintext_to_ws_payload_rest_in_chunk_ns: common::sampled_hist(),
                payload_sink_ns: common::sampled_hist(),
                total_to_sink_ns: common::sampled_hist(),
                total_to_sink_first_in_chunk_ns: common::sampled_hist(),
                total_to_sink_rest_in_chunk_ns: common::sampled_hist(),
                messages_per_recv_cqe: BatchStats::new(),
                messages_per_plaintext_chunk: BatchStats::new(),
                position_in_plaintext_chunk: BatchStats::new(),
            }
        }

        fn record_text(
            &mut self,
            payload: &str,
            transport_recv_ns: u64,
            tls_decode_start_ns: u64,
            tls_plaintext_ready_ns: u64,
            ws_payload_ready_ns: u64,
            plaintext_chunk_message_index: Option<u64>,
            clock: &BenchClock,
        ) {
            self.frames = self.frames.saturating_add(1);
            self.text_frames = self.text_frames.saturating_add(1);
            let should_record = self.should_record_sample();
            self.record_payload(
                payload.as_bytes(),
                transport_recv_ns,
                tls_decode_start_ns,
                tls_plaintext_ready_ns,
                ws_payload_ready_ns,
                plaintext_chunk_message_index,
                should_record,
                clock,
            );
        }

        fn record_binary(
            &mut self,
            payload: &[u8],
            transport_recv_ns: u64,
            tls_decode_start_ns: u64,
            tls_plaintext_ready_ns: u64,
            ws_payload_ready_ns: u64,
            plaintext_chunk_message_index: Option<u64>,
            clock: &BenchClock,
        ) {
            self.frames = self.frames.saturating_add(1);
            self.binary_frames = self.binary_frames.saturating_add(1);
            let should_record = self.should_record_sample();
            self.record_payload(
                payload,
                transport_recv_ns,
                tls_decode_start_ns,
                tls_plaintext_ready_ns,
                ws_payload_ready_ns,
                plaintext_chunk_message_index,
                should_record,
                clock,
            );
        }

        fn record_payload(
            &mut self,
            payload: &[u8],
            transport_recv_ns: u64,
            tls_decode_start_ns: u64,
            tls_plaintext_ready_ns: u64,
            ws_payload_ready_ns: u64,
            plaintext_chunk_message_index: Option<u64>,
            should_record: bool,
            clock: &BenchClock,
        ) {
            self.payload_bytes = self.payload_bytes.saturating_add(payload.len() as u64);
            if !should_record {
                return;
            }
            self.checksum = mix_checksum(self.checksum, payload);
            let sink_ready_ns = clock.now_ns();
            let marks = FrameMarks {
                transport_recv_ns,
                tls_decode_start_ns,
                tls_plaintext_ready_ns,
                ws_payload_ready_ns,
                sink_ready_ns,
            };
            let _ = self.transport_tls_ws_ns.record(marks.transport_tls_ws_ns());
            let _ = self.tls_ws_ns.record(marks.tls_ws_ns());
            let _ = self
                .recv_to_tls_plaintext_ns
                .record(marks.recv_to_tls_plaintext_ns());
            let _ = self
                .plaintext_to_ws_payload_ns
                .record(marks.plaintext_to_ws_payload_ns());
            let _ = self.payload_sink_ns.record(marks.payload_sink_ns());
            let _ = self.total_to_sink_ns.record(marks.total_to_sink_ns());
            if let Some(index) = plaintext_chunk_message_index {
                self.position_in_plaintext_chunk
                    .record(index.saturating_add(1));
                if index == 0 {
                    let _ = self
                        .plaintext_to_ws_payload_first_in_chunk_ns
                        .record(marks.plaintext_to_ws_payload_ns());
                    let _ = self
                        .total_to_sink_first_in_chunk_ns
                        .record(marks.total_to_sink_ns());
                } else {
                    let _ = self
                        .plaintext_to_ws_payload_rest_in_chunk_ns
                        .record(marks.plaintext_to_ws_payload_ns());
                    let _ = self
                        .total_to_sink_rest_in_chunk_ns
                        .record(marks.total_to_sink_ns());
                }
            }
        }

        fn should_record_sample(&self) -> bool {
            self.record_every == 1 || self.frames.is_multiple_of(self.record_every)
        }

        fn print(self) {
            println!(
                "live_ws_latency_result transport={} elapsed_ms={} frames={} text_frames={} binary_frames={} payload_bytes={} checksum={} record_every={}",
                self.transport,
                self.elapsed.as_millis(),
                self.frames,
                self.text_frames,
                self.binary_frames,
                self.payload_bytes,
                self.checksum,
                self.record_every
            );
            print_latency_hist(
                self.transport,
                "transport_tls_ws_ns",
                &self.transport_tls_ws_ns,
            );
            print_latency_hist(self.transport, "tls_ws_ns", &self.tls_ws_ns);
            print_latency_hist(
                self.transport,
                "recv_to_tls_plaintext_ns",
                &self.recv_to_tls_plaintext_ns,
            );
            print_latency_hist(
                self.transport,
                "plaintext_to_ws_payload_ns",
                &self.plaintext_to_ws_payload_ns,
            );
            print_latency_hist(
                self.transport,
                "plaintext_to_ws_payload_first_in_chunk_ns",
                &self.plaintext_to_ws_payload_first_in_chunk_ns,
            );
            print_latency_hist(
                self.transport,
                "plaintext_to_ws_payload_rest_in_chunk_ns",
                &self.plaintext_to_ws_payload_rest_in_chunk_ns,
            );
            print_latency_hist(self.transport, "payload_sink_ns", &self.payload_sink_ns);
            print_latency_hist(self.transport, "total_to_sink_ns", &self.total_to_sink_ns);
            print_latency_hist(
                self.transport,
                "total_to_sink_first_in_chunk_ns",
                &self.total_to_sink_first_in_chunk_ns,
            );
            print_latency_hist(
                self.transport,
                "total_to_sink_rest_in_chunk_ns",
                &self.total_to_sink_rest_in_chunk_ns,
            );
            self.messages_per_recv_cqe
                .print(self.transport, "messages_per_recv_cqe");
            self.messages_per_plaintext_chunk
                .print(self.transport, "messages_per_plaintext_chunk");
            self.position_in_plaintext_chunk
                .print(self.transport, "position_in_plaintext_chunk");
        }
    }

    #[derive(Debug)]
    struct BatchStats {
        samples: u64,
        zero_samples: u64,
        sum: u64,
        max: u64,
        positive: hdrhistogram::Histogram<u64>,
    }

    impl BatchStats {
        fn new() -> Self {
            Self {
                samples: 0,
                zero_samples: 0,
                sum: 0,
                max: 0,
                positive: hdrhistogram::Histogram::new_with_bounds(1, 1_000_000, 3)
                    .expect("batch histogram"),
            }
        }

        fn record(&mut self, value: u64) {
            self.samples = self.samples.saturating_add(1);
            self.sum = self.sum.saturating_add(value);
            self.max = self.max.max(value);
            if value == 0 {
                self.zero_samples = self.zero_samples.saturating_add(1);
            } else {
                let _ = self.positive.record(value);
            }
        }

        fn print(&self, transport: &str, name: &str) {
            let avg = if self.samples == 0 {
                0.0
            } else {
                self.sum as f64 / self.samples as f64
            };
            if self.positive.is_empty() {
                println!(
                    "live_ws_latency_batch transport={transport} metric={name} samples={} zero_samples={} avg={avg:.3} max={}",
                    self.samples, self.zero_samples, self.max
                );
                return;
            }
            println!(
                "live_ws_latency_batch transport={} metric={} samples={} zero_samples={} avg={:.3} p50={} p90={} p99={} max={}",
                transport,
                name,
                self.samples,
                self.zero_samples,
                avg,
                self.positive.value_at_quantile(0.50),
                self.positive.value_at_quantile(0.90),
                self.positive.value_at_quantile(0.99),
                self.max
            );
        }
    }

    fn run_talaris(config: &BenchConfig) -> Result<BenchReport, Box<dyn std::error::Error>> {
        let _pin = config
            .user_cpu
            .map(|cpu| common::PinGuard::pin("talaris-user", cpu));
        let addr = resolve_addr(&config.host, config.port)?;

        let socket = TcpSocket::new(domain_for_addr(addr))?;
        socket.set_nodelay(true)?;
        let fd = socket.as_raw_fd();
        let sock_addr = SockAddr::from_std(addr);
        let mut proactor = Proactor::new(
            ProactorConfig::default()
                .with_sq_entries(config.sq_entries)
                .with_cq_entries(config.cq_entries)
                .with_setup_flags(ProactorSetupFlags::SINGLE_ISSUER),
        )?;

        unsafe {
            proactor.submit_connect(
                fd,
                &sock_addr,
                UserData::new(OpKind::Connect, RECV_TOKEN),
                SqeFlags::NONE,
            )?;
        }
        proactor.submit_and_wait(1)?;
        let mut connect_result: Option<io::Result<usize>> = None;
        proactor.drain_completions(|c| {
            if c.user_data.kind() == Some(OpKind::Connect) {
                connect_result = Some(c.to_result());
            }
        });
        connect_result
            .ok_or("missing talaris connect completion")?
            .map(|_| ())?;

        let mut ring = BufferRing::new(&mut proactor, 0, config.buf_entries, config.buf_size)?;
        unsafe {
            proactor.submit_recv_multishot(
                fd,
                ring.bgid(),
                UserData::new(OpKind::Recv, RECV_TOKEN),
            )?;
        }
        proactor.submit()?;

        let mut tls = TlsAdapter::new_client_with_config(
            &config.host,
            Arc::new(TlsAdapter::client_config_with_cipher_preference(
                config.tls_provider,
                config.tls_cipher_preference,
            )?),
        )?;
        let mut ws_config = WsConfig::new(config.host.clone(), config.path.clone())
            .with_max_message_size(DEFAULT_MAX_MESSAGE_SIZE)
            .with_max_frame_payload(DEFAULT_MAX_FRAME_PAYLOAD)
            .with_initial_buffer_capacities(
                DEFAULT_WS_RECV_CAPACITY,
                DEFAULT_WS_MESSAGE_CAPACITY,
                DEFAULT_WS_TX_CAPACITY,
            );
        if config.assume_text_utf8 {
            // SAFETY: Binance public stream JSON text frames are UTF-8. This
            // benchmark flag measures the cost of avoiding duplicate UTF-8
            // validation before the downstream JSON decoder reads the payload.
            ws_config = unsafe { ws_config.with_assume_text_utf8_unchecked(true) };
        }
        let mut ws = WsClient::new_client(ws_config)?;
        let mut ciphertext = Vec::with_capacity(DEFAULT_WS_RECV_CAPACITY);
        tls.egress_plaintext(&[], &mut ciphertext)?;
        send_all_fd(fd, &ciphertext)?;
        ciphertext.clear();

        let clock = BenchClock::start();
        let started = Instant::now();
        let deadline = started + config.duration();
        let mut report = BenchReport::new("talaris", config.record_every);
        let mut ws_handshake_started = false;
        let mut subscribed = false;
        let mut completions = Vec::with_capacity(64);

        while Instant::now() < deadline {
            proactor.wait_for_cqe(1)?;
            completions.clear();
            proactor.drain_completions(|c| completions.push(c));

            for c in completions.drain(..) {
                if c.user_data.kind() != Some(OpKind::Recv) {
                    continue;
                }

                let transport_recv_ns = clock.now_ns();
                let bid = match c.buffer_id() {
                    Some(bid) => bid,
                    None => {
                        let n = c.to_result()?;
                        if n == 0 {
                            return Err("talaris peer closed".into());
                        }
                        continue;
                    }
                };
                let n = c.to_result()?;
                if n == 0 {
                    ring.recycle(bid);
                    return Err("talaris peer closed".into());
                }

                let frames_before_cqe = report.frames;
                let bytes = &ring.buffer(bid)[..n];
                let tls_decode_start_ns = clock.now_ns();
                let mut fed_plaintext = false;
                tls.ingest_ciphertext(bytes, &mut ciphertext, |plaintext| {
                    let tls_plaintext_ready_ns = clock.now_ns();
                    let frames_before_chunk = report.frames;
                    fed_plaintext = true;
                    if subscribed {
                        ws.drain_data_events_from_ingress(plaintext, |ev| match ev {
                            TalarisDataEvent::Text(payload) => {
                                let ws_payload_ready_ns = clock.now_ns();
                                let chunk_message_index =
                                    report.frames.saturating_sub(frames_before_chunk);
                                report.record_text(
                                    payload,
                                    transport_recv_ns,
                                    tls_decode_start_ns,
                                    tls_plaintext_ready_ns,
                                    ws_payload_ready_ns,
                                    Some(chunk_message_index),
                                    &clock,
                                );
                            }
                            TalarisDataEvent::Binary(payload) => {
                                let ws_payload_ready_ns = clock.now_ns();
                                let chunk_message_index =
                                    report.frames.saturating_sub(frames_before_chunk);
                                report.record_binary(
                                    payload,
                                    transport_recv_ns,
                                    tls_decode_start_ns,
                                    tls_plaintext_ready_ns,
                                    ws_payload_ready_ns,
                                    Some(chunk_message_index),
                                    &clock,
                                );
                            }
                        })
                        .expect("talaris ws data drain");
                    } else {
                        ws.feed_recv(plaintext);
                    }
                    report
                        .messages_per_plaintext_chunk
                        .record(report.frames.saturating_sub(frames_before_chunk));
                })?;
                report
                    .messages_per_recv_cqe
                    .record(report.frames.saturating_sub(frames_before_cqe));
                ring.recycle(bid);
                if !c.has_more() {
                    unsafe {
                        proactor.submit_recv_multishot(
                            fd,
                            ring.bgid(),
                            UserData::new(OpKind::Recv, RECV_TOKEN),
                        )?;
                    }
                    proactor.submit()?;
                }

                if !ciphertext.is_empty() {
                    send_all_fd(fd, &ciphertext)?;
                    ciphertext.clear();
                }

                if !tls.is_handshaking() && !ws_handshake_started {
                    tls.verify_alpn()?;
                    if let Some(suite) = tls.negotiated_cipher_suite() {
                        eprintln!("[talaris-user] tls_cipher_suite={suite:?}");
                    }
                    ws.begin_handshake()?;
                    flush_talaris_ws_tls(fd, &mut ws, &mut tls, &mut ciphertext)?;
                    ws_handshake_started = true;
                }

                if !fed_plaintext {
                    continue;
                }

                if !subscribed {
                    while let Some(ev) = ws.poll_event() {
                        if matches!(ev?, TalarisEvent::HandshakeComplete) {
                            ws.send_text(config.subscribe.as_bytes())?;
                            flush_talaris_ws_tls(fd, &mut ws, &mut tls, &mut ciphertext)?;
                            subscribed = true;
                            break;
                        }
                    }
                    continue;
                }
                flush_talaris_ws_tls(fd, &mut ws, &mut tls, &mut ciphertext)?;
            }
        }

        let _ = ring.unregister(&mut proactor);
        report.elapsed = started.elapsed();
        Ok(report)
    }

    fn run_tungstenite(config: &BenchConfig) -> Result<BenchReport, Box<dyn std::error::Error>> {
        let _pin = config
            .user_cpu
            .map(|cpu| common::PinGuard::pin("tungstenite-user", cpu));
        let addr = resolve_addr(&config.host, config.port)?;

        let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(10))?;
        tcp.set_nodelay(true)?;
        tcp.set_read_timeout(Some(Duration::from_millis(config.read_timeout_ms)))?;
        tcp.set_write_timeout(Some(Duration::from_secs(5)))?;

        let clock = BenchClock::start();
        let raw_marks = Rc::new(Cell::new(RawReadMark::default()));
        let frame_marks = Rc::new(Cell::new(FrameReadMark::default()));
        let metered_tcp = MeteredTcpStream {
            inner: tcp,
            clock: clock.clone(),
            marks: raw_marks.clone(),
        };

        let tls_config = TlsAdapter::client_config_with_cipher_preference(
            config.tls_provider,
            config.tls_cipher_preference,
        )?;
        let server_name = rustls::pki_types::ServerName::try_from(config.host.clone())?;
        let tls_conn = rustls::ClientConnection::new(Arc::new(tls_config), server_name)?;
        let tls_stream = MeteredTlsStream {
            inner: rustls::StreamOwned::new(tls_conn, metered_tcp),
            clock: clock.clone(),
            raw_marks: raw_marks.clone(),
            marks: frame_marks.clone(),
        };

        let request = config.endpoint_url().into_client_request()?;
        let (mut ws, _response) = tungstenite_client(request, tls_stream)?;
        ws.send(Message::Text(config.subscribe.clone().into()))?;

        let started = Instant::now();
        let deadline = started + config.duration();
        let mut report = BenchReport::new("tungstenite", config.record_every);

        while Instant::now() < deadline {
            match ws.read() {
                Ok(Message::Text(payload)) => {
                    let ws_payload_ready_ns = clock.now_ns();
                    let mark = frame_marks.get();
                    let payload: &str = payload.as_ref();
                    report.record_text(
                        payload,
                        mark.transport_recv_ns,
                        mark.tls_decode_start_ns,
                        mark.tls_plaintext_ready_ns,
                        ws_payload_ready_ns,
                        None,
                        &clock,
                    );
                }
                Ok(Message::Binary(payload)) => {
                    let ws_payload_ready_ns = clock.now_ns();
                    let mark = frame_marks.get();
                    let payload = payload.as_ref();
                    report.record_binary(
                        payload,
                        mark.transport_recv_ns,
                        mark.tls_decode_start_ns,
                        mark.tls_plaintext_ready_ns,
                        ws_payload_ready_ns,
                        None,
                        &clock,
                    );
                }
                Ok(Message::Ping(payload)) => {
                    ws.send(Message::Pong(payload))?;
                }
                Ok(Message::Close(_)) => return Err("tungstenite peer closed".into()),
                Ok(Message::Pong(_) | Message::Frame(_)) => {}
                Err(tungstenite::Error::Io(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(e) => return Err(Box::new(e)),
            }
        }

        report.elapsed = started.elapsed();
        Ok(report)
    }

    #[derive(Debug, Clone, Copy, Default)]
    struct RawReadMark {
        transport_recv_ns: u64,
        tls_decode_start_ns: u64,
    }

    #[derive(Debug, Clone, Copy, Default)]
    struct FrameReadMark {
        transport_recv_ns: u64,
        tls_decode_start_ns: u64,
        tls_plaintext_ready_ns: u64,
    }

    struct MeteredTcpStream {
        inner: TcpStream,
        clock: BenchClock,
        marks: Rc<Cell<RawReadMark>>,
    }

    impl fmt::Debug for MeteredTcpStream {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MeteredTcpStream").finish_non_exhaustive()
        }
    }

    impl Read for MeteredTcpStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.inner.read(buf)?;
            if n > 0 {
                let now = self.clock.now_ns();
                self.marks.set(RawReadMark {
                    transport_recv_ns: now,
                    tls_decode_start_ns: now,
                });
            }
            Ok(n)
        }
    }

    impl Write for MeteredTcpStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.inner.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }

    struct MeteredTlsStream {
        inner: rustls::StreamOwned<rustls::ClientConnection, MeteredTcpStream>,
        clock: BenchClock,
        raw_marks: Rc<Cell<RawReadMark>>,
        marks: Rc<Cell<FrameReadMark>>,
    }

    impl fmt::Debug for MeteredTlsStream {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MeteredTlsStream").finish_non_exhaustive()
        }
    }

    impl Read for MeteredTlsStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.inner.read(buf)?;
            if n > 0 {
                let raw = self.raw_marks.get();
                self.marks.set(FrameReadMark {
                    transport_recv_ns: raw.transport_recv_ns,
                    tls_decode_start_ns: raw.tls_decode_start_ns,
                    tls_plaintext_ready_ns: self.clock.now_ns(),
                });
            }
            Ok(n)
        }
    }

    impl Write for MeteredTlsStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.inner.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }

    fn flush_talaris_ws_tls(
        fd: i32,
        ws: &mut WsClient,
        tls: &mut TlsAdapter,
        ciphertext: &mut Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let pending_len = ws.pending_tx().len();
        if pending_len > 0 {
            tls.egress_plaintext(ws.pending_tx(), ciphertext)?;
            ws.ack_tx(pending_len);
        } else {
            tls.egress_plaintext(&[], ciphertext)?;
        }
        if !ciphertext.is_empty() {
            send_all_fd(fd, ciphertext)?;
            ciphertext.clear();
        }
        Ok(())
    }

    fn send_all_fd(fd: i32, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let rc = unsafe {
                libc::send(
                    fd,
                    bytes.as_ptr().cast::<libc::c_void>(),
                    bytes.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
            if rc == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "socket send returned 0",
                ));
            }
            bytes = &bytes[rc as usize..];
        }
        Ok(())
    }

    fn resolve_addr(host: &str, port: u16) -> io::Result<SocketAddr> {
        (host, port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "DNS returned no address"))
    }

    const fn domain_for_addr(addr: SocketAddr) -> Domain {
        match addr {
            SocketAddr::V4(_) => Domain::V4,
            SocketAddr::V6(_) => Domain::V6,
        }
    }

    fn mix_checksum(mut checksum: u64, bytes: &[u8]) -> u64 {
        for &b in bytes.iter().take(32) {
            checksum = checksum.rotate_left(5) ^ u64::from(b);
            checksum = checksum.wrapping_mul(0x9E37_79B1_85EB_CA87);
        }
        checksum ^ bytes.len() as u64
    }

    fn print_latency_hist(transport: &str, name: &str, hist: &hdrhistogram::Histogram<u64>) {
        if hist.is_empty() {
            println!("live_ws_latency_hist transport={transport} metric={name} samples=0");
            return;
        }
        println!(
            "live_ws_latency_hist transport={} metric={} samples={} min={} avg={:.0} p50={} p90={} p95={} p99={} p999={} max={}",
            transport,
            name,
            hist.len(),
            hist.min(),
            hist.mean(),
            hist.value_at_quantile(0.50),
            hist.value_at_quantile(0.90),
            hist.value_at_quantile(0.95),
            hist.value_at_quantile(0.99),
            hist.value_at_quantile(0.999),
            hist.max()
        );
    }

    fn print_usage() {
        println!(
            "Usage: cargo bench --bench live_ws_latency -- \
             [--transport talaris|tungstenite|both] [--seconds N] \
             [--host fstream.binance.com] [--port 443] [--path /public/stream] \
             [--subscribe JSON] [--user-cpu N] [--assume-text-utf8] \
             [--tls-provider aws-lc|ring] [--tls-cipher-preference default|aes128|aes256|chacha] \
             [--record-every N]"
        );
        println!(
            "Latency marks are monotonic *_ns. transport_recv_ns is user-space recv completion/read return, not NIC hardware timestamp."
        );
    }
}
