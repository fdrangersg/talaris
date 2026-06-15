#![allow(
    dead_code,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

#[cfg(not(target_os = "linux"))]
fn main() {
    common::print_linux_only("local_compare");
}

#[path = "common.rs"]
mod common;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = run() {
        eprintln!("local_compare: {e}");
        std::process::exit(1);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Transport {
    Talaris,
    Tungstenite,
    Both,
}

impl Transport {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Talaris => "talaris",
            Self::Tungstenite => "tungstenite",
            Self::Both => "both",
        }
    }
}

impl std::str::FromStr for Transport {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "talaris" => Ok(Self::Talaris),
            "tungstenite" => Ok(Self::Tungstenite),
            "both" => Ok(Self::Both),
            other => Err(format!("unknown --transport {other:?}")),
        }
    }
}

#[derive(Debug)]
struct Config {
    transport: Transport,
    seconds: u64,
    messages: u64,
    warmup_messages: u64,
    payload_len: usize,
    frames_per_write: usize,
    buf_size: u32,
    buf_entries: u16,
    sq_entries: u32,
    cq_entries: u32,
    completion_batch: usize,
    spin_iters: usize,
    user_cpu: Option<usize>,
    server_cpu: Option<usize>,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let transport = common::arg_or("--transport", Transport::Both);
        let seconds = common::arg_or("--seconds", 8_u64);
        let messages = common::arg_or("--messages", 0_u64);
        if seconds == 0 && messages == 0 {
            return Err("--seconds and --messages cannot both be zero".to_owned());
        }

        let buf_size = common::arg_or("--buf-size", 4096_u32);
        let buf_entries = common::arg_or("--buf-entries", 256_u16);
        let sq_entries = common::arg_or("--sq-entries", 512_u32);
        let cq_entries = common::arg_or("--cq-entries", 1024_u32);
        common::validate_power_of_two_u16("--buf-entries", buf_entries)?;
        common::validate_power_of_two_u32("--sq-entries", sq_entries)?;
        common::validate_power_of_two_u32("--cq-entries", cq_entries)?;

        Ok(Self {
            transport,
            seconds,
            messages,
            warmup_messages: common::arg_or("--warmup-messages", 100_000_u64),
            payload_len: common::arg_or("--payload", 256_usize).max(1),
            frames_per_write: common::arg_or("--frames-per-write", 16_usize).max(1),
            buf_size,
            buf_entries,
            sq_entries,
            cq_entries,
            completion_batch: common::arg_or("--completion-batch", 64_usize).max(1),
            spin_iters: common::arg_or("--spin-iters", 256_usize),
            user_cpu: common::optional_arg("--user-cpu"),
            server_cpu: common::optional_arg("--server-cpu"),
        })
    }

    fn print(&self) {
        println!(
            "bench_config bench=local_compare transport={} seconds={} messages={} warmup_messages={} payload={} frames_per_write={} buf={}x{} sq_entries={} cq_entries={} completion_batch={} spin_iters={}",
            self.transport.as_str(),
            self.seconds,
            self.messages,
            self.warmup_messages,
            self.payload_len,
            self.frames_per_write,
            self.buf_entries,
            self.buf_size,
            self.sq_entries,
            self.cq_entries,
            self.completion_batch,
            self.spin_iters,
        );
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    if common::flag_present("--help") {
        print_usage();
        return Ok(());
    }

    let cfg = Config::from_args()?;
    cfg.print();

    match cfg.transport {
        Transport::Talaris => run_talaris_once(&cfg)?,
        Transport::Tungstenite => run_tungstenite_once(&cfg)?,
        Transport::Both => {
            run_talaris_once(&cfg)?;
            run_tungstenite_once(&cfg)?;
        }
    }

    Ok(())
}

fn run_talaris_once(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let server =
        common::spawn_local_stream_server(cfg.payload_len, cfg.frames_per_write, cfg.server_cpu)?;
    let addr = server.addr();
    let _pin = cfg.user_cpu.map(|cpu| common::PinGuard::pin("user", cpu));

    let conn_cfg = talaris::connection::ConnectionConfig::new("localhost", addr.port(), "/")
        .with_tls(false)
        .with_sq_entries(cfg.sq_entries)
        .with_cq_entries(cfg.cq_entries)
        .with_buf_ring(cfg.buf_size, cfg.buf_entries)
        .with_ws_limits(cfg.payload_len, cfg.payload_len as u64)
        .with_ingress_stats(true)
        .with_observability_sample_rate_bps(0)
        .with_observability_histograms(false);
    let proactor_cfg = conn_cfg.proactor;
    let mut pool = talaris::Pool::new(
        talaris::PoolConfig::new(proactor_cfg).with_completion_batch_capacity(cfg.completion_batch),
    )?;
    let handle = pool.connect_blocking_to(conn_cfg, addr)?;
    assert_eq!(pool.state(handle), Some(talaris::connection::State::Open));

    let mut warmup = common::MessageStats::default();
    while warmup.messages < cfg.warmup_messages {
        pump_talaris(&mut pool, cfg.spin_iters, &mut warmup)?;
    }

    let mut stats = common::MessageStats::default();
    let cpu = common::ThreadCpuTimer::start();
    let started = std::time::Instant::now();
    while should_continue(cfg, &stats, started.elapsed()) {
        pump_talaris(&mut pool, cfg.spin_iters, &mut stats)?;
    }

    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();
    common::print_result("local_compare", "talaris", &stats, elapsed, cpu_elapsed);
    common::print_ingress_stats(handle, pool.ingress_stats(handle));

    drop(pool);
    server.join()?;
    Ok(())
}

fn pump_talaris(
    pool: &mut talaris::Pool,
    spin_iters: usize,
    stats: &mut common::MessageStats,
) -> Result<(), talaris::connection::ConnectionError> {
    if spin_iters == 0 {
        pool.pump_data(|_, ev| record_talaris_event(stats, &ev))
    } else {
        pool.pump_data_spin(spin_iters, |_, ev| record_talaris_event(stats, &ev))
            .map(|_| ())
    }
}

fn record_talaris_event(stats: &mut common::MessageStats, ev: &talaris::ws::DataEvent<'_>) {
    match ev {
        talaris::ws::DataEvent::Text(payload) => stats.record_text(payload),
        talaris::ws::DataEvent::Binary(payload) => stats.record_binary(payload),
    }
}

fn run_tungstenite_once(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    use std::net::TcpStream;

    use tungstenite::client::{IntoClientRequest, client};

    let server =
        common::spawn_local_stream_server(cfg.payload_len, cfg.frames_per_write, cfg.server_cpu)?;
    let addr = server.addr();
    let _pin = cfg.user_cpu.map(|cpu| common::PinGuard::pin("user", cpu));

    let stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;
    let request = format!("ws://localhost:{}/", addr.port()).into_client_request()?;
    let (mut socket, _) = client(request, stream)?;

    let mut warmup = common::MessageStats::default();
    while warmup.messages < cfg.warmup_messages {
        record_tungstenite_message(&mut warmup, socket.read()?)?;
    }

    let mut stats = common::MessageStats::default();
    let cpu = common::ThreadCpuTimer::start();
    let started = std::time::Instant::now();
    while should_continue(cfg, &stats, started.elapsed()) {
        record_tungstenite_message(&mut stats, socket.read()?)?;
    }

    let elapsed = started.elapsed();
    let cpu_elapsed = cpu.elapsed();
    common::print_result("local_compare", "tungstenite", &stats, elapsed, cpu_elapsed);

    drop(socket);
    server.join()?;
    Ok(())
}

fn record_tungstenite_message(
    stats: &mut common::MessageStats,
    message: tungstenite::Message,
) -> Result<(), tungstenite::Error> {
    match message {
        tungstenite::Message::Text(payload) => stats.record_text(payload.as_str()),
        tungstenite::Message::Binary(payload) => stats.record_binary(payload.as_ref()),
        tungstenite::Message::Ping(_)
        | tungstenite::Message::Pong(_)
        | tungstenite::Message::Frame(_) => {}
        tungstenite::Message::Close(_) => return Err(tungstenite::Error::ConnectionClosed),
    }
    Ok(())
}

fn should_continue(
    cfg: &Config,
    stats: &common::MessageStats,
    elapsed: std::time::Duration,
) -> bool {
    let time_ok = cfg.seconds == 0 || elapsed < std::time::Duration::from_secs(cfg.seconds);
    let messages_ok = cfg.messages == 0 || stats.messages < cfg.messages;
    time_ok && messages_ok
}

fn print_usage() {
    println!(
        "local_compare bench\n\
         \n\
         Strict local plain-WS comparison using the same loopback stream server,\n\
         payload, frames-per-write, sink checksum, and CPU pinning.\n\
         \n\
         Args:\n\
           --transport talaris|tungstenite|both\n\
           --seconds N               wall-clock run limit, 0 disables time limit\n\
           --messages N              message limit, 0 disables message limit\n\
           --warmup-messages N       messages discarded before timing each transport\n\
           --payload N               binary payload bytes per WS message\n\
           --frames-per-write N      server-side WS frames per write(2)\n\
           --buf-size N              talaris io_uring provided buffer slot size\n\
           --buf-entries N           talaris provided buffer entries, power of two\n\
           --sq-entries N            talaris io_uring SQ entries, power of two\n\
           --cq-entries N            talaris io_uring CQ entries, power of two\n\
           --completion-batch N      talaris Pool CQE scratch buffer capacity\n\
           --spin-iters N            talaris spin count; 0 uses blocking pump_data\n\
           --user-cpu N              pin benchmark thread\n\
           --server-cpu N            pin loopback server thread"
    );
}
