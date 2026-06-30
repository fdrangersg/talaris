#![allow(
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::print_stdout,
    clippy::unwrap_used
)]

#[cfg(not(target_os = "linux"))]
fn main() {
    common::print_linux_only("local_lifecycle");
}

#[path = "common.rs"]
mod common;

#[cfg(target_os = "linux")]
use std::io::{Read, Write};

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = run() {
        eprintln!("local_lifecycle: {e}");
        std::process::exit(1);
    }
}

#[cfg(target_os = "linux")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
    use std::thread;
    use std::time::Instant;

    use talaris::{Pool, PoolConfig, State};

    let cycles = common::arg_or("--cycles", 100_u64).max(1);
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?;
    let addr = listener.local_addr()?;
    let accepts = cycles + 1;
    let (server_done_tx, server_done_rx) = std::sync::mpsc::channel();
    let server = thread::spawn(move || run_server(&listener, accepts, &server_done_rx));

    let mut pool = Pool::new(PoolConfig::default())?;
    let mut handle = pool.connect_blocking_to(plain_cfg(addr.port()), addr)?;
    assert_eq!(pool.state(handle), Some(State::Open));
    let slot = handle.as_u32();
    let start_generation = handle.generation();

    let start = Instant::now();
    for _ in 0..cycles {
        let next = pool.reconnect_to(handle, plain_cfg(addr.port()), addr)?;
        assert_eq!(next.as_u32(), slot);
        assert!(next.generation() > handle.generation());
        assert_eq!(pool.state(handle), None);
        assert_eq!(pool.state(next), Some(State::Open));
        assert_eq!(pool.conn_count(), 1);
        handle = next;
    }
    let elapsed = start.elapsed();
    pool.remove_conn(handle)?;
    let _ = server_done_tx.send(());
    server.join().expect("server join")?;

    let avg_ns = elapsed.as_nanos() as f64 / cycles as f64;
    let avg_us = avg_ns / 1_000.0;
    let avg_ms = avg_us / 1_000.0;
    println!(
        "bench_result bench=local_lifecycle cycles={} total_ms={:.3} avg_reconnect_ns={:.0} avg_reconnect_us={:.3} avg_reconnect_ms={:.6} slot={} generation_start={} generation_end={} conn_count={}",
        cycles,
        elapsed.as_secs_f64() * 1_000.0,
        avg_ns,
        avg_us,
        avg_ms,
        slot,
        start_generation,
        handle.generation(),
        pool.conn_count(),
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn plain_cfg(port: u16) -> talaris::ConnectionConfig {
    talaris::ConnectionConfig::new("localhost", port, "/lifecycle").with_tls(false)
}

#[cfg(target_os = "linux")]
fn run_server(
    listener: &std::net::TcpListener,
    accepts: u64,
    done: &std::sync::mpsc::Receiver<()>,
) -> Result<(), String> {
    let mut streams = Vec::with_capacity(usize::try_from(accepts).unwrap_or(usize::MAX));
    for _ in 0..accepts {
        streams.push(accept_ws_upgrade(listener).map_err(|e| e.to_string())?);
    }
    done.recv_timeout(std::time::Duration::from_secs(10))
        .map_err(|e| e.to_string())?;
    drop(streams);
    Ok(())
}

#[cfg(target_os = "linux")]
fn accept_ws_upgrade(listener: &std::net::TcpListener) -> std::io::Result<std::net::TcpStream> {
    let (mut stream, _) = listener.accept()?;
    stream.set_nodelay(true)?;
    let mut buf = [0_u8; 4096];
    let mut req = Vec::new();
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed during handshake",
            ));
        }
        let chunk = buf.get(..n).expect("read length is within buffer");
        req.extend_from_slice(chunk);
        if req.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let req = std::str::from_utf8(&req).map_err(std::io::Error::other)?;
    let key = req
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("sec-websocket-key:"))
        .and_then(|line| line.split(':').nth(1))
        .ok_or_else(|| std::io::Error::other("missing websocket key"))?
        .trim();
    let accept = talaris::ws::handshake::compute_accept(key);
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(response.as_bytes())?;
    Ok(stream)
}
