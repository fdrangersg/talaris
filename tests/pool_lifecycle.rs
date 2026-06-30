#![cfg(target_os = "linux")]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    clippy::missing_panics_doc
)]

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use talaris::{ConnectionConfig, Pool, PoolConfig, State};

fn spawn_idle_ws(path: &'static str) -> (SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || run_idle_ws(&listener, path));
    (addr, server)
}

fn run_idle_ws(listener: &TcpListener, expected_path: &'static str) {
    let mut stream = accept_ws_upgrade(listener, expected_path);
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let mut buf = [0_u8; 512];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(e) => panic!("server read failed: {e}"),
        }
    }
}

fn accept_ws_upgrade(listener: &TcpListener, expected_path: &str) -> TcpStream {
    let (mut stream, _) = listener.accept().expect("accept");
    stream.set_nodelay(true).unwrap();

    let mut buf = [0_u8; 4096];
    let mut req = Vec::new();
    loop {
        let n = stream.read(&mut buf).unwrap();
        assert!(n > 0, "client closed before upgrade request");
        let chunk = buf.get(..n).expect("read length is within buffer");
        req.extend_from_slice(chunk);
        if req.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    let req = std::str::from_utf8(&req).unwrap();
    assert!(
        req.starts_with(&format!("GET {expected_path} HTTP/1.1\r\n")),
        "request path mismatch: {req:?}"
    );
    let key = req
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("sec-websocket-key:"))
        .and_then(|line| line.split(':').nth(1))
        .expect("Sec-WebSocket-Key")
        .trim();
    let accept = talaris::ws::handshake::compute_accept(key);
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(response.as_bytes()).unwrap();
    stream
}

fn plain_cfg(addr: SocketAddr, path: &str) -> ConnectionConfig {
    ConnectionConfig::new("localhost", addr.port(), path).with_tls(false)
}

fn drive_until_open(pool: &mut Pool, handle: talaris::ConnHandle) {
    for _ in 0..500 {
        let _ = pool.pump_nowait(|_, _| {});
        if pool.state(handle) == Some(State::Open) {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("connection did not open");
}

#[test]
fn remove_conn_reuses_slot_and_rejects_stale_handle_through_public_api() {
    let (addr_a, server_a) = spawn_idle_ws("/old");
    let mut pool = Pool::new(PoolConfig::default()).expect("pool");
    let old = pool
        .connect_blocking_to(plain_cfg(addr_a, "/old"), addr_a)
        .expect("connect old");
    assert_eq!(pool.state(old), Some(State::Open));
    assert_eq!(pool.conn_count(), 1);

    pool.remove_conn(old).expect("remove old");
    assert_eq!(pool.conn_count(), 0);
    assert_eq!(pool.state(old), None);
    assert!(pool.send_text(old, b"stale").is_err());
    server_a.join().unwrap();

    let (addr_b, server_b) = spawn_idle_ws("/new");
    let new = pool
        .connect_blocking_to(plain_cfg(addr_b, "/new"), addr_b)
        .expect("connect new");
    assert_eq!(pool.state(new), Some(State::Open));
    assert_eq!(pool.conn_count(), 1);
    assert_eq!(new.as_u32(), old.as_u32());
    assert_eq!(new.generation(), old.generation() + 1);
    assert_ne!(new, old);
    assert!(pool.send_text(old, b"still stale").is_err());

    pool.remove_conn(new).expect("remove new");
    server_b.join().unwrap();
}

#[test]
fn submit_reconnect_to_reuses_slot_and_keeps_old_handle_stale() {
    let (addr_a, server_a) = spawn_idle_ws("/old");
    let (addr_b, server_b) = spawn_idle_ws("/new");
    let mut pool = Pool::new(PoolConfig::default()).expect("pool");
    let old = pool
        .connect_blocking_to(plain_cfg(addr_a, "/old"), addr_a)
        .expect("connect old");

    let new = pool
        .submit_reconnect_to(old, plain_cfg(addr_b, "/new"), addr_b)
        .expect("submit reconnect");
    assert_eq!(pool.state(old), None);
    assert_eq!(new.as_u32(), old.as_u32());
    assert_eq!(new.generation(), old.generation() + 1);
    assert_eq!(pool.conn_count(), 1);
    server_a.join().unwrap();

    drive_until_open(&mut pool, new);
    assert_eq!(pool.state(new), Some(State::Open));
    assert!(pool.send_text(old, b"stale").is_err());

    pool.remove_conn(new).expect("remove new");
    server_b.join().unwrap();
}

#[test]
fn reconnect_failure_removes_old_and_allows_later_reuse() {
    let (addr, server) = spawn_idle_ws("/old");
    let mut pool = Pool::new(PoolConfig::default()).expect("pool");
    let old = pool
        .connect_blocking_to(plain_cfg(addr, "/old"), addr)
        .expect("connect old");

    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let refused = listener.local_addr().unwrap();
    drop(listener);
    let err = pool.reconnect_to(old, plain_cfg(refused, "/refused"), refused);
    assert!(err.is_err(), "closed listener should refuse reconnect");
    assert_eq!(pool.state(old), None);
    assert_eq!(pool.conn_count(), 0);
    server.join().unwrap();

    let (next_addr, next_server) = spawn_idle_ws("/next");
    let next = pool
        .connect_blocking_to(plain_cfg(next_addr, "/next"), next_addr)
        .expect("connect next");
    assert_eq!(next.as_u32(), old.as_u32());
    assert!(next.generation() > old.generation());
    assert_eq!(pool.conn_count(), 1);

    pool.remove_conn(next).expect("remove next");
    next_server.join().unwrap();
}
