//! 极简 TCP socket
//!
//! 不走 `std::net::TcpStream` 是因为：
//! 1. `std::net::TcpStream` 默认 blocking + 自带 buffered I/O，io_uring 不需要这些；
//! 2. 我们要拿 raw fd 直接喂给 io_uring SQE，包一层 std 没意义；
//! 3. `connect(2)` 我们走 [`super::Proactor::submit_connect`] 异步发——不在这里 block。
//!
//! 只做 socket(2) + setsockopt + Drop close。

// 模块内的 `.expect()` 全部是 c_int → socklen_t 的静态 size 断言（`sockaddr_in`
// / `sockaddr_in6` / `c_int` 的字节宽度在 libc 头里是 compile-time 常量）。
// 走到 panic 等于 libc ABI 损坏，HFT 进程应直接崩。
#![allow(clippy::expect_used)]

use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// IP 协议族。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Domain {
    V4,
    V6,
}

/// `libc::sockaddr_storage` + len 联合。给 io_uring `Connect` SQE 用。
///
/// 必须用 `repr(C)` 保证 `as_ptr()` 直接转成 `*const sockaddr`。
#[repr(C)]
pub struct SockAddr {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
}

impl std::fmt::Debug for SockAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SockAddr").field("len", &self.len).finish()
    }
}

impl SockAddr {
    /// 从 std `SocketAddr` 构造。V4 / V6 分支分别填 `sockaddr_in` / `sockaddr_in6`。
    #[must_use]
    pub fn from_std(addr: SocketAddr) -> Self {
        // SAFETY: zero-init libc::sockaddr_storage 是合法的（POD）
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let len = match addr {
            SocketAddr::V4(v4) => {
                let raw = libc::sockaddr_in {
                    sin_family: u16::try_from(libc::AF_INET).unwrap_or(libc::AF_INET as u16),
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(v4.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                // SAFETY: sockaddr_storage 比 sockaddr_in 大；write 直接覆盖。
                unsafe {
                    let dst = (&raw mut storage).cast::<libc::sockaddr_in>();
                    std::ptr::write(dst, raw);
                }
                u32::try_from(std::mem::size_of::<libc::sockaddr_in>())
                    .expect("sockaddr_in size fits in socklen_t")
            }
            SocketAddr::V6(v6) => {
                let raw = libc::sockaddr_in6 {
                    sin6_family: u16::try_from(libc::AF_INET6).unwrap_or(libc::AF_INET6 as u16),
                    sin6_port: v6.port().to_be(),
                    sin6_flowinfo: v6.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: v6.ip().octets(),
                    },
                    sin6_scope_id: v6.scope_id(),
                };
                // SAFETY: 同上
                unsafe {
                    let dst = (&raw mut storage).cast::<libc::sockaddr_in6>();
                    std::ptr::write(dst, raw);
                }
                u32::try_from(std::mem::size_of::<libc::sockaddr_in6>())
                    .expect("sockaddr_in6 size fits in socklen_t")
            }
        };
        Self { storage, len }
    }

    /// 给 io_uring Connect SQE 用的 `*const sockaddr` + len。
    ///
    /// **生命周期约束**：返回的指针在 `self` 被 drop 前都有效。caller 必须保证
    /// 把这块 SockAddr 留到对应 CQE 拿到之后再 drop（io_uring kernel 端会读它）。
    #[must_use]
    pub const fn as_ptr(&self) -> *const libc::sockaddr {
        (&raw const self.storage).cast::<libc::sockaddr>()
    }

    #[must_use]
    pub const fn len(&self) -> libc::socklen_t {
        self.len
    }
}

/// 拥有式 TCP socket。`Drop` 自动 `close(2)`。
///
/// **不带 `O_NONBLOCK`**——io_uring 自己管阻塞语义，setting nonblock 反而会让
/// 某些 op（比如 connect）的语义变奇怪。socket flag 只设 `SOCK_CLOEXEC`。
#[derive(Debug)]
pub struct TcpSocket {
    fd: OwnedFd,
}

impl TcpSocket {
    /// `socket(AF_INET / AF_INET6, SOCK_STREAM | SOCK_CLOEXEC, 0)`
    pub fn new(domain: Domain) -> io::Result<Self> {
        let af = match domain {
            Domain::V4 => libc::AF_INET,
            Domain::V6 => libc::AF_INET6,
        };
        // SAFETY: socket(2) 是 thread-safe，参数为标准常量
        let raw = unsafe { libc::socket(af, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: socket(2) 成功返回一个全新独占 fd，所有权归我们
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        Ok(Self { fd })
    }

    /// 关 Nagle。
    pub fn set_nodelay(&self, on: bool) -> io::Result<()> {
        let val: libc::c_int = on.into();
        // SAFETY: setsockopt 接受 (fd, level, optname, optval ptr, optlen) 五个参数；
        // 我们传 IPPROTO_TCP/TCP_NODELAY + 一个有效 c_int 指针 + 它的 size。
        let rc = unsafe {
            libc::setsockopt(
                self.fd.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                (&raw const val).cast::<libc::c_void>(),
                u32::try_from(std::mem::size_of_val(&val)).expect("c_int size fits in socklen_t"),
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// `SO_REUSEADDR`。
    pub fn set_reuseaddr(&self, on: bool) -> io::Result<()> {
        let val: libc::c_int = on.into();
        // SAFETY: 同 set_nodelay
        let rc = unsafe {
            libc::setsockopt(
                self.fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                (&raw const val).cast::<libc::c_void>(),
                u32::try_from(std::mem::size_of_val(&val)).expect("c_int size fits in socklen_t"),
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// raw fd —— 喂给 io_uring SQE 用。
    #[must_use]
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    #[test]
    fn sockaddr_v4_layout() {
        let std_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080));
        let sa = SockAddr::from_std(std_addr);
        assert_eq!(sa.len() as usize, std::mem::size_of::<libc::sockaddr_in>());
        // SAFETY: SockAddr::as_ptr 返回的指针在 sa 仍存活时有效
        let in4 = unsafe { &*sa.as_ptr().cast::<libc::sockaddr_in>() };
        assert_eq!(u16::from_be(in4.sin_port), 8080);
        // 127.0.0.1 大小端无关（每个字节一致）—— 这里直接对网络序构造
        let expected = u32::from_ne_bytes([127, 0, 0, 1]);
        assert_eq!(in4.sin_addr.s_addr, expected);
    }

    #[test]
    fn sockaddr_v6_layout() {
        let std_addr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 9090, 0, 0));
        let sa = SockAddr::from_std(std_addr);
        assert_eq!(sa.len() as usize, std::mem::size_of::<libc::sockaddr_in6>());
        // SAFETY: 见 v4 测试
        let in6 = unsafe { &*sa.as_ptr().cast::<libc::sockaddr_in6>() };
        assert_eq!(u16::from_be(in6.sin6_port), 9090);
    }

    #[test]
    fn socket_create_and_drop_v4() {
        let s = TcpSocket::new(Domain::V4).unwrap();
        assert!(s.as_raw_fd() >= 0);
        // drop 自动关闭——这里无法直接验证 fd 被 close，但至少 new() 成功
    }

    #[test]
    fn socket_create_and_drop_v6() {
        let s = TcpSocket::new(Domain::V6).unwrap();
        assert!(s.as_raw_fd() >= 0);
    }

    #[test]
    fn set_nodelay_succeeds() {
        let s = TcpSocket::new(Domain::V4).unwrap();
        s.set_nodelay(true).unwrap();
    }
}
