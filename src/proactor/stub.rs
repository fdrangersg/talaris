//! macOS / 非 Linux 平台占位
//!
//! io_uring 是 Linux-only。本 stub 让本地（macOS）`cargo check` / IDE 服务仍能过，
//! 但任何实际调用都会 `unimplemented!()`。CI / 生产构建必须在 Linux 上跑。
//!
//! 这里的类型签名和 [`super::uring`] / [`super::socket`] / [`super::op`] 严格 对齐，
//! 让上层代码不用 `cfg(target_os = "linux")` 包到处都是。

#![allow(dead_code, missing_debug_implementations)]
// stub 文件本身的意义就是"调用即崩"占位；`unimplemented!()` 是这层的契约。
#![allow(clippy::unimplemented)]

use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use thiserror::Error;

const STUB_PANIC: &str = "io_uring proactor is Linux-only; build on Linux to run hot path";

#[derive(Debug, Error)]
pub enum AffinityError {
    #[error("affinity is Linux-only")]
    UnsupportedPlatform,
}

pub fn pin_current_thread_to(_cpu: usize) -> Result<(), AffinityError> {
    Err(AffinityError::UnsupportedPlatform)
}

pub fn unpin_current_thread() -> Result<(), AffinityError> {
    Err(AffinityError::UnsupportedPlatform)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum OpKind {
    Connect = 1,
    Recv = 2,
    Send = 3,
    Close = 4,
    Nop = 5,
}

impl OpKind {
    #[inline]
    #[must_use]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Connect),
            2 => Some(Self::Recv),
            3 => Some(Self::Send),
            4 => Some(Self::Close),
            5 => Some(Self::Nop),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct SqeFlags(u8);

impl SqeFlags {
    pub const NONE: Self = Self(0);
    pub const IO_LINK: Self = Self(0);

    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self::NONE
    }

    #[inline]
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct UserData(u64);

impl UserData {
    const TOKEN_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

    #[inline]
    #[must_use]
    pub const fn new(kind: OpKind, token: u64) -> Self {
        Self(((kind as u64) << 56) | (token & Self::TOKEN_MASK))
    }

    #[inline]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    #[inline]
    #[must_use]
    pub fn kind(self) -> Option<OpKind> {
        OpKind::from_u8((self.0 >> 56) as u8)
    }

    #[inline]
    #[must_use]
    pub const fn token(self) -> u64 {
        self.0 & Self::TOKEN_MASK
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Completion {
    pub user_data: UserData,
    pub result: i32,
    pub flags: u32,
}

impl Completion {
    pub fn to_result(self) -> io::Result<usize> {
        if self.result >= 0 {
            #[allow(clippy::cast_sign_loss)]
            Ok(self.result as usize)
        } else {
            Err(io::Error::from_raw_os_error(-self.result))
        }
    }

    #[inline]
    #[must_use]
    pub const fn buffer_id(self) -> Option<u16> {
        None
    }

    #[inline]
    #[must_use]
    pub const fn has_more(self) -> bool {
        false
    }
}

#[derive(Debug, Error)]
pub enum BufferRingError {
    #[error("BufferRing is Linux-only")]
    UnsupportedPlatform,
}

pub struct BufferRing;

impl std::fmt::Debug for BufferRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferRing").finish()
    }
}

impl BufferRing {
    pub fn new(
        _reactor: &mut Proactor,
        _bgid: u16,
        _entries: u16,
        _buf_size: u32,
    ) -> Result<Self, BufferRingError> {
        unimplemented!("{STUB_PANIC}")
    }

    #[must_use]
    pub const fn bgid(&self) -> u16 {
        0
    }

    #[must_use]
    pub fn buffer(&self, _bid: u16) -> &[u8] {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn recycle(&mut self, _bid: u16) {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn unregister(&mut self, _reactor: &mut Proactor) -> Result<(), BufferRingError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Domain {
    V4,
    V6,
}

pub struct SockAddr;

impl std::fmt::Debug for SockAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SockAddr").finish()
    }
}

impl SockAddr {
    #[must_use]
    pub fn from_std(_addr: SocketAddr) -> Self {
        Self
    }
}

pub struct TcpSocket;

impl std::fmt::Debug for TcpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSocket").finish()
    }
}

impl TcpSocket {
    pub fn new(_domain: Domain) -> io::Result<Self> {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn set_nodelay(&self, _on: bool) -> io::Result<()> {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn set_reuseaddr(&self, _on: bool) -> io::Result<()> {
        unimplemented!("{STUB_PANIC}")
    }

    #[must_use]
    pub fn as_raw_fd(&self) -> RawFd {
        unimplemented!("{STUB_PANIC}")
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProactorConfig {
    pub sq_entries: u32,
    pub cq_entries: Option<u32>,
    pub setup_flags: ProactorSetupFlags,
}

impl Default for ProactorConfig {
    fn default() -> Self {
        Self {
            sq_entries: 256,
            cq_entries: None,
            setup_flags: ProactorSetupFlags::NONE,
        }
    }
}

impl ProactorConfig {
    #[inline]
    #[must_use]
    pub const fn with_sq_entries(mut self, entries: u32) -> Self {
        self.sq_entries = entries;
        self
    }

    #[inline]
    #[must_use]
    pub const fn with_cq_entries(mut self, entries: u32) -> Self {
        self.cq_entries = Some(entries);
        self
    }

    #[inline]
    #[must_use]
    pub const fn with_setup_flags(mut self, flags: ProactorSetupFlags) -> Self {
        self.setup_flags = flags;
        self
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Default)]
pub struct ProactorSetupFlags(u32);

impl ProactorSetupFlags {
    pub const NONE: Self = Self(0);
    pub const COOP_TASKRUN: Self = Self(1 << 0);
    pub const TASKRUN_FLAG: Self = Self(1 << 1);
    pub const SINGLE_ISSUER: Self = Self(1 << 2);
    pub const DEFER_TASKRUN: Self = Self(1 << 3);

    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self::NONE
    }

    #[inline]
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    #[inline]
    #[must_use]
    pub const fn contains(self, flag: Self) -> bool {
        (self.0 & flag.0) == flag.0
    }

    #[inline]
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }
}

impl std::fmt::Debug for ProactorSetupFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        if self.contains(Self::COOP_TASKRUN) {
            list.entry(&"COOP_TASKRUN");
        }
        if self.contains(Self::TASKRUN_FLAG) {
            list.entry(&"TASKRUN_FLAG");
        }
        if self.contains(Self::SINGLE_ISSUER) {
            list.entry(&"SINGLE_ISSUER");
        }
        if self.contains(Self::DEFER_TASKRUN) {
            list.entry(&"DEFER_TASKRUN");
        }
        list.finish()
    }
}

impl std::ops::BitOr for ProactorSetupFlags {
    type Output = Self;

    #[inline]
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for ProactorSetupFlags {
    #[inline]
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

#[derive(Debug, Error)]
pub enum ProactorError {
    #[error("io_uring init failed: {0}")]
    Init(#[source] io::Error),
    #[error("invalid proactor config: {0}")]
    InvalidConfig(&'static str),
    #[error("submission queue full")]
    SqFull,
    #[error("io_uring submit failed: {0}")]
    Submit(#[source] io::Error),
}

pub struct Proactor;

impl std::fmt::Debug for Proactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proactor").finish()
    }
}

impl Proactor {
    pub fn new(_config: ProactorConfig) -> Result<Self, ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    /// # Safety
    /// Stub —— 调用即 panic。
    pub unsafe fn submit_connect(
        &mut self,
        _fd: RawFd,
        _addr: &SockAddr,
        _user_data: UserData,
        _flags: SqeFlags,
    ) -> Result<(), ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    /// # Safety
    /// Stub —— 调用即 panic。
    pub unsafe fn submit_recv(
        &mut self,
        _fd: RawFd,
        _buf: *mut u8,
        _len: u32,
        _user_data: UserData,
        _flags: SqeFlags,
    ) -> Result<(), ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    /// # Safety
    /// Stub —— 调用即 panic。
    pub unsafe fn submit_send(
        &mut self,
        _fd: RawFd,
        _buf: *const u8,
        _len: u32,
        _user_data: UserData,
        _flags: SqeFlags,
    ) -> Result<(), ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn submit_close(
        &mut self,
        _fd: std::os::fd::OwnedFd,
        _user_data: UserData,
    ) -> Result<(), ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    /// # Safety
    /// Same contract as `super::uring::Proactor::submit_close_raw` — caller must
    /// own the fd exclusively. Stub always `unimplemented!()`s.
    pub unsafe fn submit_close_raw(
        &mut self,
        _fd: RawFd,
        _user_data: UserData,
    ) -> Result<(), ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn submit_nop(&mut self, _user_data: UserData) -> Result<(), ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    /// # Safety
    /// Stub —— 调用即 panic。
    pub unsafe fn submit_recv_multishot(
        &mut self,
        _fd: RawFd,
        _buf_group: u16,
        _user_data: UserData,
    ) -> Result<(), ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    /// # Safety
    /// Stub —— 调用即 panic。
    pub unsafe fn register_buf_ring(
        &mut self,
        _ring_addr: *const u8,
        _ring_entries: u16,
        _bgid: u16,
    ) -> Result<(), ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn unregister_buf_ring(&mut self, _bgid: u16) -> Result<(), ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn submit_and_wait(&mut self, _wait_nr: usize) -> Result<usize, ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn submit(&mut self) -> Result<usize, ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn wait_for_cqe(&mut self, _wait_nr: usize) -> Result<usize, ProactorError> {
        unimplemented!("{STUB_PANIC}")
    }

    pub fn drain_completions(&mut self, _sink: impl FnMut(Completion)) -> usize {
        unimplemented!("{STUB_PANIC}")
    }
}
