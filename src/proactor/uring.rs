//! io_uring Proactor 本体
//!
//! 本层是 thin wrapper：负责 ring setup、SQE 提交、CQE drain、provided buffer
//! ring 注册和少量 HFT 场景需要显式控制的 setup flags。高层 `Pool` 使用这里的
//! multishot recv + provided buffer ring；low-level toolkit 用户也可以直接用
//! `Proactor` 自己拼 transport / framing benchmark。
//!
//! ## Buffer 生命周期（unsafe 接口的核心约束）
//!
//! io_uring 的 kernel 端会在 SQE 提交后**异步**读写 caller 给的 buffer。Rust
//! borrow checker 看不到 kernel 这条 reference path，所以 [`Proactor::submit_recv`]
//! / [`Proactor::submit_send`] / [`Proactor::submit_connect`] 必须标 `unsafe`，
//! caller 责任：
//!
//! 1. **`*const/*mut` 指向的内存在对应 CQE 取走前必须存活**（不 drop，不
//!    realloc 触发 move）
//! 2. **recv buffer 必须独占**（kernel 写 + caller 看，期间没有别的 &mut/&）
//! 3. **send buffer 至少是 shared-readable**（kernel 读，caller 可以同时读但不
//!    可改）
//! 4. **`SockAddr` 同理**（kernel 端在 connect 完成前会读它）
//!
//! 取走 CQE 后 lifetime 约束解除——可以 drop / 复用 / move。
//!

use std::io;
use std::os::fd::{IntoRawFd, OwnedFd, RawFd};

use io_uring::{IoUring, opcode, types::Fd};
use thiserror::Error;

use super::op::{Completion, SqeFlags, UserData};
use super::socket::SockAddr;

/// io_uring setup flags 的稳定子集。
///
/// 这里只暴露对单线程行情 receive loop 有实际调参意义的 taskrun flags。
/// 默认不启用；这些 flag 对 event-loop 结构有约束，应显式 A/B 后再打开。
#[derive(Clone, Copy, Eq, PartialEq, Default)]
pub struct ProactorSetupFlags(u32);

impl ProactorSetupFlags {
    pub const NONE: Self = Self(0);
    /// `IORING_SETUP_COOP_TASKRUN`。
    pub const COOP_TASKRUN: Self = Self(1 << 0);
    /// `IORING_SETUP_TASKRUN_FLAG`，需配合 `COOP_TASKRUN` 或 `DEFER_TASKRUN` 使用。
    pub const TASKRUN_FLAG: Self = Self(1 << 1);
    /// `IORING_SETUP_SINGLE_ISSUER`。
    pub const SINGLE_ISSUER: Self = Self(1 << 2);
    /// `IORING_SETUP_DEFER_TASKRUN`，kernel 要求同时设置 `SINGLE_ISSUER`。
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

/// Proactor 构造参数。
#[derive(Debug, Clone, Copy)]
pub struct ProactorConfig {
    /// SQ 容量。必须是非零 2 的幂。默认 256。
    pub sq_entries: u32,
    /// CQ 容量覆盖。`None` 使用 kernel 默认（通常是 SQ 的 2 倍）。
    ///
    /// 对 multishot recv + provided buffer ring 的行情接收，burst 期间 CQE
    /// 可能比 SQE 多得多；建议从 `max(2 * sq_entries, buf_ring_entries)`
    /// 或更高开始 A/B。
    pub cq_entries: Option<u32>,
    /// 高级 setup flags。默认关闭。详见 [`ProactorSetupFlags`]。
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

#[derive(Debug, Error)]
pub enum ProactorError {
    #[error("io_uring init failed: {0}")]
    Init(#[source] io::Error),
    #[error("invalid proactor config: {0}")]
    InvalidConfig(&'static str),
    /// SQ 满。caller 应先 `submit_and_wait` 把现有 SQE 推进去再重试。
    #[error("submission queue full")]
    SqFull,
    #[error("io_uring submit failed: {0}")]
    Submit(#[source] io::Error),
}

/// io_uring proactor。**单线程拥有**——不实现 `Send` / `Sync`（`IoUring` 内部
/// 包了 `*mut` 元数据，跨线程使用必须由 caller 明确同步，F1 直接禁掉）。
pub struct Proactor {
    ring: IoUring,
}

impl std::fmt::Debug for Proactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // IoUring 自身不 impl Debug；只暴露 SQ/CQ 容量供调试
        let params = self.ring.params();
        f.debug_struct("Proactor")
            .field("sq_entries", &params.sq_entries())
            .field("cq_entries", &params.cq_entries())
            .finish()
    }
}

impl Proactor {
    /// 构造一个 proactor。按 [`ProactorConfig`] 决定 ring size 和 taskrun setup flags。
    pub fn new(config: ProactorConfig) -> Result<Self, ProactorError> {
        validate_config(config)?;
        let mut builder = IoUring::builder();
        if let Some(cq_entries) = config.cq_entries {
            builder.setup_cqsize(cq_entries);
        }
        apply_setup_flags(&mut builder, config.setup_flags);
        let ring = builder
            .build(config.sq_entries)
            .map_err(ProactorError::Init)?;
        Ok(Self { ring })
    }

    /// 提交 connect。
    ///
    /// `flags` 用于 chain：传 [`SqeFlags::IO_LINK`] 让下一个 SQE 依赖本 op
    /// 成功完成。绝大多数情况传 [`SqeFlags::NONE`]。
    ///
    /// # Safety
    ///
    /// `addr` 必须在对应 CQE 取走前一直存活（kernel 端会读它）。`fd` 必须是合
    /// 法的、未关闭的 socket fd。
    pub unsafe fn submit_connect(
        &mut self,
        fd: RawFd,
        addr: &SockAddr,
        user_data: UserData,
        flags: SqeFlags,
    ) -> Result<(), ProactorError> {
        let entry = opcode::Connect::new(Fd(fd), addr.as_ptr(), addr.len())
            .build()
            .flags(flags.into_io_uring())
            .user_data(user_data.raw());
        // SAFETY: caller 保证 addr 的存活；entry 自身满足 io_uring SQE 约定
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| ProactorError::SqFull)?;
        }
        Ok(())
    }

    /// 提交 recv。`flags` 见 [`submit_connect`](Self::submit_connect)。
    ///
    /// # Safety
    ///
    /// `buf` 必须指向 `len` 字节的有效可写内存，且在 CQE 取走前不被释放 / 重
    /// 分配 / 别名访问。`fd` 必须是合法、已连接的 socket fd。
    pub unsafe fn submit_recv(
        &mut self,
        fd: RawFd,
        buf: *mut u8,
        len: u32,
        user_data: UserData,
        flags: SqeFlags,
    ) -> Result<(), ProactorError> {
        let entry = opcode::Recv::new(Fd(fd), buf, len)
            .build()
            .flags(flags.into_io_uring())
            .user_data(user_data.raw());
        // SAFETY: caller 保证 buf 的存活和独占性
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| ProactorError::SqFull)?;
        }
        Ok(())
    }

    /// 提交 send。`flags` 见 [`submit_connect`](Self::submit_connect)。
    ///
    /// # Safety
    ///
    /// `buf` 必须指向 `len` 字节的有效可读内存，且在 CQE 取走前不被释放 / 重
    /// 分配 / 改写。`fd` 必须是合法、已连接的 socket fd。
    pub unsafe fn submit_send(
        &mut self,
        fd: RawFd,
        buf: *const u8,
        len: u32,
        user_data: UserData,
        flags: SqeFlags,
    ) -> Result<(), ProactorError> {
        let entry = opcode::Send::new(Fd(fd), buf, len)
            .build()
            .flags(flags.into_io_uring())
            .user_data(user_data.raw());
        // SAFETY: caller 保证 buf 在 CQE 前可读 + 不变
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| ProactorError::SqFull)?;
        }
        Ok(())
    }

    /// 提交 close。**消费** `OwnedFd` —— 提交即把 fd 所有权移交 kernel，调用方
    /// 不应（也无法）再 close 同一 fd。
    ///
    /// 早期版本签名是 `(fd: RawFd, ...)`，导致与 `TcpSocket` (持 `OwnedFd`，Drop
    /// 时 close) 同时存在 raw fd 副本 → 双 close（kernel 可能已把同 fd 重用给
    /// 别的线程，造成关错 socket）。这里直接拿 `OwnedFd` by value，借编译期
    /// move 语义杜绝。
    ///
    /// SQ 满时返回 `Err` —— **fd 已经被消费但 kernel 没收到 close**，调用方需
    /// 视该 fd 为 leak。生产代码通常会 `submit_and_wait` 推空 SQ 再 retry。
    pub fn submit_close(&mut self, fd: OwnedFd, user_data: UserData) -> Result<(), ProactorError> {
        let raw = fd.into_raw_fd();
        let entry = opcode::Close::new(Fd(raw))
            .build()
            .user_data(user_data.raw());
        // SAFETY: SubmissionQueue::push 的 unsafe 约束是 entry 内部资源有效；
        // Close 不含 buffer 指针，恒满足。
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| ProactorError::SqFull)?;
        }
        Ok(())
    }

    /// 当 caller 已经手动从 socket 拿出 raw fd（或在 io_uring 内部循环里只剩
    /// raw fd 时）才用。失去了双 close 防御 —— **首选 [`submit_close`]**。
    ///
    /// # Safety
    ///
    /// `fd` 必须是当前进程独占持有、未被任何 RAII wrapper 还在追踪的 fd。
    pub unsafe fn submit_close_raw(
        &mut self,
        fd: RawFd,
        user_data: UserData,
    ) -> Result<(), ProactorError> {
        let entry = opcode::Close::new(Fd(fd))
            .build()
            .user_data(user_data.raw());
        // SAFETY: 同 submit_close
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| ProactorError::SqFull)?;
        }
        Ok(())
    }

    /// 提交 Nop。主要用于 microbench（测 SQE→CQE 纯 proactor 开销）和未来 heartbeat。
    pub fn submit_nop(&mut self, user_data: UserData) -> Result<(), ProactorError> {
        let entry = opcode::Nop::new().build().user_data(user_data.raw());
        // SAFETY: Nop 不含任何 buffer / fd 约束
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| ProactorError::SqFull)?;
        }
        Ok(())
    }

    /// 提交 multishot recv —— kernel 在每次数据到达时**自动**从指定 `buf_group`
    /// 的 [`super::BufferRing`] 里挑一块 buffer 写入并发一个 CQE。一次提交
    /// 持续产 CQE 直到 op 被取消或错误。
    ///
    /// CQE 通过 [`Completion::buffer_id`] 取出 kernel 选的 bid；
    /// [`Completion::has_more`] 告诉你 multishot 是否还活着（false 时必须重新
    /// `submit_recv_multishot`）。
    ///
    /// # Safety
    ///
    /// `fd` 必须有效；`buf_group` 必须先用 [`Self::register_buf_ring`] 注册。
    pub unsafe fn submit_recv_multishot(
        &mut self,
        fd: RawFd,
        buf_group: u16,
        user_data: UserData,
    ) -> Result<(), ProactorError> {
        let entry = opcode::RecvMulti::new(Fd(fd), buf_group)
            .build()
            .user_data(user_data.raw());
        // SAFETY: caller 保证 fd 有效；entry 自带 BUFFER_SELECT 标志
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| ProactorError::SqFull)?;
        }
        Ok(())
    }

    /// 注册一个 provided buffer ring 到 kernel。一般通过 [`super::BufferRing::new`]
    /// 间接调用，不直接用。
    ///
    /// # Safety
    ///
    /// `ring_addr` 指向 `ring_entries × 16` 字节的 page-aligned 内存，且在
    /// `unregister_buf_ring(bgid)` 调用前持续有效。kernel 会持续访问这块内存。
    pub unsafe fn register_buf_ring(
        &mut self,
        ring_addr: *const u8,
        ring_entries: u16,
        bgid: u16,
    ) -> Result<(), ProactorError> {
        // SAFETY: caller 保证内存的有效性 + 寿命
        unsafe {
            self.ring
                .submitter()
                .register_buf_ring_with_flags(ring_addr as u64, ring_entries, bgid, 0)
                .map_err(ProactorError::Submit)?;
        }
        Ok(())
    }

    /// 解除 buffer ring 注册。drop `BufferRing` 前**必须**先调这个。
    pub fn unregister_buf_ring(&mut self, bgid: u16) -> Result<(), ProactorError> {
        self.ring
            .submitter()
            .unregister_buf_ring(bgid)
            .map_err(ProactorError::Submit)?;
        Ok(())
    }

    /// 把所有 pending SQE submit 给 kernel，并阻塞等至少 `wait_nr` 个 CQE。
    /// 返回实际 submit 的 SQE 数。`wait_nr = 0` 时不阻塞。
    pub fn submit_and_wait(&mut self, wait_nr: usize) -> Result<usize, ProactorError> {
        self.ring
            .submit_and_wait(wait_nr)
            .map_err(ProactorError::Submit)
    }

    /// 仅 submit（不等 CQE）。返回 kernel 实际收到的 SQE 数。
    pub fn submit(&mut self) -> Result<usize, ProactorError> {
        self.ring
            .submitter()
            .submit()
            .map_err(ProactorError::Submit)
    }

    /// 仅等 CQE，不 submit 任何新 SQE。若已有 ≥1 个 ready CQE 则立即返回。
    ///
    /// **非阻塞**：`wait_nr == 0` 永远不阻塞。
    /// **阻塞**：`wait_nr ≥ 1` 阻塞直到至少 N 个 CQE ready。
    pub fn wait_for_cqe(&mut self, wait_nr: usize) -> Result<usize, ProactorError> {
        if wait_nr == 0 {
            return Ok(0);
        }
        // io-uring crate 的 `submit_with_args` 是底层；这里用 submit_and_wait(N) +
        // 先调 submit() 把已有 SQE 推出去（多数情况 SQ 是空的，submit 是 noop）。
        // 实测：SQ empty 时 submit_and_wait 等价于纯 wait，开销与单独 wait 持平。
        self.ring
            .submit_and_wait(wait_nr)
            .map_err(ProactorError::Submit)
    }

    /// 取走所有 ready CQE，对每个调一次 `sink`。返回取走个数。
    ///
    /// 调用本身是非阻塞的——没 CQE 就立刻返回 0。
    pub fn drain_completions(&mut self, mut sink: impl FnMut(Completion)) -> usize {
        let cq = self.ring.completion();
        let mut count = 0;
        for cqe in cq {
            count += 1;
            sink(Completion {
                user_data: UserData::from_raw(cqe.user_data()),
                result: cqe.result(),
                flags: cqe.flags(),
            });
        }
        count
    }
}

fn validate_config(config: ProactorConfig) -> Result<(), ProactorError> {
    if config.sq_entries == 0 || !config.sq_entries.is_power_of_two() {
        return Err(ProactorError::InvalidConfig(
            "sq_entries must be a non-zero power of two",
        ));
    }
    if let Some(cq_entries) = config.cq_entries {
        if cq_entries <= config.sq_entries {
            return Err(ProactorError::InvalidConfig(
                "cq_entries must be greater than sq_entries",
            ));
        }
        if !cq_entries.is_power_of_two() {
            return Err(ProactorError::InvalidConfig(
                "cq_entries must be a power of two",
            ));
        }
    }

    let flags = config.setup_flags;
    if flags.contains(ProactorSetupFlags::DEFER_TASKRUN)
        && !flags.contains(ProactorSetupFlags::SINGLE_ISSUER)
    {
        return Err(ProactorError::InvalidConfig(
            "DEFER_TASKRUN requires SINGLE_ISSUER",
        ));
    }
    if flags.contains(ProactorSetupFlags::TASKRUN_FLAG)
        && !(flags.contains(ProactorSetupFlags::COOP_TASKRUN)
            || flags.contains(ProactorSetupFlags::DEFER_TASKRUN))
    {
        return Err(ProactorError::InvalidConfig(
            "TASKRUN_FLAG requires COOP_TASKRUN or DEFER_TASKRUN",
        ));
    }
    if flags.contains(ProactorSetupFlags::COOP_TASKRUN)
        && flags.contains(ProactorSetupFlags::DEFER_TASKRUN)
    {
        return Err(ProactorError::InvalidConfig(
            "COOP_TASKRUN and DEFER_TASKRUN are separate taskrun modes",
        ));
    }
    Ok(())
}

fn apply_setup_flags(builder: &mut io_uring::Builder, flags: ProactorSetupFlags) {
    if flags.contains(ProactorSetupFlags::COOP_TASKRUN) {
        builder.setup_coop_taskrun();
    }
    if flags.contains(ProactorSetupFlags::TASKRUN_FLAG) {
        builder.setup_taskrun_flag();
    }
    if flags.contains(ProactorSetupFlags::SINGLE_ISSUER) {
        builder.setup_single_issuer();
    }
    if flags.contains(ProactorSetupFlags::DEFER_TASKRUN) {
        builder.setup_defer_taskrun();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::proactor::op::OpKind;
    use crate::proactor::socket::{Domain, TcpSocket};
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};

    /// F1 端到端：起一个 std `TcpListener`，让 proactor connect 上去，能收到
    /// `Connect` CQE。验证 SQE → 内核 → CQE 的完整闭环。
    #[test]
    fn connect_to_local_listener_yields_cqe() {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let local_addr = listener.local_addr().unwrap();
        let sock_addr = SockAddr::from_std(local_addr);

        let socket = TcpSocket::new(Domain::V4).unwrap();
        socket.set_nodelay(true).unwrap();
        let fd = socket.as_raw_fd();

        let mut proactor = Proactor::new(ProactorConfig::default()).unwrap();
        let ud = UserData::new(OpKind::Connect, 1);

        // SAFETY: sock_addr 在 fn 退出前都存活
        unsafe {
            proactor
                .submit_connect(fd, &sock_addr, ud, SqeFlags::NONE)
                .unwrap();
        }
        proactor.submit_and_wait(1).unwrap();

        let mut got: Option<Completion> = None;
        let n = proactor.drain_completions(|c| got = Some(c));
        assert_eq!(n, 1);
        let c = got.expect("one completion");
        assert_eq!(c.user_data.kind(), Some(OpKind::Connect));
        assert_eq!(c.user_data.token(), 1);
        assert_eq!(
            c.result, 0,
            "connect should succeed (errno = {})",
            -c.result
        );

        // listener accept 一下，确认确实建上了
        let (_peer, _) = listener.accept().unwrap();
    }

    /// 单连接全链路：connect → send → 对端收 → server reply → recv。
    #[test]
    fn send_recv_roundtrip() {
        use std::io::{Read, Write};
        use std::thread;

        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let local_addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0_u8; 4];
            s.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"ping");
            s.write_all(b"pong").unwrap();
        });

        let sock_addr = SockAddr::from_std(local_addr);
        let socket = TcpSocket::new(Domain::V4).unwrap();
        socket.set_nodelay(true).unwrap();
        let fd = socket.as_raw_fd();

        let mut proactor = Proactor::new(ProactorConfig::default()).unwrap();

        // 1. connect
        let ud_connect = UserData::new(OpKind::Connect, 1);
        unsafe {
            proactor
                .submit_connect(fd, &sock_addr, ud_connect, SqeFlags::NONE)
                .unwrap();
        }
        proactor.submit_and_wait(1).unwrap();
        let mut got: Option<Completion> = None;
        proactor.drain_completions(|c| got = Some(c));
        let c = got.unwrap();
        assert_eq!(c.user_data.kind(), Some(OpKind::Connect));
        c.to_result().expect("connect ok");

        // 2. send
        let payload = *b"ping";
        let ud_send = UserData::new(OpKind::Send, 2);
        unsafe {
            proactor
                .submit_send(fd, payload.as_ptr(), 4, ud_send, SqeFlags::NONE)
                .unwrap();
        }
        proactor.submit_and_wait(1).unwrap();
        got = None;
        proactor.drain_completions(|c| got = Some(c));
        let c = got.unwrap();
        assert_eq!(c.user_data.kind(), Some(OpKind::Send));
        assert_eq!(c.to_result().unwrap(), 4);

        // 3. recv
        let mut buf = [0_u8; 4];
        let ud_recv = UserData::new(OpKind::Recv, 3);
        unsafe {
            proactor
                .submit_recv(fd, buf.as_mut_ptr(), 4, ud_recv, SqeFlags::NONE)
                .unwrap();
        }
        proactor.submit_and_wait(1).unwrap();
        got = None;
        proactor.drain_completions(|c| got = Some(c));
        let c = got.unwrap();
        assert_eq!(c.user_data.kind(), Some(OpKind::Recv));
        assert_eq!(c.to_result().unwrap(), 4);
        assert_eq!(&buf, b"pong");

        server.join().unwrap();
    }

    /// drain 一个空 CQ 应该返回 0、不调 sink。
    #[test]
    fn drain_empty_cq_returns_zero() {
        let mut proactor = Proactor::new(ProactorConfig::default()).unwrap();
        let mut sink_called = false;
        let n = proactor.drain_completions(|_| sink_called = true);
        assert_eq!(n, 0);
        assert!(!sink_called);
    }
}
