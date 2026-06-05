//! Provided Buffer Ring —— F3.3 multishot recv 的 buffer 来源
//!
//! io_uring 的 `IORING_REGISTER_PBUF_RING` 让 caller 注册一个 buffer 池给 kernel，
//! kernel 在 multishot recv 完成时**自己**从池里挑一块写入，CQE 通过
//! `IORING_CQE_F_BUFFER` 标志位 + flags 高 16 位的 buffer_id 告诉 caller 用了哪块。
//! 处理完后 caller 把 bid "归还" 到 ring tail，kernel 下次还可以用。
//!
//! ## 内存布局（与 kernel 共享）
//!
//! ```text
//! ring_mem: [BufRingEntry; entries]   page-aligned，entries 必须 2^N
//!   ┌──────────────────────────────┐
//!   │ entry[0]: addr/len/bid/RESV  │  ← RESV 是 tail field（10-byte 公用）
//!   │ entry[1]: addr/len/bid/resv  │
//!   │ ...                          │
//!   │ entry[N-1]                   │
//!   └──────────────────────────────┘
//! ```
//!
//! `tail` 通过 [`BufRingEntry::tail`] 取——它返回 entry[0] 的 resv 字段地址。
//! kernel 读 tail 知道有多少 entries 可用；caller 写 tail 把新 entries 发布。
//!
//! 注意：entry[0] 的 addr/len/bid 字段（前 14 字节）仍然可用——liburing 的
//! `io_uring_buf_ring_add` 同样这么做。我们不会写到 entry[0] 的 resv（那是 tail）。
//!
//! ## 生命周期约束
//!
//! - `BufferRing::new` 把 `ring_mem` 注册到 kernel——kernel 持续读这块内存
//! - drop 前**必须**调 [`BufferRing::unregister`] 通知 kernel；否则 kernel 仍
//!   持着已释放内存的地址（UB 风险）
//! - 我们在 Drop 里 best-effort 调 unregister，但建议显式调

#![allow(clippy::module_name_repetitions)]

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU16, Ordering};

use io_uring::types::BufRingEntry;
use thiserror::Error;

use super::uring::{Proactor, ProactorError};

/// 页大小——多数 Linux 系统是 4 KiB。kernel API 要求 ring_addr page-aligned。
const PAGE_SIZE: usize = 4096;

#[derive(Debug, Error)]
pub enum BufferRingError {
    #[error("entries ({0}) must be a non-zero power of two ≤ 32768")]
    InvalidEntryCount(u16),
    #[error("buf_size ({0}) must be > 0")]
    InvalidBufSize(u32),
    #[error("ring memory layout failed")]
    Layout,
    #[error("ring memory allocation failed")]
    AllocFailed,
    #[error("proactor: {0}")]
    Proactor(#[from] ProactorError),
}

/// 与 kernel 共享的 provided buffer pool。
///
/// **typical usage**:
///
/// ```ignore
/// let mut ring = BufferRing::new(&mut proactor, /*bgid=*/0, /*entries=*/64, /*buf_size=*/4096)?;
/// // 提交一个 multishot recv，bgid 关联到这个 ring
/// unsafe { proactor.submit_recv_multishot(fd, ring.bgid(), ud)?; }
/// // 每个 CQE：
/// proactor.drain_completions(|c| {
///     if let Some(bid) = c.buffer_id() {
///         let bytes = ring.buffer(bid).get(..c.to_result().unwrap()).unwrap();
///         // process bytes
///         ring.recycle(bid);  // 归还 buffer
///     }
/// });
/// // 业务结束：
/// ring.unregister(&mut proactor)?;
/// drop(ring);
/// ```
pub struct BufferRing {
    /// 与 kernel 共享的 ring 内存。指向 `entries * 16` 字节，page-aligned。
    ring_mem: NonNull<BufRingEntry>,
    /// 重新走 dealloc 时用。
    ring_layout: Layout,
    /// buffer 数据存储——每条 `buf_size` 字节，总 `entries * buf_size`。
    ///
    /// 用 `ManuallyDrop` 包起来是为了 Drop 时能选择性 leak：
    /// 如果 caller 漏调 [`unregister`](Self::unregister)，kernel 仍然持着 ring entry
    /// 指向 buf_storage 内地址的引用，此时**绝不能**释放 buf_storage，否则 kernel
    /// 多 shot recv 会写入已释放堆内存（UAF）。
    buf_storage: ManuallyDrop<Box<[u8]>>,
    /// 必须是 2 的幂，≤ 32768。
    entries: u16,
    /// 单 buffer 字节大小。
    buf_size: u32,
    /// buffer group id —— recv multishot SQE 通过 bgid 指向这个 ring。
    bgid: u16,
    /// 本地 tail 计数（u16 自然 wraparound）。每次 recycle 自增 + Release store 到 kernel。
    local_tail: u16,
    /// 是否已 unregister。Drop 时 best-effort 处理。
    unregistered: bool,
}

// SAFETY: BufferRing 独占 ring_mem + buf_storage 的所有权；可以在线程之间 move。
// 但**不**实现 Sync——同一时间只能有一个 thread 操作 ring tail。
unsafe impl Send for BufferRing {}

impl std::fmt::Debug for BufferRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferRing")
            .field("bgid", &self.bgid)
            .field("entries", &self.entries)
            .field("buf_size", &self.buf_size)
            .field("local_tail", &self.local_tail)
            .field("unregistered", &self.unregistered)
            .finish()
    }
}

impl BufferRing {
    /// 构造并注册一个 buffer ring。
    ///
    /// `entries` 必须是非零 2 的幂，且 ≤ 32768（kernel 上限）。
    pub fn new(
        proactor: &mut Proactor,
        bgid: u16,
        entries: u16,
        buf_size: u32,
    ) -> Result<Self, BufferRingError> {
        if entries == 0 || !entries.is_power_of_two() {
            return Err(BufferRingError::InvalidEntryCount(entries));
        }
        if buf_size == 0 {
            return Err(BufferRingError::InvalidBufSize(buf_size));
        }

        let ring_bytes = usize::from(entries) * std::mem::size_of::<BufRingEntry>();
        let ring_layout =
            Layout::from_size_align(ring_bytes, PAGE_SIZE).map_err(|_| BufferRingError::Layout)?;
        // SAFETY: layout 的 size 非零（entries ≥ 1）
        let raw = unsafe { alloc_zeroed(ring_layout) };
        // PAGE_SIZE (4096) ≫ align_of::<BufRingEntry>() (8)，layout 已保证对齐。
        #[allow(clippy::cast_ptr_alignment)]
        let ring_mem =
            NonNull::new(raw.cast::<BufRingEntry>()).ok_or(BufferRingError::AllocFailed)?;

        let buf_storage: ManuallyDrop<Box<[u8]>> = ManuallyDrop::new(
            vec![0_u8; usize::from(entries) * buf_size as usize].into_boxed_slice(),
        );
        let buf_base = buf_storage.as_ptr() as u64;

        // 初始化所有 entries：第 i 个 entry 指向 buf_storage[i*buf_size..(i+1)*buf_size]，bid = i
        for i in 0..entries {
            // SAFETY: ring_mem 至少 entries 个 BufRingEntry，i < entries
            let entry = unsafe { &mut *ring_mem.as_ptr().add(usize::from(i)) };
            entry.set_addr(buf_base + u64::from(i) * u64::from(buf_size));
            entry.set_len(buf_size);
            entry.set_bid(i);
        }

        // 发布 tail = entries（所有 entries 都可用）
        // SAFETY: ring_mem 是 entry[0] 的指针，BufRingEntry::tail 拿到 resv 字段地址
        let tail_ptr = unsafe { BufRingEntry::tail(ring_mem.as_ptr()) }.cast_mut();
        // SAFETY: tail_ptr 指向 entry[0].resv，仍在 ring_mem 范围内
        let tail_atomic = unsafe { AtomicU16::from_ptr(tail_ptr) };
        tail_atomic.store(entries, Ordering::Release);

        // 注册到 kernel
        // SAFETY: ring_mem 寿命跟 self；kernel 在 unregister 前能持续访问
        unsafe {
            proactor.register_buf_ring(ring_mem.as_ptr().cast::<u8>(), entries, bgid)?;
        }

        Ok(Self {
            ring_mem,
            ring_layout,
            buf_storage,
            entries,
            buf_size,
            bgid,
            local_tail: entries,
            unregistered: false,
        })
    }

    /// Buffer group id —— 提交 multishot recv SQE 用。
    #[inline]
    #[must_use]
    pub const fn bgid(&self) -> u16 {
        self.bgid
    }

    #[inline]
    #[must_use]
    pub const fn entries(&self) -> u16 {
        self.entries
    }

    #[inline]
    #[must_use]
    pub const fn buf_size(&self) -> u32 {
        self.buf_size
    }

    /// 取出 buffer bid 对应的字节 slice（kernel 写入区）。
    ///
    /// # Panics
    ///
    /// 如果 `bid >= entries`（bug：CQE 返回了未知 bid）。
    #[must_use]
    pub fn buffer(&self, bid: u16) -> &[u8] {
        assert!(
            bid < self.entries,
            "bid {bid} out of range {}",
            self.entries
        );
        let offset = usize::from(bid) * self.buf_size as usize;
        // SAFETY: bid < entries; buf_storage 长度 = entries * buf_size
        unsafe {
            self.buf_storage
                .get_unchecked(offset..offset + self.buf_size as usize)
        }
    }

    /// 把 buffer `bid` 归还到 ring，kernel 下次还可以用它。
    ///
    /// 业务侧处理完 multishot recv CQE 的数据后必须调一次。
    ///
    /// # 边界检查与 panic 行为
    ///
    /// `bid >= self.entries` 直接 panic —— 一个非法 bid 写进 ring 会让 kernel
    /// 把网络数据写到 `buf_storage` 之外的随机堆内存（典型 heap UAF / corrupt）。
    /// 这是 hot path 但 panic 必须保留：宁可崩在 user space 不要让 kernel 写飞。
    ///
    /// # 与 kernel 的 happens-before
    ///
    /// `ring_mem` 是与 kernel 共享的 mmap-style 内存。kernel 只在读到 `tail`
    /// 更新之后才会访问对应 slot 的 `(addr, len, bid)`。我们的顺序：
    ///
    /// 1. 先写 entry 的三个字段（普通 store，作用域 ≤ 几条指令）
    /// 2. 然后 Release-store tail
    ///
    /// Release 保证步骤 1 的写在 kernel acquire-load tail 时一定可见。`&mut entry`
    /// 的 borrow 在步骤 2 开始前已 drop，避免 stacked-borrows 角度上 entry 的
    /// `Unique` tag 在 tail 已发布后仍然存在。
    pub fn recycle(&mut self, bid: u16) {
        assert!(
            bid < self.entries,
            "BufferRing::recycle: bid {bid} out of range (entries = {})",
            self.entries
        );
        let mask = self.entries - 1;
        let slot = self.local_tail & mask;
        let buf_base = self.buf_storage.as_ptr() as u64;
        let new_addr = buf_base + u64::from(bid) * u64::from(self.buf_size);

        // entry 的 `&mut` borrow 局限在这个 block 内：3 次 set_* 之后立即 drop，
        // 后续的 tail Release-store 不与它共存。这点对 Tree Borrows 重要 ——
        // 在 tail 发布前 borrow 已结束，kernel 的后续 acquire-read 不会和我们
        // 这条 reborrow 路径打架。
        {
            // SAFETY: slot < entries（mask 保证），ring_mem 范围内；
            // kernel 在 tail 更新前不会读这个 slot（io_uring buf ring 协议）。
            let entry = unsafe { &mut *self.ring_mem.as_ptr().add(usize::from(slot)) };
            entry.set_addr(new_addr);
            entry.set_len(self.buf_size);
            entry.set_bid(bid);
        }

        self.local_tail = self.local_tail.wrapping_add(1);
        // Release store：保证 kernel acquire-load tail 时一定能看到上面 3 次
        // set_* 的写。
        // SAFETY: tail_ptr 指向 entry[0].resv，仍在 ring_mem 有效范围内；该
        // 字段被 io_uring buf ring 协议约定为 u16 tail，AtomicU16 的内存表示
        // 与裸 u16 一致。
        let tail_ptr = unsafe { BufRingEntry::tail(self.ring_mem.as_ptr()) }.cast_mut();
        // SAFETY: 同上
        unsafe { AtomicU16::from_ptr(tail_ptr) }.store(self.local_tail, Ordering::Release);
    }

    /// 通知 kernel 解除注册。在 drop 前**必须**调一次，否则 kernel 持着已
    /// 释放内存的地址（UB 风险）。
    pub fn unregister(&mut self, proactor: &mut Proactor) -> Result<(), BufferRingError> {
        if self.unregistered {
            return Ok(());
        }
        proactor.unregister_buf_ring(self.bgid)?;
        self.unregistered = true;
        Ok(())
    }
}

impl Drop for BufferRing {
    fn drop(&mut self) {
        // best-effort：如果用户忘了调 unregister，我们也没法 proactor 引用，
        // 只能 panic（debug）或 leak（release）。
        //
        // **关键：ring_mem 和 buf_storage 都要 leak**——
        // kernel 通过 ring entries 持着 buf_storage 内地址做 multishot recv，
        // 释放任何一块都会触发 UAF。`buf_storage: ManuallyDrop<Box<[u8]>>` 让我们
        // 选择性放弃 drop。
        debug_assert!(
            self.unregistered,
            "BufferRing dropped without unregister() —— kernel 仍持着已释放内存的地址"
        );
        if !self.unregistered {
            tracing::warn!(
                bgid = self.bgid,
                "BufferRing dropped without unregister; leaking ring + buf_storage to avoid kernel UAF"
            );
            // ring_mem: 不 dealloc（leak ring 内存）
            // buf_storage: 不 ManuallyDrop::drop（leak buf 数据存储）
            return;
        }
        // SAFETY: 已 unregister，kernel 不再访问；ManuallyDrop::drop 仅在此处调一次
        unsafe {
            ManuallyDrop::drop(&mut self.buf_storage);
        }
        // SAFETY: ring_mem 用 ring_layout 分配的；已经 unregister，kernel 不再访问
        unsafe {
            dealloc(self.ring_mem.as_ptr().cast::<u8>(), self.ring_layout);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::proactor::{
        Domain, OpKind, ProactorConfig, SockAddr, SqeFlags, TcpSocket, UserData,
    };
    use std::io::Write;
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
    use std::thread;

    /// 端到端：multishot recv 从 socket 拿数据，CQE 带 buffer_id，ring.buffer(bid)
    /// 取出正确字节。
    #[test]
    fn multishot_recv_roundtrip() {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let local_addr = listener.local_addr().unwrap();

        // server thread：accept + 写 "hello" 两次（两个 packet，触发两个 CQE）
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            s.set_nodelay(true).unwrap();
            s.write_all(b"hello").unwrap();
            // 给客户端时间 drain 第一个 CQE
            thread::sleep(std::time::Duration::from_millis(50));
            s.write_all(b"world").unwrap();
        });

        let sock_addr = SockAddr::from_std(local_addr);
        let socket = TcpSocket::new(Domain::V4).unwrap();
        socket.set_nodelay(true).unwrap();
        let fd = socket.as_raw_fd();

        let mut proactor = Proactor::new(ProactorConfig::default()).unwrap();

        // 1. connect
        // SAFETY: sock_addr 跟函数同寿命
        unsafe {
            proactor
                .submit_connect(
                    fd,
                    &sock_addr,
                    UserData::new(OpKind::Connect, 0),
                    SqeFlags::NONE,
                )
                .unwrap();
        }
        proactor.submit_and_wait(1).unwrap();
        let mut n = 0_usize;
        proactor.drain_completions(|c| {
            assert!(c.to_result().is_ok());
            n += 1;
        });
        assert_eq!(n, 1);

        // 2. 注册 buffer ring：4 entries × 256 bytes
        let mut ring = BufferRing::new(
            &mut proactor,
            /*bgid=*/ 1,
            /*entries=*/ 4,
            /*buf_size=*/ 256,
        )
        .expect("BufferRing");

        // 3. submit multishot recv
        // SAFETY: ring 在 proactor 之前 unregister；CQE 间 ring 持续存活
        unsafe {
            proactor
                .submit_recv_multishot(fd, ring.bgid(), UserData::new(OpKind::Recv, 0))
                .unwrap();
        }

        // 4. drain 所有 multishot CQE：包括数据 CQE 和 EOF / 终止 CQE
        let mut got_hello = false;
        let mut got_world = false;
        let mut multishot_ended = false;
        let mut bids_to_recycle: Vec<u16> = Vec::new();

        for _ in 0..10 {
            if multishot_ended {
                break;
            }
            proactor.submit_and_wait(1).unwrap();
            proactor.drain_completions(|c| {
                assert_eq!(c.user_data.kind(), Some(OpKind::Recv));
                if let Some(bid) = c.buffer_id() {
                    // 数据 CQE
                    let n = usize::try_from(c.result).unwrap();
                    let bytes = &ring.buffer(bid)[..n];
                    if bytes == b"hello" {
                        got_hello = true;
                    } else if bytes == b"world" {
                        got_world = true;
                    }
                    bids_to_recycle.push(bid);
                } else {
                    // control CQE：result=0 EOF / 错误 / multishot 结束
                    assert!(
                        !c.has_more(),
                        "control CQE 没 buffer_id 通常意味着 multishot 结束"
                    );
                    multishot_ended = true;
                }
            });
            // 归还已用 buffer，让 multishot 持续
            for bid in bids_to_recycle.drain(..) {
                ring.recycle(bid);
            }
        }

        assert!(got_hello, "should have received \"hello\"");
        assert!(got_world, "should have received \"world\"");

        // 清理
        ring.unregister(&mut proactor).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn invalid_entry_count_rejected() {
        let mut proactor = Proactor::new(ProactorConfig::default()).unwrap();
        assert!(matches!(
            BufferRing::new(&mut proactor, 0, 0, 4096),
            Err(BufferRingError::InvalidEntryCount(0))
        ));
        assert!(matches!(
            BufferRing::new(&mut proactor, 0, 3, 4096),
            Err(BufferRingError::InvalidEntryCount(3))
        ));
    }

    #[test]
    fn invalid_buf_size_rejected() {
        let mut proactor = Proactor::new(ProactorConfig::default()).unwrap();
        assert!(matches!(
            BufferRing::new(&mut proactor, 0, 4, 0),
            Err(BufferRingError::InvalidBufSize(0))
        ));
    }
}
