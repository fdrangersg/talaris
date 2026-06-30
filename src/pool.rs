//! `Pool` —— multi-connection driver
//!
//! 一个 [`Proactor`] 服务同 venue 的多条 WS。
//! CQE 通过 [`crate::proactor::UserData::token`] 携带 Pool token：低 28 位是
//! slot id，bits 55..28 是 slot generation。Pool drain 后先做 generation guard，
//! 再按 slot id 路由到对应 [`crate::connection_state::ConnectionState`]。
//!
//! ## 关键不变式
//!
//! - **单线程占用**：`Pool: !Send + !Sync`（`PhantomData<*const ()>` 标记）。
//!   io_uring 内部状态不能跨线程。
//! - **slot id 编码 ≤ 28 bit**：[`crate::proactor::UserData`] 高 8 位是 OpKind，低 56 位是
//!   caller token；Pool 约定 token 低 28 位为 slot id，bits 55..28 为 generation。
//! - **generation guard**：`remove_conn` / reconnect 会递增 slot generation；旧 handle
//!   和 late CQE 都不能 alias 到复用后的新连接。
//! - **bgid 安全复用**：每条 live conn 独占 bgid；只有 unregister buffer ring 成功，
//!   或该连接从未注册 ring 时，bgid 才进入 free list。
//! - **drain 顺序**：每轮 pump 先 submit pending send + rearm multishot，
//!   再 `submit_and_wait`，最后 drain CQE 路由 + drain ws_events。
//!

// `expect()` 用法均为 invariant 断言（just-pushed conn 一定存在；28-bit mask
// 一定 fits u32）。走到 panic 等于 Pool 内部状态已坏 —— HFT 进程应立即重启。
#![allow(clippy::expect_used)]

use std::fmt;
use std::marker::PhantomData;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::connection_meta::{
    AssignedConnectionConfig, CONN_GENERATION_MASK, CONN_ID_MASK, ConnectionConfig,
    ConnectionError, ConnectionRuntimeIdentity, IngressStats, State, encode_conn_token,
    token_conn_id, token_generation,
};
use crate::connection_state::ConnectionState;
use crate::observability::LatencyHistograms;
use crate::proactor::{Completion, Proactor, ProactorConfig, ProactorError};
use crate::ws::{
    DataEvent as WsDataEvent, DataEventBatch as WsDataEventBatch, Event as WsEvent,
    MarkedDataEvent as WsMarkedDataEvent, MarkedDataEventBatch as WsMarkedDataEventBatch,
};

/// CQE.token() layout used by Pool:
/// | bits 63..56 |  bits 55..28  | bits 27..0 |
/// |    OpKind   |  generation   |  slot_id   |
///
/// `generation` is bumped whenever a slot is removed. A late CQE from an old
/// connection can therefore never be routed into a new connection that reused
/// the same slot id.
///
/// Pool slot table 默认初始容量。0 表示按 `Vec` 默认策略延迟分配。
/// 大多数场景连接数很少，提前分配意义不大。不影响 recv/parse/pump
/// 热路径延迟，只影响 Pool 初始化或动态新增连接时的 Vec grow 内存分配行为。
pub const DEFAULT_POOL_INITIAL_CONN_CAPACITY: usize = 0;

/// 每轮 pump drain CQE 的暂存 `Vec<Completion>` 默认初始容量。
/// 每次 pump 时，Pool 会先从 io_uring CQ 里 drain 一批 CQE 到这个 Vec,
/// 然后再遍历 completions_buf，按 conn_id 路由到对应 ConnectionState
///
/// Pool 需要先把当前 CQ 里的 completions 收集起来，再统一处理。
/// 尤其 batch path 里还会看相邻 CQE 是否属于同一连接。
/// 64 的含义是：默认先给这个 Vec 分配 64 个 Completion 的容量，避免每轮 pump 重新分配。
/// 单连接或少量连接场景，一轮 pump 通常只有很少 CQE，比如 recv、send、close、nop 等；
/// 64 仍是很小的初始内存成本，同时更贴近 high fanout / burst 的生产低延迟默认。
pub const DEFAULT_POOL_COMPLETION_BATCH_CAPACITY: usize = 64;

/// Busy-spin data pumps 默认在首次 progress 后不继续额外 drain。
pub const DEFAULT_POOL_POST_PROGRESS_SPIN_ITERS: usize = 0;

/// Pool 的多连接调度层。
///
/// `proactor` 只配置 io_uring ring 本身；recv mode、provided-buffer 大小、
/// socket busy-poll、TLS/WS 等 per-connection 参数
/// 由 [`ConnectionConfig`] 控制。
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    /// 底层 io_uring 配置：SQ/CQ sizing 和 setup flags。
    pub proactor: ProactorConfig,

    /// Pool 的连接表 conns: Vec<Option<ConnectionState>> 初始容量。
    /// 高 fanout bench 可设为目标连接数，避免逐条 connect 时 slot table grow。
    /// 默认 [`DEFAULT_POOL_INITIAL_CONN_CAPACITY`]
    pub initial_conn_capacity: usize,

    /// `pump_impl` drain CQE 暂存区初始容量。高 fanout / burst 场景可增大，
    /// 避免第一轮大 batch grow。
    pub completion_batch_capacity: usize,

    /// busy-spin data 一次 pump 调用取得一次进展后，是否继续短暂 drain 附近 CQE，
    /// 此处的含义是 pump 调用内的 busy-spin 循环次数，减少函数返回/外层循环开销，也可能更快抓到刚到的 CQE
    ///
    /// 默认值为[`DEFAULT_POOL_POST_PROGRESS_SPIN_ITERS`].
    /// 如果设置成比如 256，那么当一次 pump 已经收到数据后，会继续短暂尝试 256 次 drain 后续 CQE。
    /// 这可能提高吞吐和同轮聚合能力，但也可能让第一个消息的返回路径稍微多做一点工作，这类参数需要按 feed 特征专项调
    pub post_progress_spin_iters: usize,
}

impl PoolConfig {
    #[must_use]
    pub const fn new(proactor: ProactorConfig) -> Self {
        Self {
            proactor,
            initial_conn_capacity: DEFAULT_POOL_INITIAL_CONN_CAPACITY,
            completion_batch_capacity: DEFAULT_POOL_COMPLETION_BATCH_CAPACITY,
            post_progress_spin_iters: DEFAULT_POOL_POST_PROGRESS_SPIN_ITERS,
        }
    }

    #[must_use]
    pub const fn with_initial_conn_capacity(mut self, capacity: usize) -> Self {
        self.initial_conn_capacity = capacity;
        self
    }

    #[must_use]
    pub const fn with_completion_batch_capacity(mut self, capacity: usize) -> Self {
        self.completion_batch_capacity = capacity;
        self
    }

    #[must_use]
    pub const fn with_post_progress_spin_iters(mut self, iters: usize) -> Self {
        self.post_progress_spin_iters = iters;
        self
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self::new(ProactorConfig::default())
    }
}

/// 业务面的 opaque conn 引用。**不跨 Pool 实例使用**。
/// 内部的 u32 同时是：
/// - conn_id
/// - conns slot index
/// - CQE token 低 28 位编码的连接编号
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ConnHandle(u64);

impl ConnHandle {
    #[inline]
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        token_conn_id(self.0)
    }

    #[inline]
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[inline]
    #[must_use]
    pub const fn generation(self) -> u32 {
        token_generation(self.0)
    }

    #[inline]
    #[must_use]
    const fn from_parts(conn_id: u32, generation: u32) -> Self {
        Self(encode_conn_token(conn_id, generation))
    }

    #[inline]
    #[must_use]
    const fn from_conn(conn: &ConnectionState) -> Self {
        Self(conn.token())
    }
}

/// Multi-conn driver/dispatcher:
///   - 单线程持有 [`Proactor`];
///   - 持有 `Vec<Option<ConnectionState>>` slot table;
///   - drain CQE;
///   - 从 CQE user_data 中解析 token，做 generation guard;
///   - 找到对应 `ConnectionState`（真正干活的是 ConnectionState）；
///   - 调用 `conn.handle_completion...`。
///
/// **Slot table 路由**：slot id 直接索引 `conns`，O(1)。generation guard
/// 让 stale handle / late CQE 无法命中复用后的 slot。
pub struct Pool {
    proactor: Proactor,
    /// Slot table: slot id 直接索引。`None` 表示空闲/已移除，可被 free list 复用。
    conns: Vec<Option<ConnectionState>>,
    /// Per-slot generation. Incremented when a slot is removed, so stale
    /// handles/CQEs cannot alias a future connection that reuses the slot.
    generations: Vec<u32>,
    /// Vacant slot ids available for reconnect/remove churn.
    free_conn_ids: Vec<u32>,
    /// Buffer group ids safe to reuse. We only recycle a bgid after its old
    /// BufferRing was successfully unregistered, or when no ring was ever
    /// registered for that connection.
    free_bgids: Vec<u16>,
    /// 活 conn 数。每次 push Some / 写 None 时同步维护，避免 hot path filter scan。
    active_count: u32,
    /// 下一条 fresh slot id。slot id 仍受 28-bit token 空间限制。
    next_conn_id: u32,
    /// 下一条 fresh buffer group id。复用优先走 free_bgids。
    next_bgid: u16,
    /// pump_impl 内 drain CQE 暂存区。持久字段避免每轮 alloc（dhat 审计发现
    /// 这是 hot loop 第一大 alloc：每轮 pump 重新分配一个 `Vec<Completion>`）。
    /// 默认 cap 64 让高 fanout / burst 场景更少在第一轮 hot path grow。
    completions_buf: Vec<Completion>,
    /// 从 PoolConfig 拷进来的运行时配置
    post_progress_spin_iters: usize,
    /// `Pool: !Send + !Sync` 显式标记。raw pointer phantom 不实际持有，不实际占内存，只影响类型系统
    /// 这个 Pool 不应该跨线程移动或共享。因为它内部持有 io_uring、fd、buffer ring 等线程亲和资源。
    _not_send: PhantomData<*const ()>,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("proactor", &self.proactor)
            .field("active_count", &self.active_count)
            .field("slot_len", &self.conns.len())
            .field("slot_capacity", &self.conns.capacity())
            .field("next_conn_id", &self.next_conn_id)
            .field("next_bgid", &self.next_bgid)
            .field("free_conn_ids", &self.free_conn_ids.len())
            .field("free_bgids", &self.free_bgids.len())
            .finish()
    }
}

impl Pool {
    pub fn new(cfg: PoolConfig) -> Result<Self, ProactorError> {
        let proactor = Proactor::new(cfg.proactor)?;
        Ok(Self {
            proactor,
            conns: Vec::with_capacity(cfg.initial_conn_capacity),
            generations: Vec::with_capacity(cfg.initial_conn_capacity),
            free_conn_ids: Vec::new(),
            free_bgids: Vec::new(),
            active_count: 0,
            next_conn_id: 0,
            next_bgid: 0,
            completions_buf: Vec::with_capacity(cfg.completion_batch_capacity),
            post_progress_spin_iters: cfg.post_progress_spin_iters,
            _not_send: PhantomData,
        })
    }

    /// 加一条 conn，阻塞跑到 [`State::Open`] 才返。失败时 slot 置 None
    /// （中途产生的 fd 由 `ConnectionState` drop 关闭）。
    ///
    /// io_uring/proactor 参数来自 [`PoolConfig`]；connection runtime ids are assigned internally.
    pub fn connect_blocking(
        &mut self,
        cfg: ConnectionConfig,
    ) -> Result<ConnHandle, ConnectionError> {
        let addr = resolve_addr(&cfg)?;
        self.connect_blocking_to(cfg, addr)
    }

    /// 同 `connect_blocking`，但跳过 DNS。
    /// 这是一个同步建连便利 API，适合“还没进入行情 hot loop”之前使用。
    /// 不要在 hot pool 活跃收包时动态 blocking connect；
    /// 初始化阶段用 submit_connect_to() 并发建连。
    pub fn connect_blocking_to(
        &mut self,
        cfg: ConnectionConfig,
        addr: SocketAddr,
    ) -> Result<ConnHandle, ConnectionError> {
        let handle = self.submit_connect_to(cfg, addr)?;
        let conn_id = handle.as_u32();
        match self.drive_conn_until_open(conn_id) {
            Ok(()) => Ok(handle),
            Err(e) => {
                let _ = self.retire_slot(conn_id, Some(handle.generation()));
                Err(e)
            }
        }
    }

    /// **非阻塞** connect：在 Pool 里新增一条连接，并把 TCP connect SQE 提交给 io_uring，但不等待连接完成。
    /// 仅提交 connect SQE 并 reserve 一个 slot，立刻返回 [`ConnHandle`]。
    /// 后续靠 caller `pump()` 推进 handshake，直到 `state(h) ==
    /// Open`（或 `Closed` 表失败）。
    ///
    /// 用途：N 条 conn 并发 handshake —— 单 `connect_blocking` 串行 N 次的话，
    /// TLS handshake 30 ms × N 全是开机延迟。submit 模式下 N 条同时跑，总等
    /// 时间 ≈ 一次 handshake。
    ///
    /// ```text
    /// let h1 = pool.submit_connect(cfg1)?;
    /// let h2 = pool.submit_connect(cfg2)?;
    /// loop {
    ///     pool.pump(|_, _| {})?;
    ///     if pool.state(h1) == Some(State::Open) && pool.state(h2) == Some(State::Open) {
    ///         break;
    ///     }
    ///     if matches!(pool.state(h1), Some(State::Closed))
    ///         || matches!(pool.state(h2), Some(State::Closed)) {
    ///         // 处理早夭
    ///     }
    /// }
    /// ```
    pub fn submit_connect(&mut self, cfg: ConnectionConfig) -> Result<ConnHandle, ConnectionError> {
        let addr = resolve_addr(&cfg)?;
        self.submit_connect_to(cfg, addr)
    }

    /// 同 [`submit_connect`](Self::submit_connect)，跳过 DNS。
    /// production multi-conn startup API
    pub fn submit_connect_to(
        &mut self,
        cfg: ConnectionConfig,
        addr: SocketAddr,
    ) -> Result<ConnHandle, ConnectionError> {
        let identity = self.reserve_runtime_identity()?;
        let assigned = AssignedConnectionConfig {
            user: cfg,
            identity,
        };

        let mut conn = match ConnectionState::new(assigned, addr) {
            Ok(conn) => conn,
            Err(e) => {
                self.release_reserved_identity(identity);
                return Err(e);
            }
        };
        if let Err(e) = conn.submit_connect(&mut self.proactor) {
            self.release_reserved_identity(identity);
            return Err(e);
        }

        let conn_id = identity.conn_id;
        let slot = self
            .conns
            .get_mut(conn_id as usize)
            .expect("reserved slot must exist");
        debug_assert!(slot.is_none());
        *slot = Some(conn);
        self.active_count += 1;
        Ok(ConnHandle::from_parts(
            identity.conn_id,
            identity.generation,
        ))
    }

    /// 强制移除一条连接并释放其 Pool slot / bgid 供后续重连复用。
    ///
    /// 这是运行时诊断/故障恢复 API，不执行 WebSocket close handshake；如果需要
    /// 协议级优雅关闭，先调用 [`Self::initiate_close`] 并继续 pump close handshake。
    /// stale handle 或已被移除的 slot 返回 [`ConnectionError::InvalidState`].
    pub fn remove_conn(&mut self, h: ConnHandle) -> Result<(), ConnectionError> {
        if self.retire_slot(h.as_u32(), Some(h.generation())) {
            Ok(())
        } else {
            Err(ConnectionError::InvalidState(State::Closed))
        }
    }

    /// Blocking reconnect convenience API. Resolves DNS, removes `old`, submits
    /// a fresh connection, then drives it to `Open`.
    pub fn reconnect(
        &mut self,
        old: ConnHandle,
        cfg: ConnectionConfig,
    ) -> Result<ConnHandle, ConnectionError> {
        let addr = resolve_addr(&cfg)?;
        self.reconnect_to(old, cfg, addr)
    }

    /// Blocking reconnect convenience API with caller-provided address.
    ///
    /// Production hot loops should prefer [`Self::submit_reconnect_to`] so the
    /// main pump loop keeps processing other connections while the new TCP/TLS/WS
    /// handshake progresses.
    pub fn reconnect_to(
        &mut self,
        old: ConnHandle,
        cfg: ConnectionConfig,
        addr: SocketAddr,
    ) -> Result<ConnHandle, ConnectionError> {
        let handle = self.submit_reconnect_to(old, cfg, addr)?;
        let conn_id = handle.as_u32();
        match self.drive_conn_until_open(conn_id) {
            Ok(()) => Ok(handle),
            Err(e) => {
                let _ = self.retire_slot(conn_id, Some(handle.generation()));
                Err(e)
            }
        }
    }

    /// Non-blocking reconnect. Resolves DNS, removes `old`, submits the new
    /// connection, then returns immediately.
    pub fn submit_reconnect(
        &mut self,
        old: ConnHandle,
        cfg: ConnectionConfig,
    ) -> Result<ConnHandle, ConnectionError> {
        let addr = resolve_addr(&cfg)?;
        self.submit_reconnect_to(old, cfg, addr)
    }

    /// Non-blocking reconnect with caller-provided address.
    ///
    /// The old connection is removed first; if creating/submitting the new
    /// connection fails, the old connection remains gone and the slot/bgid are
    /// returned to the freelists. This API is for diagnostic reconnect, not
    /// keep-old-until-new-open cutover.
    pub fn submit_reconnect_to(
        &mut self,
        old: ConnHandle,
        cfg: ConnectionConfig,
        addr: SocketAddr,
    ) -> Result<ConnHandle, ConnectionError> {
        self.remove_conn(old)?;
        self.submit_connect_to(cfg, addr)
    }

    fn reserve_runtime_identity(&mut self) -> Result<ConnectionRuntimeIdentity, ConnectionError> {
        let bgid = self.reserve_bgid()?;
        let conn_id = match self.reserve_conn_id() {
            Ok(conn_id) => conn_id,
            Err(e) => {
                self.release_bgid(bgid);
                return Err(e);
            }
        };
        let generation = self
            .generations
            .get(conn_id as usize)
            .copied()
            .expect("reserved generation must exist");
        Ok(ConnectionRuntimeIdentity {
            conn_id,
            generation,
            bgid,
        })
    }

    fn reserve_conn_id(&mut self) -> Result<u32, ConnectionError> {
        if let Some(conn_id) = self.free_conn_ids.pop() {
            return Ok(conn_id);
        }
        let conn_id = self.next_conn_id;
        if conn_id > CONN_ID_MASK as u32 {
            return Err(ConnectionError::IdSpaceExhausted("conn_id"));
        }
        self.next_conn_id = conn_id + 1;
        self.conns.push(None);
        self.generations.push(0);
        Ok(conn_id)
    }

    fn reserve_bgid(&mut self) -> Result<u16, ConnectionError> {
        if let Some(bgid) = self.free_bgids.pop() {
            return Ok(bgid);
        }
        let bgid = self.next_bgid;
        let Some(next_bgid) = self.next_bgid.checked_add(1) else {
            return Err(ConnectionError::IdSpaceExhausted("bgid"));
        };
        self.next_bgid = next_bgid;
        Ok(bgid)
    }

    fn release_reserved_identity(&mut self, identity: ConnectionRuntimeIdentity) {
        debug_assert!(
            self.conns
                .get(identity.conn_id as usize)
                .is_some_and(Option::is_none)
        );
        self.free_conn_ids.push(identity.conn_id);
        self.release_bgid(identity.bgid);
    }

    fn release_bgid(&mut self, bgid: u16) {
        self.free_bgids.push(bgid);
    }

    /// Retire a slot and make it reusable. Returns false when the slot is absent,
    /// vacant, or the expected generation does not match.
    fn retire_slot(&mut self, conn_id: u32, expected_generation: Option<u32>) -> bool {
        let Some(slot) = self.conns.get_mut(conn_id as usize) else {
            return false;
        };
        let Some(conn) = slot.as_ref() else {
            return false;
        };
        if expected_generation.is_some_and(|expected| expected != conn.generation()) {
            return false;
        }

        let mut dead = slot.take().expect("slot was Some above");
        let was_active = conn_is_active(&dead);
        let bgid = dead.bgid();
        let mut recycle_bgid = dead.buf_ring.is_none();
        if let Some(mut ring) = dead.buf_ring.take() {
            match ring.unregister(&mut self.proactor) {
                Ok(()) => recycle_bgid = true,
                Err(e) => tracing::warn!(
                    conn_id,
                    bgid,
                    error = %e,
                    "failed to unregister buffer ring while removing connection; bgid will not be reused"
                ),
            }
        }
        drop(dead);

        if was_active {
            self.active_count = self.active_count.saturating_sub(1);
        }
        self.recycle_conn_slot(conn_id);
        if recycle_bgid {
            self.release_bgid(bgid);
        }
        true
    }

    fn recycle_conn_slot(&mut self, conn_id: u32) {
        let Some(generation) = self.generations.get_mut(conn_id as usize) else {
            return;
        };
        if u64::from(*generation) >= CONN_GENERATION_MASK {
            tracing::warn!(
                conn_id,
                generation = *generation,
                "connection slot generation exhausted; slot will not be reused"
            );
            return;
        }
        *generation += 1;
        self.free_conn_ids.push(conn_id);
    }

    /// pump 单 conn 直到它进 Open（或失败）。其它 conn 的 CQE 也会顺道被路由
    /// 推进，但不会 drain 非目标连接已经解析出的 WS business events。
    fn drive_conn_until_open(&mut self, conn_id: u32) -> Result<(), ConnectionError> {
        loop {
            self.drive_open_once(1)?;
            self.drive_target_handshake_event(conn_id)?;

            let conn = self
                .conns
                .get_mut(conn_id as usize)
                .and_then(Option::as_mut)
                .expect("just-added conn must exist");
            conn.sync_ws_open_state();
            match conn.state() {
                State::Open => return Ok(()),
                State::Closed => return Err(ConnectionError::PeerClosed),
                _ => {}
            }
        }
    }

    fn drive_target_handshake_event(&mut self, conn_id: u32) -> Result<(), ConnectionError> {
        let Self {
            conns,
            active_count,
            ..
        } = self;
        let conn = conns
            .get_mut(conn_id as usize)
            .and_then(Option::as_mut)
            .expect("just-added conn must exist");

        if matches!(conn.state(), State::Open | State::Closed) {
            return Ok(());
        }

        let was_active = conn_is_active(conn);
        if let Some(res) = conn.ws.poll_event() {
            match res {
                Ok(WsEvent::HandshakeComplete) => {}
                Ok(_) => {}
                Err(e) => {
                    let mut first_err = None;
                    fail_conn_and_account(
                        conn,
                        ConnectionError::Ws(e),
                        &mut first_err,
                        active_count,
                        was_active,
                    );
                    return Err(first_err.expect("fail_conn_and_account stores first error"));
                }
            }
        }
        conn.sync_ws_open_state();
        conn.sync_ws_close_state();
        account_closed_transition(active_count, was_active, conn);
        Ok(())
    }

    fn drive_open_once(&mut self, wait_nr: usize) -> Result<(), ConnectionError> {
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;
        submit_conn_ops(conns, proactor, active_count, &mut first_err);
        proactor.submit()?;
        proactor.wait_for_cqe(wait_nr)?;

        completions_buf.clear();
        proactor.drain_completions(|c| completions_buf.push(c));
        for &c in completions_buf.iter() {
            if let Some(conn) = conn_for_completion(conns, c) {
                let was_active = conn_is_active(conn);
                let result = conn.handle_completion(proactor, c);
                let _ = finish_conn_result(conn, result, &mut first_err, active_count, was_active);
            }
        }

        first_err.map_or(Ok(()), Err)
    }

    pub fn send_text(&mut self, h: ConnHandle, payload: &[u8]) -> Result<(), ConnectionError> {
        let conn = self.conn_mut(h)?;
        conn.assert_open()?;
        conn.ws.send_text(payload)?;
        Ok(())
    }

    pub fn send_binary(&mut self, h: ConnHandle, payload: &[u8]) -> Result<(), ConnectionError> {
        let conn = self.conn_mut(h)?;
        conn.assert_open()?;
        conn.ws.send_binary(payload)?;
        Ok(())
    }

    pub fn send_ping(&mut self, h: ConnHandle, payload: &[u8]) -> Result<(), ConnectionError> {
        let conn = self.conn_mut(h)?;
        conn.assert_open()?;
        conn.ws.send_ping(payload)?;
        Ok(())
    }

    pub fn send_pong(&mut self, h: ConnHandle, payload: &[u8]) -> Result<(), ConnectionError> {
        let conn = self.conn_mut(h)?;
        conn.assert_open()?;
        conn.ws.send_pong(payload)?;
        Ok(())
    }

    pub fn initiate_close(
        &mut self,
        h: ConnHandle,
        code: u16,
        reason: &str,
    ) -> Result<(), ConnectionError> {
        let conn = self.conn_mut(h)?;
        // Closing / Closed 都是幂等 no-op：对端已先发 Close 时 ws 内部已 queue
        // 过 echo，再 send_close 会把第二个 Close frame 推上 wire（RFC §5.5.1
        // 要求每端最多发一个 Close）。
        if matches!(conn.state(), State::Closed | State::Closing) {
            return Ok(());
        }
        conn.ws.send_close(code, reason)?;
        if matches!(conn.state(), State::Open) {
            conn.state = State::Closing;
        }
        Ok(())
    }

    pub fn pump<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: FnMut(ConnHandle, WsEvent<'_>),
    {
        self.pump_impl(1, sink)
    }

    pub fn pump_nowait<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: FnMut(ConnHandle, WsEvent<'_>),
    {
        self.pump_impl(0, sink)
    }

    /// Busy-poll 版本的 [`pump`](Self::pump)。
    ///
    /// 先提交 pending send / multishot rearm，然后最多轮询 `spin_iters + 1`
    /// 次 CQ ring；期间不调用 [`Proactor::wait_for_cqe`]，因此不会为了等待
    /// completion 进入 `io_uring_enter(GETEVENTS)`。这只适合 isolated CPU 上的
    /// 高频 steady-state loop；低负载下会白烧 CPU。
    ///
    /// 返回值表示这一轮是否处理到了任何 CQE 或 WS event。caller 可以据此决定
    /// 继续 busy-spin，或 fallback 到阻塞 [`pump`](Self::pump)。
    pub fn pump_spin<F>(&mut self, spin_iters: usize, sink: F) -> Result<bool, ConnectionError>
    where
        F: FnMut(ConnHandle, WsEvent<'_>),
    {
        self.pump_spin_impl(spin_iters, sink)
    }

    /// Data-only pump：跟 [`pump`](Self::pump) 一样推进 io_uring 和完整 WebSocket
    /// 状态机，但只把业务 data message 交给 sink。
    ///
    /// Text JSON 和 Binary SBE 都会被分发；Ping/Pong/Close、fragmentation、
    /// auto-pong、UTF-8 校验等仍由 [`crate::ws::WsClient`] 正常处理。适合交易所
    /// 行情主循环：业务代码只关心 data payload，但连接层不能忽略 control frame。
    pub fn pump_data<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEvent<'a>),
    {
        self.pump_data_impl(1, sink)
    }

    /// 同 [`pump_data`](Self::pump_data)，但 `wait_for_cqe(0)` —— 立刻返回，
    /// 没新 CQE 也不阻塞。配合 close handshake / 退出 cleanup 用。
    pub fn pump_data_nowait<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEvent<'a>),
    {
        self.pump_data_impl(0, sink)
    }

    /// Marked data-only pump. This is the opt-in observability variant of
    /// [`Self::pump_data`]; the default API does not read clocks or construct
    /// timing metadata.
    pub fn pump_data_marked<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEvent<'a>),
    {
        self.pump_data_marked_impl(1, sink)
    }

    /// Non-blocking marked data-only pump.
    pub fn pump_data_marked_nowait<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEvent<'a>),
    {
        self.pump_data_marked_impl(0, sink)
    }

    /// Batch variant of [`Self::pump_data`].
    ///
    /// The callback receives fixed-capacity batches of data messages. Direct
    /// plaintext chunks can produce multi-message batches; fallback protocol
    /// paths may still produce one-message batches. Use
    /// [`WsDataEventBatch::is_chunk_end`] to know when all data messages from
    /// the current plaintext chunk have been delivered.
    pub fn pump_data_batches<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEventBatch<'a>),
    {
        self.pump_data_batches_impl(1, sink)
    }

    /// Non-blocking batch variant of [`Self::pump_data_nowait`].
    pub fn pump_data_batches_nowait<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEventBatch<'a>),
    {
        self.pump_data_batches_impl(0, sink)
    }

    /// Batch variant of [`Self::pump_data_marked`].
    ///
    /// Batch delivery measures sink service at the batch boundary. Messages in
    /// one emitted batch share the same `chunk_prior_sink_service_nanos`; use
    /// [`Self::pump_data_marked`] for strict per-message sink queuing metrics.
    /// Use [`WsMarkedDataEventBatch::is_chunk_end`] to coalesce all data
    /// messages from the current plaintext chunk without waiting for the next
    /// chunk.
    pub fn pump_data_marked_batches<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEventBatch<'a>),
    {
        self.pump_data_marked_batches_impl(1, sink)
    }

    /// Non-blocking batch variant of [`Self::pump_data_marked_nowait`].
    pub fn pump_data_marked_batches_nowait<F>(&mut self, sink: F) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEventBatch<'a>),
    {
        self.pump_data_marked_batches_impl(0, sink)
    }

    /// Busy-poll 版本的 [`pump_data`](Self::pump_data)。
    ///
    /// 本方法只轮询 mmap 出来的 CQ ring，不调用 [`Proactor::wait_for_cqe`]。
    /// 代价是 caller 所在线程会在没有 CQE 时持续占 CPU。
    ///
    /// 返回值表示这一轮是否处理到了任何 CQE 或 WS event。
    pub fn pump_data_spin<F>(&mut self, spin_iters: usize, sink: F) -> Result<bool, ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEvent<'a>),
    {
        self.pump_data_spin_impl(spin_iters, sink)
    }

    /// Busy-poll batch variant of [`Self::pump_data_spin`].
    pub fn pump_data_spin_batches<F>(
        &mut self,
        spin_iters: usize,
        sink: F,
    ) -> Result<bool, ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEventBatch<'a>),
    {
        self.pump_data_spin_batches_impl(spin_iters, sink)
    }

    /// Busy-poll marked data-only pump.
    ///
    /// Use this when measuring transport/TLS/WS stage latency. It carries
    /// [`crate::observability::DataEventMeta`] with each Text/Binary payload.
    /// Unmarked `pump_data_spin` stays free of clock reads.
    pub fn pump_data_spin_marked<F>(
        &mut self,
        spin_iters: usize,
        sink: F,
    ) -> Result<bool, ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEvent<'a>),
    {
        self.pump_data_spin_marked_impl(spin_iters, sink)
    }

    /// Busy-poll batch variant of [`Self::pump_data_spin_marked`].
    ///
    /// See [`Self::pump_data_marked_batches`] for batch observability
    /// semantics.
    pub fn pump_data_spin_marked_batches<F>(
        &mut self,
        spin_iters: usize,
        sink: F,
    ) -> Result<bool, ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEventBatch<'a>),
    {
        self.pump_data_spin_marked_batches_impl(spin_iters, sink)
    }

    /// data-only pump 实现。CQE drain 后按连接路由；同一连接连续 plain recv
    /// data CQE 会在连接内批量推进，仍按 CQE 顺序立刻把 Text/Binary 交给业务。
    fn pump_data_impl<F>(&mut self, wait_nr: usize, mut sink: F) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEvent<'a>),
    {
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;

        submit_conn_ops(conns, proactor, active_count, &mut first_err);

        proactor.submit()?;
        proactor.wait_for_cqe(wait_nr)?;

        completions_buf.clear();
        proactor.drain_completions(|c| completions_buf.push(c));
        dispatch_conn_completions_data(
            conns,
            proactor,
            completions_buf,
            active_count,
            &mut sink,
            &mut first_err,
        );

        first_err.map_or(Ok(()), Err)
    }

    fn pump_data_marked_impl<F>(
        &mut self,
        wait_nr: usize,
        mut sink: F,
    ) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEvent<'a>),
    {
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;

        submit_conn_ops(conns, proactor, active_count, &mut first_err);

        proactor.submit()?;
        proactor.wait_for_cqe(wait_nr)?;

        completions_buf.clear();
        proactor.drain_completions(|c| completions_buf.push(c));
        dispatch_conn_completions_data_marked(
            conns,
            proactor,
            completions_buf,
            active_count,
            &mut sink,
            &mut first_err,
        );

        first_err.map_or(Ok(()), Err)
    }

    fn pump_data_batches_impl<F>(
        &mut self,
        wait_nr: usize,
        mut sink: F,
    ) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEventBatch<'a>),
    {
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;

        submit_conn_ops(conns, proactor, active_count, &mut first_err);

        proactor.submit()?;
        proactor.wait_for_cqe(wait_nr)?;

        completions_buf.clear();
        proactor.drain_completions(|c| completions_buf.push(c));
        dispatch_conn_completions_data_batches(
            conns,
            proactor,
            completions_buf,
            active_count,
            &mut sink,
            &mut first_err,
        );

        first_err.map_or(Ok(()), Err)
    }

    fn pump_data_marked_batches_impl<F>(
        &mut self,
        wait_nr: usize,
        mut sink: F,
    ) -> Result<(), ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEventBatch<'a>),
    {
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;

        submit_conn_ops(conns, proactor, active_count, &mut first_err);

        proactor.submit()?;
        proactor.wait_for_cqe(wait_nr)?;

        completions_buf.clear();
        proactor.drain_completions(|c| completions_buf.push(c));
        dispatch_conn_completions_data_marked_batches(
            conns,
            proactor,
            completions_buf,
            active_count,
            &mut sink,
            &mut first_err,
        );

        first_err.map_or(Ok(()), Err)
    }

    fn pump_data_spin_impl<F>(
        &mut self,
        spin_iters: usize,
        mut sink: F,
    ) -> Result<bool, ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEvent<'a>),
    {
        let post_progress_spin_iters = self.post_progress_spin_iters;
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;
        let mut progressed = false;

        submit_conn_ops(conns, proactor, active_count, &mut first_err);
        proactor.submit()?;

        for iter in 0..=spin_iters {
            let cqes = drain_conn_completions_data(
                conns,
                proactor,
                completions_buf,
                active_count,
                &mut sink,
                &mut first_err,
            );
            if cqes > 0 {
                progressed = true;
                drain_post_progress(post_progress_spin_iters, &mut first_err, |first_err| {
                    let _ = drain_conn_completions_data(
                        conns,
                        proactor,
                        completions_buf,
                        active_count,
                        &mut sink,
                        first_err,
                    );
                });
            }

            if progressed || first_err.is_some() {
                break;
            }
            if iter < spin_iters {
                std::hint::spin_loop();
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(progressed),
        }
    }

    fn pump_data_spin_marked_impl<F>(
        &mut self,
        spin_iters: usize,
        mut sink: F,
    ) -> Result<bool, ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEvent<'a>),
    {
        let post_progress_spin_iters = self.post_progress_spin_iters;
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;
        let mut progressed = false;

        submit_conn_ops(conns, proactor, active_count, &mut first_err);
        proactor.submit()?;

        for iter in 0..=spin_iters {
            let cqes = drain_conn_completions_data_marked(
                conns,
                proactor,
                completions_buf,
                active_count,
                &mut sink,
                &mut first_err,
            );
            if cqes > 0 {
                progressed = true;
                drain_post_progress(post_progress_spin_iters, &mut first_err, |first_err| {
                    let _ = drain_conn_completions_data_marked(
                        conns,
                        proactor,
                        completions_buf,
                        active_count,
                        &mut sink,
                        first_err,
                    );
                });
            }

            if progressed || first_err.is_some() {
                break;
            }
            if iter < spin_iters {
                std::hint::spin_loop();
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(progressed),
        }
    }

    fn pump_data_spin_batches_impl<F>(
        &mut self,
        spin_iters: usize,
        mut sink: F,
    ) -> Result<bool, ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsDataEventBatch<'a>),
    {
        let post_progress_spin_iters = self.post_progress_spin_iters;
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;
        let mut progressed = false;

        submit_conn_ops(conns, proactor, active_count, &mut first_err);
        proactor.submit()?;

        for iter in 0..=spin_iters {
            let cqes = drain_conn_completions_data_batches(
                conns,
                proactor,
                completions_buf,
                active_count,
                &mut sink,
                &mut first_err,
            );
            if cqes > 0 {
                progressed = true;
                drain_post_progress(post_progress_spin_iters, &mut first_err, |first_err| {
                    let _ = drain_conn_completions_data_batches(
                        conns,
                        proactor,
                        completions_buf,
                        active_count,
                        &mut sink,
                        first_err,
                    );
                });
            }

            if progressed || first_err.is_some() {
                break;
            }
            if iter < spin_iters {
                std::hint::spin_loop();
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(progressed),
        }
    }

    fn pump_data_spin_marked_batches_impl<F>(
        &mut self,
        spin_iters: usize,
        mut sink: F,
    ) -> Result<bool, ConnectionError>
    where
        F: for<'a> FnMut(ConnHandle, WsMarkedDataEventBatch<'a>),
    {
        let post_progress_spin_iters = self.post_progress_spin_iters;
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;
        let mut progressed = false;

        submit_conn_ops(conns, proactor, active_count, &mut first_err);
        proactor.submit()?;

        for iter in 0..=spin_iters {
            let cqes = drain_conn_completions_data_marked_batches(
                conns,
                proactor,
                completions_buf,
                active_count,
                &mut sink,
                &mut first_err,
            );
            if cqes > 0 {
                progressed = true;
                drain_post_progress(post_progress_spin_iters, &mut first_err, |first_err| {
                    let _ = drain_conn_completions_data_marked_batches(
                        conns,
                        proactor,
                        completions_buf,
                        active_count,
                        &mut sink,
                        first_err,
                    );
                });
            }

            if progressed || first_err.is_some() {
                break;
            }
            if iter < spin_iters {
                std::hint::spin_loop();
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(progressed),
        }
    }

    /// 推进一次：所有 conn 的 pending send / multishot rearm → submit_and_wait
    /// → CQE 按 conn_id 路由 → 所有 conn drain ws_events 到 sink。
    ///
    /// **Fault tolerance**：单条 conn 出错不再 abort 整轮。早期版本 `?` 会让
    /// 后续 conn 的 CQE 直接丢、bid 不 recycle，给 kernel 留 buffer 泄漏 +
    /// 把"暂时无法 sync close state"扩散成"所有 conn 全 freeze"。现在 per-conn
    /// 错误聚合到 `first_err`，pump 结束统一 surface；出错的 conn 自动推到
    /// `State::Closed`，下一轮 try_submit_send / rearm 看到 Closed 会 short-circuit。
    fn pump_impl<F>(&mut self, wait_nr: usize, mut sink: F) -> Result<(), ConnectionError>
    where
        F: FnMut(ConnHandle, WsEvent<'_>),
    {
        // split borrow: proactor 和 conns 同时可变借
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;

        // submit phase：per-conn 失败只标这条 conn，不影响其它
        submit_conn_ops(conns, proactor, active_count, &mut first_err);

        // submit pending send / rearm SQE, then wait only when requested.
        // wait_for_cqe(0) 是纯 noop，wait_nr ≥ 1 才阻塞。失败 fatal ——
        // io_uring 状态损坏没法 per-conn 隔离。
        proactor.submit()?;
        proactor.wait_for_cqe(wait_nr)?;

        // drain 所有 ready CQE 到持久 buf，避免 drain callback 重入 proactor +
        // 每轮 alloc。F3 dhat 审计：原先每轮新建 `Vec<Completion>` 是
        // hot loop 第一大 alloc 点；移字段后 0 alloc。
        completions_buf.clear();
        proactor.drain_completions(|c| completions_buf.push(c));
        for &c in completions_buf.iter() {
            // Slot-table O(1) lookup with generation guard. Stale CQEs from a
            // removed/reused slot are ignored before they can touch new state.
            if let Some(conn) = conn_for_completion(conns, c) {
                let was_active = conn_is_active(conn);
                let result = conn.handle_completion(proactor, c);
                let _ = finish_conn_result(conn, result, &mut first_err, active_count, was_active);
            }
        }

        // 各 conn drain ws_events —— sink 出错的 event 也聚合而非 abort
        for slot in conns.iter_mut() {
            let Some(conn) = slot.as_mut() else { continue };
            if matches!(conn.state(), State::Closed) {
                conn.clear_ws_ingress_dirty();
                continue;
            }
            let handle = ConnHandle::from_conn(conn);
            while let Some(res) = conn.ws.poll_event() {
                match res {
                    Ok(ev) => sink(handle, ev),
                    Err(e) => {
                        let was_active = conn_is_active(conn);
                        fail_conn_and_account(
                            conn,
                            ConnectionError::Ws(e),
                            &mut first_err,
                            active_count,
                            was_active,
                        );
                        break;
                    }
                }
            }
            conn.sync_ws_open_state();
            conn.sync_ws_close_state();
            conn.clear_ws_ingress_dirty();
        }

        first_err.map_or(Ok(()), Err)
    }

    fn pump_spin_impl<F>(&mut self, spin_iters: usize, mut sink: F) -> Result<bool, ConnectionError>
    where
        F: FnMut(ConnHandle, WsEvent<'_>),
    {
        let Self {
            proactor,
            conns,
            completions_buf,
            active_count,
            ..
        } = self;

        let mut first_err: Option<ConnectionError> = None;
        let mut progressed = false;

        submit_conn_ops(conns, proactor, active_count, &mut first_err);
        proactor.submit()?;

        for iter in 0..=spin_iters {
            let cqes = drain_conn_completions(
                conns,
                proactor,
                completions_buf,
                active_count,
                &mut first_err,
            );
            progressed |= cqes > 0;

            for slot in conns.iter_mut() {
                let Some(conn) = slot.as_mut() else { continue };
                if matches!(conn.state(), State::Closed) {
                    conn.clear_ws_ingress_dirty();
                    continue;
                }
                let handle = ConnHandle::from_conn(conn);
                while let Some(res) = conn.ws.poll_event() {
                    progressed = true;
                    match res {
                        Ok(ev) => sink(handle, ev),
                        Err(e) => {
                            let was_active = conn_is_active(conn);
                            fail_conn_and_account(
                                conn,
                                ConnectionError::Ws(e),
                                &mut first_err,
                                active_count,
                                was_active,
                            );
                            break;
                        }
                    }
                }
                conn.sync_ws_open_state();
                conn.sync_ws_close_state();
                conn.clear_ws_ingress_dirty();
            }

            if progressed || first_err.is_some() {
                break;
            }
            if iter < spin_iters {
                std::hint::spin_loop();
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(progressed),
        }
    }

    pub fn state(&self, h: ConnHandle) -> Option<State> {
        self.conns
            .get(h.as_u32() as usize)
            .and_then(Option::as_ref)
            .filter(|conn| conn.token() == h.as_u64())
            .map(ConnectionState::state)
    }

    /// 当前 active conn 数（不含空闲 slot）。
    #[must_use]
    pub fn conn_count(&self) -> usize {
        self.active_count as usize
    }

    /// Returns opt-in ingress diagnostics for a live connection.
    #[must_use]
    pub fn ingress_stats(&self, h: ConnHandle) -> Option<IngressStats> {
        self.conns
            .get(h.as_u32() as usize)
            .and_then(Option::as_ref)
            .filter(|conn| conn.token() == h.as_u64())
            .map(ConnectionState::ingress_stats)
    }

    /// Render a Prometheus text exposition snapshot for all live connections.
    #[must_use]
    pub fn prometheus_metrics(&self) -> String {
        let mut out = String::new();
        self.write_prometheus_metrics(&mut out)
            .expect("writing Prometheus metrics to String cannot fail");
        out
    }

    /// Write a Prometheus text exposition snapshot for all live connections.
    pub fn write_prometheus_metrics<W: fmt::Write>(&self, out: &mut W) -> fmt::Result {
        LatencyHistograms::write_prometheus_help(out)?;
        write_ingress_prometheus_help(out)?;
        for conn in self.conns.iter().flatten() {
            conn.write_prometheus_metrics(out)?;
        }
        Ok(())
    }

    /// Render interval Prometheus metrics and reset interval latency histograms.
    #[must_use]
    pub fn prometheus_metrics_and_reset_interval(&mut self) -> String {
        let mut out = String::new();
        self.write_prometheus_metrics_and_reset_interval(&mut out)
            .expect("writing Prometheus metrics to String cannot fail");
        out
    }

    /// Write interval Prometheus metrics and reset interval latency histograms.
    ///
    /// Ingress counters remain lifetime cumulative; only latency histograms are
    /// reset after a successful write.
    pub fn write_prometheus_metrics_and_reset_interval<W: fmt::Write>(
        &mut self,
        out: &mut W,
    ) -> fmt::Result {
        LatencyHistograms::write_prometheus_help(out)?;
        write_ingress_prometheus_help(out)?;
        for conn in self.conns.iter_mut().flatten() {
            conn.write_prometheus_metrics_and_reset_interval(out)?;
        }
        Ok(())
    }

    fn conn_mut(&mut self, h: ConnHandle) -> Result<&mut ConnectionState, ConnectionError> {
        let conn = self
            .conns
            .get_mut(h.as_u32() as usize)
            .and_then(Option::as_mut)
            .ok_or(ConnectionError::InvalidState(State::Closed))?;
        if conn.token() == h.as_u64() {
            Ok(conn)
        } else {
            Err(ConnectionError::InvalidState(State::Closed))
        }
    }
}

fn resolve_addr(cfg: &ConnectionConfig) -> Result<SocketAddr, ConnectionError> {
    (cfg.host.as_str(), cfg.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| ConnectionError::DnsEmpty(cfg.host.clone()))
}

fn submit_conn_ops(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    active_count: &mut u32,
    first_err: &mut Option<ConnectionError>,
) {
    for slot in conns.iter_mut() {
        let Some(conn) = slot.as_mut() else { continue };
        let was_active = conn_is_active(conn);
        if let Err(e) = conn.try_submit_send(proactor) {
            fail_conn_and_account(conn, e, first_err, active_count, was_active);
            continue;
        }
        if let Err(e) = conn.try_rearm_multishot(proactor) {
            fail_conn_and_account(conn, e, first_err, active_count, was_active);
        }
    }
}

#[inline]
fn completion_token(c: Completion) -> u64 {
    c.user_data.token()
}

#[inline]
fn conn_for_completion(
    conns: &mut [Option<ConnectionState>],
    c: Completion,
) -> Option<&mut ConnectionState> {
    let token = completion_token(c);
    let conn_id = token_conn_id(token);
    let conn = conns.get_mut(conn_id as usize).and_then(Option::as_mut)?;
    (conn.token() == token).then_some(conn)
}

fn drain_conn_completions(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &mut Vec<Completion>,
    active_count: &mut u32,
    first_err: &mut Option<ConnectionError>,
) -> usize {
    completions_buf.clear();
    let count = proactor.drain_completions(|c| completions_buf.push(c));
    for &c in completions_buf.iter() {
        if let Some(conn) = conn_for_completion(conns, c) {
            let was_active = conn_is_active(conn);
            let result = conn.handle_completion(proactor, c);
            let _ = finish_conn_result(conn, result, first_err, active_count, was_active);
        }
    }
    count
}

/// Data-only hot path：每条 CQE 推进完连接状态机后立刻 drain WS data event。
/// 相比先处理整批 CQE 再统一 drain，减少 burst 内前序行情的排队时间。
fn drain_conn_completions_data<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &mut Vec<Completion>,
    active_count: &mut u32,
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) -> usize
where
    F: for<'a> FnMut(ConnHandle, WsDataEvent<'a>),
{
    completions_buf.clear();
    let count = proactor.drain_completions(|c| completions_buf.push(c));
    dispatch_conn_completions_data(
        conns,
        proactor,
        completions_buf,
        active_count,
        sink,
        first_err,
    );
    count
}

fn dispatch_conn_completions_data<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &[Completion],
    active_count: &mut u32,
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) where
    F: for<'a> FnMut(ConnHandle, WsDataEvent<'a>),
{
    let mut i = 0_usize;
    while i < completions_buf.len() {
        let c = completions_buf[i];
        let token = completion_token(c);
        let Some(conn) = conn_for_completion(conns, c) else {
            i += 1;
            continue;
        };

        let handle = ConnHandle::from_conn(conn);
        if conn.can_handle_plain_recv_data_batch(c) {
            let mut end = i + 1;
            while end < completions_buf.len() {
                let next = completions_buf[end];
                if completion_token(next) != token || !conn.can_handle_plain_recv_data_batch(next) {
                    break;
                }
                end += 1;
            }

            if end > i + 1 {
                let was_active = conn_is_active(conn);
                let result =
                    conn.handle_plain_recv_data_batch(&completions_buf[i..end], &mut |ev| {
                        sink(handle, ev);
                    });
                let _ = finish_conn_result(conn, result, first_err, active_count, was_active);
                i = end;
                continue;
            }
        }

        let was_active = conn_is_active(conn);
        let result = conn.handle_completion_data(proactor, c, |ev| sink(handle, ev));
        let _ = finish_conn_result(conn, result, first_err, active_count, was_active);
        i += 1;
    }
}

fn drain_conn_completions_data_marked<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &mut Vec<Completion>,
    active_count: &mut u32,
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) -> usize
where
    F: for<'a> FnMut(ConnHandle, WsMarkedDataEvent<'a>),
{
    completions_buf.clear();
    let count = proactor.drain_completions(|c| completions_buf.push(c));
    dispatch_conn_completions_data_marked(
        conns,
        proactor,
        completions_buf,
        active_count,
        sink,
        first_err,
    );
    count
}

fn dispatch_conn_completions_data_marked<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &[Completion],
    active_count: &mut u32,
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) where
    F: for<'a> FnMut(ConnHandle, WsMarkedDataEvent<'a>),
{
    let mut i = 0_usize;
    while i < completions_buf.len() {
        let c = completions_buf[i];
        let token = completion_token(c);
        let Some(conn) = conn_for_completion(conns, c) else {
            i += 1;
            continue;
        };

        let handle = ConnHandle::from_conn(conn);
        if conn.can_handle_plain_recv_data_batch(c) {
            let mut end = i + 1;
            while end < completions_buf.len() {
                let next = completions_buf[end];
                if completion_token(next) != token || !conn.can_handle_plain_recv_data_batch(next) {
                    break;
                }
                end += 1;
            }

            if end > i + 1 {
                let was_active = conn_is_active(conn);
                let result =
                    conn.handle_plain_recv_data_batch_marked(&completions_buf[i..end], &mut |ev| {
                        sink(handle, ev);
                    });
                let _ = finish_conn_result(conn, result, first_err, active_count, was_active);
                i = end;
                continue;
            }
        }

        let was_active = conn_is_active(conn);
        let result = conn.handle_completion_data_marked(proactor, c, |ev| sink(handle, ev));
        let _ = finish_conn_result(conn, result, first_err, active_count, was_active);
        i += 1;
    }
}

fn drain_conn_completions_data_batches<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &mut Vec<Completion>,
    active_count: &mut u32,
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) -> usize
where
    F: for<'a> FnMut(ConnHandle, WsDataEventBatch<'a>),
{
    completions_buf.clear();
    let count = proactor.drain_completions(|c| completions_buf.push(c));
    dispatch_conn_completions_data_batches(
        conns,
        proactor,
        completions_buf,
        active_count,
        sink,
        first_err,
    );
    count
}

fn dispatch_conn_completions_data_batches<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &[Completion],
    active_count: &mut u32,
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) where
    F: for<'a> FnMut(ConnHandle, WsDataEventBatch<'a>),
{
    let mut i = 0_usize;
    while i < completions_buf.len() {
        let c = completions_buf[i];
        let token = completion_token(c);
        let Some(conn) = conn_for_completion(conns, c) else {
            i += 1;
            continue;
        };

        let handle = ConnHandle::from_conn(conn);
        if conn.can_handle_plain_recv_data_batch(c) {
            let mut end = i + 1;
            while end < completions_buf.len() {
                let next = completions_buf[end];
                if completion_token(next) != token || !conn.can_handle_plain_recv_data_batch(next) {
                    break;
                }
                end += 1;
            }

            let was_active = conn_is_active(conn);
            let result =
                conn.handle_plain_recv_data_event_batches(&completions_buf[i..end], &mut |batch| {
                    sink(handle, batch);
                });
            let _ = finish_conn_result(conn, result, first_err, active_count, was_active);
            i = end;
            continue;
        }

        let was_active = conn_is_active(conn);
        let result = conn.handle_completion_data_batch(proactor, c, |batch| sink(handle, batch));
        let _ = finish_conn_result(conn, result, first_err, active_count, was_active);
        i += 1;
    }
}

fn drain_conn_completions_data_marked_batches<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &mut Vec<Completion>,
    active_count: &mut u32,
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) -> usize
where
    F: for<'a> FnMut(ConnHandle, WsMarkedDataEventBatch<'a>),
{
    completions_buf.clear();
    let count = proactor.drain_completions(|c| completions_buf.push(c));
    dispatch_conn_completions_data_marked_batches(
        conns,
        proactor,
        completions_buf,
        active_count,
        sink,
        first_err,
    );
    count
}

fn dispatch_conn_completions_data_marked_batches<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &[Completion],
    active_count: &mut u32,
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) where
    F: for<'a> FnMut(ConnHandle, WsMarkedDataEventBatch<'a>),
{
    let mut i = 0_usize;
    while i < completions_buf.len() {
        let c = completions_buf[i];
        let token = completion_token(c);
        let Some(conn) = conn_for_completion(conns, c) else {
            i += 1;
            continue;
        };

        let handle = ConnHandle::from_conn(conn);
        if conn.can_handle_plain_recv_data_batch(c) {
            let mut end = i + 1;
            while end < completions_buf.len() {
                let next = completions_buf[end];
                if completion_token(next) != token || !conn.can_handle_plain_recv_data_batch(next) {
                    break;
                }
                end += 1;
            }

            let was_active = conn_is_active(conn);
            let result = conn.handle_plain_recv_data_event_batches_marked(
                &completions_buf[i..end],
                &mut |batch| {
                    sink(handle, batch);
                },
            );
            let _ = finish_conn_result(conn, result, first_err, active_count, was_active);
            i = end;
            continue;
        }

        let was_active = conn_is_active(conn);
        let result =
            conn.handle_completion_data_marked_batch(proactor, c, |batch| sink(handle, batch));
        let _ = finish_conn_result(conn, result, first_err, active_count, was_active);
        i += 1;
    }
}

#[inline]
fn drain_post_progress<E, F>(
    post_progress_spin_iters: usize,
    first_err: &mut Option<E>,
    mut drain: F,
) where
    F: FnMut(&mut Option<E>),
{
    for _ in 0..post_progress_spin_iters {
        std::hint::spin_loop();
        drain(first_err);
        if first_err.is_some() {
            break;
        }
    }
}

fn write_ingress_prometheus_help<W: fmt::Write>(out: &mut W) -> fmt::Result {
    writeln!(
        out,
        "# HELP talaris_ingress_recv_data_cqes_total Positive-length recv data CQEs handled by a connection."
    )?;
    writeln!(out, "# TYPE talaris_ingress_recv_data_cqes_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_recv_bytes_total Bytes carried by positive-length recv data CQEs. For TLS these are ciphertext bytes; for plain TCP these are plaintext bytes."
    )?;
    writeln!(out, "# TYPE talaris_ingress_recv_bytes_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_recv_multishot_rearms_total Recv multishot SQEs submitted or rearmed."
    )?;
    writeln!(
        out,
        "# TYPE talaris_ingress_recv_multishot_rearms_total counter"
    )?;
    writeln!(
        out,
        "# HELP talaris_ingress_recv_ring_exhaustions_total Recv multishot terminations caused by provided-buffer ring exhaustion."
    )?;
    writeln!(
        out,
        "# TYPE talaris_ingress_recv_ring_exhaustions_total counter"
    )?;
    writeln!(
        out,
        "# HELP talaris_ingress_plain_recv_batches_total Consecutive plain TCP recv CQE runs handled by the data-pump batch path."
    )?;
    writeln!(
        out,
        "# TYPE talaris_ingress_plain_recv_batches_total counter"
    )?;
    writeln!(
        out,
        "# HELP talaris_ingress_plain_recv_batch_cqes_total Total recv CQEs included in plain TCP data-pump batch runs."
    )?;
    writeln!(
        out,
        "# TYPE talaris_ingress_plain_recv_batch_cqes_total counter"
    )?;
    writeln!(
        out,
        "# HELP talaris_ingress_plain_recv_copied_batches_total Plain TCP data-pump batch runs parsed through the reusable copy scratch buffer."
    )?;
    writeln!(
        out,
        "# TYPE talaris_ingress_plain_recv_copied_batches_total counter"
    )?;
    writeln!(
        out,
        "# HELP talaris_ingress_plain_recv_copied_bytes_total Bytes copied into the reusable plain TCP data-pump batch scratch buffer."
    )?;
    writeln!(
        out,
        "# TYPE talaris_ingress_plain_recv_copied_bytes_total counter"
    )?;
    writeln!(
        out,
        "# HELP talaris_ingress_plaintext_chunks_total Plaintext source chunks made available to WebSocket receive processing. TLS counts rustls plaintext slices; plain TCP counts recv/provided-buffer slices before optional copy batching."
    )?;
    writeln!(out, "# TYPE talaris_ingress_plaintext_chunks_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_plaintext_bytes_total Plaintext bytes fed into the WebSocket parser."
    )?;
    writeln!(out, "# TYPE talaris_ingress_plaintext_bytes_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_ws_data_drains_total Data-pump plaintext source chunks that reached WebSocket receive processing."
    )?;
    writeln!(out, "# TYPE talaris_ingress_ws_data_drains_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_ws_data_drain_skips_total Data-pump drain attempts skipped because no plaintext arrived."
    )?;
    writeln!(
        out,
        "# TYPE talaris_ingress_ws_data_drain_skips_total counter"
    )?;
    writeln!(
        out,
        "# HELP talaris_ingress_ws_data_events_total Text/Binary data messages emitted to the user's data sink."
    )?;
    writeln!(out, "# TYPE talaris_ingress_ws_data_events_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_ws_text_events_total Text messages emitted to the user's data sink."
    )?;
    writeln!(out, "# TYPE talaris_ingress_ws_text_events_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_ws_binary_events_total Binary messages emitted to the user's data sink."
    )?;
    writeln!(out, "# TYPE talaris_ingress_ws_binary_events_total counter")
}

/// pump 内 per-conn 错误处理：保留第一条错误，把对应 conn 推到 Closed。
/// 后续已到达的 CQE 仍会按 conn_id 路由，但 ConnectionState 只回收资源，
/// 不再推进 WS parser 或调用用户 sink。
fn fail_conn(
    conn: &mut ConnectionState,
    err: ConnectionError,
    first_err: &mut Option<ConnectionError>,
) -> bool {
    let was_active = conn_is_active(conn);
    tracing::warn!(conn_id = conn.conn_id(), error = %err, "pool conn failed");
    conn.state = State::Closed;
    if first_err.is_none() {
        *first_err = Some(err);
    }
    was_active
}

#[inline]
fn conn_is_active(conn: &ConnectionState) -> bool {
    !matches!(conn.state(), State::Closed)
}

#[inline]
fn account_closed_transition(active_count: &mut u32, was_active: bool, conn: &ConnectionState) {
    if was_active && !conn_is_active(conn) {
        *active_count = active_count.saturating_sub(1);
    }
}

#[inline]
fn finish_conn_result<T>(
    conn: &mut ConnectionState,
    result: Result<T, ConnectionError>,
    first_err: &mut Option<ConnectionError>,
    active_count: &mut u32,
    was_active: bool,
) -> Option<T> {
    match result {
        Ok(value) => {
            conn.sync_ws_open_state();
            conn.sync_ws_close_state();
            account_closed_transition(active_count, was_active, conn);
            Some(value)
        }
        Err(e) => {
            fail_conn_and_account(conn, e, first_err, active_count, was_active);
            None
        }
    }
}

#[inline]
fn fail_conn_and_account(
    conn: &mut ConnectionState,
    err: ConnectionError,
    first_err: &mut Option<ConnectionError>,
    active_count: &mut u32,
    was_active: bool,
) {
    let failed_active = fail_conn(conn, err, first_err);
    if was_active || failed_active {
        *active_count = active_count.saturating_sub(1);
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // 关键顺序：所有 conn 的 buf_ring 必须在 proactor drop 前 unregister，
        // 否则 BufferRing::Drop 触发 debug_assert（release 模式下 leak 防 UAF）。
        for slot in self.conns.iter_mut() {
            if let Some(conn) = slot.as_mut()
                && let Some(mut ring) = conn.buf_ring.take()
            {
                let _ = ring.unregister(&mut self.proactor);
            }
        }
    }
}

#[cfg(test)]
mod post_progress_tests {
    use super::drain_post_progress;

    #[test]
    fn drain_post_progress_is_noop_without_budget() {
        let mut calls = 0_u32;
        let mut first_err = None::<()>;

        drain_post_progress(0, &mut first_err, |_| {
            calls += 1;
        });

        assert_eq!(calls, 0);
        assert!(first_err.is_none());
    }

    #[test]
    fn drain_post_progress_uses_full_budget_without_error() {
        let mut calls = 0_u32;
        let mut first_err = None::<()>;

        drain_post_progress(4, &mut first_err, |_| {
            calls += 1;
        });

        assert_eq!(calls, 4);
        assert!(first_err.is_none());
    }

    #[test]
    fn drain_post_progress_stops_after_first_error() {
        let mut calls = 0_u32;
        let mut first_err = None::<()>;

        drain_post_progress(8, &mut first_err, |err| {
            calls += 1;
            if calls == 3 {
                *err = Some(());
            }
        });

        assert_eq!(calls, 3);
        assert_eq!(first_err, Some(()));
    }
}

// 这些测试真正调 io_uring；非 Linux 平台走 stub.rs 的 unimplemented!() panic。
// 编译时仍 type-check（macOS 也能改 pool 立刻发现错误），运行时只在 Linux 跑。
#[cfg(all(test, target_os = "linux"))]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use crate::connection_meta::{ConnectionConfig, State};
    use crate::observability::MarkedDataEvent;
    use crate::proactor::{OpKind, UserData};
    use crate::test_helpers::{read_one_frame, run_echo_server};
    use crate::ws::frame::{MAX_HEADER_LEN, encode_header};
    use crate::ws::handshake::compute_accept;
    use crate::ws::mask::mask_inplace;
    use crate::ws::{DataEvent as WsDataEvent, OpCode, WsConfig};
    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    static COPY_BATCH_PAYLOAD: [u8; 512] = [b'x'; 512];

    fn spawn_server<F>(f: F) -> (SocketAddr, thread::JoinHandle<()>)
    where
        F: FnOnce(TcpListener) + Send + 'static,
    {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        (addr, thread::spawn(move || f(listener)))
    }

    fn accept_ws_upgrade(listener: TcpListener) -> TcpStream {
        let (mut stream, _) = listener.accept().expect("accept");
        stream.set_nodelay(true).unwrap();

        let mut buf = [0_u8; 4096];
        let mut req = Vec::new();
        loop {
            let n = stream.read(&mut buf).unwrap();
            assert!(n > 0, "client closed before sending request");
            req.extend_from_slice(&buf[..n]);
            if req.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }

        let req_str = std::str::from_utf8(&req).unwrap();
        let key = req_str
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with("sec-websocket-key:"))
            .and_then(|line| line.split(':').nth(1))
            .expect("Sec-WebSocket-Key header")
            .trim();
        let accept = compute_accept(key);
        let resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        stream.write_all(resp.as_bytes()).unwrap();
        stream
    }

    fn accept_ws_upgrade_recording_request(listener: TcpListener) -> (TcpStream, String) {
        let (mut stream, _) = listener.accept().expect("accept");
        stream.set_nodelay(true).unwrap();

        let mut buf = [0_u8; 4096];
        let mut req = Vec::new();
        loop {
            let n = stream.read(&mut buf).unwrap();
            assert!(n > 0, "client closed before sending request");
            req.extend_from_slice(&buf[..n]);
            if req.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }

        let req_str = std::str::from_utf8(&req).unwrap().to_owned();
        let key = req_str
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with("sec-websocket-key:"))
            .and_then(|line| line.split(':').nth(1))
            .expect("Sec-WebSocket-Key header")
            .trim();
        let accept = compute_accept(key);
        let resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        stream.write_all(resp.as_bytes()).unwrap();
        (stream, req_str)
    }

    fn run_request_assertion_server(listener: TcpListener) {
        let (mut stream, req) = accept_ws_upgrade_recording_request(listener);
        assert!(
            req.starts_with("GET /real HTTP/1.1\r\n"),
            "request was {req:?}"
        );
        assert!(req.contains("Host: localhost"), "request was {req:?}");
        assert!(!req.contains("wrong-host"), "request was {req:?}");
        assert!(!req.contains("GET /wrong"), "request was {req:?}");
        let (opcode, _) = read_one_frame(&mut stream);
        assert_eq!(opcode, OpCode::Close);
        write_server_close(&mut stream);
    }

    fn write_server_frame(
        out: &mut Vec<u8>,
        opcode: OpCode,
        payload: &[u8],
        mask: Option<[u8; 4]>,
    ) {
        let mut header = [0_u8; MAX_HEADER_LEN];
        let hn = encode_header(&mut header, true, opcode, mask, payload.len() as u64);
        out.extend_from_slice(&header[..hn]);
        if let Some(mask_key) = mask {
            let mut masked = payload.to_vec();
            mask_inplace(&mut masked, mask_key);
            out.extend_from_slice(&masked);
        } else {
            out.extend_from_slice(payload);
        }
    }

    fn write_server_texts(stream: &mut TcpStream, messages: &[&[u8]]) {
        let mut frames = Vec::new();
        for message in messages {
            write_server_frame(&mut frames, OpCode::Text, message, None);
        }
        stream.write_all(&frames).unwrap();
    }

    fn write_server_close(stream: &mut TcpStream) {
        let mut frame = Vec::new();
        write_server_frame(&mut frame, OpCode::Close, &1000_u16.to_be_bytes(), None);
        stream.write_all(&frame).unwrap();
    }

    fn run_push_server(listener: TcpListener, messages: Vec<&'static [u8]>) {
        let mut stream = accept_ws_upgrade(listener);
        write_server_texts(&mut stream, &messages);
        let (opcode, _) = read_one_frame(&mut stream);
        assert_eq!(opcode, OpCode::Close);
        write_server_close(&mut stream);
    }

    fn run_delayed_push_server(listener: TcpListener, messages: Vec<&'static [u8]>) {
        let mut stream = accept_ws_upgrade(listener);
        thread::sleep(Duration::from_millis(10));
        write_server_texts(&mut stream, &messages);
        let (opcode, _) = read_one_frame(&mut stream);
        assert_eq!(opcode, OpCode::Close);
        write_server_close(&mut stream);
    }

    fn run_idle_server(listener: TcpListener) {
        let mut stream = accept_ws_upgrade(listener);
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut buf = [0_u8; 1024];
        let _ = stream.read(&mut buf);
    }

    fn run_close_after_client_text_server(listener: TcpListener) {
        let mut stream = accept_ws_upgrade(listener);
        let (opcode, _) = read_one_frame(&mut stream);
        assert_eq!(opcode, OpCode::Text);
        write_server_close(&mut stream);
    }

    fn run_invalid_after_client_text_server(listener: TcpListener) {
        let mut stream = accept_ws_upgrade(listener);
        let (opcode, _) = read_one_frame(&mut stream);
        assert_eq!(opcode, OpCode::Text);
        let mut frame = Vec::new();
        write_server_frame(
            &mut frame,
            OpCode::Text,
            b"masked-from-server",
            Some([1, 2, 3, 4]),
        );
        stream.write_all(&frame).unwrap();
    }

    fn plain_cfg(addr: SocketAddr, path: &str) -> ConnectionConfig {
        ConnectionConfig::new("localhost", addr.port(), path).with_tls(false)
    }

    fn drive_until_open(pool: &mut Pool, handles: &[ConnHandle]) {
        for _ in 0..500 {
            pool.pump_nowait(|_, _| {}).unwrap();
            if handles
                .iter()
                .all(|&handle| pool.state(handle) == Some(State::Open))
            {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("connections did not open");
    }

    fn drive_until_closed(pool: &mut Pool, handle: ConnHandle) {
        for _ in 0..500 {
            let _ = pool.pump_nowait(|_, _| {});
            if pool.state(handle) == Some(State::Closed) {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("connection did not close");
    }

    fn close_and_join(pool: &mut Pool, handle: ConnHandle, server: thread::JoinHandle<()>) {
        pool.initiate_close(handle, 1000, "bye").unwrap();
        drive_until_closed(pool, handle);
        server.join().unwrap();
    }

    fn wait_for_text_event(pool: &mut Pool, handle: ConnHandle, expected: &str) {
        let mut got: Option<String> = None;
        for _ in 0..500 {
            pool.pump_nowait(|event_handle, event| {
                assert_eq!(event_handle, handle);
                if let WsEvent::Text(text) = event {
                    got = Some(text.to_owned());
                }
            })
            .unwrap();
            if got.is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(got.as_deref(), Some(expected));
    }

    fn spin_until<F>(mut f: F)
    where
        F: FnMut() -> bool,
    {
        for _ in 0..500 {
            if f() {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("spin pump did not observe event");
    }

    /// 单 conn 走 Pool 路径（从 connection.rs 搬过来——Migration Step 3 后
    /// `Connection` thin wrapper 删除，单 conn 流程同样走 `Pool::connect_blocking`）。
    #[test]
    fn pool_single_conn_plain_ws_echo_roundtrip() {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let local_addr = listener.local_addr().unwrap();

        let (_shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
        let server = thread::spawn(move || run_echo_server(listener, shutdown_rx));

        let cfg = ConnectionConfig::new("localhost", local_addr.port(), "/echo").with_tls(false);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let handle = pool.connect_blocking_to(cfg, local_addr).expect("connect");
        assert_eq!(pool.state(handle), Some(State::Open));

        pool.send_text(handle, b"hello").unwrap();

        let mut got_text: Option<String> = None;
        for _ in 0..50 {
            pool.pump_data(|h, ev| {
                assert_eq!(h, handle);
                if let WsDataEvent::Text(s) = ev {
                    got_text = Some(s.to_owned());
                }
            })
            .unwrap();
            if got_text.is_some() {
                break;
            }
        }
        assert_eq!(got_text.as_deref(), Some("hello"));

        pool.initiate_close(handle, 1000, "bye").unwrap();
        for _ in 0..50 {
            if matches!(pool.state(handle), Some(State::Closed | State::Closing)) {
                let _ = pool.pump_nowait(|_, _| {});
            }
            if matches!(pool.state(handle), Some(State::Closed)) {
                break;
            }
            let _ = pool.pump(|_, _| {});
        }

        server.join().unwrap();
    }

    /// TLS path smoke test：连 Deribit testnet，发 `public/test` JSON-RPC，
    /// 拿任意响应即认为 TLS+WS handshake 跑通。
    ///
    /// 默认 `#[ignore]`——不污染 CI 稳定性。手动跑：
    /// `cargo test -p network --lib pool::tests::tls_smoke_deribit_testnet -- --ignored --nocapture`
    #[test]
    #[ignore = "需要外网 + test.deribit.com 可达；手动 --ignored 跑"]
    fn tls_smoke_deribit_testnet() {
        let cfg = ConnectionConfig::new("test.deribit.com", 443, "/ws/api/v2");
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let handle = pool
            .connect_blocking(cfg)
            .expect("tls handshake + ws upgrade");
        assert_eq!(pool.state(handle), Some(State::Open));
        eprintln!("TLS+WS handshake OK, sending public/test ...");

        pool.send_text(
            handle,
            br#"{"jsonrpc":"2.0","id":1,"method":"public/test","params":{}}"#,
        )
        .unwrap();

        let mut got = false;
        for _ in 0..100 {
            pool.pump(|_h, ev| {
                if let WsEvent::Text(s) = ev {
                    eprintln!("got text: {s}");
                    got = true;
                }
            })
            .unwrap();
            if got {
                break;
            }
        }
        assert!(got, "no response from test.deribit.com");

        pool.initiate_close(handle, 1000, "bye").unwrap();
        for _ in 0..20 {
            let _ = pool.pump_nowait(|_, _| {});
            if matches!(pool.state(handle), Some(State::Closed)) {
                break;
            }
        }
    }

    /// Migration Step 2 验收：一个 Pool 同时驱动两条 plain WS，CQE 按 conn_id
    /// 路由到对应 ConnHandle，事件互不串。
    #[test]
    fn pool_two_conns_no_cross_talk() {
        let listener_a = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let listener_b = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr_b = listener_b.local_addr().unwrap();

        let (_tx_a, rx_a) = mpsc::channel::<()>();
        let (_tx_b, rx_b) = mpsc::channel::<()>();
        let server_a = thread::spawn(move || run_echo_server(listener_a, rx_a));
        let server_b = thread::spawn(move || run_echo_server(listener_b, rx_b));

        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let cfg_a = ConnectionConfig::new("localhost", addr_a.port(), "/a").with_tls(false);
        let cfg_b = ConnectionConfig::new("localhost", addr_b.port(), "/b").with_tls(false);
        let h_a = pool.connect_blocking_to(cfg_a, addr_a).expect("connect a");
        let h_b = pool.connect_blocking_to(cfg_b, addr_b).expect("connect b");

        assert_eq!(pool.conn_count(), 2);
        assert_ne!(h_a, h_b);
        assert_eq!(pool.state(h_a), Some(State::Open));
        assert_eq!(pool.state(h_b), Some(State::Open));
        // conn_id 单调：第二条比第一条大；bgid 同理由 Pool 各占一个
        assert!(h_b.as_u32() > h_a.as_u32());

        pool.send_text(h_a, b"alpha").unwrap();
        pool.send_text(h_b, b"bravo").unwrap();

        let mut a_text: Option<String> = None;
        let mut b_text: Option<String> = None;
        let mut wrong_route = false;

        for _ in 0..200 {
            pool.pump(|h, ev| {
                if let WsEvent::Text(s) = ev {
                    if h == h_a {
                        if s != "alpha" {
                            wrong_route = true;
                        }
                        a_text = Some(s.to_owned());
                    } else if h == h_b {
                        if s != "bravo" {
                            wrong_route = true;
                        }
                        b_text = Some(s.to_owned());
                    } else {
                        wrong_route = true;
                    }
                }
            })
            .unwrap();
            if a_text.is_some() && b_text.is_some() {
                break;
            }
        }

        assert!(
            !wrong_route,
            "CQE 路由错位：handle 收到了不属于它的 payload"
        );
        assert_eq!(a_text.as_deref(), Some("alpha"));
        assert_eq!(b_text.as_deref(), Some("bravo"));

        pool.initiate_close(h_a, 1000, "bye").unwrap();
        pool.initiate_close(h_b, 1000, "bye").unwrap();
        for _ in 0..50 {
            let _ = pool.pump_nowait(|_, _| {});
            let done_a = matches!(pool.state(h_a), Some(State::Closed));
            let done_b = matches!(pool.state(h_b), Some(State::Closed));
            if done_a && done_b {
                break;
            }
            let _ = pool.pump(|_, _| {});
        }

        server_a.join().unwrap();
        server_b.join().unwrap();
    }

    #[test]
    fn connect_blocking_to_does_not_drain_existing_connection_data() {
        let (addr_a, server_a) =
            spawn_server(|listener| run_push_server(listener, vec![b"pending-a"]));
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let h_a = pool
            .connect_blocking_to(plain_cfg(addr_a, "/a"), addr_a)
            .expect("connect a");
        assert_eq!(pool.state(h_a), Some(State::Open));

        let (addr_b, server_b) = spawn_server(|listener| run_push_server(listener, Vec::new()));
        let h_b = pool
            .connect_blocking_to(plain_cfg(addr_b, "/b"), addr_b)
            .expect("connect b");
        assert_eq!(pool.state(h_b), Some(State::Open));
        assert_eq!(pool.conn_count(), 2);

        wait_for_text_event(&mut pool, h_a, "pending-a");

        close_and_join(&mut pool, h_a, server_a);
        close_and_join(&mut pool, h_b, server_b);
    }

    #[test]
    fn submit_connect_to_can_drive_multiple_handshakes_concurrently() {
        let (addr_a, server_a) = spawn_server(|listener| run_push_server(listener, Vec::new()));
        let (addr_b, server_b) = spawn_server(|listener| run_push_server(listener, Vec::new()));

        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let h_a = pool
            .submit_connect_to(plain_cfg(addr_a, "/a"), addr_a)
            .expect("submit a");
        let h_b = pool
            .submit_connect_to(plain_cfg(addr_b, "/b"), addr_b)
            .expect("submit b");

        drive_until_open(&mut pool, &[h_a, h_b]);
        assert_eq!(pool.conn_count(), 2);
        assert_eq!(pool.state(h_a), Some(State::Open));
        assert_eq!(pool.state(h_b), Some(State::Open));

        close_and_join(&mut pool, h_a, server_a);
        close_and_join(&mut pool, h_b, server_b);
    }

    #[test]
    fn active_count_tracks_remote_close_parse_error_connect_failure_and_remove() {
        let (close_addr, close_server) = spawn_server(run_close_after_client_text_server);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let close_handle = pool
            .connect_blocking_to(plain_cfg(close_addr, "/close"), close_addr)
            .expect("connect close server");
        assert_eq!(pool.conn_count(), 1);
        pool.send_text(close_handle, b"trigger").unwrap();
        drive_until_closed(&mut pool, close_handle);
        assert_eq!(pool.conn_count(), 0);
        close_server.join().unwrap();

        let (bad_addr, bad_server) = spawn_server(run_invalid_after_client_text_server);
        let bad_handle = pool
            .connect_blocking_to(plain_cfg(bad_addr, "/bad"), bad_addr)
            .expect("connect bad server");
        assert_eq!(pool.conn_count(), 1);
        pool.send_text(bad_handle, b"trigger").unwrap();
        let mut saw_error = false;
        for _ in 0..500 {
            if pool.pump_nowait(|_, _| {}).is_err() {
                saw_error = true;
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert!(saw_error, "masked server frame must fail the connection");
        assert_eq!(pool.state(bad_handle), Some(State::Closed));
        assert_eq!(pool.conn_count(), 0);
        bad_server.join().unwrap();

        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let refused_addr = listener.local_addr().unwrap();
        drop(listener);
        let err = pool.connect_blocking_to(plain_cfg(refused_addr, "/refused"), refused_addr);
        assert!(err.is_err(), "connecting to a closed listener should fail");
        assert_eq!(pool.conn_count(), 0);

        let (idle_addr, idle_server) = spawn_server(run_idle_server);
        let idle_handle = pool
            .connect_blocking_to(plain_cfg(idle_addr, "/idle"), idle_addr)
            .expect("connect idle server");
        assert_eq!(pool.conn_count(), 1);
        pool.remove_conn(idle_handle).expect("remove idle conn");
        assert_eq!(pool.conn_count(), 0);
        assert_eq!(pool.state(idle_handle), None);
        idle_server.join().unwrap();
    }

    #[test]
    fn remove_conn_reuses_slot_with_new_generation_and_rejects_stale_handle() {
        let (addr_a, server_a) = spawn_server(run_idle_server);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let old = pool
            .connect_blocking_to(plain_cfg(addr_a, "/old"), addr_a)
            .expect("connect old");
        assert_eq!(pool.conn_count(), 1);
        assert_eq!(pool.state(old), Some(State::Open));
        assert_eq!(old.as_u32(), 0);
        assert_eq!(old.generation(), 0);

        pool.remove_conn(old).expect("remove old");
        assert_eq!(pool.conn_count(), 0);
        assert_eq!(pool.state(old), None);
        assert!(pool.send_text(old, b"stale").is_err());
        server_a.join().unwrap();

        let (addr_b, server_b) = spawn_server(run_idle_server);
        let new = pool
            .connect_blocking_to(plain_cfg(addr_b, "/new"), addr_b)
            .expect("connect new");
        assert_eq!(pool.conn_count(), 1);
        assert_eq!(pool.state(new), Some(State::Open));
        assert_eq!(new.as_u32(), old.as_u32());
        assert_eq!(new.generation(), old.generation() + 1);
        assert_ne!(new, old);
        assert_eq!(pool.state(old), None);
        assert!(pool.send_text(old, b"still stale").is_err());

        close_and_join(&mut pool, new, server_b);
    }

    #[test]
    fn stale_completion_from_removed_generation_is_ignored_after_slot_reuse() {
        let (addr_a, server_a) = spawn_server(run_idle_server);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let old = pool
            .connect_blocking_to(plain_cfg(addr_a, "/old"), addr_a)
            .expect("connect old");
        let old_token = old.as_u64();
        pool.remove_conn(old).expect("remove old");
        server_a.join().unwrap();

        let (addr_b, server_b) = spawn_server(run_idle_server);
        let new = pool
            .connect_blocking_to(plain_cfg(addr_b, "/new"), addr_b)
            .expect("connect new");
        assert_eq!(new.as_u32(), old.as_u32());
        assert_ne!(new.as_u64(), old_token);

        let stale = Completion {
            user_data: UserData::new(OpKind::Recv, old_token),
            result: -libc::ECANCELED,
            flags: 0,
        };
        let mut first_err = None;
        let Pool {
            proactor,
            conns,
            active_count,
            ..
        } = &mut pool;
        dispatch_conn_completions_data(
            conns,
            proactor,
            &[stale],
            active_count,
            &mut |_, _| panic!("stale completion must not reach sink"),
            &mut first_err,
        );
        assert!(first_err.is_none());
        assert_eq!(pool.state(new), Some(State::Open));
        assert_eq!(pool.conn_count(), 1);

        close_and_join(&mut pool, new, server_b);
    }

    #[test]
    fn submit_reconnect_reuses_slot_and_invalidates_old_handle() {
        let (addr_a, server_a) = spawn_server(run_idle_server);
        let (addr_b, server_b) = spawn_server(run_idle_server);
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

        drive_until_open(&mut pool, &[new]);
        assert_eq!(pool.state(new), Some(State::Open));
        assert!(pool.send_text(old, b"stale").is_err());

        close_and_join(&mut pool, new, server_b);
    }

    #[test]
    fn reconnect_failure_retires_new_slot_and_keeps_pool_count_consistent() {
        let (addr, server) = spawn_server(run_idle_server);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let old = pool
            .connect_blocking_to(plain_cfg(addr, "/old"), addr)
            .expect("connect old");
        assert_eq!(pool.conn_count(), 1);

        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let refused_addr = listener.local_addr().unwrap();
        drop(listener);
        let err = pool.reconnect_to(old, plain_cfg(refused_addr, "/refused"), refused_addr);
        assert!(err.is_err(), "reconnect to a closed listener should fail");
        assert_eq!(pool.state(old), None);
        assert_eq!(pool.conn_count(), 0);
        server.join().unwrap();

        let (addr_next, server_next) = spawn_server(run_idle_server);
        let next = pool
            .connect_blocking_to(plain_cfg(addr_next, "/next"), addr_next)
            .expect("connect after failed reconnect");
        assert_eq!(next.as_u32(), old.as_u32());
        assert!(next.generation() > old.generation());
        assert_eq!(pool.conn_count(), 1);
        close_and_join(&mut pool, next, server_next);
    }

    #[test]
    fn pool_data_batch_and_marked_pumps_preserve_handle_routing() {
        let (batch_addr, batch_server) = spawn_server(|listener| {
            run_delayed_push_server(listener, vec![b"batch-a", b"batch-b"]);
        });
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let batch_handle = pool
            .connect_blocking_to(plain_cfg(batch_addr, "/batch"), batch_addr)
            .expect("connect batch server");

        let mut batch_texts = Vec::new();
        for _ in 0..500 {
            pool.pump_data_batches_nowait(|event_handle, batch| {
                assert_eq!(event_handle, batch_handle);
                assert!(!batch.is_empty());
                for event in batch.iter() {
                    if let WsDataEvent::Text(text) = event {
                        batch_texts.push(text.to_owned());
                    }
                }
            })
            .unwrap();
            if batch_texts.len() >= 2 {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(batch_texts, ["batch-a", "batch-b"]);
        close_and_join(&mut pool, batch_handle, batch_server);

        let (marked_addr, marked_server) = spawn_server(|listener| {
            run_delayed_push_server(listener, vec![b"marked"]);
        });
        let marked_cfg = plain_cfg(marked_addr, "/marked")
            .with_observability_sample_rate_bps(10_000)
            .with_observability_histograms(true);
        let marked_handle = pool
            .connect_blocking_to(marked_cfg, marked_addr)
            .expect("connect marked server");
        let mut marked_text: Option<String> = None;
        let mut marked_sampled = false;
        for _ in 0..500 {
            pool.pump_data_marked_nowait(|event_handle, event| {
                assert_eq!(event_handle, marked_handle);
                if let MarkedDataEvent::Text { payload, meta } = event {
                    marked_text = Some(payload.to_owned());
                    marked_sampled = meta.sampled;
                }
            })
            .unwrap();
            if marked_text.is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(marked_text.as_deref(), Some("marked"));
        assert!(marked_sampled);
        close_and_join(&mut pool, marked_handle, marked_server);
    }

    #[test]
    fn connection_config_endpoint_overrides_inner_ws_config_at_handshake() {
        let (addr, server) = spawn_server(run_request_assertion_server);
        let ws = WsConfig::new("wrong-host", "/wrong").with_initial_buffer_capacities(11, 22, 33);
        let cfg = plain_cfg(addr, "/real").with_ws_config(ws);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let handle = pool.connect_blocking_to(cfg, addr).expect("connect");
        assert_eq!(pool.state(handle), Some(State::Open));
        close_and_join(&mut pool, handle, server);
    }

    #[test]
    fn plain_copy_batch_records_ingress_stats() {
        let (addr, server) = spawn_server(|listener| {
            run_delayed_push_server(listener, vec![&COPY_BATCH_PAYLOAD]);
        });
        let cfg = plain_cfg(addr, "/copy-batch")
            .with_buf_ring(64, 64)
            .with_plain_recv_batch_copy_max_bytes(4096)
            .with_ingress_stats(true);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let handle = pool.connect_blocking_to(cfg, addr).expect("connect");

        let mut got_len = None;
        for _ in 0..500 {
            pool.pump_data_nowait(|event_handle, event| {
                assert_eq!(event_handle, handle);
                if let WsDataEvent::Text(text) = event {
                    got_len = Some(text.len());
                }
            })
            .unwrap();
            if got_len.is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(got_len, Some(COPY_BATCH_PAYLOAD.len()));

        let stats = pool.ingress_stats(handle).expect("stats");
        assert!(stats.recv_data_cqes > 1, "stats: {stats:?}");
        assert!(stats.plain_recv_batches > 0, "stats: {stats:?}");
        assert!(stats.plain_recv_batch_cqes > 1, "stats: {stats:?}");
        assert!(stats.plain_recv_copied_batches > 0, "stats: {stats:?}");
        assert!(
            stats.plain_recv_copied_bytes >= COPY_BATCH_PAYLOAD.len() as u64,
            "stats: {stats:?}"
        );

        close_and_join(&mut pool, handle, server);
    }

    #[test]
    #[ignore = "requires Linux 6.10+ IORING_RECVSEND_BUNDLE support"]
    fn multishot_bundle_plain_ws_echo_roundtrip() {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let local_addr = listener.local_addr().unwrap();

        let (_shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
        let server = thread::spawn(move || run_echo_server(listener, shutdown_rx));

        let cfg = ConnectionConfig::new("localhost", local_addr.port(), "/echo")
            .with_tls(false)
            .with_recv_mode(crate::connection_meta::RecvMode::MultishotBundle);
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let handle = pool.connect_blocking_to(cfg, local_addr).expect("connect");
        assert_eq!(pool.state(handle), Some(State::Open));

        pool.send_text(handle, b"bundle").unwrap();
        wait_for_text_event(&mut pool, handle, "bundle");
        close_and_join(&mut pool, handle, server);
    }

    #[test]
    fn pool_spin_data_pumps_preserve_handle_routing() {
        let (data_addr, data_server) = spawn_server(|listener| {
            run_delayed_push_server(listener, vec![b"spin-data"]);
        });
        let mut pool = Pool::new(PoolConfig::default()).expect("pool");
        let data_handle = pool
            .connect_blocking_to(plain_cfg(data_addr, "/spin-data"), data_addr)
            .expect("connect spin data server");
        let mut data_text: Option<String> = None;
        spin_until(|| {
            pool.pump_data_spin(1024, |event_handle, event| {
                assert_eq!(event_handle, data_handle);
                if let WsDataEvent::Text(text) = event {
                    data_text = Some(text.to_owned());
                }
            })
            .unwrap();
            data_text.is_some()
        });
        assert_eq!(data_text.as_deref(), Some("spin-data"));
        close_and_join(&mut pool, data_handle, data_server);

        let (batch_addr, batch_server) = spawn_server(|listener| {
            run_delayed_push_server(listener, vec![b"spin-batch"]);
        });
        let batch_handle = pool
            .connect_blocking_to(plain_cfg(batch_addr, "/spin-batch"), batch_addr)
            .expect("connect spin batch server");
        let mut batch_text: Option<String> = None;
        spin_until(|| {
            pool.pump_data_spin_batches(1024, |event_handle, batch| {
                assert_eq!(event_handle, batch_handle);
                for event in batch.iter() {
                    if let WsDataEvent::Text(text) = event {
                        batch_text = Some(text.to_owned());
                    }
                }
            })
            .unwrap();
            batch_text.is_some()
        });
        assert_eq!(batch_text.as_deref(), Some("spin-batch"));
        close_and_join(&mut pool, batch_handle, batch_server);

        let (marked_addr, marked_server) = spawn_server(|listener| {
            run_delayed_push_server(listener, vec![b"spin-marked"]);
        });
        let marked_cfg =
            plain_cfg(marked_addr, "/spin-marked").with_observability_sample_rate_bps(10_000);
        let marked_handle = pool
            .connect_blocking_to(marked_cfg, marked_addr)
            .expect("connect spin marked server");
        let mut marked_text: Option<String> = None;
        let mut marked_sampled = false;
        spin_until(|| {
            pool.pump_data_spin_marked(1024, |event_handle, event| {
                assert_eq!(event_handle, marked_handle);
                if let MarkedDataEvent::Text { payload, meta } = event {
                    marked_text = Some(payload.to_owned());
                    marked_sampled = meta.sampled;
                }
            })
            .unwrap();
            marked_text.is_some()
        });
        assert_eq!(marked_text.as_deref(), Some("spin-marked"));
        assert!(marked_sampled);
        close_and_join(&mut pool, marked_handle, marked_server);

        let (marked_batch_addr, marked_batch_server) = spawn_server(|listener| {
            run_delayed_push_server(listener, vec![b"spin-marked-batch"]);
        });
        let marked_batch_cfg = plain_cfg(marked_batch_addr, "/spin-marked-batch")
            .with_observability_sample_rate_bps(10_000);
        let marked_batch_handle = pool
            .connect_blocking_to(marked_batch_cfg, marked_batch_addr)
            .expect("connect spin marked batch server");
        let mut marked_batch_text: Option<String> = None;
        let mut marked_batch_sampled = false;
        spin_until(|| {
            pool.pump_data_spin_marked_batches(1024, |event_handle, batch| {
                assert_eq!(event_handle, marked_batch_handle);
                for event in batch.iter() {
                    if let MarkedDataEvent::Text { payload, meta } = event {
                        marked_batch_text = Some(payload.to_owned());
                        marked_batch_sampled = meta.sampled;
                    }
                }
            })
            .unwrap();
            marked_batch_text.is_some()
        });
        assert_eq!(marked_batch_text.as_deref(), Some("spin-marked-batch"));
        assert!(marked_batch_sampled);
        close_and_join(&mut pool, marked_batch_handle, marked_batch_server);
    }
}
