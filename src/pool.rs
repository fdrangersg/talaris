//! `Pool` —— multi-conn 驱动
//!
//! 一个 [`Proactor`] 服务同 venue 的多条 WS。CQE 通过
//! [`crate::proactor::UserData::token`] 低 28 位编码 conn_id；Pool drain 后按 id
//! 路由到对应 `ConnectionState`。
//!
//! ## 关键不变式
//!
//! - **单线程占用**：`Pool: !Send + !Sync`（`PhantomData<*const ()>` 标记）。
//!   io_uring 内部状态不能跨线程。
//! - **conn_id 编码 ≤ 28 bit**：[`UserData`] 高 8 位是 OpKind，低 56 位是
//!   caller token；这里再约定低 28 位为 conn_id，bits 55..28 预留给 op-seq
//!   （v1 不使用）。
//! - **bgid 单调递增**：每条 conn 独占一个 bgid（kernel 看，跨 conn 不重叠）。
//!   v1 简单方案不回收 bgid（短期）；64 K 上限远超实际部署。
//! - **drain 顺序**：每轮 pump 先 submit pending send + rearm multishot，再
//!   `submit_and_wait`，最后 drain CQE 路由 + drain ws_events。
//!
//! ## v1 与 v2 范围
//!
//! 本文件落地 plan.md "Network Pool 详细设计" 的 Migration Step 1：skeleton +
//! ConnectionState 拆分。
//!
//! - 单 conn 路径与旧版单连接 wrapper 等价。
//! - 多 conn pump 已能跑（CQE 按 conn_id 路由）。
//! - 还未做：slot 复用 / Tombstone / pool 内重连 / 多 venue 共 Pool。
//!
//! [`UserData`]: crate::proactor::UserData

// `expect()` 用法均为 invariant 断言（just-pushed conn 一定存在；28-bit mask
// 一定 fits u32）。走到 panic 等于 Pool 内部状态已坏 —— HFT 进程应立即重启。
#![allow(clippy::expect_used)]

use std::fmt;
use std::marker::PhantomData;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::connection::{ConnectionConfig, ConnectionError, IngressStats, State};
use crate::connection_state::ConnectionState;
use crate::observability::LatencyHistograms;
use crate::proactor::{Completion, Proactor, ProactorConfig, ProactorError};
use crate::ws::{
    ConnState as WsConnState, DataEvent as WsDataEvent, Event as WsEvent,
    MarkedDataEvent as WsMarkedDataEvent,
};

/// CQE.token() 中 conn_id 的位掩码 —— 28 bit，最多 ~2.6 亿条 conn / Pool，
/// 远超任何实际场景。bits 55..28 预留给 op-seq dedup（v1 不使用）。
const CONN_ID_MASK: u64 = 0x0FFF_FFFF;

/// Pool slot table 默认初始容量。0 表示按 `Vec` 默认策略延迟分配。
pub const DEFAULT_POOL_INITIAL_CONN_CAPACITY: usize = 0;

/// 每轮 pump drain CQE 的暂存 `Vec<Completion>` 默认初始容量。
pub const DEFAULT_POOL_COMPLETION_BATCH_CAPACITY: usize = 16;

/// Pool 构造参数。透传 [`ProactorConfig`]，conn 自身参数走
/// [`Pool::connect_blocking`] 时传 [`ConnectionConfig`]。
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    pub proactor: ProactorConfig,
    /// `conns` slot table 初始容量。高 fanout bench 可设为目标连接数，避免
    /// 逐条 connect 时 slot table grow。
    pub initial_conn_capacity: usize,
    /// `pump_impl` drain CQE 暂存区初始容量。高 fanout / burst 场景可增大，
    /// 避免第一轮大 batch grow。
    pub completion_batch_capacity: usize,
    /// Busy-spin data pumps stop after first progress by default. This optional
    /// budget keeps draining briefly after progress to coalesce nearby CQEs in
    /// one pump call without delaying the first emitted event.
    pub post_progress_spin_iters: usize,
}

impl PoolConfig {
    #[must_use]
    pub const fn new(proactor: ProactorConfig) -> Self {
        Self {
            proactor,
            initial_conn_capacity: DEFAULT_POOL_INITIAL_CONN_CAPACITY,
            completion_batch_capacity: DEFAULT_POOL_COMPLETION_BATCH_CAPACITY,
            post_progress_spin_iters: 0,
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
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ConnHandle(u32);

impl ConnHandle {
    #[inline]
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

/// Multi-conn driver。单线程持有 [`Proactor`] + N 条 `ConnectionState`。
///
/// **Slot table 路由**（P2）：`conns: Vec<Option<ConnectionState>>`，conn_id
/// 既是 routing token 也是 slot index —— CQE 拿到 user_data 解出 conn_id 后
/// `conns.get(conn_id as usize)` 是 O(1)，取代了早期 `iter().find(|c| c.conn_id() ...)`
/// 的 O(N)。N 上去之后这条 hot path 决定整体吞吐。
pub struct Pool {
    proactor: Proactor,
    /// slot table：conn_id 直接索引。None 是关闭/失败留下的 tombstone（暂不复用）。
    conns: Vec<Option<ConnectionState>>,
    /// 活 conn 数。每次 push Some / 写 None 时同步维护，避免 hot path filter scan。
    active_count: u32,
    next_conn_id: u32,
    next_bgid: u16,
    /// pump_impl 内 drain CQE 暂存区。持久字段避免每轮 alloc（F3 dhat 审计发现
    /// 这是 hot loop 第一大 alloc：每轮 pump 一次 `Vec::with_capacity(16)`）。
    /// 初始 cap 16 已足够单 conn 单轮 ≤ 4 CQE；多 conn 高峰按需 grow 一次后稳定。
    completions_buf: Vec<Completion>,
    post_progress_spin_iters: usize,
    /// `Pool: !Send + !Sync` 显式标记。raw pointer phantom 不实际持有。
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
            .finish()
    }
}

impl Pool {
    pub fn new(cfg: PoolConfig) -> Result<Self, ProactorError> {
        let proactor = Proactor::new(cfg.proactor)?;
        Ok(Self {
            proactor,
            conns: Vec::with_capacity(cfg.initial_conn_capacity),
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
    /// `cfg.proactor` 字段在此忽略——proactor 由 Pool 持有。`cfg.conn_id` /
    /// `cfg.bgid` 也会被 Pool 覆盖为内部分配的值。
    pub fn connect_blocking(
        &mut self,
        cfg: ConnectionConfig,
    ) -> Result<ConnHandle, ConnectionError> {
        let addr = resolve_addr(&cfg)?;
        self.connect_blocking_to(cfg, addr)
    }

    /// 同 `connect_blocking`，但跳过 DNS。
    pub fn connect_blocking_to(
        &mut self,
        cfg: ConnectionConfig,
        addr: SocketAddr,
    ) -> Result<ConnHandle, ConnectionError> {
        let handle = self.submit_connect_to(cfg, addr)?;
        let conn_id = handle.0;
        match self.drive_conn_until_open(conn_id) {
            Ok(()) => Ok(handle),
            Err(e) => {
                self.drop_slot(conn_id);
                Err(e)
            }
        }
    }

    /// **非阻塞** connect：仅提交 connect SQE 并 reserve 一个 slot，立刻返回
    /// [`ConnHandle`]。后续靠 caller `pump()` 推进 handshake，直到 `state(h) ==
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
    pub fn submit_connect_to(
        &mut self,
        mut cfg: ConnectionConfig,
        addr: SocketAddr,
    ) -> Result<ConnHandle, ConnectionError> {
        // 预先 reserve 一个 conn_id / bgid，超额时直接 Err（早期 .expect() 会
        // 在长跑 reconnect 后 panic 整个 HFT 进程）。conn_id 上限是 28-bit
        // mask 而非 u32::MAX。
        let conn_id = self.next_conn_id;
        if conn_id > CONN_ID_MASK as u32 {
            return Err(ConnectionError::IdSpaceExhausted("conn_id"));
        }
        let bgid = self.next_bgid;
        let Some(next_bgid) = self.next_bgid.checked_add(1) else {
            return Err(ConnectionError::IdSpaceExhausted("bgid"));
        };
        cfg.conn_id = conn_id;
        cfg.bgid = bgid;

        let mut conn = ConnectionState::new(cfg, addr)?;
        // 没 push 之前失败：socket / addr 由 conn drop 清理；id 不回退（单调）。
        conn.submit_connect(&mut self.proactor)?;
        // conn_id == conns.len() 不变式：slot table 直接 push 到末尾保证
        // conns[conn_id] = Some(conn)，O(1) 查找。
        debug_assert_eq!(self.conns.len(), conn_id as usize);
        self.conns.push(Some(conn));
        self.active_count += 1;
        // 计数器单调推进。bgid 溢出必须在提交 SQE 前检查，避免留下
        // "已提交 connect 但 Pool 不再持有 conn" 的半完成状态。
        self.next_conn_id = conn_id + 1;
        self.next_bgid = next_bgid;
        Ok(ConnHandle(conn_id))
    }

    /// 把 slot 置 None 并 unregister 其 buf_ring。`active_count` 同步 -1。
    /// 重复 drop 同一 slot 是 no-op（idempotent）。
    fn drop_slot(&mut self, conn_id: u32) {
        let Some(slot) = self.conns.get_mut(conn_id as usize) else {
            return;
        };
        if let Some(mut dead) = slot.take() {
            if let Some(mut ring) = dead.buf_ring.take() {
                let _ = ring.unregister(&mut self.proactor);
            }
            self.active_count = self.active_count.saturating_sub(1);
        }
    }

    /// pump 单 conn 直到它进 Open（或失败）。其它 conn 的 CQE 也会顺道被路由
    /// 推进（pump_impl 内部对所有 conn 一视同仁）。
    fn drive_conn_until_open(&mut self, conn_id: u32) -> Result<(), ConnectionError> {
        loop {
            self.pump_impl(1, |_, _| { /* discard pre-open events */ })?;

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

        for slot in conns.iter_mut() {
            let Some(conn) = slot.as_mut() else { continue };
            if let Err(e) = conn.try_submit_send(proactor) {
                fail_conn(conn, e, &mut first_err);
                continue;
            }
            if let Err(e) = conn.try_rearm_multishot(proactor) {
                fail_conn(conn, e, &mut first_err);
            }
        }

        proactor.submit()?;
        proactor.wait_for_cqe(wait_nr)?;

        completions_buf.clear();
        proactor.drain_completions(|c| completions_buf.push(c));
        dispatch_conn_completions_data(conns, proactor, completions_buf, &mut sink, &mut first_err);

        sync_active_count(conns, active_count);
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

        for slot in conns.iter_mut() {
            let Some(conn) = slot.as_mut() else { continue };
            if let Err(e) = conn.try_submit_send(proactor) {
                fail_conn(conn, e, &mut first_err);
                continue;
            }
            if let Err(e) = conn.try_rearm_multishot(proactor) {
                fail_conn(conn, e, &mut first_err);
            }
        }

        proactor.submit()?;
        proactor.wait_for_cqe(wait_nr)?;

        completions_buf.clear();
        proactor.drain_completions(|c| completions_buf.push(c));
        for &c in completions_buf.iter() {
            let conn_id =
                u32::try_from(c.user_data.token() & CONN_ID_MASK).expect("28-bit mask fits u32");
            if let Some(conn) = conns.get_mut(conn_id as usize).and_then(Option::as_mut) {
                let handle = ConnHandle(conn.conn_id());
                match conn.handle_completion_data_marked(proactor, c, |ev| sink(handle, ev)) {
                    Ok(_) => {
                        conn.sync_ws_open_state();
                        conn.sync_ws_close_state();
                    }
                    Err(e) => fail_conn(conn, e, &mut first_err),
                }
            }
        }

        sync_active_count(conns, active_count);
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

        submit_conn_ops(conns, proactor, &mut first_err);
        proactor.submit()?;

        for iter in 0..=spin_iters {
            let cqes = drain_conn_completions_data(
                conns,
                proactor,
                completions_buf,
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

        sync_active_count(conns, active_count);
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

        submit_conn_ops(conns, proactor, &mut first_err);
        proactor.submit()?;

        for iter in 0..=spin_iters {
            let cqes = drain_conn_completions_data_marked(
                conns,
                proactor,
                completions_buf,
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

        sync_active_count(conns, active_count);
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
        for slot in conns.iter_mut() {
            let Some(conn) = slot.as_mut() else { continue };
            if let Err(e) = conn.try_submit_send(proactor) {
                fail_conn(conn, e, &mut first_err);
                continue;
            }
            if let Err(e) = conn.try_rearm_multishot(proactor) {
                fail_conn(conn, e, &mut first_err);
            }
        }

        // submit pending send / rearm SQE, then wait only when requested.
        // wait_for_cqe(0) 是纯 noop，wait_nr ≥ 1 才阻塞。失败 fatal ——
        // io_uring 状态损坏没法 per-conn 隔离。
        proactor.submit()?;
        proactor.wait_for_cqe(wait_nr)?;

        // drain 所有 ready CQE 到持久 buf，避免 drain callback 重入 proactor +
        // 每轮 alloc。F3 dhat 审计：原 `Vec::with_capacity(16)` 每轮 alloc 256 B
        // × ~3/s = hot loop 第一大 alloc 点；移字段后 0 alloc。
        completions_buf.clear();
        proactor.drain_completions(|c| completions_buf.push(c));
        for &c in completions_buf.iter() {
            let conn_id =
                u32::try_from(c.user_data.token() & CONN_ID_MASK).expect("28-bit mask fits u32");
            // Slot-table O(1) lookup（早期 iter().find 是 O(N)）。
            // stale CQE（已 close 的 conn 残留）落到 None 分支 → 忽略
            if let Some(conn) = conns.get_mut(conn_id as usize).and_then(Option::as_mut)
                && let Err(e) = conn.handle_completion(proactor, c)
            {
                fail_conn(conn, e, &mut first_err);
            }
        }

        // 各 conn drain ws_events —— sink 出错的 event 也聚合而非 abort
        for slot in conns.iter_mut() {
            let Some(conn) = slot.as_mut() else { continue };
            let handle = ConnHandle(conn.conn_id());
            while let Some(res) = conn.ws.poll_event() {
                match res {
                    Ok(ev) => sink(handle, ev),
                    Err(e) => {
                        fail_conn(conn, ConnectionError::Ws(e), &mut first_err);
                        break;
                    }
                }
            }
            conn.sync_ws_open_state();
            conn.sync_ws_close_state();
            conn.clear_ws_ingress_dirty();
        }

        sync_active_count(conns, active_count);
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

        submit_conn_ops(conns, proactor, &mut first_err);
        proactor.submit()?;

        for iter in 0..=spin_iters {
            let cqes = drain_conn_completions(conns, proactor, completions_buf, &mut first_err);
            progressed |= cqes > 0;

            for slot in conns.iter_mut() {
                let Some(conn) = slot.as_mut() else { continue };
                let handle = ConnHandle(conn.conn_id());
                while let Some(res) = conn.ws.poll_event() {
                    progressed = true;
                    match res {
                        Ok(ev) => sink(handle, ev),
                        Err(e) => {
                            fail_conn(conn, ConnectionError::Ws(e), &mut first_err);
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

        sync_active_count(conns, active_count);
        match first_err {
            Some(e) => Err(e),
            None => Ok(progressed),
        }
    }

    pub fn state(&self, h: ConnHandle) -> Option<State> {
        self.conns
            .get(h.0 as usize)
            .and_then(Option::as_ref)
            .map(ConnectionState::state)
    }

    /// 当前 active conn 数（不含 tombstone slot）。
    #[must_use]
    pub fn conn_count(&self) -> usize {
        self.active_count as usize
    }

    /// Returns opt-in ingress diagnostics for a live connection.
    #[must_use]
    pub fn ingress_stats(&self, h: ConnHandle) -> Option<IngressStats> {
        self.conns
            .get(h.0 as usize)
            .and_then(Option::as_ref)
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
        self.conns
            .get_mut(h.0 as usize)
            .and_then(Option::as_mut)
            .ok_or(ConnectionError::InvalidState(State::Closed))
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
    first_err: &mut Option<ConnectionError>,
) {
    for slot in conns.iter_mut() {
        let Some(conn) = slot.as_mut() else { continue };
        if let Err(e) = conn.try_submit_send(proactor) {
            fail_conn(conn, e, first_err);
            continue;
        }
        if let Err(e) = conn.try_rearm_multishot(proactor) {
            fail_conn(conn, e, first_err);
        }
    }
}

#[inline]
fn completion_conn_id(c: Completion) -> u32 {
    u32::try_from(c.user_data.token() & CONN_ID_MASK).expect("28-bit mask fits u32")
}

fn drain_conn_completions(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &mut Vec<Completion>,
    first_err: &mut Option<ConnectionError>,
) -> usize {
    completions_buf.clear();
    let count = proactor.drain_completions(|c| completions_buf.push(c));
    for &c in completions_buf.iter() {
        let conn_id =
            u32::try_from(c.user_data.token() & CONN_ID_MASK).expect("28-bit mask fits u32");
        if let Some(conn) = conns.get_mut(conn_id as usize).and_then(Option::as_mut)
            && let Err(e) = conn.handle_completion(proactor, c)
        {
            fail_conn(conn, e, first_err);
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
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) -> usize
where
    F: for<'a> FnMut(ConnHandle, WsDataEvent<'a>),
{
    completions_buf.clear();
    let count = proactor.drain_completions(|c| completions_buf.push(c));
    dispatch_conn_completions_data(conns, proactor, completions_buf, sink, first_err);
    count
}

fn dispatch_conn_completions_data<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &[Completion],
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) where
    F: for<'a> FnMut(ConnHandle, WsDataEvent<'a>),
{
    let mut i = 0_usize;
    while i < completions_buf.len() {
        let c = completions_buf[i];
        let conn_id = completion_conn_id(c);
        let Some(conn) = conns.get_mut(conn_id as usize).and_then(Option::as_mut) else {
            i += 1;
            continue;
        };

        let handle = ConnHandle(conn.conn_id());
        if conn.can_handle_plain_recv_data_batch(c) {
            let mut end = i + 1;
            while end < completions_buf.len() {
                let next = completions_buf[end];
                if completion_conn_id(next) != conn_id
                    || !conn.can_handle_plain_recv_data_batch(next)
                {
                    break;
                }
                end += 1;
            }

            if end > i + 1 {
                match conn.handle_plain_recv_data_batch(&completions_buf[i..end], &mut |ev| {
                    sink(handle, ev);
                }) {
                    Ok(_) => {
                        conn.sync_ws_open_state();
                        conn.sync_ws_close_state();
                    }
                    Err(e) => fail_conn(conn, e, first_err),
                }
                i = end;
                continue;
            }
        }

        match conn.handle_completion_data(proactor, c, |ev| sink(handle, ev)) {
            Ok(_) => {
                conn.sync_ws_open_state();
                conn.sync_ws_close_state();
            }
            Err(e) => fail_conn(conn, e, first_err),
        }
        i += 1;
    }
}

fn drain_conn_completions_data_marked<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &mut Vec<Completion>,
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) -> usize
where
    F: for<'a> FnMut(ConnHandle, WsMarkedDataEvent<'a>),
{
    completions_buf.clear();
    let count = proactor.drain_completions(|c| completions_buf.push(c));
    dispatch_conn_completions_data_marked(conns, proactor, completions_buf, sink, first_err);
    count
}

fn dispatch_conn_completions_data_marked<F>(
    conns: &mut [Option<ConnectionState>],
    proactor: &mut Proactor,
    completions_buf: &[Completion],
    sink: &mut F,
    first_err: &mut Option<ConnectionError>,
) where
    F: for<'a> FnMut(ConnHandle, WsMarkedDataEvent<'a>),
{
    let mut i = 0_usize;
    while i < completions_buf.len() {
        let c = completions_buf[i];
        let conn_id = completion_conn_id(c);
        let Some(conn) = conns.get_mut(conn_id as usize).and_then(Option::as_mut) else {
            i += 1;
            continue;
        };

        let handle = ConnHandle(conn.conn_id());
        if conn.can_handle_plain_recv_data_batch(c) {
            let mut end = i + 1;
            while end < completions_buf.len() {
                let next = completions_buf[end];
                if completion_conn_id(next) != conn_id
                    || !conn.can_handle_plain_recv_data_batch(next)
                {
                    break;
                }
                end += 1;
            }

            if end > i + 1 {
                match conn.handle_plain_recv_data_batch_marked(
                    &completions_buf[i..end],
                    &mut |ev| {
                        sink(handle, ev);
                    },
                ) {
                    Ok(_) => {
                        conn.sync_ws_open_state();
                        conn.sync_ws_close_state();
                    }
                    Err(e) => fail_conn(conn, e, first_err),
                }
                i = end;
                continue;
            }
        }

        match conn.handle_completion_data_marked(proactor, c, |ev| sink(handle, ev)) {
            Ok(_) => {
                conn.sync_ws_open_state();
                conn.sync_ws_close_state();
            }
            Err(e) => fail_conn(conn, e, first_err),
        }
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
        "# HELP talaris_ingress_recv_bytes_total Ciphertext bytes carried by recv data CQEs."
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
        "# HELP talaris_ingress_plaintext_chunks_total Plaintext chunks fed into the WebSocket parser."
    )?;
    writeln!(out, "# TYPE talaris_ingress_plaintext_chunks_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_plaintext_bytes_total Plaintext bytes fed into the WebSocket parser."
    )?;
    writeln!(out, "# TYPE talaris_ingress_plaintext_bytes_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_ws_data_drains_total Data-pump CQEs that fed plaintext into WebSocket receive processing."
    )?;
    writeln!(out, "# TYPE talaris_ingress_ws_data_drains_total counter")?;
    writeln!(
        out,
        "# HELP talaris_ingress_ws_data_drain_skips_total Data-pump CQEs that skipped WebSocket draining because no plaintext arrived."
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

/// pump 内 per-conn 错误处理：保留第一条错误，把对应 conn 推到 Closed 以便
/// 下一轮 submit/rearm short-circuit；这条 conn 在 kernel 端可能仍有 in-flight
/// op，不强制 cancel（Drop / 显式 close 时清理）。
fn fail_conn(
    conn: &mut ConnectionState,
    err: ConnectionError,
    first_err: &mut Option<ConnectionError>,
) {
    tracing::warn!(conn_id = conn.conn_id(), error = %err, "pool conn failed");
    let ws_close_in_progress = matches!(conn.ws.state(), WsConnState::Closing);
    conn.state = if ws_close_in_progress {
        State::Closing
    } else {
        State::Closed
    };
    if first_err.is_none() {
        *first_err = Some(err);
    }
}

fn sync_active_count(conns: &[Option<ConnectionState>], active_count: &mut u32) {
    let count = conns
        .iter()
        .filter(|slot| {
            slot.as_ref()
                .is_some_and(|conn| !matches!(conn.state(), State::Closed))
        })
        .count();
    *active_count = u32::try_from(count).unwrap_or(u32::MAX);
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
    use crate::connection::{ConnectionConfig, State};
    use crate::test_helpers::run_echo_server;
    use crate::ws::DataEvent as WsDataEvent;
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
    use std::sync::mpsc;
    use std::thread;

    /// 单 conn 走 Pool 路径（从 connection.rs 搬过来——Migration Step 3 后
    /// `Connection` thin wrapper 删除，单 conn 流程同样走 `Pool::connect_blocking`）。
    #[test]
    fn pool_single_conn_plain_ws_echo_roundtrip() {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let local_addr = listener.local_addr().unwrap();

        let (_shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
        let server = thread::spawn(move || run_echo_server(listener, shutdown_rx));

        let cfg = ConnectionConfig::new("localhost", local_addr.port(), "/echo").with_tls(false);
        let mut pool = Pool::new(PoolConfig::new(cfg.proactor)).expect("pool");
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
        let mut pool = Pool::new(PoolConfig::new(cfg.proactor)).expect("pool");
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
}
