# talaris

> Predictable-latency, low-jitter HFT transport toolkit for Linux.
> 给 HFT 行情 / 下单链路用的可预测低延迟 WebSocket / TCP / TLS low-level toolkit。

[![crates.io](https://img.shields.io/crates/v/talaris.svg)](https://crates.io/crates/talaris)
[![docs.rs](https://docs.rs/talaris/badge.svg)](https://docs.rs/talaris)
[![license](https://img.shields.io/badge/license-GPL--3.0--or--later-blue.svg)](LICENSE)

> 名字 `talaria`（Hermes 的飞翼凉鞋）在 crates.io 已经被占了，所以本 crate 用拉丁文单数
> `talaris`。`Pool` 是推荐入口；`ws` / `proactor` / `http` / `tls` 模块也会作为
> low-level HFT toolkit 暴露给需要自己拼 transport / framing 的用户。

---

## TL;DR

```plain
当前实盘环境实际链路：
socket
  -> BufferRing slot               // TLS ciphertext
  -> rustls.read_tls()
  -> rustls.process_new_packets()
  -> rustls.reader().fill_buf()    // borrowed TLS plaintext chunk
  -> WsClient.recv_buf
  -> complete Text/Binary frame    // borrowed payload -> sink
     or fragmented fallback        // copy into msg_buf for reassembly
```

talaris 是**为一类很狭窄的 workload 量身做的 io_uring WebSocket / TCP toolkit**：
HFT 行情订阅，单线程吃满 N 条 TCP/TLS WebSocket，要的是 p99.9 尾延迟
可预测，不是通用 async runtime。

如果你只是写 Web app / 微服务，**用 tokio 别想这个**。如果你的 workload 满足下面三条
里至少两条，再考虑 talaris：

- 单进程驱动 ≥1 条 WebSocket，**收行情是主要负载**（订阅类 / 高频 inbound）
- 你愿意 / 已经做了 `isolcpus` + 核绑定 + 关 NOHZ 这些运维操作
- p99.9 / max 抖动是产品要求，不是"nice to have"

---

## 心智模型：你需要先理解的 5 件事

### 1. talaris 不是 runtime，是一根"行情吸管"

```
                  ┌─────────────────────────┐
   wire ──TCP──▶  │  Pool (单 OS 线程)        │  ──回调──▶  你的策略 / 解码 / 路由
                  │  ├─ 1 个 io_uring        │
                  │  ├─ N 条 WS conn         │
                  │  └─ 单线程 hot loop      │
                  └─────────────────────────┘
```

跟 tokio 的最大区别：**没有 executor，没有 future，没有任务调度**。整个 Pool 就是一个
死循环：`while running { pool.pump(...) }`。你的代码在 `pump` 的回调里同步跑。

这是 1973 年风格的设计 —— 一个线程，一个 hot loop，用 io_uring 让 kernel 把数据
copy 到你预留的 buffer 里，你只负责取出来用。

### 2. Proactor vs Reactor（io_uring 不只是个更快的 epoll）

|      | Reactor (epoll / tokio)                           | Proactor (io_uring / talaris)                         |
|------|---------------------------------------------------|-------------------------------------------------------|
| 通知粒度 | "fd 可读了"                                          | "数据已经在你的 buffer 里"                                    |
| 谁干活  | **应用** 调 `read()` syscall 把数据从 kernel 拷到用户 buffer | **kernel** 直接写进你预先注册的 provided buffer，user 端不 syscall |
| 主循环  | epoll_wait → 遍历 ready fd → 每个 read()              | submit & wait → drain CQE → 数据已就位                     |

talaris 用的是 multishot recv：**一次 submit**，kernel 持续往你 buffer ring 里
塞数据 + 每次塞完 post 一个 CQE 告诉你"buffer 哪一格、有多少字节"。SQ_POLL 下
submit 路径通常不进 syscall；默认阻塞 `pump` 仍会用一次 `wait_for_cqe(1)` 进入
`io_uring_enter(GETEVENTS)` 等 CQE。要把 steady-state receive loop 做到不等 CQE
syscall，用 busy-poll 版本 `pump_spin` / `pump_data_spin`，代价是持续占用一个 CPU。

详见 `src/proactor.rs` 顶部的注释 —— 它解释了为什么我们叫 `Proactor` 而不是
跟 tokio-uring 那样还叫 Reactor。

### 3. Pool 是单线程的、Send/Sync 都不实现

```rust
let mut pool = Pool::new(PoolConfig::default())?;
// pool: !Send, !Sync
```

这是**故意**的。io_uring 的 SQ/CQ 共享内存只能由一个 OS 线程访问，跨线程要么用锁
（破坏低延迟语义）要么塞一份 lock-free SPSC queue（破坏简洁性）。我们直接不让你
跨线程：每条独立的链路开一个 OS 线程 + 一个 Pool。

需要多 venue 多线程并发？开多个 OS 线程，每个线程自己 `Pool::new`，互不影响。

### 4. 一帧 WebSocket Binary 帧的完整生命周期

按时间顺序：

```
[wire]   ─ TCP segment 到达 NIC
[kernel] ─ NIC IRQ → kernel TCP stack 处理
[kernel] ─ 数据 copy 到 io_uring provided buffer ring 的某一格 (bid=N)
[kernel] ─ 生成 CQE: { user_data: conn_id, result: bytes_written, flags: bid|F_MORE }
[user]   ─ pool.pump() 在 wait_for_cqe 那一行被唤醒
[user]   ─ Pool 从 CQE 解出 conn_id, 路由到对应 ConnectionState
[user]   ─ ConnectionState 拿到 buffer ring entry slice, 喂给 rustls 解密
[user]   ─ rustls plaintext chunk 借给 WsClient, copy 到 recv buffer
[user]   ─ buf_ring.recycle(bid) 把密文 buffer 那一格还给 kernel
[user]   ─ 完整单帧直接借用 recv buffer payload；fragmented message 才 copy 到 msg_buf
[user]   ─ 你的 sink 回调拿到 &[u8] payload, 同步处理
```

阻塞 `pump` 路径仍有一次 wait syscall；busy-poll `pump_data_spin` 路径则只轮询
mmap 出来的 CQ ring，不进 `wait_for_cqe`。跟 tokio 的 read syscall + epoll_wait 比，
主要省的是 per-frame read syscall + scheduler 介入。

### 5. general events vs data-only dispatch

我们有**两个**收数据的 API：

```rust
// General events — 完整 RFC 6455 状态机，control/data 都交给业务
pool.pump(|handle, event| match event {
    WsEvent::Text(s) => ...,
    WsEvent::Binary(buf) => ...,
    WsEvent::Ping(_) => ...,    // 默认 auto_pong=true 时 Pong 已排队
    WsEvent::Close { code, reason } => ...,
    ...
})?;

// Data-only dispatch — WS 层仍处理 Ping/Pong/Close，业务只拿 Text/Binary
pool.pump_data(|handle, data| match data {
    WsDataEvent::Text(s) => parse_json(s),
    WsDataEvent::Binary(buf) => parse_sbe(buf),
})?;
```

`pump_data` 不是 binary-only fast mode。它走同一套 `WsClient` 状态机，所以：
- Text JSON feed 可以直接解析 JSON。
- Binary SBE / protobuf feed 可以直接解析二进制 payload。
- WebSocket Ping/Pong/Close、fragmentation、UTF-8 校验和 auto-pong 仍然正常工作。

要自己观察 Ping/Pong/Close 事件时用 `pump`；行情主循环只关心业务 payload 时用
`pump_data`。

---

## 一句话术语表（cheat sheet）

| 术语                   | 一句话解释                                                                      | 在代码里                                       |
|----------------------|----------------------------------------------------------------------------|--------------------------------------------|
| **Proactor**         | io_uring 包了一层的薄壳，提供 submit_recv / submit_send / submit_connect / drain CQE | `src/proactor/uring.rs::Proactor`          |
| **Pool**             | 一个 Proactor 驱动 N 条 conn 的 multi-conn driver；单 OS 线程持有                      | `src/pool.rs::Pool`                        |
| **ConnHandle**       | 对外的不透明 conn 引用；本质是个 u32 conn_id                                            | `src/pool.rs::ConnHandle`                  |
| **ConnectionConfig** | 单条 conn 的配置：host/port/tls/SQ_POLL/buf_ring                                 | `src/connection.rs::ConnectionConfig`      |
| **BufferRing**       | io_uring provided buffer ring：256 格 × 4 KiB 的预注册 buffer 池，kernel 自己挑格子写    | `src/proactor/buf_ring.rs`                 |
| **WsClient**         | RFC 6455 client 全状态机（handshake / fragmentation / control / auto-pong）      | `src/ws/client.rs::WsClient`               |
| **SQ_POLL**          | io_uring kernel 端起一条 kthread 在 isolated CPU 上 spin，submit 路径**零 syscall**  | `ConnectionConfig::with_sq_poll`           |
| **pin**              | 把当前线程钉死在一个 CPU 上，配合 `isolcpus` 用，砍 scheduler 迁移抖动                          | `talaris::proactor::pin_current_thread_to` |
| **pump**             | 推进一次 IO：submit + wait + drain CQE + 回调；通用路径                                | `Pool::pump`                               |
| **pump_data**        | 同上但只把 Text/Binary data 交给业务；control frame 仍由 WS 层处理                         | `Pool::pump_data`                          |

---

## 30 秒上手

### Cargo.toml

```toml
[dependencies]
talaris = "0.1"
```

### 可运行 quickstart: 本地 plain-WS echo

`examples/quickstart.rs` 在同进程里起一个最小 plain-WS echo server，不依赖外部
公网服务，适合作为发布包里的 smoke test：

```bash
cargo run --example quickstart

# 延迟调优时建议显式给进程父 affinity，并把 user thread / SQ_POLL kthread 分开：
taskset -c 0-7 cargo run --release --example quickstart -- \
    --user-cpu 1 --sq-poll-cpu 5
```

### 生产配置: pin + SQ_POLL + data-only dispatch

```rust
use talaris::connection::{ConnectionConfig, State};
use talaris::proactor::pin_current_thread_to;
use talaris::ws::DataEvent as WsDataEvent;
use talaris::{Pool, PoolConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 0. 进程父 affinity 必须覆盖目标 CPU。先在 shell 套 taskset：
    //    taskset -c 0-7 cargo run --release ...
    //    (运维层另外做 isolcpus=1-5 把 CPU 1-5 从普通 scheduler 摘出来)

    // 1. 把当前 OS 线程钉到 isolated CPU 1
    pin_current_thread_to(1)?;

    // 2. 配置一条订阅 conn
    //    - SQ_POLL kthread 钉到 CPU 3（先试独立 physical core；再与 SMT sibling A/B）
    //    - buf_ring 单格 8 KiB（payload ~400B → 8KiB 一格装 ~20 帧）
    let cfg = ConnectionConfig::new("test.deribit.com", 443, "/ws/api/v2")
        .with_tls(true)
        .with_sq_poll(10_000, Some(3))
        .with_buf_ring(8 * 1024, 256);

    // 3. 起 Pool, handshake
    let mut pool = Pool::new(PoolConfig::new(cfg.proactor))?;
    let handle = pool.connect_blocking(cfg)?;

    // 4. (生产里通常这里发 subscribe 消息, 用 pool.send_text)

    // 5. 进入 data-only 数据循环。WS 层仍处理 Ping/Pong/Close；
    //    业务层只拿 JSON Text 或 SBE Binary payload。
    loop {
        pool.pump_data(|_h, data| match data {
            WsDataEvent::Text(s) => decode_json_market_data(s),
            WsDataEvent::Binary(payload) => decode_sbe_market_data(payload),
        })?;
    }
}
# fn decode_json_market_data(_: &str) {}
# fn decode_sbe_market_data(_: &[u8]) {}
```

更完整的可运行例子见 [`examples/quickstart.rs`](examples/quickstart.rs)。

---

## API surface（最常用的 5 个方法）

```rust
// 起 Pool
let mut pool = Pool::new(PoolConfig::new(cfg.proactor))?;

// 阻塞 connect（包了 TCP connect + TLS handshake + WS upgrade）
let h: ConnHandle = pool.connect_blocking(cfg)?;
// pool.connect_blocking_to(cfg, addr)   // 同上但跳过 DNS

// 主动发
pool.send_text(h, b"...")?;       // RFC 6455 Text frame
pool.send_binary(h, b"...")?;     // RFC 6455 Binary frame
pool.send_ping(h, b"hb")?;        // RFC 6455 Ping control frame
pool.send_pong(h, b"hb")?;        // RFC 6455 Pong control frame
pool.initiate_close(h, 1000, "bye")?;  // 主动关连接

// 推进 IO 一次 (这是 hot loop)
pool.pump(|h, ev| { ... })?;          // 通用路径
pool.pump_data(|h, data| { ... })?;   // 只分发 Text/Binary data
let got = pool.pump_data_spin(256, |h, data| { ... })?; // busy-poll, 不等 CQE syscall

// 查状态
pool.state(h);  // Option<State>: Init / Connecting / TlsHandshake / WsHandshake / Open / Closing / Closed
pool.conn_count();  // 当前 active conn 数
```

---

## 调优参数（按 ROI 排）

### `with_buf_ring(buf_size, entries)` —— 决定吞吐上限

经验法则：**buf_size ≈ 20 × 你最常见的 payload 大小**。

| 典型 payload | 推荐 buf_size |
|---|---|
| trades / quotes 100-300 B | 4 KiB（默认） |
| L2 book delta 300-800 B | 8 KiB |
| 价目快照 / orderbook full 1-4 KiB | 32 KiB |
| 大 snapshot 4-16 KiB | 64 KiB+ |

`entries` 默认 256，整池字节 = `entries × buf_size`。够撑你 burst 期的瞬时
buffer 占用就行；太小 multishot recv 会撞 `-ENOBUFS` 自动停（Pool 下一轮 pump 会
re-arm，但 burst 头几帧延迟会受影响）。

### `with_cq_entries(entries)` —— 给 multishot burst 留 CQ 空间

`ProactorConfig::sq_entries` 控制 SQ 容量；`with_cq_entries` 单独覆盖
`IORING_SETUP_CQSIZE`。默认 CQ 通常是 SQ 的 2 倍，但行情 burst 下一个
`recv_multishot` SQE 会连续产很多 CQE，CQ 压力和 SQ 压力不是一个量级。

经验起点：

```text
cq_entries >= max(2 * sq_entries, buf_ring_entries)
```

更激进的行情 fanout / burst 场景可以从 `2x ~ 4x buf_ring_entries` 做 A/B。
`cq_entries` 必须大于 `sq_entries`，并保持 2 的幂。

### `with_sq_poll(idle_ms, cpu)` —— 砍 submit syscall

**只在高频 submit 下回本**：kthread 在 isolated CPU 上持续轮询，user 端 submit
就不进 syscall。代价是 idle 期间也会持续占用那个 CPU。

- 高频 send / order channel / 频繁 rearm：优先测试。
- 纯行情订阅：`connect -> subscribe -> recv_multishot` 后提交 SQE 很少，收益通常有限。
- 单次 RPC / 偶发 IO：**别开**，反而慢（kthread 协调开销 > 省下的 syscall 成本）。

SQ_POLL 在低频 / 偶发提交下可能是负优化：kthread 协调和 idle spin 会压过省掉的
submit syscall。上线前应在目标机器用 `ws_ingress_raw` / `ws_ingress_tls` 对
SQ_POLL 开关、CPU 拓扑和 workload 做 A/B。

### `with_proactor_setup_flags(flags)` —— 高级 taskrun 控制

可选暴露 `IORING_SETUP_COOP_TASKRUN`、`IORING_SETUP_TASKRUN_FLAG`、
`IORING_SETUP_SINGLE_ISSUER`、`IORING_SETUP_DEFER_TASKRUN`。默认全部关闭。

推荐只按明确假设打开：

```rust
use talaris::proactor::ProactorSetupFlags;

let flags = ProactorSetupFlags::SINGLE_ISSUER
    | ProactorSetupFlags::DEFER_TASKRUN
    | ProactorSetupFlags::TASKRUN_FLAG;
let cfg = cfg.with_proactor_setup_flags(flags);
```

`DEFER_TASKRUN` 要求 `SINGLE_ISSUER`，并要求同一提交线程周期性进入 kernel 拉
completion；长时间只做 userspace spin/drain 的 loop 不应无脑开启。

### `with_ingress_stats(true)` —— 临时量化 recv CQE

默认关闭。调 buf ring 时可临时开启，并通过 `pool.ingress_stats(h)` 读取
`recv_data_cqes`、`recv_bytes`、`recv_ring_exhaustions`、`ws_data_drains` 和
`ws_data_drain_skips`。生产连接保持关闭，避免在 hot path 上更新计数器。

### `pin_current_thread_to(cpu)` —— 砍尾抖动

`isolcpus=N-M` 把 CPU 从普通 scheduler 摘出来 + 钉线程到那个 CPU，主要目标是减少
scheduler migration 和普通 OS noise 对 p99 / max 的影响。它通常不改变 p50，
收益取决于目标机器的 CPU 拓扑、IRQ 绑定和隔离质量；HFT 要看的是 tail，不是 mean。

### CPU 拓扑建议（8 vCPU 机器为例，`isolcpus=1-5`）

```
CPU 0          ← OS noise (IRQ / kthread / cron)
CPU 1   (iso)  ← talaris user thread (pin here)
CPU 3   (iso)  ← talaris SQ_POLL kthread (先试独立 physical core)
CPU 5   (iso)  ← CPU 1 的 SMT sibling；作为 SQ_POLL A/B 候选
CPU 2,4 (iso)  ← 备用 / 第二条 Pool
CPU 6, 7       ← OS noise
```

SQ_POLL 和 user thread 放在独立 physical core 通常是更稳妥的起点。SMT sibling pair
可能缩短 cacheline 传递距离，但两条线程也会共享执行资源；最终必须在目标机器上
对独立 physical core、SMT sibling 和关闭 SQ_POLL 三种拓扑做 A/B。

---

## 心智模型对比：talaris vs tokio

| | tokio + tokio-tungstenite | talaris |
|---|---|---|
| **抽象层** | Future / Stream / async fn / executor / waker | 同步函数调用 + pump loop |
| **IO 模型** | epoll / kqueue (Reactor) | io_uring multishot recv (Proactor) |
| **线程模型** | 默认 multi-thread runtime + work stealing | 单线程持 Pool, 跨线程要多开几个 Pool |
| **每帧 cost** | epoll_wait + read syscall + waker poll + Stream::poll_next | drain_completions + parse_header + sink |
| **schedule jitter** | executor 调度 + work stealing 漂移 | 无 executor 调度；仍受 OS / IRQ / CPU 拓扑影响 |
| **依赖** | tokio (~20+ transitive) + tokio-tungstenite + futures + ... | rustls / ring / io-uring + 小型 codec deps；无 async runtime |
| **何时选 tokio** | web server / 通用 microservice / mixed IO | |
| **何时选 talaris** | | HFT 数据流 / latency-sensitive subscribe loop |

### 什么时候**不要**用 talaris

- **macOS / Windows 部署**：talaris **Linux only**（io_uring 是 Linux 独有）
- **kernel < 6.0**：multishot recv + buffer ring 要 5.19+，建议 6.x。低版本退回 epoll
- **业务里 IO 不是热点**：你的 hot path 是策略计算 / DB / 跨进程通信而不是 WS 收发，framing / transport 优化会被其它开销淹没
- **不想做 CPU 隔离运维**：不 isolcpus 不 pin，talaris 大部分优势消失
- **WS server**：talaris 是 client-only，没 listener 实现

---

## 常见坑

### 1. 同一线程必须独占一个 Pool

```rust
let pool1 = Pool::new(...)?;
let pool2 = Pool::new(...)?;
// 同一线程持两个 Pool 也行但意义不大 (一个 Pool 就能驱动 N conn)
// 跨线程share 一个 Pool？编译就过不了 —— Pool: !Send
```

### 2. `pump` 是阻塞的（除非用 `pump_nowait`）

`pool.pump(...)` 内部走 `wait_for_cqe(1)`，**至少等到 1 个 CQE 才返回**。如果你不希望
阻塞（譬如要在同一 loop 里做别的事），用 `pool.pump_nowait(...)`。

如果你愿意在 isolated CPU 上 busy-spin，`pool.pump_spin(spin_iters, ...)` /
`pool.pump_data_spin(spin_iters, ...)` 会只轮询 CQ ring，不调用 `wait_for_cqe(1)`。
返回的 `bool` 表示这一轮是否处理到了 CQE / frame；返回 `false` 时可以继续 spin，
或降级到阻塞 `pump`。

### 3. 业务只想要行情 payload 时用 `pump_data`

交易所 WebSocket 通常会混合 Text JSON、Binary SBE 和 Ping/Pong/Close control
frame。`pump_data` 会完整处理 control frame，只把 Text/Binary data 交给业务；
如果你需要记录 Pong 延迟或 Close reason，改用 `pump`。

### 4. SQ_POLL 在低负载下反而慢

```rust
// 错误用法: 偶尔的 RPC 也开 SQ_POLL
let cfg = ConnectionConfig::new(...)
    .with_sq_poll(10_000, Some(5));  // ← 不持续推数据的话别开
let mut pool = Pool::new(...)?;
pool.connect_blocking(cfg)?;
let response = single_request_response(...);  // ← 等响应那段 kthread 干瞪眼烧 CPU
```

### 5. taskset / isolcpus / pin 三件套必须一致

```bash
# 运维层
isolcpus=1-5 nohz_full=1-5 rcu_nocbs=1-5  # kernel cmdline

# 启动时
taskset -c 0-7 ./your-binary   # 进程父 affinity 必须覆盖 1-5

# 代码里
pin_current_thread_to(1);  // 钉到 1
with_sq_poll(10000, Some(3));  // kthread 钉到独立 physical core 候选
```

少了 `taskset` → `pin_current_thread_to(1)` 会 fail（CPU 1 不在进程 affinity 里）。
少了 `isolcpus` → CPU 1 上有其它任务抢，pin 失去意义。

### 6. buf_ring 太小会 ENOBUFS

burst 期 N 个 buffer 还没来得及 recycle，下一帧 kernel 找不到空格 → 整条 multishot
停 → Pool 下一轮 pump 才 re-arm。表现：burst 头几帧延迟跳一下。
解决：调大 `entries` 或 `buf_size`，让 `entries × buf_size` ≥ 你 burst 期峰值字节数。

---

## What's in the box

- **io_uring proactor** — configurable SQ/CQ sizing, SQ_POLL, taskrun setup
  flags, pin-to-core, multishot `recv` over a registered `BufferRing`,
  `IO_LINK` chains, owned-fd `close`.
- **WebSocket client** (RFC 6455) — frame codec, masking (AVX2 + 8-byte
  chunked scalar fallback), streaming parser, fragment reassembly, close
  handshake, auto-pong, CSPRNG mask keys (RFC §10.3 compliant).
- **TLS** — `rustls` 0.23 driven by raw bytes (no `tokio` / no `async-std`),
  ALPN `http/1.1` requested **and verified**, `close_notify` surfaced to the
  caller.
- **HTTP/1.1 codec** — minimal, sized for WS Upgrade. Header size cap (16 KiB)
  / count cap (64) / explicit `Transfer-Encoding` reject for DoS hardening.
- **Pool** — single io_uring drives N WebSocket connections. CQE routing is
  O(1) slot-table lookup. `submit_connect` returns a handle immediately so
  N connections can hand-shake concurrently.

## Platform

Linux only at runtime. The crate compiles cleanly on macOS / Windows (a stub
`proactor` keeps types in scope so non-Linux IDEs can type-check the full
codebase), but the hot path is not implemented there: affinity helpers return
`UnsupportedPlatform` and io_uring operations are stubbed. CI / production
builds must target Linux.

Tested on Linux 6.x with io_uring features: `SETUP_CQSIZE`, `SETUP_SQPOLL`,
`SETUP_COOP_TASKRUN`, `SETUP_SINGLE_ISSUER`, `SETUP_DEFER_TASKRUN`,
`REGISTER_PBUF_RING`, `OP_RECV_MULTISHOT`, `IOSQE_IO_LINK`.

---

## Benchmark suite

`benches/` 下面是分层 baseline，每层只比下一层多一个组件，方便归因延迟来源：

| bench | 测什么 |
|---|---|
| `ws_framing` | 纯 CPU 帧编解码（mask / encode_header / parse_header / compute_accept） |
| `ws_ingress_raw` | 绕开 Pool + WsClient, 直接 Proactor + BufferRing 收 Binary 帧 |
| `ws_ingress_single` | Pool.pump vs Pool.pump_data vs tokio (单 conn, 稳态满速) |
| `ws_ingress_fanout` | N ∈ {1,4,16,64} 条 conn 同时收 (talaris Pool 路由 vs tokio N task) |
| `ws_ingress_tls` | loopback WSS：talaris、fair tokio rustls + 同一 WsClient、tokio bare 下界、rustls unbuffered probe、软件 kTLS probe |
| `binance_live` | Binance Spot 公共 WSS 实盘 TLS ingress：多 symbol 高频 Text JSON 行情 |
| `binance_futures_live` | Binance USD-M perpetual 实盘行情：BBO / L2 depth / public trade 分频道报告样本量、CPU/frame 和 tail lag |

跑法：
```bash
taskset -c 0-7 cargo bench --bench ws_ingress_single -- \
    --seconds 5 --payload 400 --buf-size 8192

taskset -c 0-7 cargo bench --bench ws_ingress_tls -- \
    --frames 50000000 --payload 256 --sample-every 0 \
    --server-cpu 4 --talaris-cpu 1 --sq-poll-cpu 3 --tokio-cpu 2 \
    --spin-iters 256 --buf-size 4096 --buf-entries 256 \
    --sq-entries 256 --cq-entries 1024 --taskrun default

taskset -c 0-7 cargo bench --bench ws_ingress_tls -- \
    --frames 10000000 --payload 256 --sample-every 0 \
    --server-cpu 4 --talaris-cpu 1 --sq-poll-cpu 3 --tokio-cpu 2 \
    --spin-iters 256 --buf-size 4096 --buf-entries 256 \
    --sq-entries 256 --cq-entries 1024 --taskrun defer-flag \
    --ingress-stats true

taskset -c 0-7 cargo bench --bench binance_live -- \
    --seconds 30 --warmup-seconds 5 --user-cpu 1 --sq-poll-cpu 3

taskset -c 0-7 cargo bench --bench binance_futures_live -- \
    --seconds 60 --warmup-seconds 10 --symbols btcusdt,ethusdt \
    --talaris-pump spin --talaris-cpu 1 --tokio-cpu 2 --sq-poll-cpu 5 \
    --buf-size 8192 --buf-entries 256 --sq-entries 256 --cq-entries 1024
```

`ws_ingress_tls` 的 `--sample-every 0` 用于测吞吐，避免逐帧 `Instant::now()` 污染结果；
测相邻帧 delivery jitter 时改成 `--sample-every 1`。fair tokio 组复用同一个
`WsClient`，用来隔离 IO/TLS 驱动开销；bare 组只是理论下界，不是功能等价实现。

Linux 软件 kTLS 组只用于判断 ceiling，不是生产 transport 路径。kTLS RX 的 control
record 必须经 `recvmsg` ancillary data 处理；完整生产实现还要处理 TLS 1.3 KeyUpdate
和 session ticket。是否值得引入这条复杂路径，必须先在部署机器上跑 probe。

### TLS ingress batching 实验结论

东京测试机 Linux 6.17、256 B payload、`4 KiB × 256` buf ring 的 1000 万帧统计：
普通 multishot recv 已达到约 `3.87 KiB/CQE`，`ENOBUFS` 通常接近 `0`。扩大到
`16 KiB × 256` 后约 `13.1 KiB/CQE`，但 5000 万帧长跑没有稳定吞吐收益，说明 CQE
路由不是当前 TLS hot path 的主要瓶颈。

Linux 6.10+ `IORING_RECVSEND_BUNDLE` 也做过原型验证，但内核会无上限地合并当前
可用 provided buffers。本机观察到平均约 `134-217 slots/CQE`，即
`0.55-0.89 MiB/CQE`，吞吐和交付粒度都变差，因此没有保留生产开关。rustls
unbuffered API 也保留为 benchmark-only probe；当前稳态不优于 buffered 路径。

有界、record-aware staging 也已完成 A/B：最多只缓存一条未完成 TLS record，观测到
高水位 `16,406 B`。但 `4 KiB` CQE 下 `158,748 / 158,753` 条 record 都需要 staging，
等价于复制几乎 `100%` 的密文。即使把 ring 加深到 `1024` entries 消除 `ENOBUFS`，
逐帧 jitter 对照仍从 `10.88 M frame/s, p99=3.09 us` 退化到
`9.93 M frame/s, p99=3.59 us`，因此实现已删除。

保留下来的是更窄的优化：`pump_data` 只在 rustls 确实产出 plaintext 后 drain
WebSocket。一次 `1000 万帧` spin 诊断里，`650,865` 次 CQE 中有 `492,096` 次可跳过
空 drain。此优化减少无效工作，但东京机 `5000 万帧`、统计关闭的长跑仍为
`12.08 M frame/s`，低于 fair tokio 同 `WsClient` 的 `14.84 M frame/s`；当前树不能
宣称吞吐已经超过 tokio。

相关上游语义见
[io-uring 6.10 bundle 说明](https://github.com/axboe/liburing/wiki/What%27s-new-with-io_uring-in-6.10)
和 [rustls unbuffered state machine](https://github.com/rustls/rustls/blob/main/rustls/src/conn/unbuffered.rs)。

实测数据见 `src/connection.rs::ConnectionConfig::with_buf_ring` 的 doc。

---

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
