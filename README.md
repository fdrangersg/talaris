# talaris

> Predictable & low-latency, zero-jitter HFT data-pipeline transport for Linux.
> 给 HFT 行情 / 下单链路用的可预测低延迟 WebSocket / TCP / TLS 字节泵。

[![crates.io](https://img.shields.io/crates/v/talaris.svg)](https://crates.io/crates/talaris)
[![docs.rs](https://docs.rs/talaris/badge.svg)](https://docs.rs/talaris)
[![license](https://img.shields.io/badge/license-GPL--3.0--or--later-blue.svg)](LICENSE)

> 名字 `talaria`（Hermes 的飞翼凉鞋）在 crates.io 已经被占了，所以本 crate 用拉丁文单数
> `talaris`。`cargo add talaris`、`use talaris::*;`。

---

## TL;DR

talaris 是**为一类很狭窄的 workload 量身做的 io_uring WebSocket client**：
HFT 行情订阅 / 下单链路，单线程吃满 N 条 TCP/TLS WebSocket，要的是 p99.9 尾延迟
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

这是 1973 年风格的设计 —— 一个线程，一个 hot loop，用 io_uring 让 kernel 直接把数据
DMA 到你预留的 buffer 里，你只负责取出来用。

### 2. Proactor vs Reactor（io_uring 不只是个更快的 epoll）

| | Reactor (epoll / tokio) | Proactor (io_uring / talaris) |
|---|---|---|
| 通知粒度 | "fd 可读了" | "数据已经在你的 buffer 里" |
| 谁干活 | **应用** 调 `read()` syscall 把数据从 kernel 拷到用户 buffer | **kernel** 直接写进你预先注册的 provided buffer，user 端不 syscall |
| 主循环 | epoll_wait → 遍历 ready fd → 每个 read() | submit & wait → drain CQE → 数据已就位 |

talaris 用的是 multishot recv：**一次 submit**，kernel 持续往你 buffer ring 里
塞数据 + 每次塞完 post 一个 CQE 告诉你"buffer 哪一格、有多少字节"。整个 inbound
hot path **零 syscall**（kernel 异步推 + SQ_POLL kthread spin）。

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
[user]   ─ ConnectionState 拿到 buffer ring entry slice, 喂给 WsClient (或 zero-copy 直解)
[user]   ─ WsClient 解 frame header, 累积 payload, emit Event::Binary(&[u8])
[user]   ─ 你的 sink 回调拿到 &[u8] payload, 同步处理
[user]   ─ buf_ring.recycle(bid) 把那一格还给 kernel
```

整条链路 **user 端零 syscall**（除了进 `wait_for_cqe` 那一次 io_uring_enter）。
跟 tokio 的 read syscall + epoll_wait 比，主要省的是 syscall + scheduler 介入。

### 5. fast path vs general path —— 一定要知道的取舍

我们有**两个**收数据的 API：

```rust
// General path — 完整 RFC 6455 状态机
pool.pump(|handle, event| match event {
    WsEvent::Text(s) => ...,
    WsEvent::Binary(buf) => ...,
    WsEvent::Ping(_) => ...,    // 自动 Pong 已经回了
    WsEvent::Close { code, reason } => ...,
    ...
})?;

// Fast path — 只认 FIN + Binary + unmasked, 其它都判 protocol error 关连接
pool.pump_binary(|handle, payload: &[u8]| {
    // payload 是 kernel buffer 上的 slice, 这次 callback 内有效
})?;
```

`pump_binary` 跑得快**得多**（实测 200B payload ~1.33× tokio），代价是：
- ❌ 不支持 fragmented message
- ❌ 不支持 Ping / Pong / Close frame（自动关连接当 error）
- ❌ 不能 auto-pong

订阅 venue 行情（venue 端只发 FIN-Binary 帧）→ 用 `pump_binary`。
要做完整 WS 协议（auth / Ping 保活 / graceful close）→ 用 `pump`。

实际生产典型用法：handshake 阶段用 `pump`（让 venue 那边发 HTTP 101 + 跑 WS upgrade 状态机），
进入 `Open` 状态后**整个数据循环切到 `pump_binary`**。

---

## 一句话术语表（cheat sheet）

| 术语 | 一句话解释 | 在代码里 |
|---|---|---|
| **Proactor** | io_uring 包了一层的薄壳，提供 submit_recv / submit_send / submit_connect / drain CQE | `src/proactor/uring.rs::Proactor` |
| **Pool** | 一个 Proactor 驱动 N 条 conn 的 multi-conn driver；单 OS 线程持有 | `src/pool.rs::Pool` |
| **ConnHandle** | 对外的不透明 conn 引用；本质是个 u32 conn_id | `src/pool.rs::ConnHandle` |
| **ConnectionConfig** | 单条 conn 的配置：host/port/tls/SQ_POLL/buf_ring | `src/connection.rs::ConnectionConfig` |
| **BufferRing** | io_uring provided buffer ring：256 格 × 4 KiB 的预注册 buffer 池，kernel 自己挑格子写 | `src/proactor/buf_ring.rs` |
| **WsClient** | RFC 6455 client 全状态机（handshake / fragmentation / control / auto-pong） | `src/ws/client.rs::WsClient` |
| **SQ_POLL** | io_uring kernel 端起一条 kthread 在 isolated CPU 上 spin，submit 路径**零 syscall** | `ConnectionConfig::with_sq_poll` |
| **pin** | 把当前线程钉死在一个 CPU 上，配合 `isolcpus` 用，砍 scheduler 迁移抖动 | `talaris::proactor::pin_current_thread_to` |
| **pump** | 推进一次 IO：submit + wait + drain CQE + 回调；通用路径 | `Pool::pump` |
| **pump_binary** | 同上但跳过 WsClient 状态机，只认 Binary frame；inbound stream 用 | `Pool::pump_binary` |

---

## 30 秒上手

### Cargo.toml

```toml
[dependencies]
talaris = "0.1"
```

### Hello world: 连一个 plain WS server 收一帧

```rust
use talaris::connection::{ConnectionConfig, State};
use talaris::ws::Event as WsEvent;
use talaris::{Pool, PoolConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 构造配置
    let cfg = ConnectionConfig::new("echo.websocket.events", 443, "/")
        .with_tls(true);

    // 2. 起 Pool, 阻塞 connect 直到 WS handshake 完成
    let mut pool = Pool::new(PoolConfig::new(cfg.proactor))?;
    let handle = pool.connect_blocking(cfg)?;
    assert_eq!(pool.state(handle), Some(State::Open));

    // 3. 主动发一条
    pool.send_text(handle, br#"{"ping":"talaris"}"#)?;

    // 4. 循环 pump 等收消息
    loop {
        pool.pump(|h, ev| {
            if let WsEvent::Text(s) = ev {
                println!("{h:?}: {s}");
            }
        })?;
    }
}
```

### 生产配置: pin + SQ_POLL + fast-path

```rust
use talaris::connection::{ConnectionConfig, State};
use talaris::proactor::pin_current_thread_to;
use talaris::{Pool, PoolConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 0. 进程父 affinity 必须覆盖目标 CPU。先在 shell 套 taskset：
    //    taskset -c 0-7 cargo run --release ...
    //    (运维层另外做 isolcpus=1-5 把 CPU 1-5 从普通 scheduler 摘出来)

    // 1. 把当前 OS 线程钉到 isolated CPU 1
    pin_current_thread_to(1)?;

    // 2. 配置一条订阅 conn
    //    - SQ_POLL kthread 钉到 CPU 5（CPU 1 的 SMT sibling，L1/L2 共享）
    //    - buf_ring 单格 8 KiB（payload ~400B → 8KiB 一格装 ~20 帧）
    let cfg = ConnectionConfig::new("test.deribit.com", 443, "/ws/api/v2")
        .with_tls(true)
        .with_sq_poll(10_000, Some(5))
        .with_buf_ring(8 * 1024, 256);

    // 3. 起 Pool, handshake
    let mut pool = Pool::new(PoolConfig::new(cfg.proactor))?;
    let handle = pool.connect_blocking(cfg)?;

    // 4. (生产里通常这里发 subscribe 消息, 用 pool.send_text)

    // 5. 进入 fast-path 数据循环
    loop {
        pool.pump_binary(|_h, payload| {
            // payload 是 kernel buf_ring entry 上的 slice, 这次 callback 内有效
            // 这里做你的解码 / 路由 / 策略
            decode_market_data(payload);
        })?;
    }
}
# fn decode_market_data(_: &[u8]) {}
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
pool.initiate_close(h, 1000, "bye")?;  // 主动关连接

// 推进 IO 一次 (这是 hot loop)
pool.pump(|h, ev| { ... })?;          // 通用路径
pool.pump_binary(|h, payload| { ... })?;  // 高吞吐 inbound 用

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

### `with_sq_poll(idle_ms, cpu)` —— 砍 submit syscall

**只在持续高频 IO 下回本**：kthread 在 isolated CPU 上 spin 持续 spin，user 端 submit
就不进 syscall。代价是 idle 期间也烧那一个 CPU。

- 持续行情 push（venue feed）：开。
- 单次 RPC / 偶发 IO：**别开**，反而慢（kthread 协调开销 > 省下的 syscall 成本）。

详细见 `benches/proactor_overhead.rs` 注释里的数据 —— SQ_POLL 在 Nop bench 里
实际是负优化，这是设计本性，不是 bug。

### `pin_current_thread_to(cpu)` —— 砍尾抖动

`isolcpus=N-M` 把 CPU 从普通 scheduler 摘出来 + 钉线程到那个 CPU = 主要砍 p99 / max
抖动。实测 (`proactor_overhead`)：
- vanilla（不 pin）: max 36 µs
- pinned: max 8 µs

**对 p50 几乎没贡献，对 max 是 4-5×**。HFT 要看的是 max，不是 mean。

### CPU 拓扑建议（8 vCPU 机器为例，`isolcpus=1-5`）

```
CPU 0          ← OS noise (IRQ / kthread / cron)
CPU 1   (iso)  ← talaris user thread (pin here)
CPU 5   (iso)  ← talaris SQ_POLL kthread (CPU 1 的 SMT sibling, L1/L2 共享)
CPU 2,3,4 (iso)← 备用 / 第二条 Pool
CPU 6, 7       ← OS noise
```

SMT sibling pair 把 user thread 和 SQ_POLL kthread 钉到同一物理核的两条 SMT 上，
buffer ring / SQ ring 的 cache coherency 几乎免费。

---

## 心智模型对比：talaris vs tokio

| | tokio + tokio-tungstenite | talaris |
|---|---|---|
| **抽象层** | Future / Stream / async fn / executor / waker | 同步函数调用 + pump loop |
| **IO 模型** | epoll / kqueue (Reactor) | io_uring multishot recv (Proactor) |
| **线程模型** | 默认 multi-thread runtime + work stealing | 单线程持 Pool, 跨线程要多开几个 Pool |
| **每帧 cost** | epoll_wait + read syscall + waker poll + Stream::poll_next | drain_completions + parse_header + sink |
| **schedule jitter** | executor 调度 + work stealing 漂移 | 完全没有 (没 executor) |
| **依赖** | tokio (~20+ transitive) + tokio-tungstenite + futures + ... | ring + rustls + io_uring crate 4 个核心 dep |
| **何时选 tokio** | web server / 通用 microservice / mixed IO | |
| **何时选 talaris** | | HFT 数据流 / latency-sensitive subscribe loop |

### 什么时候**不要**用 talaris

- **macOS / Windows 部署**：talaris **Linux only**（io_uring 是 Linux 独有）
- **kernel < 6.0**：multishot recv + buffer ring 要 5.19+，建议 6.x。低版本退回 epoll
- **业务里 IO 不是热点**：你的 hot path 是策略计算 / DB / 跨进程通信而不是 WS 收发，talaris 给你的 ~50ns/frame 优化淹没在其它开销里
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

### 3. `pump_binary` 看到任何非 Binary 帧就关连接

包括 Ping。venue 如果开了 keep-alive Ping，你 fast-path 会被立刻判 protocol error。
解决：要么 venue 端关 Ping，要么混用 pump（定期 pump 几次让 Ping 走通用路径），
要么自己接管 Ping（接 raw TCP 自己解，更激进）。

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
with_sq_poll(10000, Some(5));  // kthread 钉到 5
```

少了 `taskset` → `pin_current_thread_to(1)` 会 fail（CPU 1 不在进程 affinity 里）。
少了 `isolcpus` → CPU 1 上有其它任务抢，pin 失去意义。

### 6. buf_ring 太小会 ENOBUFS

burst 期 N 个 buffer 还没来得及 recycle，下一帧 kernel 找不到空格 → 整条 multishot
停 → Pool 下一轮 pump 才 re-arm。表现：burst 头几帧延迟跳一下。
解决：调大 `entries` 或 `buf_size`，让 `entries × buf_size` ≥ 你 burst 期峰值字节数。

---

## What's in the box

- **io_uring proactor** — F1/F2/F3 features: SQ_POLL, pin-to-core, multishot
  `recv` over a registered `BufferRing`, `IO_LINK` chains, owned-fd `close`.
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
codebase), but every io_uring entry point panics with `unimplemented!()`
outside Linux. CI / production builds must target Linux.

Tested on Linux 6.x with io_uring features: `SETUP_SQPOLL`,
`REGISTER_PBUF_RING`, `OP_RECV_MULTISHOT`, `IOSQE_IO_LINK`.

---

## Benchmark suite

`benches/` 下面是分层 baseline，每层只比下一层多一个组件，方便归因延迟来源：

| bench | 测什么 |
|---|---|
| `ws_framing` | 纯 CPU 帧编解码（mask / encode_header / parse_header / compute_accept） |
| `ws_ingress_raw` | 绕开 Pool + WsClient, 直接 Proactor + BufferRing 收 Binary 帧 |
| `ws_ingress_single` | Pool.pump vs Pool.pump_binary vs tokio (单 conn, 稳态满速) |
| `ws_ingress_fanout` | N ∈ {1,4,16,64} 条 conn 同时收 (talaris Pool 路由 vs tokio N task) |

跑法：
```bash
taskset -c 0-7 cargo bench --bench ws_ingress_single -- \
    --seconds 5 --payload 400 --buf-size 8192
```

实测数据见 `src/connection.rs::ConnectionConfig::with_buf_ring` 的 doc。

---

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
