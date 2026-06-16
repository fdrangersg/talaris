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
塞数据 + 每次塞完 post 一个 CQE 告诉你"buffer 哪一格、有多少字节"。默认阻塞
`pump` 会用一次 `wait_for_cqe(1)` 进入 `io_uring_enter(GETEVENTS)` 等 CQE。
要把 steady-state receive loop 做到不等 CQE syscall，用 busy-poll 版本
`pump_spin` / `pump_data_spin`，代价是持续占用一个 CPU。

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
| **ConnectionConfig** | 单条 conn 的配置：host/port/tls/buf_ring/proactor tuning                         | `src/connection.rs::ConnectionConfig`      |
| **BufferRing**       | io_uring provided buffer ring：256 格 × 4 KiB 的预注册 buffer 池，kernel 自己挑格子写    | `src/proactor/buf_ring.rs`                 |
| **WsClient**         | RFC 6455 client 全状态机（handshake / fragmentation / control / auto-pong）      | `src/ws/client.rs::WsClient`               |
| **pin**              | 把当前线程钉死在一个 CPU 上，配合 `isolcpus` 用，砍 scheduler 迁移抖动                          | `talaris::proactor::pin_current_thread_to` |
| **pump**             | 推进一次 IO：submit + wait + drain CQE + 回调；通用路径                                | `Pool::pump`                               |
| **pump_data**        | 同上但只把 Text/Binary data 交给业务；control frame 仍由 WS 层处理                         | `Pool::pump_data`                          |

---

## 30 秒上手

### Cargo.toml

```toml
[dependencies]
talaris = "0.2"
```

### 可运行 quickstart: 本地 plain-WS echo

`examples/quickstart.rs` 在同进程里起一个最小 plain-WS echo server，不依赖外部
公网服务，适合作为发布包里的 smoke test：

```bash
cargo run --example quickstart

# 延迟调优时建议显式给进程父 affinity，并把 user thread 钉到 isolated CPU：
taskset -c 0-7 cargo run --release --example quickstart -- \
    --user-cpu 1
```

### 生产配置: pin + data-only dispatch

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
    //    - buf_ring 单格 8 KiB（payload ~400B → 8KiB 一格装 ~20 帧）
    let cfg = ConnectionConfig::new("test.deribit.com", 443, "/ws/api/v2")
        .with_tls(true)
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

### Opt-in observability

默认 hot path 不读时钟、不构造 metadata。需要定位瓶颈时显式切到 marked API。
marked API 默认 100% 采样；需要降采样时在连接配置里设置 basis points：

```rust
let cfg = ConnectionConfig::new(host, 443, path)
    .with_observability_sample_rate_bps(1_000); // 10%; 10_000 = 100%
```

```rust
use talaris::ws::MarkedDataEvent;

let got = pool.pump_data_spin_marked(256, |h, data| match data {
    MarkedDataEvent::Text { payload, meta } => {
        if meta.sampled {
            // meta.source_recv_time_nanos: Unix epoch nanos，可写入下游 wire
            // meta.recv_sequence: 本连接 marked recv CQE 序号
            // meta.message_sequence: 本连接 marked data message 序号
            let recv_to_plaintext = meta.recv_to_plaintext_nanos();
            let plaintext_to_ws = meta.plaintext_to_ws_nanos();
            let recv_to_ws = meta.recv_to_ws_nanos();
            let prior_sink = meta.chunk_prior_sink_service_nanos();
            let plaintext_to_ws_net = meta.plaintext_to_ws_excluding_prior_sink_nanos();
            let recv_to_ws_net = meta.recv_to_ws_excluding_prior_sink_nanos();
        }
        parse_json(payload);
    }
    MarkedDataEvent::Binary { payload, meta } => {
        decode_binary(payload, meta);
    }
})?;
```

`source_recv_time_nanos` 是用户态观察到 recv CQE 时采样的 Unix epoch nanos；
跨机器使用它需要 chrony/PTP 等时钟同步。`*_mono_nanos` 只能在本进程内做差，
不要跨机器比较。`recv_sequence` / `message_sequence` 是每条连接 marked data-pump
内部维护的 `u64` 序号；普通未 marked pump 不推进这些序号。`tls_plaintext_chunk_index`
和 `chunk_message_index` 是单个 recv / plaintext chunk 内的 `u16` 索引，极端情况下会
saturate 到 `u16::MAX`。当前采样在 recv CQE 粒度做确定性选择；被采样 CQE 产生的
data message 会带 `sampled = true` 和分段时间戳，未采样事件仍正常分发但时间戳为 0，
delta helper 返回 `None`。

marked pump 是同步 sink 模型：一条 message 交给用户 sink 后，talaris 只有等这个
sink 返回才会继续解析同一 plaintext chunk 内的后续 message。因此
`recv_to_ws_nanos()` / `plaintext_to_ws_nanos()` 对 queued message 真实反映
"被 pump 到上层" 的延迟，但其中可能包含同 chunk 前序 message 的 sink 回调耗时。
`chunk_prior_sink_service_nanos()` 暴露这部分累计耗时；
`*_excluding_prior_sink_nanos()` 则把它扣除，用于观察 talaris parse/dispatch 本身的净
staging cost。

生产模式下可以让 talaris 用 HdrHistogram 直接维护本地 quantile，并导出
Prometheus text exposition。记录仍只发生在 marked pump 路径里：

```rust
let cfg = ConnectionConfig::new(host, 443, path)
    .with_observability_sample_rate_bps(10_000)
    .with_observability_histograms(true);

let h = pool.connect_blocking(cfg)?;

pool.pump_data_spin_marked(256, |h, data| {
    // 正常业务处理；talaris 会在调用 sink 前记录 sampled 事件的 stage latency。
})?;

// 在你的 /metrics HTTP handler 中返回这个 body（连接生命周期累计窗口）。
let body = pool.prometheus_metrics();

// 更适合 dashboard / alert 的 interval 窗口：导出后 reset interval histograms。
let interval_body = pool.prometheus_metrics_and_reset_interval();
```

Prometheus 输出包含 `talaris_ws_latency_quantile_ns`、`samples`、`sum_ns`、
`max_ns`，并用 label 区分 `window="cumulative|interval"`、`scope="chunk|message"`、
`stage` 和 `chunk_position`：

- `scope="chunk", stage="recv_to_plaintext", chunk_position="chunk"`：plaintext
  chunk 产出至少一条 data message 时记录一次。
- `scope="message", stage="plaintext_to_ws|recv_to_ws", chunk_position="all"`：
  所有 sampled data message。
- `scope="message", stage="plaintext_to_ws_excluding_prior_sink|recv_to_ws_excluding_prior_sink"`：
  扣除同 chunk 前序 sink 回调累计耗时后的净 WS parse/dispatch latency。
- `scope="message", stage="chunk_prior_sink_service", chunk_position="queued"`：
  queued message 在同 chunk 内已经承受的前序 sink callback/service backpressure。
- `chunk_position="first"`：plaintext chunk 内第一条 WS data message。
- `chunk_position="queued"`：同一 plaintext chunk 内后续 WS data message，能直接观察
  chunk 内排队和前序 sink 回调带来的影响。

这些 quantile 是每条连接本地 HdrHistogram 的客户端 quantile，不适合跨连接直接聚合。
`prometheus_metrics_and_reset_interval()` 只 reset interval latency histograms；
ingress counters 仍是 lifetime cumulative counters。

长期低成本 counters 也默认关闭，需要按连接开启：

```rust
let cfg = ConnectionConfig::new(host, 443, path)
    .with_ingress_stats(true);

let stats = pool.ingress_stats(h);
```

这会统计 recv CQE、ciphertext bytes、multishot rearm、ring exhaustion、
plaintext chunk/bytes、WS Text/Binary event 等。没有开启 `with_ingress_stats(true)`
时，这些 counter 不更新。

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
CPU 2,3,4 (iso)← 备用 / 第二条 Pool / tokio 对照组
CPU 5   (iso)  ← CPU 1 的 SMT sibling；谨慎用于同一条 hot loop 的对照实验
CPU 6, 7       ← OS noise
```

优先把 talaris user thread 放在独立 physical core。SMT sibling 会共享执行资源；
如果要在同一物理核上跑策略解析或对照 worker，必须用真实行情 burst 做 A/B。

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

### 4. `pump_data_spin` 只在愿意烧 isolated CPU 时用

对行情订阅客户端来说，steady-state 的 submit 很少，真正影响尾部的是 CQE 到达后
user thread 多快看到它。`pump_data_spin` 只轮询 CQ ring，不进入
`io_uring_enter(GETEVENTS)` 等待 completion，适合一条 isolated CPU 专门喂策略的
部署形态。低负载或同机 CPU 紧张时用阻塞 `pump_data`。

### 5. taskset / isolcpus / pin 三件套必须一致

```bash
# 运维层
isolcpus=1-5 nohz_full=1-5 rcu_nocbs=1-5  # kernel cmdline

# 启动时
taskset -c 0-7 ./your-binary   # 进程父 affinity 必须覆盖 1-5

# 代码里
pin_current_thread_to(1);  // 钉到 1
```

少了 `taskset` → `pin_current_thread_to(1)` 会 fail（CPU 1 不在进程 affinity 里）。
少了 `isolcpus` → CPU 1 上有其它任务抢，pin 失去意义。

### 6. buf_ring 太小会 ENOBUFS

burst 期 N 个 buffer 还没来得及 recycle，下一帧 kernel 找不到空格 → 整条 multishot
停 → Pool 下一轮 pump 才 re-arm。表现：burst 头几帧延迟跳一下。
解决：调大 `entries` 或 `buf_size`，让 `entries × buf_size` ≥ 你 burst 期峰值字节数。

---

## What's in the box

- **io_uring proactor** — configurable SQ/CQ sizing, taskrun setup
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

Tested on Linux 6.x with io_uring features: `SETUP_CQSIZE`, `SETUP_COOP_TASKRUN`,
`SETUP_SINGLE_ISSUER`, `SETUP_DEFER_TASKRUN`, `REGISTER_PBUF_RING`,
`OP_RECV_MULTISHOT`, `IOSQE_IO_LINK`.

---

## Benchmark suite

`benches/` 现在保留 Linux-only pipeline、tuning 和 strict-compare benches。它们不使用 Criterion
sampling：talaris 的 hot path 是长生命周期 io_uring `recv_multishot`、PBUF
recycle、TLS/WS staging 和 CQE drain，不适合把一次 `pump_data` 包进短采样
iteration。非 Linux 只构建并打印 `skipped`，用于保持本地
`cargo check --benches` 可用。

测试环境：

- Host: `ripple-testnet-tokyo`
- Kernel: Linux `6.17.0-1012-aws`
- Rust: `rustc 1.95.0`
- CPU: bench 进程 `taskset -c 0-2`；local bench pin user thread 到 CPU 1、
  server thread 到 CPU 2。

| bench | 测什么 |
|---|---|
| `local_pipeline` | loopback plain WS，真实 `Pool + io_uring + PBUF + WS pump`。用于比较 unmarked、marked、采样和 HdrHistogram 记录的 hot-path 成本 |
| `local_tuning` | loopback plain WS talaris 参数矩阵：扫 `payload × frames-per-write × buf_size × buf_entries × completion_batch × spin_iters`，输出 CSV 和 top variants |
| `local_compare` | loopback plain WS strict A/B：同一个 stream server、payload、frames-per-write、sink checksum 和 CPU pinning，比较 talaris baseline 与 tungstenite |
| `live_pipeline` | live TLS WebSocket，使用生产 `Pool::pump_data_spin_marked` 和当前 observability / Prometheus 导出口径 |
| `live_compare` | live Binance USD-M BBO strict A/B：talaris 与 tungstenite 同时订阅相同 combined streams，记录同类 socket/read-to-message 延迟 |
| `local_redundancy` | loopback BBO redundant-connection race simulation：同一 seq stream 多连接输入，评估去重前 duplicate 放大成本 |
| `live_redundancy` | live Binance BBO 多冗余连接观测：记录 fastest-copy / duplicate / stale 分类和 duplicate lag |

跑法示例：
```bash
taskset -c 0-2 cargo bench --bench local_pipeline -- \
    --mode hist_100pct \
    --seconds 30 \
    --payload 256 \
    --frames-per-write 16 \
    --buf-size 4096 \
    --buf-entries 256 \
    --spin-iters 256 \
    --metrics-interval-ms 1000 \
    --prom-out /tmp/talaris-local.prom \
    --user-cpu 1 --server-cpu 2

taskset -c 0-2 cargo bench --bench local_compare -- \
    --transport both \
    --seconds 8 \
    --payload 256 \
    --frames-per-write 16 \
    --warmup-messages 100000 \
    --user-cpu 1 --server-cpu 2

taskset -c 0-2 cargo bench --bench local_tuning -- \
    --seconds 1 \
    --payloads 64,256,1024 \
    --frames-per-write 1,4,16,32 \
    --buf-sizes 1024,2048,4096,8192,16384,32768 \
    --buf-entries 256,512 \
    --completion-batches 64,256 \
    --spin-iters 256,1024 \
    --warmup-messages 200000 \
    --csv /tmp/talaris-tuning.csv \
    --user-cpu 1 --server-cpu 2

taskset -c 0-2 cargo bench --bench live_pipeline -- \
    --seconds 60 \
    --host fstream.binance.com \
    --port 443 \
    --path /ws/btcusdt@bookTicker \
    --sample-bps 10000 \
    --metrics-interval-ms 1000 \
    --prom-out /tmp/talaris-live.prom \
    --user-cpu 1

taskset -c 0-3 cargo bench --bench live_compare -- \
    --transport both \
    --stream-counts 4 \
    --redundancy-counts 1,2,4,8,16,32 \
    --seconds 30 \
    --symbols btcusdt,ethusdt,bnbusdt,solusdt \
    --sample-bps 10000 \
    --buf-size 1024 \
    --buf-entries 512 \
    --completion-batch 64 \
    --spin-iters 256 \
    --talaris-cpu 1 \
    --tungstenite-cpu 2
```

### 2026-06-15 live Binance BBO conclusions

以下结果来自 `ripple-testnet-tokyo`，commit `7e6fc78`。测试目标是验证
Ripple 当前 workload：Binance USD-M Perpetual BBO，TLS WebSocket inbound，
业务追求可预测、极低的 message pump latency，并接受 busy-spin 独占 CPU。

测试口径：

- `live_compare` 同时启动 talaris 与 tungstenite，订阅相同 Binance combined
  streams，避免顺序 A/B 被市场活跃度变化污染。
- talaris：单线程、单 `Pool`、单 io_uring、`pump_data_spin_marked(256)`、
  pin CPU 1。
- tungstenite：blocking read；冗余矩阵里每条冗余连接一个 reader thread，
  全部 pin CPU 2 后聚合。
- talaris latency 使用 `recv_to_ws`，并拆 `recv_to_plaintext` /
  `plaintext_to_ws`；tungstenite 使用底层 socket read return 到
  `Message` 返回，记为 `socket_read_to_ws`。
- 这里的 recv/read timestamp 都是用户态观察点，不是 NIC hardware timestamp。

#### 2/3/4 路 combined-stream A/B

日志：
`/tmp/talaris-benches/78d55a2-live-compare-20260615T124646Z/live_compare_2_3_4.log`

单位：microseconds。

| streams | client | msg/s | p50 | p99 | p999 | queued p99 |
|---:|---|---:|---:|---:|---:|---:|
| 2 | talaris | 968.017 | 1.049 | 13.351 | 137.471 | 16.991 |
| 2 | tungstenite | 967.368 | 5.919 | 24.511 | 35.775 | 26.735 |
| 3 | talaris | 993.683 | 1.024 | 5.959 | 11.575 | 7.471 |
| 3 | tungstenite | 997.360 | 6.195 | 18.111 | 28.255 | 19.983 |
| 4 | talaris | 703.900 | 0.980 | 5.059 | 15.583 | 6.531 |
| 4 | tungstenite | 703.923 | 6.039 | 14.855 | 22.031 | 16.831 |

结论：

- 在 2/3/4 路 live BBO 下，talaris 的 p50 / p99 明显低于 tungstenite。
- 2 路 talaris 有一次 `recv_to_plaintext` 128 us 级 p999 outlier，导致总
  p999 高于 tungstenite；这不是 WS parser 本身的常态表现。
- 3/4 路下，talaris p50 / p99 / p999 均优于 tungstenite。
- 对当前 Ripple 低延迟行情入口目标，talaris 是更合适的生产方案；前提是
  给它独占 CPU，并接受 busy-spin 的 CPU 成本。

#### 4-stream BBO redundancy amplification

日志：

- `/tmp/talaris-benches/7e6fc78-redundancy-boundary-20260615T125939Z/live_compare_stream4_red_1_32.log`
- `/tmp/talaris-benches/7e6fc78-redundancy-boundary-20260615T125939Z/live_compare_stream4_red_24_40.log`

单位：microseconds。冗余数表示同一 4-stream combined BBO 建 N 条相同
WebSocket 连接；talaris 用一个 Pool 单线程处理 N 条连接，tungstenite 用 N
个 blocking reader thread 后聚合。

| redundancy | talaris msg/s | tung msg/s | talaris p50 | tung p50 | talaris p99 | tung p99 | talaris p999 | tung p999 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,104 | 1,105 | 1.004 | 6.287 | 7.655 | 17.951 | 31.855 | 24.927 |
| 2 | 4,436 | 4,435 | 1.008 | 4.503 | 11.791 | 29.199 | 21.295 | 49.503 |
| 4 | 6,174 | 6,173 | 0.809 | 2.187 | 11.159 | 24.415 | 19.455 | 58.335 |
| 8 | 14,323 | 14,323 | 0.921 | 2.171 | 10.207 | 25.759 | 20.431 | 84.031 |
| 16 | 17,935 | 17,933 | 0.815 | 1.870 | 13.807 | 20.703 | 28.959 | 121.343 |
| 24 | 38,708 | 38,696 | 1.000 | 2.167 | 18.975 | 17.743 | 40.639 | 51.551 |
| 32 | 30,814 | 31,229 | 0.681 | 1.886 | 21.199 | 18.495 | 55.487 | 66.751 |
| 40 | 44,288 | 44,363 | 1.071 | 2.611 | 30.191 | 19.151 | 84.543 | 45.407 |

分界判断：

- `p50`：到 40 路为止，talaris 仍明显更低。
- `p99`：放大分界点在 `16 -> 24` 之间。16 路 talaris 仍领先；24 路开始
  被 tungstenite 追上 / 反超。
- `p999`：分界点在 `32 -> 40` 之间。32 路 talaris 仍略好；40 路 tail
  明显恶化。
- 吞吐：到 40 路没有出现明显吞吐断崖，两边 msg/s 基本一致；32/40 路
  tungstenite 略高，但差距很小。
- 18/20/22 路补点在建连阶段被 Binance peer reset / connection reset，
  未采样成功；继续强打会被服务端连接保护机制污染。

talaris 高冗余恶化来源可以通过 staging 解释。40 路时：

| stage | p50 | p99 | p999 |
|---|---:|---:|---:|
| `recv_to_plaintext` | 0.535 | 14.991 | 47.455 |
| `plaintext_to_ws` | 0.434 | 22.223 | 67.967 |
| `recv_to_ws` | 1.071 | 30.191 | 84.543 |

这说明 40 路的 tail 放大不是单纯 TLS 解密问题；同一 reactor 上多连接、
同 chunk queued message parsing/dispatch 也开始成为 tail 来源。

生产建议：

- 默认冗余度建议先取 4 路。4 路下 talaris p50/p99/p999 为
  `0.809 / 11.159 / 19.455 us`，明显优于 tungstenite，且距离 tail 放大区
  有足够余量。
- 8 路可作为高波动或特殊行情增强档；8 路仍保持明显 p99/p999 优势。
- 不建议默认上 16+。16 路 bench 仍有优势，但已经接近 p99 放大区；超过
  16 路必须按目标机器、目标 feed 和当前参数重新跑 `live_compare`。
- 当前证据支持：在 Ripple 这类 BBO/行情入口场景下，若目标是可预测极低延迟
  且可接受独占 CPU，talaris 是比 tungstenite 更合适的 IO 模型。

#### Feed placement / tuning principle

talaris 的低延迟优势来自单 reactor 在热路径上 inline 完成 CQE drain、TLS
decrypt、WS parse 和 dispatch；对应风险是不同 feed 混跑时会产生
head-of-line blocking。因此，benchmark 和生产配置都应该按 feed class 隔离，
而不是把不同交易所、不同消息形态混在同一个 `Pool` / io_uring 中调一个平均值。

推荐原则：

- 一个 `Pool` / io_uring 对应一个 latency class / feed class。
- 同一个 feed class 内可以包含多个 symbol 和冗余连接；例如 Binance USD-M
  Perpetual BBO 多 symbol、4 路冗余可以放在同一个 `Pool` 中联合调参。
- 不同 `message size`、消息频率、burst pattern、冗余路数、parser 成本或
  latency SLO 的 feed 应该分到不同 `Pool`，分别 benchmark。
- 每个 feed class 单独寻找最优 `buf_size`、`buf_entries`、
  `completion_batch`、`spin_iters`、冗余路数、CPU pinning 和采样率。
- 引入新交易所或新 feed 时，先归类为 BBO / trade / depth delta /
  snapshot / large JSON 等 workload，再跑专项 bench；不要直接复用其它
  feed class 的最优参数。

这条原则用于指导本 crate 在上层项目中按交易所和 feed 类型做针对性基准测试：
先定义 feed class，再为该 class 建立参数矩阵和 latency envelope，最后把最优
参数固化到对应生产配置。

`local_pipeline --mode` 当前支持：

- `baseline`：unmarked `pump_data_spin`，不构造 metadata。
- `marked_0_nohist`：marked pump，采样率 0%，不写 histograms，用于观察
  metadata 分发成本。
- `marked_100_nohist`：marked pump，采样率 100%，不写 histograms，用于观察
  timestamp 成本。
- `hist_1pct` / `hist_10pct` / `hist_100pct`：marked pump + HdrHistogram，
  分别用 1%、10%、100% 采样率记录 observability histograms。

`--prom-out PATH` 会写 Prometheus text exposition snapshots。每个 interval
snapshot 调用 `Pool::prometheus_metrics_and_reset_interval()`；最后额外写一次
final interval 和 cumulative snapshot。主要 histogram family：

- `talaris_ws_latency_quantile_ns`
- `talaris_ws_latency_samples`
- `talaris_ws_latency_sum_ns`
- `talaris_ws_latency_max_ns`

关键 label：

- `stage="recv_to_plaintext" | "plaintext_to_ws" | "recv_to_ws"`
- `chunk_position="chunk" | "all" | "first" | "queued"`
- `window="interval" | "cumulative"`

`recv_to_plaintext` 是 chunk-level；`plaintext_to_ws` 和 `recv_to_ws` 是
message-level，并按同一 plaintext chunk 内第一条 message 与后续 queued
message 拆分。这里的 recv 时间是用户态观察 recv CQE 的时间，不是 NIC 硬件
timestamp。

---

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
