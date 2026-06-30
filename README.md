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
实际 inbound hot path（WSS / io_uring multishot recv）：
NIC / kernel TCP stack
  -> socket receive queue                  // 有序 TLS ciphertext byte stream
  -> IORING_OP_RECV_MULTISHOT              // kernel 从 socket queue copy 到 provided buffer
  -> per-connection BufferRing slot        // CQE 携带 conn token + bid + len；slot 内仍是 ciphertext
  -> Pool drain CQE batch                  // 单线程按 CQE 顺序路由到 ConnectionState
  -> ConnectionState resolves bid -> &[u8] // 借用 BufferRing slot；处理完立即 recycle
  -> rustls.read_tls()                     // ciphertext 进入 rustls deframer
  -> rustls.process_new_packets()          // 解出完整 TLS records
  -> rustls.reader().fill_buf()            // borrowed plaintext slice
  -> WsClient::drain_*_from_ingress()
     -> direct data fast path              // 完整、未分片 Text/Binary：header parse + borrowed payload
        -> user sink                       // payload 生命周期只覆盖本次回调 / batch 回调
     -> fallback state machine             // partial frame / control frame / fragmented message / close
        -> CursorBuf recv_buf              // 保存跨 plaintext slice 的未完成 WS frame bytes
        -> msg_buf                         // 仅 fragmented message reassembly 时 copy payload

Plain TCP / WS 路径跳过 rustls：BufferRing slot 里的 bytes 直接作为 plaintext 喂给 WsClient。
```

talaris 是**为一类很狭窄的 workload 量身做的 io_uring WebSocket / TCP toolkit**：
HFT 行情订阅，单线程吃满 N 条 TCP/TLS WebSocket，要的是 p999 尾延迟
可预测，不是通用 async runtime。

如果你只是写 Web app / 微服务，tokio / tokio-tungstenite 通常是更合适的默认选择。
如果你的 workload 满足下面三条里至少两条，再考虑 talaris：

- 单进程驱动 ≥1 条 WebSocket，**收行情是主要负载**（订阅类 / 高频 inbound）
- 你愿意 / 已经做了 `isolcpus` + 核绑定 + 关 NOHZ 这些运维操作
- p999 / max 抖动是产品要求，不是"nice to have"

---

## 心智模型：你需要先理解的 7 件事

### 1. talaris 不是 runtime，是一个同步 hot-loop transport driver

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

这是刻意收窄的同步设计：一个线程、一个 hot loop，用 io_uring 让 kernel 把数据
copy 到你预留的 buffer 里，业务代码在回调里同步消费。

### 2. Proactor vs Reactor

|      | Reactor (epoll / tokio)                           | Proactor (io_uring / talaris)                         |
|------|---------------------------------------------------|-------------------------------------------------------|
| 通知粒度 | "fd 可读了"                                          | "数据已经在你的 buffer 里"                                    |
| 谁干活  | **应用** 在 readiness 后调 `read()` 把数据从 kernel 拷到用户 buffer | **kernel** 直接写进 registered provided buffer；recv data copy 不走用户态 `read()` |
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

这是**故意**的。io_uring 本身可以跨线程使用，但 talaris 把一个 `Pool` 设计成
单 OS 线程 owner：SQ/CQ drain、BufferRing recycle、WS parser state 和 sink 回调都在
同一个 hot loop 里完成。跨线程共享 Pool 必然引入同步、ownership 迁移或 SPSC queue，
这些都会改变低延迟语义。我们直接在类型层面禁止：每条独立链路开一个 OS 线程 + 一个 Pool。

需要多 venue 多线程并发？开多个 OS 线程，每个线程自己 `Pool::new`，互不影响。

### 4. 一个 WebSocket Binary 帧的完整生命周期

按时间顺序：

```
[wire]   ─ TCP segment 到达 NIC
[kernel] ─ NIC IRQ → kernel TCP stack 处理
[kernel] ─ 数据 copy 到 io_uring 某个 provided buffer ring（conn_id=M） 的某一个 slot (bid=N)
[kernel] ─ 生成 CQE: { user_data: conn_id, result: bytes_written, flags: bid|F_MORE }
[user]   ─ 阻塞 pump 且 CQ 为空时在 wait_for_cqe 被唤醒；spin pump 则直接轮询 CQ ring
[user]   ─ Pool 从 CQE 解出 conn_id, 路由到对应 ConnectionState
[user]   ─ ConnectionState 按 bid/len 借用 BufferRing slot slice, 喂给 rustls 解密
[user]   ─ rustls plaintext slice 借给 WsClient::drain_*_from_ingress()
[user]   ─ direct path: 完整、未分片 Text/Binary 直接借用 plaintext payload 到 sink，不进 recv_buf
[user]   ─ fallback path: partial/control/fragmented 剩余 bytes 进入 CursorBuf recv_buf
[user]   ─ fragmented message reassembly 才把 payload copy 到 msg_buf
[user]   ─ buf_ring.recycle(bid) 把密文 buffer slot 还给 kernel
```

阻塞 `pump` 只有在 CQ 为空、需要等待 completion 时才进入一次 `io_uring_enter(GETEVENTS)`；
busy-poll `pump_data_spin` 路径只轮询 mmap 出来的 CQ ring，不进 `wait_for_cqe`。
跟 epoll/tokio 比，talaris 省掉的是 readiness 返回后应用侧 `read()` syscall 路径，
以及 executor / waker / scheduler 介入；不是承诺“一条 WS frame 对应一次 read syscall”。

### 5. chunk 是 parser 的 plaintext 输入批次，不是网络包

在 talaris 里，**chunk** 最准确的理解是：一次交给 WebSocket parser 的连续
plaintext byte slice。它不是协议层天然单位，而是 talaris hot path 里的处理批次单位。

```text
WSS:
socket queue 中的 TLS ciphertext bytes
  -> io_uring recv 到 BufferRing slot
  -> rustls 解密
  -> 产出一段 plaintext slice
  -> 这一段 plaintext slice = 一个 plaintext chunk
  -> WsClient 在这个 chunk 上流式解析 WS frame/message

Plain WS:
BufferRing slot 里的 bytes 本身就是 plaintext
  -> 这段 bytes = 一个 plaintext chunk
```

它：

- 不是 NIC packet。
- 不是 skb。
- 不是 TCP segment。
- 不一定等于 CQE。
- 不一定等于 TLS record。
- 不一定等于一个 WebSocket message。
- 不一定包含完整 WebSocket frame。

一个 chunk 里可能出现：

```text
case A: [完整 WS message]
case B: [完整 WS msg][完整 WS msg][完整 WS msg]
case C: [半个 WS frame]
case D: [后半个 WS frame][完整 WS msg]
case E: [Ping][Text][Pong][Text]
```

所以 talaris 必须做**流式 WS parse**，不能假设 chunk 边界就是 message 边界。
同一个连接内，chunk 内 bytes 有序，chunk 与 chunk 之间也按 TCP stream 顺序推进；
不同连接之间没有全局顺序保证。

observability 里的 `chunk_position` 就基于这个定义：

- `first`：这个 plaintext chunk 里解析出来的第一条 WS data message。
- `queued`：同一个 plaintext chunk 里，第一条之后的 WS data message。
- `chunk_prior_sink_service`：当前 queued message 前面那些同 chunk message 已经花在
  sink 里的时间。

一句话：chunk 是 talaris 从 TLS/plain ingress 中拿到的一段连续明文字节，并在一次
parser drain 中处理它；它是 parser 的输入批次，不是网络包，也不是 WS message。

### 6. general events vs data-only dispatch

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

### 7. batch data dispatch：同一 plaintext chunk 内批量交付

对 Binance BBO 这类高频小消息，调用方经常需要在 decode 前先看同一个
plaintext chunk 内的多个冗余 message，并只保留最大 seq。`pump_data_*_batches`
不会替业务做 coalescing；它只把同一 plaintext chunk 内已经 parse 出来的 data message
用固定容量 view 批量交付，方便调用方在 sink 里自己扫描、去重、发布 winner：

```rust
pool.pump_data_spin_marked_batches(256, |_handle, batch| {
    for event in batch.iter() {
        // 扫描 raw Text/Binary，找当前 chunk 内最大 seq / 最新快照。
    }

    if batch.is_chunk_end() {
        // 当前 plaintext chunk 的所有 data message 已经交付完毕；
        // 这里可以立刻发布 coalesced winner，不需要等下一个 chunk。
    }
})?;
```

batch 是固定容量的 hot-path view。一个很大的 plaintext chunk 可能拆成多个
batch；除最后一个外，`is_chunk_end()` 都是 `false`。非 direct fallback 路径
仍可能退化成 one-message batch，但 control frame、fragmentation、auto-pong
语义保持不变。

---

## 一句话术语表（cheat sheet）

| 术语                   | 一句话解释                                                                 | 在代码里                                       |
|----------------------|-----------------------------------------------------------------------|--------------------------------------------|
| **Proactor**         | io_uring 薄封装：提交 connect/recv/send/close，drain completion queue。        | `src/proactor/uring.rs::Proactor`          |
| **CQE**              | io_uring completion event；recv CQE 告诉用户态哪个 conn、哪个 buffer、多少 bytes。 | `src/proactor/op.rs::Completion`           |
| **Pool**             | 一个 Proactor 驱动 N 条 WS conn 的单线程 owner；负责 CQE 路由和回调。                 | `src/pool.rs::Pool`                        |
| **PoolConfig**       | Pool 级配置：io_uring SQ/CQ sizing、setup flags、completion batch 等。          | `src/pool.rs::PoolConfig`                  |
| **ConnHandle**       | 对外的不透明 conn 引用；包含 slot id + generation，避免 stale handle / late CQE 串到复用 slot。 | `src/pool.rs::ConnHandle`                  |
| **ConnectionConfig** | 单条 conn 的配置：host/path/TLS、recv mode、BufferRing、socket、observability。 | `src/connection_meta.rs::ConnectionConfig` |
| **BufferRing**       | 每条 conn 自己的 provided-buffer ring；kernel 写 slot，CQE 通过 `bid` 指回该 slot。 | `src/proactor/buf_ring.rs::BufferRing`     |
| **bid**              | buffer id；BufferRing 内一个 slot 的编号，处理完必须 recycle 还给 kernel。          | `Completion::buffer_id()`                  |
| **plaintext chunk**  | 一段交给 `WsClient` 的 plaintext slice；TLS 下来自 rustls，plain WS 下来自 recv。     | `ConnectionState` inbound path             |
| **WsClient**         | RFC 6455 client 状态机；direct data fast path 旁路 copy，fallback 处理控制/分片。   | `src/ws/client.rs::WsClient`               |
| **DataEventBatch**   | 同一 plaintext chunk 内 data messages 的固定容量 view；只批量交付，不替业务 coalesce。  | `src/ws/client.rs::DataEventBatch`         |
| **pin**              | 把当前线程钉死在一个 CPU 上，配合 `isolcpus` 用，减少 scheduler 迁移抖动。                 | `talaris::proactor::pin_current_thread_to` |
| **pump**             | 阻塞推进通用 WS event；CQ 为空时会等待 completion，业务可观察 control/data。            | `Pool::pump`                               |
| **pump_data**        | data-only dispatch；只把 Text/Binary 交给业务，control frame 仍由 WS 层处理。        | `Pool::pump_data`                          |
| **pump_data_spin**   | data-only busy-spin 版本；持续轮询 CQ ring，适合 isolated CPU。                   | `Pool::pump_data_spin`                     |

---

## 30 秒上手

### Cargo.toml

```toml
[dependencies]
talaris = "0.9"
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
use talaris::connection_meta::ConnectionConfig;
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
    let mut pool = Pool::new(PoolConfig::default())?;
    let _handle = pool.connect_blocking(cfg)?;

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

## API surface（常用入口）

```rust
// 1. 起一个单线程 Pool。默认 PoolConfig 已包含默认 ProactorConfig。
let mut pool = Pool::new(PoolConfig::default())?;

// 2. 建连接。
// 阻塞版会完成 TCP connect + TLS handshake + WS upgrade；适合启动期 / smoke / tests。
let h: ConnHandle = pool.connect_blocking(cfg)?;
// let h = pool.connect_blocking_to(cfg, addr)?; // 跳过 DNS / IP racing 时使用

// 并发建连用 submit_*：先 reserve slot，再由后续 pump 驱动到 Open。
let h2 = pool.submit_connect(cfg2)?;
// let h2 = pool.submit_connect_to(cfg2, addr2)?; // 已经完成 DNS/IP racing 时使用

// 3. 主动发送。Text/Binary 是业务 data frame；Ping/Pong/Close 是 control frame。
pool.send_text(h, br#"{"op":"subscribe"}"#)?;
pool.send_binary(h, payload)?;
pool.send_ping(h, b"hb")?;
pool.send_pong(h, b"hb")?;
pool.initiate_close(h, 1000, "bye")?;

// 4. 推进 IO。生产行情主循环通常优先用 data-only 路径。
pool.pump(|h, ev| { /* general WS events: data + control */ })?;
pool.pump_data(|h, data| { /* only Text/Binary data */ })?;
let progressed = pool.pump_data_spin(256, |h, data| { /* busy-poll data path */ })?;
let progressed = pool.pump_data_spin_batches(256, |h, batch| { /* chunk-level batch */ })?;

// 5. 生命周期管理。
pool.remove_conn(h2)?; // hard remove：释放 slot/bgid，旧 ConnHandle 立即失效
let h = pool.submit_reconnect_to(h, cfg, addr)?; // 非阻塞 diagnostic reconnect
// let h = pool.reconnect_to(h, cfg, addr)?;      // 阻塞 convenience，不建议放 hot loop

// 6. 观测和状态。
let state = pool.state(h);       // Option<State>
let active = pool.conn_count();  // active conn count
let metrics = pool.prometheus_metrics();
```

`pump` / `pump_data` 是阻塞路径：CQ 为空时会等待 completion。
`*_nowait` 立即返回，适合 close cleanup 或和外部 loop 集成。
`*_spin` 不等待 syscall，适合 isolated CPU 上的低抖动 hot loop。
`*_marked` / `*_marked_batches` 会携带 observability metadata；未 marked 路径不读时钟。

### Pool lifecycle / reconnect 语义

`ConnHandle` 是 opaque token，不只是 slot id。内部编码了 `slot_id + generation`：
slot 被 `remove_conn` / reconnect 回收后 generation 会递增，旧 handle 和旧 generation
的 late CQE 都不会命中新连接。这是长期运行进程能安全诊断重连的核心约束。

- `connect_blocking` / `connect_blocking_to`：启动期和测试最方便；会自己 drive open。
  专用 open-driving path 不会 drain 已有连接的业务 Text/Binary。
- `submit_connect` / `submit_connect_to`：生产启动期推荐；可一次提交多条连接，让 TCP/TLS/WS
  handshake 并发推进，后续用 `pump*` 驱动到 `State::Open`。
- `remove_conn`：hard remove，不发送 graceful close。它会 unregister buffer ring、drop fd、
  递减 active count，并在安全时回收 slot/bgid。旧 handle 后续 `state/send/remove` 都会失败或返回 `None`。
- `submit_reconnect` / `submit_reconnect_to`：先移除旧连接，再提交新连接；用于故障诊断 / 重连。
  如果新连接失败，旧连接不会自动恢复，调用方应按策略重新提交。
- `reconnect` / `reconnect_to`：blocking convenience，适合测试 / CLI / smoke；生产 hot loop
  更推荐 `submit_reconnect_to`，避免在关键路径里阻塞握手。

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

#### Observability metrics summary

Prometheus 输出分两类：**stage latency histograms** 和 **ingress counters**。
前者用于回答“从 recv 到 WS message 被 pump 到上层到底花了多久”；后者用于回答
“这条连接 inbound 路径到底发生了多少 recv / chunk / WS event / ring pressure”。

**Stage latency histograms** 只有在 marked pump 路径里才会记录；默认 hot path 不读时钟。
同时开启 `.with_observability_histograms(true)` 后，talaris 用本地 HdrHistogram 维护
quantile，并导出这些 Prometheus family：

- `talaris_ws_latency_quantile_ns`：本地 HdrHistogram quantile，单位 ns。
- `talaris_ws_latency_samples`：该 histogram 的样本数。
- `talaris_ws_latency_sum_ns`：样本耗时总和，可用于均值或 sanity check。
- `talaris_ws_latency_max_ns`：该窗口内最大样本。

通用 label：

- `conn_id`：Pool 分配的连接 id，只在当前 Pool 生命周期内有意义。
- `window="cumulative" | "interval"`：连接生命周期累计窗口或 interval 窗口。
- `scope="chunk" | "message"`：chunk-level 或 message-level latency。
- `stage`：具体阶段。
- `chunk_position`：message 在 plaintext chunk 中的位置。
- `quantile`：只出现在 `talaris_ws_latency_quantile_ns`。

当前 stage 语义：

| scope | stage | chunk_position | 语义 |
|---|---|---|---|
| `chunk` | `recv_to_plaintext` | `chunk` | 用户态观察到 recv CQE 到 TLS/plain plaintext chunk ready。Plain WS 下基本是 recv 到 parser 输入；WSS 下包含 rustls 解密/deframe。 |
| `message` | `plaintext_to_ws` | `all` / `first` / `queued` | plaintext chunk ready 到 WS Text/Binary payload ready 并即将进入 sink。queued message 会包含同 chunk 前序 sink service。 |
| `message` | `recv_to_ws` | `all` / `first` / `queued` | 用户态观察 recv CQE 到 WS Text/Binary payload ready 并即将进入 sink。最接近“收到数据到上层可见”的端到端指标。 |
| `message` | `plaintext_to_ws_excluding_prior_sink` | `all` / `first` / `queued` | 从 `plaintext_to_ws` 中扣除同 chunk 前序 sink 回调累计耗时，观察当前 message 自身 parse/dispatch 净成本。 |
| `message` | `recv_to_ws_excluding_prior_sink` | `all` / `first` / `queued` | 从 `recv_to_ws` 中扣除同 chunk 前序 sink 回调累计耗时，观察不受前序 sink 排队污染的 recv-to-message 净成本。 |
| `message` | `chunk_prior_sink_service` | `queued` | 当前 queued message 前面那些同 chunk message 已经花在 sink 里的累计时间；first message 没有这个值。 |

`chunk_position` 的读法：

- `chunk`：chunk-level histogram，不对应单条 WS message。
- `all`：所有 sampled data message。
- `first`：plaintext chunk 内第一条 WS data message。
- `queued`：同一 plaintext chunk 内第一条之后的 WS data message，用来观察 chunk 内排队。

这些 quantile 是每条连接本地 HdrHistogram 的客户端 quantile，不适合让 Prometheus
跨连接直接聚合成全局 quantile。`prometheus_metrics()` 导出 cumulative 窗口；
`prometheus_metrics_and_reset_interval()` 导出 interval 窗口并 reset interval latency
histograms，适合 dashboard / alert。两者都不会 reset ingress counters。

**Ingress counters** 是长期低成本计数器，也默认关闭，需要按连接开启：

```rust
let cfg = ConnectionConfig::new(host, 443, path)
    .with_ingress_stats(true);

let stats = pool.ingress_stats(h);
```

开启后，`pool.prometheus_metrics()` 会额外导出这些 lifetime counters：

| counter | 语义 |
|---|---|
| `talaris_ingress_recv_data_cqes_total` | 正长度 recv data CQE 数。 |
| `talaris_ingress_recv_bytes_total` | 正长度 recv data CQE 携带的 bytes；TLS 下是 ciphertext bytes，Plain TCP 下是 plaintext bytes。 |
| `talaris_ingress_recv_multishot_rearms_total` | recv multishot SQE submit / rearm 次数。 |
| `talaris_ingress_recv_ring_exhaustions_total` | provided-buffer ring exhaustion 导致 multishot recv 停止的次数。 |
| `talaris_ingress_plain_recv_batches_total` | Plain TCP data-pump batch path 处理的连续 recv CQE run 数。 |
| `talaris_ingress_plain_recv_batch_cqes_total` | 上述 Plain TCP batch runs 包含的 recv CQE 总数。 |
| `talaris_ingress_plain_recv_copied_batches_total` | Plain TCP batch runs 中走 reusable copy scratch buffer 的次数。 |
| `talaris_ingress_plain_recv_copied_bytes_total` | copy scratch buffer 累计复制 bytes。 |
| `talaris_ingress_plaintext_chunks_total` | 进入 WebSocket parser 的 plaintext source chunks；TLS 下是 rustls plaintext slices，Plain TCP 下是 recv/provided-buffer slices。 |
| `talaris_ingress_plaintext_bytes_total` | 喂给 WebSocket parser 的 plaintext bytes。 |
| `talaris_ingress_ws_data_drains_total` | data pump 中到达 WS receive processing 的 plaintext source chunks。 |
| `talaris_ingress_ws_data_drain_skips_total` | data-pump drain attempt 中没有 plaintext 可处理的次数。 |
| `talaris_ingress_ws_data_events_total` | emit 到用户 data sink 的 Text/Binary data message 数。 |
| `talaris_ingress_ws_text_events_total` | emit 到用户 data sink 的 Text message 数。 |
| `talaris_ingress_ws_binary_events_total` | emit 到用户 data sink 的 Binary message 数。 |

没有开启 `with_ingress_stats(true)` 时，这些 counter 不更新。ingress counters 成本低于
latency histograms，但仍会在 hot path 更新计数器；benchmark / 排障时可以打开，生产默认
保持关闭，除非目标机器和 feed 已经确认这部分成本可接受。latency histograms 则按需要
打开采样，采样率通过 `with_observability_sample_rate_bps()` 控制，`10_000` 表示 100%。

---

## 调优参数（按 ROI 排）

先给原则：**按 feed 特征调参，不按交易所名字调参**。同一类 message size / frequency /
burst pattern 的 feed 可以共用一个 Pool；BBO、trade burst、book snapshot 这类特征差异
很大的 feed，要用真实行情或 replay benchmark 单独定参数。

### `ConnectionConfig::with_buf_ring(slot_size, entries)` —— 先定 recv 粒度

`slot_size` 是单个 provided-buffer slot 的字节数。WSS 下 slot 内是 TLS ciphertext；
plain WS 下是 plaintext。它决定 kernel 一次 recv CQE 最多搬多少 bytes 到用户态，也决定
同一个 CQE / plaintext chunk 里可能包含多少 WS messages。

经验起点：**slot_size 约为常见 payload 的 8-20 倍**，然后用真实 feed A/B。

| feed / payload 形态 | 建议起点 |
|---|---|
| BBO / quote 小消息 100-300 B | 2-4 KiB |
| trade / quote burst 100-500 B | 4 KiB |
| L2 book delta 300-800 B | 4-8 KiB |
| book snapshot 1-4 KiB | 16-32 KiB |
| 大 snapshot 4-16 KiB | 32-64 KiB+ |

`entries` 默认 256，必须是非零 2 的幂。整池字节数是 `entries × slot_size`；太小会让
multishot recv 在 burst 期撞 provided-buffer `ENOBUFS`，然后等下一轮 pump re-arm，尾部
会跳。太大则增加 locked memory 和 cache footprint。

```rust
let cfg = ConnectionConfig::new(host, 443, path)
    .with_buf_ring(2 * 1024, 256);
```

### `ProactorConfig::with_cq_entries` + `PoolConfig::with_completion_batch_capacity`

这两个不是一回事：

- `cq_entries` 是 kernel io_uring CQ 容量，防止 multishot recv burst 把 CQ 撑爆。
- `completion_batch_capacity` 是 Pool 内部 `Vec<Completion>` 初始容量，避免 hot loop 第一轮 grow，
  也决定一轮 drain 后按 conn 分组处理时的 scratch 起点。

经验起点：

```text
cq_entries >= max(2 * sq_entries, buf_ring_entries)
completion_batch_capacity = 64 起步；高 fanout / burst 可试 128 或 256
```

`cq_entries` 必须大于 `sq_entries`，并保持 2 的幂。

```rust
use talaris::proactor::ProactorConfig;
use talaris::{Pool, PoolConfig};

let proactor = ProactorConfig::default()
    .with_cq_entries(1024);

let pool_cfg = PoolConfig::new(proactor)
    .with_completion_batch_capacity(64);

let mut pool = Pool::new(pool_cfg)?;
```

### `PoolConfig::with_post_progress_spin_iters(iters)` —— progress 后额外短 spin

这个参数只影响 busy-spin data pump：一次 pump 已经取得进展后，是否继续短暂 drain 附近
刚到的 CQE。它可能提高同轮聚合能力和 burst 吞吐，但也可能让第一条 message 在返回到
外层业务 loop 前多等一点。

默认是 `0`。只有在明确观察到“多 CQE 批内聚合不足”时才试，例如 `64/128/256` 矩阵。
不要把它当成通用低延迟开关。

```rust
let pool_cfg = PoolConfig::default()
    .with_completion_batch_capacity(64)
    .with_post_progress_spin_iters(0);
```

### `ConnectionConfig::with_recv_mode(mode)` —— bundle 仍是实验项

默认 `RecvMode::Multishot` 是当前推荐。`RecvMode::MultishotBundle` 依赖较新的 kernel
能力，一个 CQE 可能覆盖多个 provided buffers；它更偏吞吐/burst 实验，不是小消息 BBO 的
默认低延迟配置。

```rust
use talaris::connection_meta::RecvMode;

let cfg = cfg.with_recv_mode(RecvMode::Multishot);
```

### `ProactorConfig::with_setup_flags(flags)` —— 高级 taskrun 控制

可选暴露 `IORING_SETUP_COOP_TASKRUN`、`IORING_SETUP_TASKRUN_FLAG`、
`IORING_SETUP_SINGLE_ISSUER`、`IORING_SETUP_DEFER_TASKRUN`。默认全部关闭。

推荐只按明确假设打开，并用 benchmark 验证：

```rust
use talaris::proactor::{ProactorConfig, ProactorSetupFlags};
use talaris::PoolConfig;

let flags = ProactorSetupFlags::SINGLE_ISSUER
    | ProactorSetupFlags::DEFER_TASKRUN
    | ProactorSetupFlags::TASKRUN_FLAG;

let proactor = ProactorConfig::default().with_setup_flags(flags);
let pool_cfg = PoolConfig::new(proactor);
```

约束：`DEFER_TASKRUN` 要求 `SINGLE_ISSUER`；`TASKRUN_FLAG` 要求 `COOP_TASKRUN` 或
`DEFER_TASKRUN`；`COOP_TASKRUN` 和 `DEFER_TASKRUN` 是不同 taskrun 模式，不能一起开。
长时间只做 userspace spin/drain 的 loop 不应无脑开启。

### `ConnectionConfig::with_ingress_stats(true)` —— 临时量化 recv/CQE 结构

默认关闭。调 buf ring、CQ sizing、chunk/CQE 聚合时可临时开启，并通过
`pool.ingress_stats(h)` 读取：

```text
recv_data_cqes, recv_bytes, recv_ring_exhaustions,
plaintext_source_chunks, plaintext_bytes,
ws_data_drains, ws_data_drain_skips, ws_data_events
```

生产默认保持关闭；需要排障或已验证成本可接受时再按连接开启。

### `ConnectionConfig::with_plain_recv_batch_copy_max_bytes(bytes)` —— 只针对 plain WS

这个参数只影响 **plain TCP / plain WS** 的 unmarked data pump：把同一轮连续 recv CQE copy
进可复用 scratch buffer，再作为一个更大的 WS input slice 解析。它是吞吐向参数，会 copy bytes，
也会让第一条 message 等待这一轮 copy 完成。

WSS/TLS 和 marked observability 路径会保留 per-CQE / per-plaintext staging；生产 WSS
默认不靠这个参数调延迟。

### `ConnectionConfig::with_socket_busy_poll_usecs(usecs)` —— 只作为实验开关

`SO_BUSY_POLL` 是 Linux per-socket busy polling budget。talaris 暴露它是为了让特定
kernel / NIC / feed 组合做 A/B。它不是默认低延迟开关，也不会降低 CPU；当用户态已经
busy-spin 时，它只是把部分等待挪到 kernel/NIC poll path。

生产默认保持关闭。只有在目标机器、目标 kernel、目标 feed 的 live/replay benchmark
证明有稳定 ROI 后，才应该把它写进生产配置。

### `pin_current_thread_to(cpu)` —— 砍尾抖动

`isolcpus=N-M` 把 CPU 从普通 scheduler 摘出来 + 钉线程到那个 CPU，主要目标是减少
scheduler migration 和普通 OS noise 对 p999 / max 的影响。它通常不改变 p50，
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
| **IO 进展 cost** | readiness event + `read()` + waker poll + `Stream::poll_next` | CQE drain + TLS/WS parse + sink |
| **schedule jitter** | executor 调度 + work stealing 漂移 | 无 executor 调度；仍受 OS / IRQ / CPU 拓扑影响 |
| **依赖** | tokio (~20+ transitive) + tokio-tungstenite + futures + ... | rustls / ring / io-uring + 小型 codec deps；无 async runtime |
| **何时选 tokio** | web server / 通用 microservice / mixed IO | |
| **何时选 talaris** | | HFT 数据流 / latency-sensitive subscribe loop |

### 什么时候**不要**用 talaris

- **macOS / Windows 部署**：talaris **Linux only**（io_uring 是 Linux 独有）
- **kernel < 6.0**：multishot recv + buffer ring 要 5.19+，生产建议 6.x；
  talaris 不提供 epoll fallback，低版本应升级内核或选择其它 WebSocket 客户端
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

`pool.pump(...)` 以 `wait_nr=1` 推进：如果 CQ 里已经有 completion，会先直接 drain；
如果 CQ 为空，才调用 `wait_for_cqe(1)` 等到至少 1 个 CQE。你不希望阻塞
（譬如要在同一 loop 里做别的事）时，用 `pool.pump_nowait(...)`。

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

burst 期 N 个 buffer 还没来得及 recycle，下一次 multishot recv 找不到空 slot →
整条 multishot recv 停止 → Pool 下一轮 pump 才 re-arm。表现：burst 头几条
数据延迟跳一下。
解决：调大 `entries` 或 `slot_size`，让 `entries × slot_size` 覆盖 burst 期
in-flight ciphertext bytes，并给 recycle 留余量。

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
  O(1) generation-guarded slot-table lookup. `submit_connect` returns a
  handle immediately so N connections can hand-shake concurrently; `remove_conn`
  / `submit_reconnect_to` recycle slots without letting stale handles or late CQEs
  alias a new connection.

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

本地 loopback bench 默认绑核口径：

- bench 进程 `taskset -c 0-2`。
- local bench pin user thread 到 CPU 1、server thread 到 CPU 2。
- live bench 必须随结果记录目标机器、kernel、CPU governor、IRQ 绑定和
  `taskset` / pinning；没有环境上下文的 live 数字不要作为结论引用。

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

### Feed placement / tuning principle

talaris 的低延迟优势来自 single `Pool` / proactor hot loop 在热路径上 inline 完成
CQE drain、TLS decrypt、WS parse 和 dispatch；对应风险是不同 feed 混跑时会产生
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
final interval 和 cumulative snapshot。输出格式与上文 **Observability metrics summary**
一致。

---

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
