# Framing / Transport Benchmark Report - 2026-06-02

Benchmark code commit: `fe242e4 Refine framing and transport benchmarks`

Test host: `ripple-testnet-tokyo`

```text
Linux 6.17.0-1012-aws x86_64
CPU: Intel(R) Xeon(R) Platinum 8488C, 8 vCPU, 4 cores x 2 SMT
Topology used by benches:
  CPU 4: loopback server
  CPU 1: talaris user thread
  CPU 5: talaris SQ_POLL thread
  CPU 2: tokio worker
```

Raw logs:

```text
/tmp/talaris-benches/framing_transport_ws_framing.log
/tmp/talaris-benches/framing_transport_ws_ingress_single.log
/tmp/talaris-benches/framing_transport_ws_ingress_tls.log
/tmp/talaris-benches/framing_transport_ws_ingress_fanout.log
/tmp/talaris-benches/binance_futures_live_300s_r1.log
```

## Bench Changes

The benches now separate protocol framing and transport more clearly.

- `ws_framing` now includes in-memory WS stream decode:
  - `talaris FrameParser`: raw frame parser lower bound.
  - `talaris WsClient`: full talaris WS client path after handshake.
  - `tungstenite`: full tungstenite raw-socket client path.
- `ws_ingress_single`, `ws_ingress_tls`, `ws_ingress_json`, and `ws_ingress_fanout` now default `--sample-every 0`, so diagnostic inter-arrival histograms no longer perturb transport throughput by default.
- `ws_ingress_fanout` no longer sorts per-frame arrival timestamps unless diagnostic sampling is explicitly enabled.
- `binance_futures_live` already reports CPU/frame, payload, per-kind sample counts, and E-to-app lag; inter-arrival and JSON classify histograms were removed from default output.

## Metric Boundaries

Framing capability means:

```text
WebSocket bytes -> complete WS data message
```

Transport capability means:

```text
kernel/TLS/plaintext ingress -> WS bytes -> complete WS data message
```

JSON parsing, orderbook merge, and strategy logic are outside this benchmark scope.

`cpu ns/frame` is client user-thread CPU only. It does not include the talaris SQ_POLL kernel thread CPU.

## 1. Framing-Only

Command:

```bash
taskset -c 0-7 cargo bench --bench ws_framing -- \
  --iters 10000000 \
  --stream-frames 200000 \
  --stream-payloads 64,256,1024 \
  --user-cpu 1
```

Micro operations:

```text
mask 8B      5.87 ns/op
mask 1KiB   19.56 ns/op
mask 64KiB  1119.47 ns/op
encode short header   1.90 ns/op
encode medium header  2.15 ns/op
parse short header    3.88 ns/op
parse medium header   5.54 ns/op
compute_accept        126.13 ns/op
```

In-memory stream decode:

```text
payload  variant               ns/frame   frames/s
64B      talaris FrameParser    16         61.37M
64B      talaris WsClient       51         19.44M
64B      tungstenite            69         14.42M

256B     talaris FrameParser    33         30.13M
256B     talaris WsClient       160        6.23M
256B     tungstenite            93         10.68M

1024B    talaris FrameParser    57         17.49M
1024B    talaris WsClient       556        1.80M
1024B    tungstenite            181        5.52M
```

Interpretation:

- The raw talaris `FrameParser` lower bound is strong.
- The full talaris `WsClient` path is faster than tungstenite for 64B messages, but loses at 256B and 1024B.
- The most likely cause is not header parse itself; it is full-client buffer/copy behavior around `feed_recv`, `recv_buf`, and borrowed/message payload handling.
- Framing optimization target: preserve the raw parser advantage through the full `WsClient` path, especially for medium/large single-frame payloads.

## 2. Plain Transport

Command:

```bash
taskset -c 0-7 cargo bench --bench ws_ingress_single -- \
  --frames 10000000 \
  --payload 64 \
  --sample-every 0 \
  --server-cpu 4 --talaris-cpu 1 --tokio-cpu 2 --sq-poll-cpu 5 \
  --buf-size 4096 --buf-entries 256
```

Results:

```text
variant              frames/s    MiB/s    cpu ns/frame
talaris Pool.pump    38.18M      2330     25
talaris pump_data    67.44M      4116     10
talaris data spin    70.06M      4276     14
tokio                110.00M     6714     9
```

Interpretation:

- `pump_data` improves talaris over the general event path by `1.77x`.
- Controlled plain TCP peak ingress still favors tokio: talaris `pump_data` is `0.61x` tokio, spin is `0.64x`.
- This is a transport-path issue, not a WebSocket header parser issue.

## 3. TLS Transport

Command:

```bash
taskset -c 0-7 cargo bench --bench ws_ingress_tls -- \
  --frames 10000000 \
  --payload 256 \
  --sample-every 0 \
  --ingress-stats true \
  --server-cpu 4 --talaris-cpu 1 --tokio-cpu 2 --sq-poll-cpu 5 \
  --buf-size 8192 --buf-entries 256
```

Results:

```text
variant                   frames/s    MiB/s    cpu ns/frame
talaris pump_data         11.58M      2828     71
talaris data spin         15.15M      3699     66
tokio + rustls + WS       14.82M      3618     67
tokio bare lower bound    15.72M      3839     63
tokio unbuffered bare     14.58M      3560     65
tokio kTLS ceiling        13.55M      3309     66
```

Ratios:

```text
talaris pump_data / tokio same WS   0.78x
talaris data spin / tokio same WS   1.02x
tokio unbuffered / tokio bare       0.93x
tokio kTLS / tokio bare             0.86x
```

talaris ingress diagnostics:

```text
talaris pump_data:
  recv CQEs       356,983
  bytes/CQE       7293.1
  ENOBUFS         0
  ws-drains       158,732
  ws-drain-skips  198,248

talaris data spin:
  recv CQEs       322,884
  bytes/CQE       8063.5
  ENOBUFS         11
  ws-drains       158,735
  ws-drain-skips  164,157
```

Interpretation:

- TLS changes the picture: talaris busy-poll reaches parity/slightly above tokio same-WS.
- Blocking `pump_data` is still behind tokio same-WS.
- `bytes/CQE` is close to the 8KiB buffer size in spin mode, so provided buffers are being used efficiently.
- The remaining gap in blocking mode likely sits in pump/wait/drain scheduling rather than raw frame parsing.

## 4. Fanout Transport

Command:

```bash
taskset -c 0-7 cargo bench --bench ws_ingress_fanout -- \
  --frames 20000000 \
  --payload 64 \
  --n-list 1,4,16,64 \
  --sample-every 0 \
  --server-cpu 4 --talaris-cpu 1 --tokio-cpu 2 --sq-poll-cpu 5
```

Results:

```text
N    variant   frames/s    MiB/s    cpu ns/frame   talaris/tokio
1    talaris   38.37M      2342     25             0.36x
1    tokio     105.55M     6442     9

4    talaris   33.95M      2072     28             0.30x
4    tokio     114.41M     6983     8

16   talaris   26.70M      1629     32             0.26x
16   tokio     102.89M     6280     9

64   talaris   21.63M      1320     35             0.32x
64   tokio     66.78M      4076     14
```

Interpretation:

- Controlled fanout peak ingress favors tokio across N=1/4/16/64.
- talaris CPU/frame increases with N, which points to Pool routing/drain overhead and connection-state scanning/dispatch cost as optimization candidates.
- Tokio also degrades at N=64, but from a much higher base.

## 5. Live Market Sanity Check

The 300s Binance USD-M live run is not a controlled transport benchmark. It is useful only as production-shape sanity.

Prior 300s result:

```text
talaris             379,900 frames, 1266.18 frames/s, 4,483 cpu ns/frame
tokio-tungstenite   379,901 frames, 1266.18 frames/s, 13,603 cpu ns/frame
```

Per-kind sample counts:

```text
BBO/bookTicker   ~94.18%
L2/depth         ~1.55%
aggTrade         ~4.27%
```

Interpretation:

- At real exchange feed rate, both clients are feed-limited, not peak-throughput-limited.
- In that low-rate blocking live scenario, talaris uses much less user-thread CPU/frame.
- This does not contradict controlled loopback: loopback measures peak drain capacity; live measures production-shape CPU cost under sparse bursts and WAN feed pacing.

## Conclusions

Current ROI summary:

- Framing raw parser: positive. `FrameParser` is extremely cheap.
- Full framing client: mixed. `WsClient` is strong at 64B but weak at 256B/1024B versus tungstenite.
- Plain transport peak: negative versus tokio in current bench.
- TLS transport peak: spin mode reaches parity with tokio same-WS; blocking mode does not.
- Fanout peak: negative versus tokio across tested N.
- Live sparse-feed CPU/frame: positive for talaris, but this is not a peak transport proof.

Recommended optimization order:

1. Preserve `FrameParser` lower-bound performance through `WsClient`.
   Focus on `feed_recv`, `CursorBuf`, borrowed payload lifetime, and copies for medium/large unfragmented frames.
2. Reduce `Pool` hot-loop overhead in plain/fanout paths.
   Inspect per-pump connection scanning, CQE drain batching, event dispatch, and close/control bookkeeping.
3. Improve blocking `pump_data` wake/drain behavior.
   TLS spin is near parity; blocking mode lag suggests wait/drain scheduling cost.
4. Keep live market bench as sanity only.
   Its main value is CPU/frame at realistic feed rates and E-to-app tail, not peak framing/transport capacity.
