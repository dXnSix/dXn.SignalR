---
title: Performance
description: Measured throughput and latency of dxn-signalr against the official .NET client, how the comparison is run, and how to read it.
group: Project
order: 220
---

The repository measures the client against the official .NET client (`Microsoft.AspNetCore.SignalR.Client` 10) in the same run. Both clients use release builds and identical warm-ups, and each figure is the median of three alternating rounds. The harness checks two budgets on those figures: latency at most 1.10 times the .NET client's, and throughput at least 0.95 times.

## Results

The figures below are the client as of 2026-11-04 (commit `d83ed2b`), from three full-scale runs that day. Each figure is the median of all nine rounds per client, because single runs of the throughput workloads swing widely. The runs were on macOS 26.6.2, an Apple M5 (arm64) with 10 logical processors, with .NET 10.0.13 and rustc 1.97.0. The client, the test service and the .NET reference shared the machine.

| Workload | Protocol | Rust client | .NET client | Ratio |
|---|---|---|---|---|
| Invoke latency, p50 / p99 | JSON | 78.5 / 132.2 µs | 111.7 / 161.2 µs | 0.70x / 0.82x the latency |
| Invoke latency, p50 / p99 | MessagePack | 78.8 / 132.3 µs | 111.8 / 158.3 µs | 0.70x / 0.84x the latency |
| Send throughput | JSON | 1,851,438 sends/s | 1,736,455 sends/s | 1.07x |
| Send throughput | MessagePack | 2,178,274 sends/s | 2,228,531 sends/s | 0.98x |
| Server stream | JSON | 1,829,367 items/s | 829,160 items/s | 2.21x |
| Server stream | MessagePack | 1,795,320 items/s | 1,641,543 items/s | 1.09x |
| Fan-in (8 clients) | JSON | 44,378 calls/s | 40,450 calls/s | 1.10x |
| Fan-in (8 clients) | MessagePack | 44,381 calls/s | 40,318 calls/s | 1.10x |

Each client's process CPU time during the timed window, per operation (`cpu_us_per_op`):

| Workload | Protocol | Rust client | .NET client | Ratio |
|---|---|---|---|---|
| Invoke | JSON | 26.3 µs | 311.1 µs | 0.08x |
| Invoke | MessagePack | 26.3 µs | 307.7 µs | 0.09x |
| Send | JSON | 0.64 µs | 1.01 µs | 0.63x |
| Send | MessagePack | 0.62 µs | 1.01 µs | 0.61x |
| Server stream item | JSON | 0.81 µs | 6.05 µs | 0.13x |
| Server stream item | MessagePack | 0.76 µs | 2.22 µs | 0.34x |
| Fan-in call | JSON | 28.3 µs | 71.8 µs | 0.39x |
| Fan-in call | MessagePack | 27.7 µs | 63.5 µs | 0.44x |

One of the three runs was budget-checked (`ReferenceClientBudget`). It held every budget except MessagePack send throughput, which came out at 0.90x: the .NET client had its fastest send run of the day there, while the Rust client's rate matched its other runs. The other two runs measured that row at 1.04x and 1.08x.

How to read them:

- **Invoke latency** is about 0.7 times the .NET client's at the median and 0.8 times at p99. A call costs the Rust client about 26 µs of CPU, under a tenth of the .NET client's.
- **Fan-in** is faster in every run, and **server streams** in all but one. The stream ratio swings most from run to run (MessagePack 0.94x to 1.87x, JSON 1.46x to 2.45x), because the service sets the pace and the .NET client's rate varies.
- **Send throughput** is paced by the test service, which shares the machine. Both clients send within a few percent of each other, and the ratio ranged from 0.90x to 1.09x across runs. The CPU per send shows the client's own cost: about 0.6 times the .NET client's.
- **CPU per operation** follows the client itself, even where the service sets the throughput.

:::note Ratios within a run, not absolute numbers
The numbers come from one machine and one day, and the raw reports are not committed to the repository. Budgets compare the two clients within a run; absolute figures on your hardware will differ.
:::

### What changed

Before the client's performance work of 2026-11-04, the same machine measured this (one full run on 2026-11-03):

| Workload | Protocol | Rust client then | Ratio then | Ratio now |
|---|---|---|---|---|
| Invoke latency, p50 | JSON / MessagePack | 87.4 / 87.2 µs | 0.77x / 0.79x | 0.70x / 0.70x |
| Server stream | JSON / MessagePack | 1,550,789 / 1,466,009 items/s | 1.23x / 0.83x | 2.21x / 1.09x |
| Send throughput | JSON / MessagePack | 1,726,713 / 2,058,224 sends/s | 1.01x / 1.07x | 1.07x / 0.98x |
| Fan-in (8 clients) | JSON / MessagePack | 43,914 / 43,991 calls/s | 1.07x / 1.15x | 1.10x / 1.10x |

The changes work on the client's own cost, measured by the loopback benchmark below:

- A call that waits for its result is written straight to the socket while the writer task is idle, so the round trip saves a thread wake.
- Invocation arguments are encoded into one buffer per call, in both protocols.
- A server stream's queue takes one short lock, its consumer drains it in batches, and a contended lock spins briefly before it blocks.
- The receive loop reads the clock once per wait instead of four times per frame, and takes an already buffered frame without arming a timer.
- A send shares the connection's writer handle instead of copying it.

On the loopback benchmark (`benches/connection.rs`), an invocation round trip went from 26.6 µs to 21.3 µs (JSON) and from 27.1 µs to 22.0 µs (MessagePack). A send went from 319 ns to 240 ns (JSON) and from 340 ns to 259 ns (MessagePack). A server-stream item went from 981 ns to 489 ns (JSON) and from 878 ns to 400 ns (MessagePack).

The previously published run, on Windows 11 x64 on 2026-10-08 and before these changes, measured invoke latency at 0.97x to 0.98x the .NET client's, server streams at 1.54x to 2.57x, fan-in at 1.39x to 1.42x and send throughput at 0.99x (JSON) and 1.73x (MessagePack). That run is not comparable figure by figure with the one above: different hardware, operating system and toolchains.

## Workloads

| Workload | Timed work (full scale; quick is a tenth) | Metrics |
|---|---|---|
| `invoke-latency` | 2,000 sequential invocations | `p50_us`, `p99_us`, `max_us` |
| `stream-throughput` | one server stream of 100,000 items | `items_per_second` |
| `send-throughput` | 50,000 sends acknowledged by a final invocation | `sends_per_second` |
| `fan-in` | 8 concurrent clients each invoking 1,000 times | aggregate `calls_per_second` |

Each workload warms up first on the measured connection for at least two seconds, so the .NET client's tiered JIT settles before the timed window. Every workload also reports `cpu_us_per_op`, the client process's CPU time during the timed window per operation: where the shared service sets the pace, this is the client's own cost.

## Run it yourself

The comparison is the `Performance` and `ReferenceClientBudget` categories of the .NET test project, run through Aspire. It needs the .NET SDK pinned in the repository's `global.json`, the Aspire CLI and Rust. From a checkout of the repository:

```bash
DXN_SIGNALR_PERF_WORKLOADS=all DXN_SIGNALR_PERF_SCALE=full dotnet test dotnet/dXn.SignalR.E2E.Tests --filter "Category=Performance|Category=ReferenceClientBudget"
```

`DXN_SIGNALR_PERF_SCALE=quick` is a smoke run. The GitHub Actions workflow `perf / compare` runs the same on demand and uploads the report as the `perf-report` artifact.

For the crate's internal hot paths there are Criterion micro-benchmarks over the codec and action storage:

```bash
cargo bench --bench hot_paths --features messagepack,bench-internals
```

The `bench-internals` feature is unstable and exists only for these benchmarks; applications never enable it.

To measure the client alone, `benches/connection.rs` runs invocation round trips, sends and server-stream items through a real `SignalRClient` over a loopback WebSocket, against a minimal hub in the same process:

```bash
cargo bench --bench connection --features messagepack
```

## Next

[Testing](./testing.md) describes the harness that produces these numbers.
