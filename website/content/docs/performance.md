---
title: Performance
description: Measured throughput and latency of dxn-signalr against the official .NET client, how the comparison is run, and how to read it.
group: Project
order: 220
---

The repository measures the client against the official .NET client (`Microsoft.AspNetCore.SignalR.Client` 10) in the same run. Both clients use release builds and identical warm-ups, and each figure is the median of three alternating rounds. The harness checks two budgets on those figures: latency at most 1.10 times the .NET client's, and throughput at least 0.95 times.

## Results

The figures below are the budget-checked full-scale run of 2026-10-08, recorded in the library's [architecture notes](https://github.com/dXnSix/dXn.SignalR/blob/master/docs/13-architecture.md#measured-performance). It ran on Windows 11 (10.0.26300), x64 with 16 logical processors, .NET 10.0.0 and rustc 1.92.0. The client, the test service and the .NET reference shared the machine. Every budget held.

| Workload | Protocol | Rust client | .NET client | Ratio |
|---|---|---|---|---|
| Invoke latency, p50 / p99 | JSON | 159.6 / 194.8 µs | 163.4 / 200.1 µs | 0.98x / 0.97x the latency |
| Invoke latency, p50 / p99 | MessagePack | 158.6 / 192.1 µs | 164.0 / 198.0 µs | 0.97x / 0.97x the latency |
| Send throughput | JSON | 690,585 sends/s | 700,756 sends/s | 0.99x |
| Send throughput | MessagePack | 668,365 sends/s | 385,706 sends/s | 1.73x |
| Server stream | JSON | 1,056,953 items/s | 411,642 items/s | 2.57x |
| Server stream | MessagePack | 1,212,761 items/s | 789,110 items/s | 1.54x |
| Fan-in (8 clients) | JSON | 42,698 calls/s | 30,170 calls/s | 1.42x |
| Fan-in (8 clients) | MessagePack | 42,717 calls/s | 30,694 calls/s | 1.39x |

How to read them:

- **Invoke latency** is on par with the .NET client.
- **Streaming and fan-in** are consistently faster, which the library attributes to its single-pass wire codec.
- **Send throughput** is close to the .NET client's and varies most between runs, because each `send` completes once the transport accepts the frame, as `SendAsync` does. The .NET client's own MessagePack send rate ranged from about 390,000 to 710,000 sends/s across runs, and an earlier full run measured the Rust client at 0.92x for send throughput. Treat the 1.73x as noise in the reference, not as a lead.

:::note Ratios within one run, not absolute numbers
The numbers come from one machine and one day, and the raw report is a CI artifact that is not committed to the repository. Budgets compare the two clients within a run; absolute figures on your hardware will differ.
:::

## Workloads

| Workload | Timed work (full scale; quick is a tenth) | Metrics |
|---|---|---|
| `invoke-latency` | 2,000 sequential invocations | `p50_us`, `p99_us`, `max_us` |
| `stream-throughput` | one server stream of 100,000 items | `items_per_second` |
| `send-throughput` | 50,000 sends acknowledged by a final invocation | `sends_per_second` |
| `fan-in` | 8 concurrent clients each invoking 1,000 times | aggregate `calls_per_second` |

Each workload warms up first on the measured connection for at least two seconds, so the .NET client's tiered JIT settles before the timed window.

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

## Next

[Testing](./testing.md) describes the harness that produces these numbers.
