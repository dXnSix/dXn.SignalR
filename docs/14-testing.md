# Testing and CI

## Layers

| Layer | Where | Runs with |
|---|---|---|
| Unit and contract tests | `#[cfg(test)]` modules beside the code, `tests/public_connection_configuration.rs` | `cargo test` (no server needed) |
| End-to-end scenarios (native) | `tests/e2e/<scenario>.rs` | the Aspire runner only |
| End-to-end scenarios (browser) | `tests/wasm-fixture` (Rust) and `tools-node/tests/*.spec.ts` (Playwright) | the Aspire runner only |
| Static contracts | `dotnet/RayX.SignalR.E2E.Tests/src/Contracts` | `dotnet test` categories |
| Micro-benchmarks | `benches/hot_paths.rs` | `cargo bench` (see [Performance](#performance)) |
| Reference-client workloads | `tests/perf` and `dotnet/RayX.SignalR.PerfReference` | the Aspire `Performance` category only |

Plain `cargo test` never needs a server: the `e2e` test target is declared with `test = false` in
`Cargo.toml`.

## Rust checks

```bash
cargo fmt --all --check
cargo clippy --all-targets --test e2e --all-features -- -D warnings
cargo check --all-targets --test e2e --all-features
cargo check --target wasm32-unknown-unknown --all-features --lib
cargo test
cargo test --features messagepack
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
```

The toolchain is pinned to Rust 1.92.0 with `clippy`, `rustfmt` and the
`wasm32-unknown-unknown` target (`rust-toolchain.toml`).

## Testing with Aspire

Service-dependent tests run only through .NET Aspire. The AppHost
(`dotnet/RayX.SignalR.AppHost`, selected by the root `aspire.config.json`) composes:

- `signalr-test-service`: the ASP.NET Core SignalR test service
  (`dotnet/RayX.SignalR.TestService`) on HTTP and HTTPS, with a CA and server certificate generated
  per run;
- `wasm-bindgen-cli`: the fixture's exact `wasm-bindgen` CLI, installed under
  `artifacts-temp/tools/wasm-bindgen/<version>` (a machine-wide CLI is never used);
- `wasm-fixture-build` and `wasm-fixture-bindgen`: the browser fixture build;
- `fixture-host`: a static host for the fixture and the host pages, on its own origin;
- optionally the threaded (atomics) fixture, served cross-origin isolated.

Every endpoint is allocated dynamically; nothing binds a fixed port. The end-to-end project
(`dotnet/RayX.SignalR.E2E.Tests`, xUnit) starts the same AppHost through `Aspire.Hosting.Testing`
and is the single entry point, locally and in GitHub Actions. There are no test scripts.

### Prerequisites

- .NET SDK 10.0.100 (pinned by `global.json`) and the Aspire CLI
  (`dotnet tool install --global Aspire.Cli --version 13.5.3`);
- Rust 1.92.0, with `wasm32-unknown-unknown` for browser cells;
- browser cells: Node 24 with pnpm and the repository-local Playwright browsers
  (`pnpm --dir tools-node install --frozen-lockfile`, then
  `pnpm --dir tools-node exec playwright install chromium firefox webkit`); the `threaded-wasm`
  cells also need `nightly-2026-06-23` with `rust-src`;
- Android cells: the Android SDK (`ANDROID_HOME`) with an NDK, `cargo install cargo-ndk`,
  `rustup target add x86_64-linux-android`, and one running x86_64 emulator
  (`emulator -avd <name>`), or `RAYX_SIGNALR_E2E_ANDROID_SERIAL` naming the device;
- iOS cells (macOS only): Xcode with an iOS simulator runtime and
  `rustup target add aarch64-apple-ios-sim`; `RAYX_SIGNALR_E2E_IOS_DEVICE` picks a simulator by
  name or UDID (default: the newest available iPhone).

### Running

Start the graph with the Aspire dashboard for interactive debugging:

```bash
aspire run
```

Run end-to-end cells:

```bash
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
RAYX_SIGNALR_E2E_TARGETS=chromium,firefox,webkit dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
RAYX_SIGNALR_E2E_TARGETS=android dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
RAYX_SIGNALR_E2E_TARGETS=native RAYX_SIGNALR_E2E_PROTOCOLS=json RAYX_SIGNALR_E2E_SCENARIOS=invoke,server-stream dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
```

| Variable | Values | Default |
|---|---|---|
| `RAYX_SIGNALR_E2E_TARGETS` | `native`, `chromium`, `firefox`, `webkit`, `android`, `ios`, or `all` | `native` |
| `RAYX_SIGNALR_E2E_PROTOCOLS` | `json`, `messagepack`, or `all` | `all` |
| `RAYX_SIGNALR_E2E_SCENARIOS` | scenario names, or `all` | `all` |
| `RAYX_SIGNALR_E2E_ANDROID_SERIAL` | an `adb` device serial | the single running emulator |
| `RAYX_SIGNALR_E2E_IOS_DEVICE` | a simulator name or UDID | the newest iPhone |
| `RAYX_SIGNALR_SOAK_SECONDS` | how long `soak-chaos` runs, in seconds (forwarded to every target) | `120` |

Lists are comma-separated. A selection that matches no cataloged cell fails.

### Cells

A cell is one scenario × protocol × target. The scenario catalog
(`dotnet/RayX.SignalR.E2E.Tests/src/Scenarios/ScenarioCatalog.cs`) decides which targets run which
scenarios:

- everywhere: `connect-negotiate`, `authentication`, `invoke`, `send`, `server-stream`,
  `callbacks`, `concurrency-and-clones`, `large-payload`, `server-close`,
  `reconnect-exhaustion`, `tls`, `negotiate-redirect`, `skip-negotiation`, `ordinary-reconnect`,
  `stateful-reconnect`, `stream-cancellation`, `keepalive-idle`, `server-timeout`,
  `handshake-timeout`, `pause-resume`, `pause-resume-stateful`, `unsent-stream`, `error-kinds`,
  `disconnect-every-handle`, `callback-dispatch`, `client-result-errors`, `connected-callback`,
  `restart-after-stop`, `lifecycle-events`, `connection-id`, `graceful-stop`,
  `reconnect-call-wait`, `request-headers`, `stateful-transparent-resume`,
  `stateful-resume-fallback`; `msgpack-fidelity` everywhere with MessagePack only;
- native, Android and iOS: `executor-independence`;
- browsers: `message-burst`, `worker-transport`, `threaded-wasm`, `short-stream-chaining`;
- native, Chromium and Android: `soak-chaos` and `stateful-resume-stress`.

`stateful-resume-stress` gives five fresh stateful connections 100 transport aborts each, every
abort followed by an invocation and a short stream, and fails on any operation slower than 20 s.
Its Rust cells log the `rayx_signalr::stateful=debug` sequence trace into the cell output; in
Chromium the fixture keeps the last 400 trace records and returns them with a failure.

`soak-chaos` runs mixed concurrent traffic (invocations with results, sends, server streams,
server-to-client callbacks, client results) against an ordinary and a stateful chaos hub at once
for `RAYX_SIGNALR_SOAK_SECONDS`, while the test service's test-only `ChaosControl` aborts the
transports at seeded random 0.5-5 s intervals (the seed is in the cell output). Stateful mode must
lose, duplicate or reorder nothing and keep its logical connection; ordinary mode must complete or
fail every call within 30 s and recover. Natively the clients run on their own runtime, whose
alive-task count must return to its pre-connect value after `disconnect`; in Chromium every
WebSocket must be closed.

Each cell runs exactly one scenario: the Rust `e2e` binary with `--exact <scenario>::<protocol>` on
the host, the Android emulator or the iOS simulator, or Playwright with one exact title. Cells are
bounded to 60 seconds (`soak-chaos`: `RAYX_SIGNALR_SOAK_SECONDS`, default 120, plus 60 seconds;
`stateful-resume-stress`: 600 seconds) with process cleanup. On failure a cell writes bounded, token-redacted output
plus the Aspire resource logs (and an Android logcat excerpt) to `artifacts-temp/e2e/<cell>.json`.

- Android cells push the binary built by `cargo ndk` to `/data/local/tmp` (removed after the run)
  and reach the service at `10.0.2.2`.
- iOS cells run the binary with `xcrun simctl spawn` and shut down a simulator the run booted.
  Because spawned simulator processes are not foreground apps, the runner first runs the binary
  once untimed (`--list`, then one networked scenario), the scenario thread asks for the
  user-interactive QoS class, and a cell whose test already reported its single pass is accepted
  when `simctl spawn` itself does not exit. Such events are recorded in
  `artifacts-temp/e2e/runner-notes.log`.
- Rust cells run with `--nocapture` and `RUST_LOG=rayx_signalr=info`. The end-to-end binaries use
  the default TLS features plus `messagepack`; the Android binary adds `tls-webpki-roots` and
  verifies with webpki.

### Adding a scenario

Add it to `ScenarioCatalog.cs`, `tests/e2e/<scenario>.rs` (registered in `tests/e2e/main.rs`) and
the browser fixture plus Playwright spec in the same change. The `Catalog` category fails on any
mismatch. Fault controls and probes belong to the test service, never to the library.

## Performance

Two harnesses measure the client; neither is part of the end-to-end selection.

**Micro-benchmarks.** `benches/hot_paths.rs` runs Criterion over the crate's internal codec and
action storage through the hidden `bench_support` module. The `bench-internals` feature that
exposes it is unstable and only for these benchmarks; it changes no behavior and applications never
enable it.

```bash
cargo bench --bench hot_paths --features messagepack,bench-internals
cargo bench --bench hot_paths --features messagepack,bench-internals -- --save-baseline <name>
cargo bench --bench hot_paths --features messagepack,bench-internals -- --baseline <name>
```

Benchmark ids are stable, `hot_paths/<inbound|outbound|framing>/<json|messagepack>/<message>/<size>`,
so a named baseline saved on one machine compares a later run on the same machine. `inbound` covers
decoding one received frame plus its dispatch through the action storage (`invocation`,
`completion`, `completion-void`, `stream-item`, `ping`), `outbound` building and encoding an
invocation with framing, and `framing` splitting a frame of eight records (`split-8`), at payload
sizes of 32 B, 1 KiB and 64 KiB.

**Reference-client workloads.** The `Performance` category runs the same workloads with a release
build of the Rust client (`tests/perf`, a `test = false` binary) and with the official .NET client
(`dotnet/RayX.SignalR.PerfReference`, `Microsoft.AspNetCore.SignalR.Client` with its MessagePack
protocol) against the test service's `/perf` hub in one Aspire run:

| Workload | Timed work (`full` scale; `quick` is a tenth) | Untimed warm-up first | Metrics |
|---|---|---|---|
| `invoke-latency` | 2,000 sequential invocations | at least 200 invocations | `p50_us`, `p99_us`, `max_us` |
| `stream-throughput` | one server stream of 100,000 items | whole streams of 10,000 items | `items_per_second` |
| `send-throughput` | 50,000 sends acknowledged by a final invocation | sends confirmed by `SinkCount` | `sends_per_second` |
| `fan-in` | 8 concurrent clients each invoking 1,000 times | invocations on every client | aggregate `calls_per_second` |

Both clients run every warm-up identically, on the measured connections, for at least 2 seconds
(0.2 seconds at `quick`), so the official client is measured in steady state: .NET's tiered JIT
(left at its defaults, including dynamic PGO) and first-use initialization settle before the timed
window. Each result also records its warm-up size (`warmup_calls`, `warmup_items` or
`warmup_sends`, and `warmup_ms`).

```bash
RAYX_SIGNALR_PERF_WORKLOADS=all RAYX_SIGNALR_PERF_SCALE=quick dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=Performance
```

| Variable | Values | Default |
|---|---|---|
| `RAYX_SIGNALR_PERF_WORKLOADS` | workload names, or `all` | none: the category requires a selection |
| `RAYX_SIGNALR_PERF_SCALE` | `quick` (smoke checks) or `full` (budgets) | `quick` |

Each row is one workload and protocol. It runs both clients three times, alternating which goes
first, and records the median of each metric with the build profile and the machine (OS,
architecture, processors, .NET and Rust versions, commit) in `artifacts-temp/perf/<run>.json`.
Budgets are always relative to the .NET client in the same run, never absolute numbers.

## Static contract categories

```bash
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter "Category=WorkflowContract|Category=Repository|Category=Catalog|Category=Toolchain"
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=Orchestration
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=Platform
cargo install cargo-audit --locked
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=DependencySecurity
```

| Category | Proves |
|---|---|
| `WorkflowContract` | The GitHub Actions workflows keep their governance (check names, permissions, failure uploads) and run service tests only through Aspire. |
| `Repository` | No test scripts, no test hooks in library code, no fixed ports, and the documentation describes the test workflow. |
| `Catalog` | Cell selection works, every cataloged scenario is implemented where claimed, and the mobile runners select devices and rewrite endpoints correctly. |
| `Toolchain` | The AppHost provisions the fixture's exact wasm-bindgen CLI. |
| `Platform` | Manifest hygiene, the per-target TLS graph and Android build, WASM target gating, the event-driven browser transport, and the threaded browser build. |
| `DependencySecurity` | `cargo audit` and the NuGet vulnerability scan are clean. |
| `Orchestration` | The AppHost starts every resource on dynamically allocated ports (local only). |

CI runs every category except `Orchestration`.

## CI checks

The least-privilege workflows (`contents: read`, no secrets) run on pull requests to `master` and
on pushes to `master`; the end-to-end workflow can also be started manually. They cancel superseded
pull-request runs and upload bounded, redacted diagnostics only after failures.

Quality checks:

- `quality / rustfmt`
- `quality / clippy`
- `quality / native (<os>, Rust <toolchain>)`
- `quality / wasm (<feature mode>)`
- `quality / unit-contract (<protocol>)`
- `quality / android (cargo ndk)`
- `quality / ios (<target>)`
- `quality / threaded-wasm`
- `quality / platform`
- `quality / rustdoc`
- `quality / dependency-security`
- `quality / contracts`

End-to-end checks:

- `e2e / native (<os>)`
- `e2e / browser (<engine>)` for `chromium`, `firefox` and `webkit`
- `e2e / android`
- `e2e / ios`

## Consumer conformance gates

Consumers can run selected cells against this repository. RayX.GameX's dependency-conformance gate
runs:

```bash
RAYX_SIGNALR_E2E_TARGETS=chromium RAYX_SIGNALR_E2E_SCENARIOS=ordinary-reconnect,stateful-reconnect dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
```
