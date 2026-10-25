# Testing and CI

## Layers

| Layer | Where | Runs with |
|---|---|---|
| Unit and contract tests | `#[cfg(test)]` modules beside the code, `tests/public_connection_configuration.rs`, `tests/public_error_types.rs` | `cargo test` (no server needed) |
| Documentation examples | rustdoc examples in `src/` (doctests), and `tests/documentation_examples.rs`: a compiled copy of every ```` ```rust ```` block of `README.md` and `docs/`, which the `documentation_examples_` test matches against the documents | `cargo test` (browser copies: `cargo check --target wasm32-unknown-unknown --all-features --test documentation_examples`) |
| End-to-end scenarios (native) | `tests/e2e/<scenario>.rs` | the Aspire runner only |
| End-to-end scenarios (browser) | `tests/wasm-fixture` (Rust) and `tools-node/tests/*.spec.ts` (Playwright) | the Aspire runner only |
| Static contracts | `dotnet/dXn.SignalR.E2E.Tests/src/Contracts` | `dotnet test` categories |
| Micro-benchmarks | `benches/hot_paths.rs` | `cargo bench` (see [Performance](#performance)) |
| Reference-client workloads | `tests/perf` and `dotnet/dXn.SignalR.PerfReference` | the Aspire `Performance` category only |

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

The toolchain is pinned to Rust 1.97.0 with `clippy`, `rustfmt` and the
`wasm32-unknown-unknown` target (`rust-toolchain.toml`).

## Testing with Aspire

Service-dependent tests run only through .NET Aspire. The AppHost
(`dotnet/dXn.SignalR.AppHost`, selected by the root `aspire.config.json`) composes:

- `signalr-test-service`: the ASP.NET Core SignalR test service
  (`dotnet/dXn.SignalR.TestService`) on HTTP and HTTPS (HTTP/1.1 and HTTP/2), with a CA and server
  certificate generated per run, plus two protocol-pinned endpoints for the `http2` scenario: `h2c`
  (HTTP/2 only over plaintext) and `https-http1` (HTTP/1.1 only over TLS), whose protocols the
  service sets by the target ports the AppHost passes (`TestService__Http2OnlyPort`,
  `TestService__Http1OnlyPort`). Cells receive them as `DXN_SIGNALR_E2E_H2C_URL` and
  `DXN_SIGNALR_E2E_HTTP1_TLS_URL`. Described requests (`NegotiateRequest`, `TransportRequest`)
  include their `protocol` and `connection` id. On hosts with msquic (`QuicSupport`: Windows 11 or
  Server 2022, Linux with `libmsquic`), an `http3` HTTPS endpoint also serves HTTP/3
  (`TestService__Http3Port`, `HttpProtocols.Http1AndHttp2AndHttp3`). Aspire's endpoint proxy
  forwards only TCP, so this endpoint is not proxied and Kestrel binds its dynamically allocated
  port itself. Cells receive it as `DXN_SIGNALR_E2E_HTTP3_URL`;
- `wasm-bindgen-cli`: the fixture's exact `wasm-bindgen` CLI, installed under
  `artifacts-temp/tools/wasm-bindgen/<version>` (a machine-wide CLI is never used);
- `wasm-fixture-build` and `wasm-fixture-bindgen`: the browser fixture build;
- `test-proxy`: a test-only HTTP `CONNECT` and SOCKS5 proxy (`dotnet/dXn.SignalR.TestProxy`)
  for the `proxy`, `proxy-socks` and `proxy-pac` scenarios; a connection whose first byte is 5 is
  a SOCKS5 greeting. The same proxy also listens behind TLS with the run's test certificate on a
  dynamic loopback port (`tlsPort`, counting `tlsSessions`) for `https://` proxies
  (`proxy-https`). `GET /proxy.pac?http=<route>&https=<route>` serves a PAC script routing
  plain and secure endpoints as `proxy` (HTTP `CONNECT`), `socks5`, `direct`, `dead` (an
  unreachable proxy, then `DIRECT`) or `throw`, over a second, credential-free listener on a
  dynamic loopback port (PAC routes cannot carry credentials). It requires the run's credentials (user `dxn`, a password the test runner generates
  per run; RFC 1929 for SOCKS5), refuses `denied.invalid:80` (403, or SOCKS reply 2), counts its
  tunnels and SOCKS requests, and answers `GET /stats` with the counters, the local ports of its
  tunnels and the address types of the SOCKS requests. Tunnels to the service's allocated ports
  go straight to the ports the service listens on (it reports them at `/e2e/listening`), past
  Aspire's own endpoint proxy, so the service sees the tunnel as its peer (hub method `PeerPort`).
  Cells receive `DXN_SIGNALR_E2E_PROXY_URL` and `DXN_SIGNALR_E2E_PROXY_CREDENTIALS`. Native
  cells and the performance binaries never inherit the machine's proxy variables, so the
  client's default system proxy cannot route them through a developer proxy; builds and tool
  commands keep them;
- `fixture-host`: a static host for the fixture and the host pages, on its own origin;
- optionally the threaded (atomics) fixture, served cross-origin isolated.

Every endpoint is allocated dynamically; nothing binds a fixed port. The end-to-end project
(`dotnet/dXn.SignalR.E2E.Tests`, xUnit) starts the same AppHost through `Aspire.Hosting.Testing`
and is the single entry point, locally and in GitHub Actions. There are no test scripts.

### Prerequisites

- .NET SDK 10.0.402 (pinned by `global.json`) and the Aspire CLI
  (`dotnet tool install --global Aspire.Cli --version 13.5.3`);
- Rust 1.97.0, with `wasm32-unknown-unknown` for browser cells;
- browser cells: Node 24 with pnpm and the repository-local Playwright browsers
  (`pnpm --dir tools-node install --frozen-lockfile`, then
  `pnpm --dir tools-node exec playwright install chromium firefox webkit`); the `threaded-wasm`
  cells also need `nightly-2026-06-23` with `rust-src`;
- Android cells: the Android SDK (`ANDROID_HOME`) with an NDK, `cargo install cargo-ndk`,
  `rustup target add x86_64-linux-android`, and one running x86_64 emulator
  (`emulator -avd <name>`), or `DXN_SIGNALR_E2E_ANDROID_SERIAL` naming the device;
- Android app cells (`android-app`, `android-app-release`): additionally JDK 17 or later
  (`JAVA_HOME`), platform 36 in the Android SDK, network access for the first Gradle build (the
  wrapper's Gradle 9.4.1, the Android Gradle plugin and the `rustls-platform-verifier` Maven
  archive), and an emulator without accounts or other users, because the runner makes the test
  app device owner;
- iOS cells (macOS only): Xcode with an iOS simulator runtime and
  `rustup target add aarch64-apple-ios-sim`; `DXN_SIGNALR_E2E_IOS_DEVICE` picks a simulator by
  name or UDID (default: the newest available iPhone).

### Running

Start the graph with the Aspire dashboard for interactive debugging:

```bash
aspire run
```

Run end-to-end cells:

```bash
dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
DXN_SIGNALR_E2E_TARGETS=chromium,firefox,webkit dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
DXN_SIGNALR_E2E_TARGETS=android dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
DXN_SIGNALR_E2E_TARGETS=android-app,android-app-release DXN_SIGNALR_E2E_SCENARIOS=tls,connect-negotiate,invoke dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
DXN_SIGNALR_E2E_TARGETS=native DXN_SIGNALR_E2E_PROTOCOLS=json DXN_SIGNALR_E2E_SCENARIOS=invoke,server-stream dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
```

| Variable | Values | Default |
|---|---|---|
| `DXN_SIGNALR_E2E_TARGETS` | `native`, `chromium`, `firefox`, `webkit`, `android`, `ios`, `android-app`, `android-app-release`, or `all` | `native` |
| `DXN_SIGNALR_E2E_PROTOCOLS` | `json`, `messagepack`, or `all` | `all` |
| `DXN_SIGNALR_E2E_SCENARIOS` | scenario names, or `all` | `all` |
| `DXN_SIGNALR_E2E_ANDROID_SERIAL` | an `adb` device serial | the single running emulator |
| `DXN_SIGNALR_E2E_IOS_DEVICE` | a simulator name or UDID | the newest iPhone |
| `DXN_SIGNALR_E2E_TRANSPORTS` | `websockets`, `long-polling`, `server-sent-events`, or `all` | `websockets` |
| `DXN_SIGNALR_SOAK_SECONDS` | how long `soak-chaos` runs, in seconds (forwarded to every target) | `120` |
| `DXN_SIGNALR_E2E_TLS_PROVIDER` | the rustls provider of the native, Android and iOS `e2e` builds: `ring` (the crate's default features) or `aws-lc-rs` (`--no-default-features --features messagepack,proxy-pac,http3,tls-platform-verifier,tls-aws-lc-rs`, Android adds `tls-webpki-roots`; built into `target/e2e-aws-lc` and `target/android-e2e-aws-lc`). A missing CMake, NASM or clang is reported as an environment error. | `ring` |

Lists are comma-separated. A selection that matches no cataloged cell fails.

### Cells

A cell is one scenario × protocol × target. The scenario catalog
(`dotnet/dXn.SignalR.E2E.Tests/src/Scenarios/ScenarioCatalog.cs`) decides which targets run which
scenarios:

- everywhere: `connect-negotiate`, `authentication`, `invoke`, `send`, `server-stream`,
  `callbacks`, `concurrency-and-clones`, `large-payload`, `server-close`,
  `reconnect-exhaustion`, `tls`, `negotiate-redirect`, `skip-negotiation`, `ordinary-reconnect`,
  `stateful-reconnect`, `stream-cancellation`, `keepalive-idle`, `server-timeout`,
  `handshake-timeout`, `pause-resume`, `pause-resume-stateful`, `unsent-stream`, `error-kinds`,
  `disconnect-every-handle`, `callback-dispatch`, `client-result-errors`, `connected-callback`,
  `restart-after-stop`, `lifecycle-events`, `connection-id`, `graceful-stop`,
  `reconnect-call-wait`, `request-headers`, `stateful-transparent-resume`,
  `stateful-resume-fallback`, `negotiate-timeout`, `transport-fallback`,
  `client-to-server-stream`; `msgpack-fidelity` everywhere with MessagePack only;
- the Android app (`android-app`) and, on CI runners that opt in with
  `DXN_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES=1` on Windows and macOS, native: `os-proxy`. The runner
  points the system proxy at the test proxy's credential-free listener around each cell (the
  emulator's global HTTP proxy with `adb shell settings put global http_proxy`, the Windows user's
  WinINet settings, or every macOS network service's web proxy with `networksetup`) and restores
  it afterwards; a developer machine's settings never change. The scenario connects to
  `dxn-os-proxy.test`, which only the test proxy resolves (to the service);
- native, Android and iOS: `executor-independence`, `http2`, `proxy`, `proxy-socks`, `proxy-https`, `proxy-pac` (not iOS), `cookies`,
  `client-certificate`;
- native and Android on hosts with msquic: `http3`. The catalog selects it only where
  `QuicSupport` holds, and excludes iOS because macOS has no msquic. Android reaches the QUIC
  endpoint over UDP through `10.0.2.2`;
- native on Windows hosts only: `windows-auth` (the test service's `/test-windows` hub behind
  ASP.NET Core Negotiate authentication, with the current account); the catalog selects it only
  on Windows and excludes the other targets with the reason;
- additionally inside the Android test app (`android-app`, `android-app-release`): `tls`,
  `connect-negotiate`, `invoke`, `error-kinds` (its untrusted-certificate case uses the second
  CA's listener, because the app trusts the run's CA);
- native, Chromium, Android and iOS: `suspend-resume`;
- browsers: `message-burst`, `worker-transport`, `threaded-wasm`, `short-stream-chaining`;
- native, Chromium and Android: `soak-chaos` and `stateful-resume-stress`.

`suspend-resume` asks the runner to suspend the scenario process twice by printing
`DXN_SIGNALR_SUSPEND_READY pid=<pid> seconds=<n>`: for 20 s (both a stateful and an ordinary
client keep their connections) and for 40 s, longer than the service's client timeout (both detect
the loss, fail their pending invocation and reconnect within 5 s). The test-only controls
(`Scenarios/Suspension.cs`) use `NtSuspendProcess`/`NtResumeProcess` on Windows, `SIGSTOP`/`SIGCONT` through
libc's `kill(2)` on Unix hosts and the iOS simulator (a `/bin/kill` helper failed to resume
processes on macOS runners), and `adb shell kill -STOP`/`-CONT` on Android. In Chromium the
spec stops the page's JavaScript through CDP `Debugger.pause`, because Playwright keeps every page
visible and Chromium's `Page.setWebLifecycleState` freeze has no effect on a visible page; Firefox
and WebKit are excluded (Playwright offers CDP only for Chromium). The cell bound is 120 s.

`stateful-resume-stress` gives five fresh stateful connections 100 transport aborts each, every
abort followed by an invocation and a short stream, and fails on any operation slower than 20 s.
Its Rust cells log the `dxn_signalr::stateful=debug` sequence trace into the cell output; in
Chromium the fixture keeps the last 400 trace records and returns them with a failure.

`soak-chaos` runs mixed concurrent traffic (invocations with results, sends, server streams,
server-to-client callbacks, client results) against an ordinary and a stateful chaos hub at once
for `DXN_SIGNALR_SOAK_SECONDS`, while the test service's test-only `ChaosControl` aborts the
transports at seeded random 0.5-5 s intervals (the seed is in the cell output), sparing a
connection for 3 s after aborting it so a stateful resume completes before the next abort. Stateful mode must
lose, duplicate or reorder nothing and keep its logical connection; ordinary mode must complete or
fail every call within 30 s and recover. Natively the clients run on their own runtime, whose
alive-task count must return to its pre-connect value after `disconnect`; in Chromium every
WebSocket must be closed.

A cell is target × scenario × protocol × transport. The transport-baseline scenarios
(`connect-negotiate`, `authentication`, `invoke`, `send`, `server-stream`, `callbacks`,
`concurrency-and-clones`, `large-payload`, `server-close`, `reconnect-exhaustion`,
`ordinary-reconnect`, `keepalive-idle`, `server-timeout`, `pause-resume`, `token-factory`) run on
every selected transport; every other scenario runs on WebSockets only, and Server-Sent Events
never run with MessagePack. WebSockets cells keep their names (`native/json/invoke`, Playwright
`invoke JSON`); the others carry the transport (`native/json/invoke/long-polling`, Playwright
`invoke (JSON, Long Polling)`). The runner passes `DXN_SIGNALR_TRANSPORT` to native and mobile
cells and a `transport` page parameter to the browser fixture, which restrict every connection
with `with_transports`. The test service also maps `/test-longpolling` and
`/test-no-websockets` with restricted transports.

Over Long Polling the transport fault (`AbortPhysicalTransport`, `TokenHub.Abort`) aborts the
current poll and answers every later poll of that connection token with 404, as a server that lost
the connection would, so the official .NET client and this client both reconnect with a fresh
negotiate. Long Polling has inherent keep-alive, so its `keepalive-idle` and `server-timeout`
cells instead assert that an idle client stays connected for 45 s (both scenarios have a 120 s
catalog bound); their hubs hold a poll at most 5 s, so the idle client crosses many empty poll
answers.

Each cell runs exactly one scenario: the Rust `e2e` binary with `--exact <scenario>::<protocol>` on
the host, the Android emulator or the iOS simulator, or Playwright with one exact title. Cells are
bounded to 60 seconds (`soak-chaos`: `DXN_SIGNALR_SOAK_SECONDS`, default 120, plus 60 seconds;
`stateful-resume-stress`: 600 seconds; `keepalive-idle`, `server-timeout`, `proxy` and
`suspend-resume`: 120 seconds) with process cleanup. On failure a cell writes bounded, token-redacted output
plus the Aspire resource logs (and an Android logcat excerpt) to `artifacts-temp/e2e/<cell>.json`.

- Android cells push the binary built by `cargo ndk` to `/data/local/tmp` (removed after the run)
  and reach the service at `10.0.2.2`.
- Android app cells build the `tests/android-harness` JNI library (it runs the `tests/e2e`
  scenarios through `tests/e2e-scenarios`, a library crate that compiles the same sources),
  assemble `tests/android-app` with its Gradle wrapper (`android-app-release`: the minified release
  APK with the documented keep rule), install it, make it device owner (`dpm set-device-owner`)
  and run each cell with `am instrument`. The app initializes the platform verifier from its
  `Context`, installs the run's test CA as a user CA for the scenario and removes it; the `tls`
  scenario then trusts it without extra roots and rejects the test service's untrusted listener,
  whose certificate a second CA issued. After the run the device admin and the app are removed.
- iOS cells run the binary with `xcrun simctl spawn` and shut down a simulator the run booted.
  Because spawned simulator processes are not foreground apps, the runner first runs the binary
  once untimed (`--list`, then one networked scenario), the scenario thread asks for the
  user-interactive QoS class, and a cell whose test already reported its single pass is accepted
  when `simctl spawn` itself does not exit. Such events are recorded in
  `artifacts-temp/e2e/runner-notes.log`.
- Rust cells run with `--nocapture` and `RUST_LOG=dxn_signalr=info`. The end-to-end binaries use
  the default TLS features plus `messagepack`, `proxy-pac` and `http3`; the Android binary adds
  `tls-webpki-roots` and verifies with webpki.

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
(`dotnet/dXn.SignalR.PerfReference`, `Microsoft.AspNetCore.SignalR.Client` with its MessagePack
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
DXN_SIGNALR_PERF_WORKLOADS=all DXN_SIGNALR_PERF_SCALE=quick dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=Performance
```

| Variable | Values | Default |
|---|---|---|
| `DXN_SIGNALR_PERF_WORKLOADS` | workload names, or `all` | none: the category requires a selection |
| `DXN_SIGNALR_PERF_SCALE` | `quick` (smoke checks) or `full` (budgets) | `quick` |

Each row is one workload and protocol. It runs both clients three times, alternating which goes
first, and records the median of each metric with the build profile and the machine (OS,
architecture, processors, .NET and Rust versions, commit) in `artifacts-temp/perf/<run>.json`.
Budgets are always relative to the .NET client in the same run, never absolute numbers.

The `ReferenceClientBudget` category checks the R2.C4 budgets on a full run. Latency (p50 and
p99) must be at most 1.10× the .NET client's; throughput (sends, stream items, fan-in calls) at
least 0.95×. It runs any rows the `Performance` category has not run in the same process, and
writes `artifacts-temp/perf/reference-budget-<run>.txt`:

```bash
DXN_SIGNALR_PERF_WORKLOADS=all DXN_SIGNALR_PERF_SCALE=full dotnet test dotnet/dXn.SignalR.E2E.Tests --filter "Category=Performance|Category=ReferenceClientBudget"
```

## Static contract categories

```bash
dotnet test dotnet/dXn.SignalR.E2E.Tests --filter "Category=WorkflowContract|Category=Repository|Category=Catalog|Category=Toolchain"
dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=Orchestration
dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=Platform
cargo install cargo-audit --locked
dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=DependencySecurity
```

| Category | Proves |
|---|---|
| `WorkflowContract` | The GitHub Actions workflows keep their governance (check names, permissions, failure uploads) and run service tests only through Aspire; the release workflow, the triggers of every workflow and the release pull request guards (`Case=release-automation`). |
| `Repository` | No test scripts, no test hooks in library code, no fixed ports, and the documentation describes the test workflow; the release-plz configuration (`Case=release-automation`), the 1.x version with its changelog section, and tag-form install snippets (`Case=release-consumption`). |
| `Catalog` | Cell selection works, every cataloged scenario is implemented where claimed, and the mobile runners select devices and rewrite endpoints correctly. |
| `Toolchain` | The AppHost provisions the fixture's exact wasm-bindgen CLI. |
| `Platform` | Manifest hygiene, the per-target TLS graph and Android build, WASM target gating, the event-driven browser transport, and the threaded browser build. |
| `DependencySecurity` | `cargo audit` and the NuGet vulnerability scan are clean. |
| `Orchestration` | The AppHost starts every resource on dynamically allocated ports (local only). |

CI runs every category except `Orchestration`.

## CI checks

The least-privilege test workflows (`contents: read`, no secrets) cancel superseded runs and
upload bounded, redacted diagnostics only after failures. Nothing runs on a schedule.

- **Quality** (`quality.yml`) runs every job on pull requests to `master`. Implementation is pushed
  straight to `master`, and a push runs only `quality / rustfmt`, `quality / clippy` and
  `quality / unit-contract (<protocol>)`; every other Quality job is skipped on a push.
- **End-to-end** (`e2e.yml`) runs on pull requests to `master` and when started manually; never on
  a push.
- **Performance** (`perf.yml`) runs on demand only.
- **Release-plz** (`release-plz.yml`) also runs on pushes to `master` and runs no tests:
  `release-plz / release` tags a new version and creates its GitHub Release, and
  `release-plz / release-pr` opens or updates the release pull request. It is the only workflow
  with write permissions and a secret (`RELEASE_PLZ_TOKEN`); see [Releasing](releasing.md).
- A **release pull request** (a same-repository `release-plz-*` branch) changes only `Cargo.toml`
  and `CHANGELOG.md`, so it runs only `quality / rustfmt` and `quality / clippy`; every other
  Quality job and every End-to-end job is skipped by a job-level condition.

Pushes that only change the AgentX epic state and memory (`.agentx/**`) start no Quality or
End-to-end run.

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

Every end-to-end job runs every transport (`DXN_SIGNALR_E2E_TRANSPORTS=all`), and every job's
selection includes the typed-error scenario `error-kinds`:

- `e2e / native (<os>)` runs the whole catalog, including the SOCKS, PAC, HTTPS-proxy, HTTP/2
  and parity scenarios. `windows-auth` runs on Windows only. `http3` runs on ubuntu, which
  installs `libmsquic`, and on Windows.
- `e2e / browser (<engine>)` for `chromium`, `firefox` and `webkit`.
- `e2e / android` runs on an x86_64 emulator, including `http2` and `http3` (the ubuntu host
  installs `libmsquic`).
- `e2e / android-app` runs the debug and minified release test apps (`android-app`,
  `android-app-release`) on an emulator with JDK 17.
- `e2e / ios`.
- `e2e / os-proxy (<os>)` runs on ephemeral Windows and macOS runners that opt in with
  `DXN_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES=1`. It is the only job that changes a host's proxy
  settings.
- `e2e / tls-aws-lc (native, <os>)`, `e2e / tls-aws-lc (android)` and `e2e / tls-aws-lc (ios)`
  run `tls`, `connect-negotiate`, `negotiate-redirect` and `proxy` with
  `DXN_SIGNALR_E2E_TLS_PROVIDER=aws-lc-rs`.
- `e2e / soak` runs only when the workflow is started manually: `soak-chaos` for 900 seconds.

Performance check:

- `perf / compare` runs `Category=Performance|Category=ReferenceClientBudget` with
  `DXN_SIGNALR_PERF_WORKLOADS=all` and `DXN_SIGNALR_PERF_SCALE=full`, so a budget miss fails
  it. It then uploads `artifacts-temp/perf` as the `perf-report` artifact, kept for 30 days.

## Consumer conformance gates

Consumers can run selected cells against this repository. RayX.GameX's dependency-conformance gate
runs:

```bash
DXN_SIGNALR_E2E_TARGETS=chromium DXN_SIGNALR_E2E_SCENARIOS=ordinary-reconnect,stateful-reconnect dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
```
