---
title: Testing and CI
description: How dxn-signalr is tested - unit tests, compiled documentation examples, end-to-end scenarios against an ASP.NET Core test service through Aspire, and the CI checks.
group: Project
order: 230
---

For contributors, and for readers deciding how far to trust a platform claim. The test suite runs the real client against a real ASP.NET Core SignalR service on every supported target.

| Layer | Where | Runs with |
|---|---|---|
| Unit and contract tests | `#[cfg(test)]` modules beside the code, `tests/public_connection_configuration.rs`, `tests/public_error_types.rs` | `cargo test` (no server needed) |
| Documentation examples | rustdoc examples in `src/`, and `tests/documentation_examples.rs`: a compiled copy of every `rust` block of the README and the library's `docs/` | `cargo test` |
| End-to-end scenarios (native) | `tests/e2e/<scenario>.rs` | the Aspire runner only |
| End-to-end scenarios (browser) | `tests/wasm-fixture` and Playwright specs in `tools-node/` | the Aspire runner only |
| Static contracts | `dotnet/dXn.SignalR.E2E.Tests` | `dotnet test` categories |
| Micro-benchmarks and reference workloads | `benches/hot_paths.rs`, `tests/perf` | see [Performance](./performance.md) |

Plain `cargo test` never needs a server: the `e2e` test target is declared with `test = false`.

## Rust checks

The toolchain is pinned to Rust 1.92.0 (`rust-toolchain.toml`), which is also the minimum supported version. Before committing a library change:

```bash
cargo fmt --all --check
cargo clippy --all-targets --test e2e --all-features -- -D warnings
cargo check --all-targets --test e2e --all-features
cargo check --target wasm32-unknown-unknown --all-features --lib
cargo test
cargo test --features messagepack
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
```

## End-to-end tests with Aspire

Service-dependent tests run only through .NET Aspire. The AppHost composes the ASP.NET Core test service (HTTP and HTTPS, with a CA and certificate generated per run, plus HTTP/2-only, HTTP/1.1-only and, where msquic exists, HTTP/3 endpoints), a test proxy for HTTP `CONNECT`, SOCKS5, HTTPS and PAC scenarios, a static host for the browser fixture, and the exact `wasm-bindgen` CLI the fixture needs. Every endpoint is allocated dynamically; nothing binds a fixed port.

A **cell** is one scenario on one protocol (JSON or MessagePack), target and transport. Targets are `native`, `chromium`, `firefox`, `webkit`, `android` (an emulator), `ios` (the simulator) and two Android test-app variants. The scenario catalog covers connecting and negotiation, authentication, every call style, callbacks, client results, reconnection (ordinary, stateful and exhausted), pause and resume, lifecycle events, TLS, proxies, HTTP versions, cookies, client certificates, and soak and stress runs under injected transport faults.

```bash
dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
DXN_SIGNALR_E2E_TARGETS=native DXN_SIGNALR_E2E_PROTOCOLS=json DXN_SIGNALR_E2E_SCENARIOS=invoke,server-stream dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
DXN_SIGNALR_E2E_TARGETS=chromium,firefox,webkit dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
```

| Variable | Values | Default |
|---|---|---|
| `DXN_SIGNALR_E2E_TARGETS` | `native`, `chromium`, `firefox`, `webkit`, `android`, `ios`, `android-app`, `android-app-release`, or `all` | `native` |
| `DXN_SIGNALR_E2E_PROTOCOLS` | `json`, `messagepack`, or `all` | `all` |
| `DXN_SIGNALR_E2E_SCENARIOS` | scenario names, or `all` | `all` |
| `DXN_SIGNALR_E2E_TRANSPORTS` | `websockets`, `long-polling`, `server-sent-events`, or `all` | `websockets` |
| `DXN_SIGNALR_SOAK_SECONDS` | how long `soak-chaos` runs | `120` |

Prerequisites are the .NET SDK pinned in `global.json`, the Aspire CLI, Rust 1.92.0, and per target: the `wasm32-unknown-unknown` target plus Node 24 with Playwright browsers for browser cells, the Android SDK with an NDK and an x86_64 emulator for Android, and Xcode with a simulator runtime for iOS. The repository's `docs/14-testing.md` has the exact commands and every scenario's rationale.

The service under test, `dotnet/dXn.SignalR.TestService`, is a useful reference for what a hub looks like for each client feature.

## CI

- **Quality** (`quality.yml`) runs rustfmt, clippy, native checks on Linux, Windows and macOS with Rust 1.92.0 and stable, WASM checks, unit and contract tests with and without MessagePack, Android and iOS compile checks, a threaded-WASM check, and the .NET static contracts. A push to `master` runs only rustfmt, clippy and the unit tests.
- **End-to-end** (`e2e.yml`) runs on pull requests and on demand: native on three operating systems, three browsers, Android, the Android test app, the iOS simulator, OS proxy settings, the `aws-lc-rs` provider, and a manual soak run.
- **Performance** (`perf.yml`) runs on demand.

## Next

[Releases](./releasing.md) explains how a tested commit becomes a tag you can depend on.
