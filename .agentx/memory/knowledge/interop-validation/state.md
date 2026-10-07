---
agentx:
  kind: state
  scope: interop-validation
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-08T03:30:00+02:00"
  topics:
    - rust-tests
    - aspnet
    - wasm
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

Compatibility validation spans service-independent Rust unit/contract tests, the checked-in ASP.NET Core SignalR service, black-box native scenarios in the `tests/e2e` target, and a real browser WASM fixture. Catalog targets are `native`, `chromium`, `firefox`, `webkit`, `android` (the `e2e` binary built with `cargo ndk` into `target/android-e2e`, pushed with `adb`, endpoints rewritten to `10.0.2.2`, timed-out cells killed with `pkill -x e2e`, logcat on failure) and `ios` (macOS: `aarch64-apple-ios-sim` binary run with `xcrun simctl spawn` and `SIMCTL_CHILD_*` variables); Rust cells run with `--nocapture` and `RUST_LOG=rayx_signalr=info` so a cell killed at its bound keeps its log. The Aspire AppHost (`dotnet/RayX.SignalR.AppHost`, `aspire run`) composes `signalr-test-service`, pinned `wasm-bindgen` CLI provisioning under `artifacts-temp/tools/wasm-bindgen/<version>`, the fixture build and `fixture-host`, all on dynamic ports. `dotnet/RayX.SignalR.E2E.Tests` is the only entry point: its C# scenario catalog selects cells by target × protocol × scenario, native cells run the prebuilt `e2e` binary with `--exact <scenario>::<protocol>`, browser cells run repository-local Playwright with one exact title against `fixture-host` (`?service=<url>`), and failures write redacted bounded output plus resource logs to `artifacts-temp/e2e/<cell>.json`. Static verification is .NET test categories (`Orchestration`, `Toolchain`, `Catalog`, `Repository`, `WorkflowContract`, `DependencySecurity`, `Platform`). No test scripts remain. GitHub Actions run every target through Aspire on pull requests to `master`: `e2e / native (<os>)`, `e2e / browser (<engine>)`, `e2e / android` (`reactivecircus/android-emulator-runner@v2`, API 34 x86_64, KVM, 2 GB data disk after freeing runner space) and `e2e / ios` (macos-latest), plus Android, iOS, threaded-WASM and Platform quality jobs; the harness forces `CARGO_TERM_COLOR=never` because CI colors cargo output.

Long Polling proof (S7): the transport baseline runs over Long Polling on native, Chromium, Firefox, WebKit and Android. Over Long Polling `HttpContext.Abort()` does not end the connection (and the official .NET client survives a single aborted poll because its `HttpClient` retries it), so the test service `TransportFaults.Sever` (used by `AbortPhysicalTransport` and `TokenHub.Abort`) aborts the current poll and answers every later poll of that token with 404; WebSockets keep the request abort. `/test-keepalive` and `/test-silent` hold a Long Polling poll at most 5 s; over Long Polling `keepalive-idle` and `server-timeout` assert a 45 s idle connection (catalog `Limit` 120 s, native `LIVENESS_SCENARIO_TIMEOUT` 110 s, Playwright 110 s); `token-factory` expects 1 factory call per connect and 3 after rotation and reconnect; browser `authentication` sees the credentials as `header`. Probing the official .NET client with a throwaway xUnit test (temporary `Microsoft.AspNetCore.SignalR.Client` reference in the E2E project, removed afterwards) is the way to settle parity questions.

## Backlinks

- Source anchors: [tests/e2e](../../../../tests/e2e), [dotnet/RayX.SignalR.TestService](../../../../dotnet/RayX.SignalR.TestService), [AppHost](../../../../dotnet/RayX.SignalR.AppHost/src/AppHost.cs), [end-to-end project](../../../../dotnet/RayX.SignalR.E2E.Tests), [Playwright configuration](../../../../tools-node/playwright.config.ts).
- 2026-10-06 current [Run every service-dependent test through Aspire on every target](../../decisions/interop-validation/rust-tests/2026-10-06-interop-validation-rust-tests-aspire-is-the-only-test-orchestrator.md).
- 2026-10-06 current [Prove browser behavior with Playwright on Chromium, Firefox and WebKit](../../decisions/interop-validation/rust-tests/2026-10-06-interop-validation-rust-tests-playwright-browser-matrix.md) (supersedes the 2026-08-10 Chromium-only decision).
