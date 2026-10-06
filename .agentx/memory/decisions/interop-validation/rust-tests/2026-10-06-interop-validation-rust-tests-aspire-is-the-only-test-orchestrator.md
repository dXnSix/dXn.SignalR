---
agentx:
  kind: decision
  scope: interop-validation/rust-tests
  status: current
  created: "2026-10-06T21:00:00+02:00"
  updated: "2026-10-06T21:00:00+02:00"
  topics:
    - aspire
    - e2e
    - ci
    - mobile
  supersedes: []
  superseded_by: []
---
## Decision: Run every service-dependent test through Aspire on every target

### Context: The fork must prove the client on Windows, Linux, macOS, Android, iOS and browsers (main thread, workers, threaded WASM) for JSON and MessagePack, locally and in CI, without hand-started services, fixed ports or test scripts.

### Alternatives considered: PowerShell/shell scripts starting the test service on fixed ports (the previous harness), per-platform ad-hoc runners, and `wasm-bindgen-test` for browsers.

### Reasoning: One Aspire AppHost (`dotnet/RayX.SignalR.AppHost`) composes the SignalR test service (HTTP and HTTPS with a per-run CA), the pinned `wasm-bindgen` provisioning, the fixture builds and `fixture-host` on dynamic ports. `dotnet/RayX.SignalR.E2E.Tests` is the single entry point: a C# `ScenarioCatalog` is the source of truth for target support, each cell runs exactly one scenario (the Rust `e2e` binary with `--exact <scenario>::<protocol>` on the host, the Android emulator via `cargo ndk`/`adb` or the iOS simulator via `simctl`, or one exact Playwright title), bounded to 60 s with redacted diagnostics. CI runs the same command per target (`e2e / native (<os>)`, `e2e / browser (<engine>)`, `e2e / android`, `e2e / ios`), and static gates are .NET test categories, so local and CI proof are identical.

### Trade-offs accepted: Contributors need the .NET SDK, the Aspire CLI and per-target toolchains (Node/Playwright, Android SDK and emulator, Xcode); each cell pays process start-up; iOS cells rely on harness accommodations for `simctl spawn` (untimed warm-up, raised QoS, a launcher-hang allowance recorded in `runner-notes.log`).

### Supersedes: None.

### Source anchors: [AppHost](../../../../../dotnet/RayX.SignalR.AppHost/AppHost.cs), [scenario catalog](../../../../../dotnet/RayX.SignalR.E2E.Tests/Scenarios/ScenarioCatalog.cs), [cell runners](../../../../../dotnet/RayX.SignalR.E2E.Tests/Scenarios/CellRunners.cs), [mobile runners](../../../../../dotnet/RayX.SignalR.E2E.Tests/Scenarios/MobileCellRunners.cs), [end-to-end workflow](../../../../../.github/workflows/e2e.yml).
