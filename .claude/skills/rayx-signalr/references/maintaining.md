# Maintaining

Full chapters: [Architecture](../../../../docs/13-architecture.md),
[Testing and CI](../../../../docs/14-testing.md).

## Where things live

| Area | Source |
|---|---|
| Public client, handlers | `src/client/client.rs` |
| Configuration and recipe | `src/client/configuration.rs` |
| Callback context | `src/client/context.rs` |
| Negotiation (shared) | `src/communication/negotiation.rs`; native HTTP `negotiate_http.rs`; browser `common.rs` |
| Native connection | `src/communication/client_tokio.rs` |
| Browser connection | `src/communication/client_wasm.rs`, `browser_socket.rs` |
| Liveness | `src/communication/liveness.rs` |
| Lifecycle, policies, status | `src/communication/reconnection.rs` |
| Stateful buffer | `src/communication/stateful.rs` |
| TLS | `src/communication/tls.rs`, `build.rs` (derives the `rayx_tls` cfg) |
| Pending calls, streams, callbacks | `src/execution/`, `src/completer/` |
| Wire format | `src/protocol/` (MessagePack in `msgpack.rs`) |

## Invariants

- Serde wire names are preserved in outbound MessagePack; the handshake stays JSON; control frames
  and client results use the selected protocol.
- Pending invocations always complete (result or error).
- Every lifecycle transition is generation-checked; stale attempts never complete into `Connected`
  or leave `Paused`/`Closed`.
- Ordinary reconnect renegotiates and never reuses a token; stateful resume never renegotiates or
  re-handshakes; a resumable stateful transport is dropped without a WebSocket Close.
- One rustls `ClientConfig` per native connection, explicit provider and verifier, no process
  default, no fallback.
- The browser transport never touches `window`; the client stays `!Send` there.
- No test hooks in library code; fault controls live in the test service.
- No UI framework coupling.

## Validation

Focused while iterating:

```bash
cargo test <module_or_test_filter>
cargo test --features messagepack <filter>
cargo check --target wasm32-unknown-unknown --all-features --lib
```

Before committing (the `AGENTS.md` list): `cargo fmt --all --check`,
`cargo clippy --all-targets --test e2e --all-features -- -D warnings`,
`cargo check --all-targets --test e2e --all-features`, the wasm check, `cargo test` with and without
`messagepack`, and `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps`.

Service-dependent behavior is proven only through Aspire:

```bash
RAYX_SIGNALR_E2E_TARGETS=native RAYX_SIGNALR_E2E_SCENARIOS=<scenario> dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
RAYX_SIGNALR_E2E_TARGETS=chromium,firefox,webkit RAYX_SIGNALR_E2E_SCENARIOS=<scenario> dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter "Category=WorkflowContract|Category=Repository|Category=Catalog|Category=Toolchain"
```

- Never start the test service by hand or use fixed ports; Aspire allocates everything.
- Runtime, lifecycle or reconnect claims need real native **and** browser cells; mobile claims need
  `android` or `ios` cells.
- `Category=Platform` for dependency-graph, gating, mobile or threaded-build changes;
  `Category=DependencySecurity` (needs `cargo-audit`) for dependency changes.
- Android app cells (`android-app`, `android-app-release`; `Scenarios/AndroidAppCellRunner.cs`):
  `tests/android-harness` (cdylib over `tests/e2e-scenarios`, which compiles the `tests/e2e`
  sources by path; scenario modules in `tests/e2e/main.rs` are `pub(crate)`) is built with
  `cargo ndk`, `tests/android-app` with its Gradle wrapper (JDK 17+); the app is made device owner
  to install the run's CA as a user CA, then removed. The app runs `tls`, `connect-negotiate`,
  `invoke` (`rayx_signalr_e2e_scenarios::run`).
- TLS provider cells: `RAYX_SIGNALR_E2E_TLS_PROVIDER=aws-lc-rs` builds the native/Android/iOS
  `e2e` binaries without `ring` (`Scenarios/TlsProvider.cs`, separate `*-aws-lc` target
  directories); run `tls,connect-negotiate,negotiate-redirect,proxy` with it.
- Suspension (`suspend-resume`, `Scenario.Suspends`): the scenario prints
  `RAYX_SIGNALR_SUSPEND_READY pid=<pid> seconds=<n>`; `Scenarios/Suspension.cs` suspends that
  process (Windows `NtSuspendProcess`, `kill -STOP`, Android `adb shell kill -STOP`) and always
  resumes it; Chromium pauses the page with CDP `Debugger.pause` (`Page.setWebLifecycleState` is a
  no-op under Playwright).
- New scenario: add it to `dotnet/RayX.SignalR.E2E.Tests/src/Scenarios/ScenarioCatalog.cs`,
  `tests/e2e/<scenario>.rs` (+ `tests/e2e/main.rs`), and the wasm fixture plus Playwright spec
  together; `Category=Catalog` fails on a mismatch.
- .NET projects keep sources under `src/` next to the `.csproj`; the test service is
  `dotnet/RayX.SignalR.TestService`; the `proxy` scenario's CONNECT proxy is
  `dotnet/RayX.SignalR.TestProxy` (Aspire resource `test-proxy`).

## Documentation duty

A public API or behavior change updates the README, the matching `docs/` chapter, and this skill's
references in the same change. Keep every Rust example compilable.
