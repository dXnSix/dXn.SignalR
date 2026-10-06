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
- New scenario: add it to `dotnet/RayX.SignalR.E2E.Tests/src/Scenarios/ScenarioCatalog.cs`,
  `tests/e2e/<scenario>.rs` (+ `tests/e2e/main.rs`), and the wasm fixture plus Playwright spec
  together; `Category=Catalog` fails on a mismatch.
- .NET projects keep sources under `src/` next to the `.csproj`; the test service is
  `dotnet/RayX.SignalR.TestService`.

## Documentation duty

A public API or behavior change updates the README, the matching `docs/` chapter, and this skill's
references in the same change. Keep every Rust example compilable.
