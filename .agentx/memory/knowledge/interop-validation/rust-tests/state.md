---
agentx:
  kind: state
  scope: interop-validation/rust-tests
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T00:30:00+02:00"
  topics:
    - tokio
    - wasm
    - messagepack
    - unit-tests
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

Black-box native scenarios in `tests/e2e` (`test = false`; public API only; transports dropped through the hub's `AbortPhysicalTransport`) cover ordinary reconnect, stateful reconnect (aborts during a stream and during a pending invocation, verified by `TransportAbortCount`), and 64-round dropped-stream cancellation in JSON and MessagePack. The WASM fixture remains a `wasm_bindgen(start)` browser program, but Playwright 1.59.1 drives it in real Chromium with the same reconnect/cancellation matrix plus short stream chaining. Cancellation cells require zero active server enumerators, one observed server cancellation and one type-5 wire frame per drop, a still-usable connection, and no page errors; natural completion requires zero cancellation frames. Protocol and buffer unit tests cover exact JSON/MessagePack CancelInvocation shapes, Ack/Sequence golden frames, bounded replay, backpressure, duplicate suppression, gaps, overflow, disposal, and cumulative acknowledgements.

## Backlinks

- Source anchors: [tests/e2e](../../../../../tests/e2e), [WASM fixture](../../../../../tests/wasm-fixture), [Playwright tests](../../../../../tools-node/tests), [scenario catalog](../../../../../dotnet/RayX.SignalR.E2E.Tests/Scenarios/ScenarioCatalog.cs).
- 2026-08-10 current [Decision: use repository-local Playwright for browser runtime validation](../../../decisions/interop-validation/rust-tests/2026-08-10-interop-validation-rust-tests-use-playwright-for-browser-runtime.md).
