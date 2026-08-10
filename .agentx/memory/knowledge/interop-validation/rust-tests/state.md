---
agentx:
  kind: state
  scope: interop-validation/rust-tests
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
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

Native Tokio tests cover baseline interop plus ordinary and stateful reconnect in JSON and MessagePack. The WASM fixture remains a `wasm_bindgen(start)` browser program, but Playwright 1.59.1 now drives it in real Chromium with dedicated ordinary/stateful JSON/MessagePack assertions. Protocol and buffer unit tests cover Ack/Sequence golden frames, bounded replay, backpressure, duplicate suppression, gaps, overflow, disposal, and cumulative acknowledgements.

## Backlinks

- Source anchors: [src/tests](../../../../../src/tests), [WASM fixture](../../../../../tests/wasm-fixture), [Playwright tests](../../../../../tools-node/tests/signalr-wasm.spec.ts), [run-interop.ps1](../../../../../scripts/ci/run-interop.ps1).
- 2026-08-10 current [Decision: use repository-local Playwright for browser runtime validation](../../../decisions/interop-validation/rust-tests/2026-08-10-interop-validation-rust-tests-use-playwright-for-browser-runtime.md).
