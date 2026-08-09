---
agentx:
  kind: state
  scope: interop-validation/rust-tests
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
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

The native Tokio integration test exercises invoke, streaming, argument serialization, callbacks, cloning, and disconnect against the service. A feature-gated MessagePack integration test covers invoke and streams. `src/protocol/msgpack.rs` includes frame/value unit tests. The WASM path is a `wasm_bindgen(start)` browser program rather than a `wasm-bindgen-test` suite.

## Backlinks

- Source anchors: [src/tests](../../../../../src/tests), [src/protocol/msgpack.rs](../../../../../src/protocol/msgpack.rs), [index.html](../../../../../index.html).
