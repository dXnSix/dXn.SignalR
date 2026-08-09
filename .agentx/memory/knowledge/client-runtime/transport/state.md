---
agentx:
  kind: state
  scope: client-runtime/transport
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - http
    - websocket
    - authentication
    - tokio
    - wasm
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

Shared `HttpClient` posts to SignalR negotiate version 1, selects WebSockets with the protocol-required transfer format, and constructs connection data. Native transport uses `tokio-websockets` with native TLS support; browser transport uses `wasm-sockets::PollingClient` on a 100 ms JavaScript interval.

## Backlinks

- Source anchors: [src/communication/common.rs](../../../../../src/communication/common.rs), [src/communication/client_tokio.rs](../../../../../src/communication/client_tokio.rs), [src/communication/client_wasm.rs](../../../../../src/communication/client_wasm.rs).
