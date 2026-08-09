---
agentx:
  kind: state
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - connect
    - disconnect
    - reconnection
    - tokio
    - wasm
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

Connection setup negotiates first, opens a WebSocket, completes the SignalR handshake, and exposes transport-owned storage. Native Tokio supports no-retry, constant, linear, and exponential reconnection policies plus manual disconnection handlers. WASM currently has a polling lifecycle and an empty `ReconnectionHandler`; the configured reconnection path is not applied there.

## Backlinks

- Source anchors: [src/client/client.rs](../../../../../src/client/client.rs), [src/communication/reconnection.rs](../../../../../src/communication/reconnection.rs), [src/communication/client_tokio.rs](../../../../../src/communication/client_tokio.rs), [src/communication/client_wasm.rs](../../../../../src/communication/client_wasm.rs).
