---
agentx:
  kind: state
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
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

Connection setup negotiates first, opens a WebSocket, completes the SignalR handshake, and exposes transport-owned storage. Native Tokio and browser WASM share a single-flight `ReconnectCoordinator`, no-retry/constant/linear/exponential policies, automatic reconnect, and manual handler control. Ordinary reconnect performs fresh negotiation plus one handshake and fails pending work without replay. Opt-in stateful reconnect resumes the same logical connection token without a second handshake, replays bounded serialized frames, and suppresses duplicates. Local close and terminal server close suppress reconnect.

## Backlinks

- Source anchors: [src/client/client.rs](../../../../../src/client/client.rs), [src/communication/reconnection.rs](../../../../../src/communication/reconnection.rs), [src/communication/client_tokio.rs](../../../../../src/communication/client_tokio.rs), [src/communication/client_wasm.rs](../../../../../src/communication/client_wasm.rs).
