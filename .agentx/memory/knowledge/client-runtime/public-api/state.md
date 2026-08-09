---
agentx:
  kind: state
  scope: client-runtime/public-api
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - SignalRClient
    - configuration
    - callbacks
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

`SignalRClient` exposes `connect`, configurable `connect_with`, request/response invocation, fire-and-forget send, server streaming, callback registration, cloning, and consuming disconnect. Configuration covers endpoint security/port/hub, Basic or Bearer authentication, native disconnection/reconnection hooks, and optional MessagePack selection.

## Backlinks

- Source anchors: [src/client/client.rs](../../../../../src/client/client.rs), [src/client/configuration.rs](../../../../../src/client/configuration.rs), [src/lib.rs](../../../../../src/lib.rs).
