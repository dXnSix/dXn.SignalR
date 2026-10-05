---
agentx:
  kind: state
  scope: client-runtime/public-api
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T03:00:00+02:00"
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

`SignalRClient` exposes `connect`, configurable `connect_with`, request/response invocation, fire-and-forget send, server streaming, callback registration, cloning, and consuming disconnect. Configuration covers endpoint security/port/hub, Basic or Bearer authentication, cross-target disconnection/reconnection hooks, optional MessagePack selection, and opt-in `StatefulReconnectConfig` with a default 100,000-byte replay buffer. `ConnectionConfiguration` is exported from the crate root so consumer adapters, such as RayX `rayx_devkit::SignalRService::connect_with`, can forward connection options. `enumerate`/`enumerate_with_args` return the public `ManualStream<T>`, whose `error()` reports a server or connection error that ended the stream. Native clients send the configured `Authorization` header on negotiate and on the WebSocket upgrade; browsers send it on negotiate and pass Bearer tokens as `access_token`.

## Backlinks

- Source anchors: [src/client/client.rs](../../../../../src/client/client.rs), [src/client/configuration.rs](../../../../../src/client/configuration.rs), [src/lib.rs](../../../../../src/lib.rs), [tests/public_connection_configuration.rs](../../../../../tests/public_connection_configuration.rs).
- 2026-06-24 current [AD-009 backend network client decision](../../../decisions/client-runtime/public-api/2026-06-24-client-runtime-public-api-backend-network-client.md).
