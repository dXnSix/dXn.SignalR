---
agentx:
  kind: decision
  scope: client-runtime/public-api
  status: current
  created: "2026-06-24T14:25:00+02:00"
  updated: "2026-10-05T12:00:00+02:00"
  topics:
    - AD-009
    - network-client
    - rayx-integration
  supersedes: []
  superseded_by: []
---
## Decision: AD-009 SignalR is a backend network client, not a RayX UI bridge

Transplanted on 2026-10-05 from the RayX repository decision `runtime/services/2026-06-24-runtime-services-signalr-network-client.md`, which the RayX Rust rewrite recorded when it removed the managed RayX bridge.

### Context: why this came up

The RayX Rust rewrite still needs backend communication but removed the RayX managed bridge, so SignalR could no longer double as the UI/interop channel.

### Alternatives considered: what else was on the table

Using SignalR as a UI bridge or coupling it to RayX interop.

### Reasoning: why this option won

SignalR stays a normal network client that RayX apps and services use to talk to .NET backends. The crate therefore carries no RayX UI, GPUI, or interop dependency.

### Trade-offs accepted: what you gave up

Transport, hub protocol, reconnect, authentication, and streaming behavior must be implemented as Rust client functionality in this crate rather than delegated to a host bridge.

### Supersedes: link to prior decision, if replacing

None.

### Source anchors

- [Cargo.toml](../../../../../Cargo.toml)
- [src/client/client.rs](../../../../../src/client/client.rs)
