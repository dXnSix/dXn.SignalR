---
agentx:
  kind: state
  scope: client-runtime
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - rust
    - signalr
    - tokio
    - wasm
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

The repository builds one Rust library for native Tokio and browser WASM targets. `SignalRClient` composes negotiation, platform transport, hub-protocol serialization, shared in-flight action storage, and a cross-target reconnect lifecycle. JSON is the default protocol; MessagePack is optional. Ordinary reconnect performs a fresh logical connection, while opt-in stateful reconnect preserves the logical connection and replays bounded serialized frames.

## Backlinks

- Source anchors: [src/lib.rs](../../../../src/lib.rs), [Cargo.toml](../../../../Cargo.toml).
