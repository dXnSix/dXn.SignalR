---
agentx:
  kind: state
  scope: interop-validation
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - rust-tests
    - aspnet
    - wasm
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

Compatibility validation spans in-crate Rust tests and a checked-in ASP.NET Core SignalR service. Networked tests target the `/test` hub on `localhost:5220`; browser execution is wired through Trunk on `127.0.0.1:8089`.

## Backlinks

- Source anchors: [src/tests](../../../../src/tests), [dotnet/SignalRTestService](../../../../dotnet/SignalRTestService), [Trunk.toml](../../../../Trunk.toml).
