---
agentx:
  kind: state
  scope: interop-validation
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
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

Compatibility validation spans service-independent Rust contract tests, native Tokio runtime tests, a checked-in ASP.NET Core SignalR service, and a real Chromium WASM fixture. `scripts/ci/run-interop.ps1` owns bounded service startup, health checks, exact one-test selection, cleanup, and redacted failure diagnostics. Networked clients target the `/test` hub on port 5220; the Playwright fixture is served on port 7878.

## Backlinks

- Source anchors: [src/tests](../../../../src/tests), [dotnet/SignalRTestService](../../../../dotnet/SignalRTestService), [run-interop.ps1](../../../../scripts/ci/run-interop.ps1), [Playwright configuration](../../../../tools-node/playwright.config.ts).
