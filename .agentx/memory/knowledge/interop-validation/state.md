---
agentx:
  kind: state
  scope: interop-validation
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T00:30:00+02:00"
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

Compatibility validation spans service-independent Rust unit/contract tests, the checked-in ASP.NET Core SignalR service, black-box native scenarios in the `tests/e2e` target, and a real Chromium WASM fixture. The Aspire AppHost (`dotnet/RayX.SignalR.AppHost`, `aspire run`) composes `signalr-test-service`, pinned `wasm-bindgen` CLI provisioning under `artifacts-temp/tools/wasm-bindgen/<version>`, the fixture build and `fixture-host`, all on dynamic ports. `dotnet/RayX.SignalR.E2E.Tests` is the only entry point: its C# scenario catalog selects cells by target × protocol × scenario, native cells run the prebuilt `e2e` binary with `--exact <scenario>::<protocol>`, browser cells run repository-local Playwright with one exact title against `fixture-host` (`?service=<url>`), and failures write redacted bounded output plus resource logs to `artifacts-temp/e2e/<cell>.json`. Static verification is .NET test categories (`Orchestration`, `Toolchain`, `Catalog`, `Repository`, `WorkflowContract`, `DependencySecurity`). No test scripts remain.

## Backlinks

- Source anchors: [tests/e2e](../../../../tests/e2e), [dotnet/SignalRTestService](../../../../dotnet/SignalRTestService), [AppHost](../../../../dotnet/RayX.SignalR.AppHost/AppHost.cs), [end-to-end project](../../../../dotnet/RayX.SignalR.E2E.Tests), [Playwright configuration](../../../../tools-node/playwright.config.ts).
