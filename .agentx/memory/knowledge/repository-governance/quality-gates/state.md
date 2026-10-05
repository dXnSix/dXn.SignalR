---
agentx:
  kind: state
  scope: repository-governance/quality-gates
  status: current
  created: "2026-08-10T03:15:00+02:00"
  updated: "2026-10-06T00:30:00+02:00"
  topics:
    - github-actions
    - toolchain
    - interop
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before changing production validation or required-check contracts and update it whenever that surface changes.

## Current Technical Shape

`.github/workflows/quality.yml` and `.github/workflows/e2e.yml` run on pull requests and pushes to `master`; e2e also supports manual dispatch. Quality pins Rust 1.92 while checking stable Rust, Windows/Linux/macOS native targets (including the `e2e` test target in Clippy and native checks), default/all-feature WASM, JSON/MessagePack unit contracts, rustdoc, `DependencySecurity` (RustSec and NuGet) and the `quality / contracts` job (`WorkflowContract`, `Repository`, `Catalog`, `Toolchain`). E2e runs every cataloged cell through Aspire (`dotnet test` after installing the Aspire CLI) as `e2e / native (<os>)` and `e2e / browser (<engine>)`. Workflows reference no script files; the `WorkflowContract` category is the verifier. Both workflows use `contents: read`, cancellation concurrency, no secrets or publishing, bounded timeouts, and failure-only artifacts.

## Backlinks

- Source anchors: [quality workflow](../../../../../.github/workflows/quality.yml), [end-to-end workflow](../../../../../.github/workflows/e2e.yml), [workflow contract tests](../../../../../dotnet/RayX.SignalR.E2E.Tests/Contracts/WorkflowContractTests.cs), [production validation docs](../../../../../README.md).
