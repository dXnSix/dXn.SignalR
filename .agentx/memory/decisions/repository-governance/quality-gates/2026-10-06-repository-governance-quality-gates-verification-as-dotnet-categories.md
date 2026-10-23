---
agentx:
  kind: decision
  scope: repository-governance/quality-gates
  status: current
  created: "2026-10-06T21:00:00+02:00"
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - quality-gates
    - platform
    - wasm
  supersedes: []
  superseded_by: []
---
## Decision: Express static and platform gates as .NET test categories and keep WASM browser-only

### Context: Workflow, repository, catalog, toolchain, dependency-security and platform rules were enforced by scripts, and the crate compiled for non-browser wasm targets where it cannot work.

### Alternatives considered: Script-based verifiers, CI-only YAML checks, and allowing WASI builds with stub transports.

### Reasoning: Each rule is an xUnit fact in a named category (`WorkflowContract`, `Repository`, `Catalog`, `Toolchain`, `DependencySecurity`, `Orchestration`, `Platform` with `Case` traits) that runs identically locally and in CI; `WorkflowContract` asserts every job's commands per job. The library is an `rlib` with hygienic normal dependencies, and any `wasm32` target other than `wasm32-unknown-unknown` fails with an explanatory `compile_error!` (WASI is unsupported).

### Trade-offs accepted: Gates need the .NET SDK; platform cases need the matching Rust targets, cargo-ndk and a pinned nightly.

### Supersedes: None.

### Source anchors: [contract tests](../../../../../dotnet/dXn.SignalR.E2E.Tests/src/Contracts), [quality workflow](../../../../../.github/workflows/quality.yml), [crate root](../../../../../src/lib.rs).
