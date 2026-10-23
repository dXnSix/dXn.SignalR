---
agentx:
  kind: decision
  scope: repository-governance/agent-memory
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - taxonomy
    - initialization
    - repository-architecture
  supersedes: []
  superseded_by: []
---
# Decision: Organize memory around the client runtime, interoperability validation, and fork governance

## Context

dXn.SignalR is a Rust library with distinct client, communication, protocol, execution, and completion modules; it also contains an ASP.NET Core compatibility service and explicit fork policy. AgentX initialization needed a small taxonomy that follows those checked-in boundaries.

## Alternatives considered

- Mirror every `src/` directory as a top-level domain.
- Make native Tokio and browser WASM separate top-level domains.
- Add top-level domains for build output, captured command logs, or repository-local maintenance tooling.

## Reasoning

One `client-runtime` domain keeps the tightly coupled library layers together while its subdomains preserve retrieval precision. `interop-validation` is separate because the Rust tests and .NET service form an executable cross-language boundary. `repository-governance` owns the hard-fork constraints, upstream-maintenance workflow, and AgentX decisions. Generated output and captured logs are evidence, while the repository-local skill currently reinforces fork governance rather than defining another product boundary.

## Trade-offs accepted

The taxonomy is intentionally coarser than the source directory tree. Future work may add a subdomain when current source establishes a new durable boundary, but should avoid new top-level domains without an architectural reason.

## Supersedes

None.

## Source anchors

- [Cargo.toml](../../../../../Cargo.toml)
- [src/lib.rs](../../../../../src/lib.rs)
- [dotnet/dXn.SignalR.TestService/src/Program.cs](../../../../../dotnet/dXn.SignalR.TestService/src/Program.cs)
- `FORK.md` (removed; see 2026-10-06 current [Independent library decision](../fork-policy/2026-10-06-repository-governance-fork-policy-independent-library.md))
- [dXn.SignalR skill](../../../../../.claude/skills/dxn-signalr/SKILL.md)
