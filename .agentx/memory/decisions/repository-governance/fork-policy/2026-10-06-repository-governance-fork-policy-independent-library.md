---
agentx:
  kind: decision
  scope: repository-governance/fork-policy
  status: current
  created: "2026-10-06T21:10:00+02:00"
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - provenance
    - project-identity
    - attribution
  supersedes:
    - 2026-08-09-repository-governance-fork-policy-use-direct-source-url.md
  superseded_by: []
---
# Decision: Treat dXn.SignalR as an independent library

## Context

dXn.SignalR started as a hard fork of `signalr-client` (`d32bd04`, `v0.3.2`) with an upstream-import workflow (direct source URL, provenance lines, `FORK.md`, a maintenance skill). Since then the client was largely rewritten (native rustls transport, event-driven browser transport, lifecycle state machine, stateful reconnect, liveness, Aspire validation) and its API diverged. Upstream imports no longer apply cleanly or add value.

## Alternatives considered

- Keep the hard-fork policy and its import workflow.
- Keep a provenance file but drop the import workflow.
- Treat the repository as an independent library and keep attribution only where licensing and courtesy require it.

## Reasoning

The repository owner declared the library independent. Its behavior, API and test infrastructure are its own, so an import workflow would only mislead agents. MIT licensing requires the original copyright notice to stay in `LICENSE`; `Cargo.toml` authors and the README acknowledgements credit the original author.

## Trade-offs accepted

Upstream fixes are no longer tracked. An idea from upstream is adopted, if ever, as an ordinary change reviewed against current source, without provenance ceremony.

## Supersedes

2026-08-09 superseded [Use the direct source URL for upstream updates](2026-08-09-repository-governance-fork-policy-use-direct-source-url.md).

## Source anchors

- [LICENSE](../../../../../LICENSE)
- [Cargo.toml](../../../../../Cargo.toml)
- [README.md](../../../../../README.md)
- [dXn.SignalR skill](../../../../../.claude/skills/dxn-signalr/SKILL.md)
