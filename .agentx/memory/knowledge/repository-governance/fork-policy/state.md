---
agentx:
  kind: state
  scope: repository-governance/fork-policy
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T22:04:26+02:00"
  topics:
    - fork
    - provenance
    - compatibility
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

The fork baseline is upstream `d32bd04` (`v0.3.2`). RayX owns the `rayx-signalr` package and `rayx_signalr` crate identity, starting at version `0.1.0`; `publish = false`. `origin` is the only persistent remote and its default/integration branch is `master`. Upstream candidates are discovered and exact commits are fetched directly from `https://github.com/danielleiszen/rust_signalr_client.git` without configuring another remote.

## Backlinks

- Source anchors: [FORK.md](../../../../../FORK.md), [Cargo.toml](../../../../../Cargo.toml), [README.md](../../../../../README.md), [maintenance skill](../../../../../.codex/skills/rayx-signalr/SKILL.md), [upstream workflow](../../../../../.codex/skills/rayx-signalr/references/update-from-upstream.md).
- 2026-08-09 current [Direct source URL decision](../../../decisions/repository-governance/fork-policy/2026-08-09-repository-governance-fork-policy-use-direct-source-url.md).
