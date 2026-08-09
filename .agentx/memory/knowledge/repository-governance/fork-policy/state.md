---
agentx:
  kind: state
  scope: repository-governance/fork-policy
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:58:45+02:00"
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

The fork baseline is upstream `d32bd04` (`v0.3.2`). RayX owns the `rayx-signalr` package and `rayx_signalr` crate identity, starting at version `0.1.0`; `publish = false`. `origin` is the RayX repository, its default/integration branch is `master`, and `upstream` remains fetch-only/read-only with its independent `upstream/main` branch.

## Backlinks

- Source anchors: [FORK.md](../../../../../FORK.md), [Cargo.toml](../../../../../Cargo.toml), [README.md](../../../../../README.md), [maintenance skill](../../../../../.codex/skills/rayx-signalr/SKILL.md), [upstream workflow](../../../../../.codex/skills/rayx-signalr/references/update-from-upstream.md).
