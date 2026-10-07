---
agentx:
  kind: state
  scope: repository-governance/fork-policy
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T23:30:00+02:00"
  topics:
    - provenance
    - project-identity
    - attribution
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work on package identity, provenance, attribution, remotes, or publishing; update it when that policy changes.

## Current Technical Shape

RayX.SignalR is an independent library. It started from `signalr-client` `d32bd04` (`v0.3.2`), whose Git history is retained, and has no upstream-import workflow. The package is `rayx-signalr`, the crate `rayx_signalr`, version `0.2.0` (the typed error model was the breaking change from 0.1), `publish = false`. Attribution to the original author stays in `LICENSE` (MIT copyright notice), the `Cargo.toml` authors, and the README acknowledgements. `origin` is the only remote; `master` is the default and integration branch.

## Backlinks

- Source anchors: [LICENSE](../../../../../LICENSE), [Cargo.toml](../../../../../Cargo.toml), [README.md](../../../../../README.md).
- 2026-10-06 current [Independent library decision](../../../decisions/repository-governance/fork-policy/2026-10-06-repository-governance-fork-policy-independent-library.md).
