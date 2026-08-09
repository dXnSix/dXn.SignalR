---
agentx:
  kind: state
  scope: repository-governance
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - fork-policy
    - agent-memory
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

RayX.SignalR is a RayX-owned hard fork of upstream `signalr-client`. Repository policy preserves upstream history while owning the `rayx-signalr` package identity, release direction, and local patch surface; publication remains disabled. AgentX repository configuration and source-derived memory routing are initialized.

## Backlinks

- Source anchors: [FORK.md](../../../../FORK.md), [Cargo.toml](../../../../Cargo.toml), [RayX.SignalR skill](../../../../.codex/skills/rayx-signalr/SKILL.md), [.agentx/agentx.config.json](../../../agentx.config.json).
