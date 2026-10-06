---
agentx:
  kind: state
  scope: repository-governance
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T19:00:00+02:00"
  topics:
    - fork-policy
    - agent-memory
    - quality-gates
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Use this state when governance context is relevant; update it when the current governance boundary changes.

## Current Technical Shape

RayX.SignalR is a RayX-owned hard fork of upstream `signalr-client`. Repository policy preserves upstream history while owning the `rayx-signalr` package identity, release direction, and local patch surface; publication remains disabled. AgentX repository configuration and source-derived memory routing are initialized. Production quality and interop workflows now enforce the Rust 1.92 baseline, cross-platform/feature checks, dependency scans, and the Aspire end-to-end catalog on native, browser (Chromium, Firefox, WebKit), Android-emulator and iOS-simulator targets with read-only permissions.

Root contributor guidance routes requested workflows and retains mechanical completion gates; specialized procedures live in the active skills.

## Backlinks

- [AGENTS.md](../../../../AGENTS.md)

- Source anchors: [FORK.md](../../../../FORK.md), [Cargo.toml](../../../../Cargo.toml), [workflows](../../../../.github/workflows), [RayX.SignalR skill](../../../../.codex/skills/rayx-signalr/SKILL.md), [.agentx/agentx.config.json](../../../agentx.config.json).
