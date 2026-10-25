---
agentx:
  kind: state
  scope: repository-governance
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: 2026-10-23T22:00:00+02:00
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

dXn.SignalR is an independent dXn library (package `dxn-signalr`, crate `dxn_signalr`) that started from `signalr-client` and keeps its original author's attribution; publication remains disabled. AgentX repository configuration and source-derived memory routing are initialized. Production quality and interop workflows enforce the Rust 1.97 baseline, cross-platform/feature checks, dependency scans, and the Aspire end-to-end catalog on native, browser (Chromium, Firefox, WebKit), Android-emulator and iOS-simulator targets with read-only permissions.

Root contributor guidance (`AGENTS.md`, mirrored in `CLAUDE.md`) carries the layout, validation and AgentX rules; library usage, platform setup and validation routing live in the repository's `dxn-signalr` skill, and long-form documentation in `docs/`.

## Backlinks

- [AGENTS.md](../../../../AGENTS.md)

- Source anchors: [Cargo.toml](../../../../Cargo.toml), [LICENSE](../../../../LICENSE), [workflows](../../../../.github/workflows), [dXn.SignalR skill](../../../../.claude/skills/dxn-signalr/SKILL.md), [docs](../../../../docs/README.md), [.agentx/agentx.config.json](../../../agentx.config.json).
- 2026-10-06 current [Independent library decision](../../decisions/repository-governance/fork-policy/2026-10-06-repository-governance-fork-policy-independent-library.md).
