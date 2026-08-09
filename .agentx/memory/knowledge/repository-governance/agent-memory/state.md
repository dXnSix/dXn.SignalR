---
agentx:
  kind: state
  scope: repository-governance/agent-memory
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - agent-memory
    - configuration
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

AgentX uses the repository configuration in `.agentx/agentx.config.json`; its default base is `../.agentx-worktrees`, namespaced as `RayX.SignalR`. The knowledge tree routes through `client-runtime`, `interop-validation`, and `repository-governance`.

## Backlinks

- [configuration](../../../../agentx.config.json)
- 2026-08-09 current [knowledge index](../../INDEX.md)
- 2026-08-09 current [taxonomy decision](../../../decisions/repository-governance/agent-memory/2026-08-09-repository-governance-agent-memory-initial-taxonomy.md)
