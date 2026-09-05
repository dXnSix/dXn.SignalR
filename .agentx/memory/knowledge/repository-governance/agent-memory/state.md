---
agentx:
  kind: state
  scope: repository-governance/agent-memory
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-09-06T01:49:01+02:00"
  topics:
    - agent-memory
    - configuration
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Use this state for memory governance questions; update it when that policy changes.

## Current Technical Shape

AgentX uses the repository configuration in `.agentx/agentx.config.json`; its default base is `../.agentx-worktrees`, namespaced as `RayX.SignalR`. The knowledge tree routes through `client-runtime`, `interop-validation`, and `repository-governance`.

Memory retrieval is conditional on the current question. Authorized implementation synchronizes affected durable memory; read-only audits report drift. Existing epic or experiment metadata does not select unrelated work.

## Backlinks

- [AGENTS.md](../../../../../AGENTS.md)
- 2026-09-06 current [contextual workflow decision](../../../decisions/repository-governance/agent-memory/2026-09-06-repository-governance-agent-memory-contextual-workflow-routing.md)
- [configuration](../../../../agentx.config.json)
- 2026-08-09 current [knowledge index](../../INDEX.md)
- 2026-08-09 current [taxonomy decision](../../../decisions/repository-governance/agent-memory/2026-08-09-repository-governance-agent-memory-initial-taxonomy.md)
