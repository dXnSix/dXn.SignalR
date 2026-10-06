---
agentx:
  kind: knowledge
  scope: repository-governance/fork-policy
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T21:10:00+02:00"
  topics:
    - provenance
    - history
  supersedes: []
  superseded_by: []
---
# Knowledge

- Until 2026-10-06 the repository was a hard fork with an upstream-import workflow (`FORK.md`, a `.codex` maintenance skill, provenance lines in import commits). Those artifacts were removed; older commits still carry `Source upstream commit:` provenance lines.
- A typed public error model remains follow-up hardening; see the limitations chapter in [docs](../../../../../docs/12-limitations.md).
