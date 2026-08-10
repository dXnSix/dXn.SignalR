---
agentx:
  kind: knowledge
  scope: repository-governance
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - governance
  supersedes: []
  superseded_by: []
---
# Knowledge

- Fork policy and AgentX memory governance are cross-cutting repository concerns; product behavior belongs under `client-runtime`.
- The checked-in source and project files remain authoritative over these memories.
- Stable CI check names and exact local reproduction commands are part of the repository governance contract; publishing and branch-protection changes still require separate owner authorization.
