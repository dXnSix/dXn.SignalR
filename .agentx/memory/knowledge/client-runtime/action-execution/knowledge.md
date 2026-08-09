---
agentx:
  kind: knowledge
  scope: client-runtime/action-execution
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - storage
    - completion
  supersedes: []
  superseded_by: []
---
# Knowledge

- Completion messages update and then remove the matching action; stream items update without removal until completion.
- Server invocations route by target to callback actions.
- Storage is cleared only when the final shared storage owner is dropped.
- Platform-specific containers share the same `Storage` behavior contract.
