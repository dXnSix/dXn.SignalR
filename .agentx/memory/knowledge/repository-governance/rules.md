---
agentx:
  kind: rules
  scope: repository-governance
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - governance
  supersedes: []
  superseded_by: []
---
# Rules

- Inspect current source and Git state before applying fork or memory guidance.
- Keep product behavior out of repository-governance memories unless it establishes a cross-cutting repository policy.
- Preserve least-privilege workflow permissions, stable check names, failure-only redacted artifacts, and the no-publishing boundary unless the repository owner explicitly authorizes a governance change.
