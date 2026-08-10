---
agentx:
  kind: knowledge
  scope: repository-governance/quality-gates
  status: current
  created: "2026-08-10T03:15:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - local-parity
    - check-names
  supersedes: []
  superseded_by: []
---
# Knowledge

- `scripts/ci/verify-workflows.ps1` verifies workflow structure and reproduces quality/security commands locally.
- `scripts/ci/run-interop.ps1` is the single runtime entry point used locally and by CI matrix cells.
- Stable `quality / ...` and `interop / ...` job names are documented in `README.md` for branch-protection configuration by repository owners.
