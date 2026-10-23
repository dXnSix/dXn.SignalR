---
agentx:
  kind: knowledge
  scope: repository-governance/quality-gates
  status: current
  created: "2026-08-10T03:15:00+02:00"
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - local-parity
    - check-names
  supersedes: []
  superseded_by: []
---
# Knowledge

- The `WorkflowContract`, `Repository` and `DependencySecurity` .NET test categories verify workflow structure, repository hygiene and dependency security with the same `dotnet test` command locally and in CI.
- `dotnet/dXn.SignalR.E2E.Tests` (`--filter Category=E2E`) is the single runtime entry point used locally and by CI cells.
- Stable `quality / ...` and `e2e / ...` job names are documented in `README.md` for branch-protection configuration by repository owners.
