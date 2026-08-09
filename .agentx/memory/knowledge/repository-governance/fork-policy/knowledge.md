---
agentx:
  kind: knowledge
  scope: repository-governance/fork-policy
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - fork
    - hardening
  supersedes: []
  superseded_by: []
---
# Knowledge

- The full upstream Git history is retained while RayX owns package identity, API direction, platform support, and release/version policy.
- `origin` points to `rayx-ui/RayX.SignalR`; `upstream` tracks the source repository and has push disabled.
- Upstream imports use an explicit source SHA, preserve provenance in commit bodies, and reconcile the RayX patch surface instead of rebasing, resetting, or mirroring.
- Typed errors, explicit cross-platform lifecycle behavior, stronger native/browser integration coverage, golden frames, and CI gates remain required hardening work; they are constraints, not current capability claims.
