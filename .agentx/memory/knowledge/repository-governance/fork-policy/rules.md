---
agentx:
  kind: rules
  scope: repository-governance/fork-policy
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:58:45+02:00"
  topics:
    - fork
    - compatibility
    - remote-policy
  supersedes: []
  superseded_by: []
---
# Rules

- Preserve the `rayx-signalr` Cargo package, `rayx_signalr` crate, and RayX repository identity across upstream imports.
- Fetch only from `upstream`, push only to `origin`, and never reset/rebase/mirror the fork onto upstream state.
- Base RayX integration and upstream-import branches on `master`; do not rename or treat `upstream/main` as the RayX integration branch.
- Do not publish the package until an explicit RayX registry and release policy exists.
- Keep upstream imports and fork-local repairs reviewable with exact source-commit provenance.
- Do not present items under `FORK.md` required hardening as already implemented.
