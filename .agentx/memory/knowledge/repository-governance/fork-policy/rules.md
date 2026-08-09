---
agentx:
  kind: rules
  scope: repository-governance/fork-policy
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T22:04:26+02:00"
  topics:
    - fork
    - compatibility
    - remote-policy
  supersedes: []
  superseded_by: []
---
# Rules

- Preserve the `rayx-signalr` Cargo package, `rayx_signalr` crate, and RayX repository identity across upstream imports.
- Keep `origin` as the only persistent remote, push only to `origin`, and never reset/rebase/mirror the fork onto upstream state.
- Discover and fetch exact upstream commits directly from `https://github.com/danielleiszen/rust_signalr_client.git`; do not configure an `upstream` remote or push to the source URL.
- Base RayX integration and upstream-import branches on `master`; do not treat the source repository's `main` branch as the RayX integration branch.
- Do not publish the package until an explicit RayX registry and release policy exists.
- Keep upstream imports and fork-local repairs reviewable with exact source-commit provenance.
- Do not present items under `FORK.md` required hardening as already implemented.
