---
agentx:
  kind: rules
  scope: repository-governance/fork-policy
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T21:10:00+02:00"
  topics:
    - project-identity
    - attribution
    - remote-policy
  supersedes: []
  superseded_by: []
---
# Rules

- Preserve the `rayx-signalr` package and `rayx_signalr` crate identity.
- Do not import or sync `signalr-client` upstream; adopt an outside idea only as an ordinary change reviewed against current source.
- Keep the original author's MIT copyright notice in `LICENSE` and the attribution in `Cargo.toml` and the README.
- Keep `origin` as the only persistent remote and integrate on `master`.
- Do not publish the package until an explicit registry and release policy exists.
