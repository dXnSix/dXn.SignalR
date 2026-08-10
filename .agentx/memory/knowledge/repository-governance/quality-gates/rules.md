---
agentx:
  kind: rules
  scope: repository-governance/quality-gates
  status: current
  created: "2026-08-10T03:15:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - ci
    - least-privilege
  supersedes: []
  superseded_by: []
---
# Rules

- Keep workflow and local verifier commands behaviorally aligned; a CI-only green path is insufficient.
- Preserve `contents: read`, disabled credential persistence, no secrets, no publishing, and failure-only redacted artifacts.
- Do not reduce the native OS, pinned/stable Rust, default/all-feature WASM, JSON/MessagePack, or ordinary/stateful reconnect matrices to obtain green checks.
- Resolve warnings through source or precise feature gating; do not add global warning or Clippy suppression.
