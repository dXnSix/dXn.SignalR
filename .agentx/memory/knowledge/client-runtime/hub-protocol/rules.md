---
agentx:
  kind: rules
  scope: client-runtime/hub-protocol
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - wire-compatibility
    - messagepack
  supersedes: []
  superseded_by: []
---
# Rules

- Preserve application-defined Serde wire names in outbound MessagePack values.
- Keep handshake handling JSON even when the negotiated hub protocol is MessagePack.
- Add or update frame-level tests for any wire-format change; do not rely only on round-tripping through the same implementation.
