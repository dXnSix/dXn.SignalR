---
agentx:
  kind: rules
  scope: client-runtime/hub-protocol
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
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
- Keep Ack cumulative and Sequence nonzero, and validate JSON and MessagePack control frames with the same logical assertions.
- Encode client-result callbacks with the selected hub protocol; a MessagePack connection must not send JSON Completion payloads.
