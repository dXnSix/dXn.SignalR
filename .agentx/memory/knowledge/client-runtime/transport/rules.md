---
agentx:
  kind: rules
  scope: client-runtime/transport
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - transport
    - security
    - cross-platform
  supersedes: []
  superseded_by: []
---
# Rules

- Preserve browser-specific authentication constraints when changing endpoint construction.
- Keep protocol transfer-format selection consistent across negotiation and WebSocket message handling.
- Validate secure and insecure native endpoint construction when changing transport setup.
- Preserve the ordinary/stateful transport split: never reuse a logical token in ordinary mode and never renegotiate or re-handshake during stateful resume.
