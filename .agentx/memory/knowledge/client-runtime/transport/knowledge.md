---
agentx:
  kind: knowledge
  scope: client-runtime/transport
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - negotiation
    - authentication
  supersedes: []
  superseded_by: []
---
# Knowledge

- Negotiate uses Basic or Bearer authorization headers through `ehttp`.
- The negotiated endpoint appends a Bearer `access_token` query parameter because browser WebSocket APIs cannot set custom headers.
- Only the WebSockets transport is accepted; transfer format must match JSON text or MessagePack binary.
