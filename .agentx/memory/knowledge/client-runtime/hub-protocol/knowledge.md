---
agentx:
  kind: knowledge
  scope: client-runtime/hub-protocol
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - messagepack
    - wire-names
  supersedes: []
  superseded_by: []
---
# Knowledge

- MessagePack outbound arguments preserve serializer-owned field names; transport-level recursive casing rewrites would corrupt compact schemas and Serde renames.
- The WASM handshake accepts the JSON response from either text or binary WebSocket frames before switching to protocol-specific message processing.
- Message type dispatch ultimately updates the same action storage for JSON text and MessagePack binary payloads.
