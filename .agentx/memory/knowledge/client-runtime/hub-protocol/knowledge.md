---
agentx:
  kind: knowledge
  scope: client-runtime/hub-protocol
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-07T13:30:00+02:00"
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
- Most of the JSON inbound cost after S3 is the payload's own deserialization; a `RawValue` capture is a second full scan of the payload, which is why the compact ASP.NET Core shape decodes the payload together with the record. Benchmark frames must use ASP.NET Core's member order (`serde_json::json!` sorts members) or they bypass that path.
- `rmp_serde` decodes MessagePack `bin` only into types that accept bytes; `Vec<u8>` expects an array of numbers.
- JSON and MessagePack stateful control frames represent the same nonzero sequence contract; MessagePack arrays use message types 8 (Ack) and 9 (Sequence).
- The receive side emits cumulative Ack values only after accepting ordered tracked messages; duplicate messages are suppressed and future gaps fail closed.
