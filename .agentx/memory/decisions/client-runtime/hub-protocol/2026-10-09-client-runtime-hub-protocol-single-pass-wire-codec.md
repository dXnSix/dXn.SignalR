---
agentx:
  kind: decision
  scope: client-runtime/hub-protocol
  status: current
  created: "2026-10-09T18:00:00+02:00"
  updated: "2026-10-09T18:00:00+02:00"
  topics:
    - codec
    - json
    - messagepack
    - performance
  supersedes: []
  superseded_by: []
---
## Decision: Decode every inbound frame once into borrowed messages and build outbound frames from typed values

### Context: The receive path parsed each JSON message up to three times and MessagePack frames into `rmpv` values more than once, and every outbound frame was re-parsed to learn its type (`production-grade-client` D2, R2).

### Alternatives considered: Keeping `rmpv`/`serde_json::Value` trees with caching; a hand-written full-message parser.

### Reasoning: `protocol::codec` yields `InboundMessage<'a>` with undecoded payloads (`RawValue` for JSON, byte ranges for MessagePack) that the waiting action deserializes once into its awaited type; outbound messages know their `MessageType`, so stateful tracking never re-parses, and MessagePack frames are written in one buffer with the length prefix reserved in front. Serde stays the only deserialization mechanism, so application Serde names are preserved. Measured against the official .NET client in the same run: server streams 1.5-2.6x, fan-in about 1.4x, latency and send throughput on par (`docs/13-architecture.md`).

### Trade-offs accepted: Lifetimes in the decode path; payloads appear in logs only at `trace`.

### Supersedes: None.

### Source anchors: [codec](../../../../../src/protocol/codec.rs), [MessagePack](../../../../../src/protocol/msgpack.rs).
