---
agentx:
  kind: state
  scope: client-runtime/hub-protocol
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-07T13:30:00+02:00"
  topics:
    - handshake
    - json
    - messagepack
    - framing
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

The shared protocol layer models negotiation, handshake, invocation, completion, streaming, cancel, ping, close, Ack, and Sequence messages. JSON uses the SignalR record separator. Optional MessagePack uses varint length framing and binary Hub Protocol arrays; the initial handshake remains JSON. Outbound CancelInvocation is encoded as JSON type 5 or the framed MessagePack `[5, Headers, InvocationId]` array. Ack is cumulative, Sequence establishes the post-resume receive position, zero sequence IDs are rejected, and MessagePack callback results use protocol-aware Completion encoding. Void completions (JSON without `result`/`error`, MessagePack ResultKind 2) complete invocations whose result type accepts no value (such as `()`) and fail the others with `ErrorKind::Serialization`; error completions surface as `ErrorKind::Hub` with the server's message (through `ManualStream::error` for streams), results that do not decode into the requested type as `Serialization`, and malformed or unexpected frames as `Protocol`. Since S3 of `production-grade-client`, `src/protocol/codec.rs` decodes each inbound record once into an `InboundMessage` that borrows ids and targets and leaves payloads undecoded (a JSON `RawValue`, or the byte range of one MessagePack value found by a zero-copy marker reader); the awaiting action deserializes its payload once. ASP.NET Core's compact JSON stream item and result completion (`{"type":2|3,"invocationId":"…","item"|"result":…}`) are recognized by prefix and decoded in one pass with the record; a record carrying both `result` and `error` is the server's error on both paths. A malformed Close still closes without reconnect permission. Outbound messages are `OutboundMessage`s with their type set at construction; each argument is encoded once in the connection's protocol (MessagePack named maps copied verbatim), and `SerializedFrame` holds shared `bytes::Bytes`. MessagePack callback arguments decode from the original values (`byte[]` needs a bytes type such as `serde_bytes::ByteBuf`; `DateTime` is extension -1 via `rmp_serde`'s `_ExtStruct`; integer map keys survive). The protocol modules deny `unwrap`/`expect`/`panic`/indexing outside tests and the `robustness_` proptest group fuzzes the decoders.

## Backlinks

- Source anchors: [src/protocol](../../../../../src/protocol), [src/client/client.rs](../../../../../src/client/client.rs).
