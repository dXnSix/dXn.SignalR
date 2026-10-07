---
agentx:
  kind: state
  scope: client-runtime/hub-protocol
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T23:30:00+02:00"
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

The shared protocol layer models negotiation, handshake, invocation, completion, streaming, cancel, ping, close, Ack, and Sequence messages. JSON uses the SignalR record separator. Optional MessagePack uses varint length framing and binary Hub Protocol arrays; the initial handshake remains JSON. Outbound CancelInvocation is encoded as JSON type 5 or the framed MessagePack `[5, Headers, InvocationId]` array. Ack is cumulative, Sequence establishes the post-resume receive position, zero sequence IDs are rejected, and MessagePack callback results use protocol-aware Completion encoding. Void completions (JSON without `result`/`error`, MessagePack ResultKind 2) complete invocations whose result type accepts no value (such as `()`) and fail the others with `ErrorKind::Serialization`; error completions surface as `ErrorKind::Hub` with the server's message (through `ManualStream::error` for streams), results that do not decode into the requested type as `Serialization`, and malformed or unexpected frames as `Protocol`.

## Backlinks

- Source anchors: [src/protocol](../../../../../src/protocol), [src/client/client.rs](../../../../../src/client/client.rs).
