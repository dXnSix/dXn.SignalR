---
agentx:
  kind: state
  scope: client-runtime/hub-protocol
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
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

The shared protocol layer models negotiation, handshake, invocation, completion, streaming, cancel, ping, and close messages. JSON uses the SignalR record separator. Optional MessagePack uses varint length framing and binary Hub Protocol arrays; the initial handshake remains JSON.

## Backlinks

- Source anchors: [src/protocol](../../../../../src/protocol), [src/client/client.rs](../../../../../src/client/client.rs).
