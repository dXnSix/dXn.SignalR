---
agentx:
  kind: state
  scope: client-runtime/transport
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T05:00:00+02:00"
  topics:
    - http
    - websocket
    - authentication
    - tokio
    - wasm
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

Shared `HttpClient` posts to SignalR negotiate version 1, selects WebSockets with the protocol-required transfer format, and constructs connection data. Stateful opt-in adds `useStatefulReconnect=true` to negotiation and retains the negotiated connection token across physical transport replacement. Native negotiate is one hyper HTTP/1.1 POST over the connection's own TCP or rustls stream (non-2xx errors carry a bounded, credential-redacted body excerpt); the native WebSocket is `tokio-websockets` with `Connector::Rustls`. Each secure native connection builds one rustls `ClientConfig` at `connect` (`src/communication/tls.rs`) with an explicit provider (`tls-aws-lc-rs` over `tls-ring`) and verifier (`tls-platform-verifier` by default, `tls-webpki-roots` bundled roots, `use_webpki_roots()` per connection; Android extra roots switch to webpki) and shares it with every negotiate, WebSocket connect and reconnect; extra PEM roots are validated before I/O and builds without TLS features reject secure endpoints. Native clients send `Authorization` on the WebSocket upgrade. Browser negotiate stays on `ehttp` (browser-only) and the browser transport uses `wasm-sockets::PollingClient` on a JavaScript interval.

## Backlinks

- Source anchors: [src/communication/common.rs](../../../../../src/communication/common.rs), [src/communication/client_tokio.rs](../../../../../src/communication/client_tokio.rs), [src/communication/client_wasm.rs](../../../../../src/communication/client_wasm.rs).
