---
agentx:
  kind: state
  scope: client-runtime/transport
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T23:30:00+02:00"
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

Shared `HttpClient` posts to SignalR negotiate version 1, selects WebSockets with the protocol-required transfer format, and constructs connection data. Stateful opt-in adds `useStatefulReconnect=true` to negotiation and retains the negotiated connection token across physical transport replacement. Negotiation (`src/communication/negotiation.rs`, shared by both targets over an injected request function) classifies each response as connection, redirect (`url` + optional `accessToken`, up to 100, queries and the stateful flag kept) or `error`, lets a redirect token replace the configured credentials for the next negotiate and the transport (`ConnectionRecipe::transport_authorization`), and derives the ws/wss URL from the final URL with percent-encoded `id`/`access_token`; `skip_negotiation()` connects directly (stateful + skip fails before I/O). Ordinary reconnect renegotiates from the configured endpoint; stateful resume reuses the negotiated transport URL. Native negotiate is one hyper HTTP/1.1 POST over the connection's own TCP or rustls stream (non-2xx errors carry a bounded, credential-redacted body excerpt); the native WebSocket is `tokio-websockets` with `Connector::Rustls`. Each secure native connection builds one rustls `ClientConfig` at `connect` (`src/communication/tls.rs`) with an explicit provider (`tls-aws-lc-rs` over `tls-ring`) and verifier (`tls-platform-verifier` by default, `tls-webpki-roots` bundled roots, `use_webpki_roots()` per connection; Android extra roots switch to webpki) and shares it with every negotiate, WebSocket connect and reconnect; extra PEM roots are validated before I/O and builds without TLS features reject secure endpoints. Native clients send `Authorization` on the WebSocket upgrade. Native negotiate and WebSocket connections open their own TCP stream (`negotiate_http::connect_tcp`): every resolved address is tried in turn and `TCP_NODELAY` is set, so small frames (stateful Acks, Pings) never wait for delayed TCP acknowledgements; the stream is then wrapped with the connection's rustls connector and handed to `tokio-websockets` with `connect_on`. Connection setup on both targets (each negotiate request; natively TCP, TLS and the WebSocket upgrade; in browsers opening the WebSocket) is bounded by the configurable `with_connect_timeout` (`LivenessConfig::connect_timeout`, default 120 s like the official .NET client's negotiate bound, validated nonzero before I/O); the SignalR handshake keeps its own `handshake_timeout`. Browser negotiate uses `gloo-net` fetch on the global scope with an `AbortController` that aborts the request on timeout or when the caller stops waiting. Setup errors are typed: negotiate 401/403 are `Authentication` and other non-2xx `Negotiate` (both with `status()`), a negotiate `error` answer, unreadable body or redirect overflow is `Negotiate` without a status, a server without a usable transport is `TransportUnavailable`, exceeding the bound is `Timeout` naming `Negotiate` or `Connect`, an untrusted certificate is `Tls`, and DNS/TCP failures are `Transport`. The browser transport (`client_wasm.rs` + `browser_socket.rs`) is event-driven: a small `web-sys` WebSocket wrapper whose listeners are detached with it feeds one generation-checked receiver task per socket; timers are cancellable `gloo-timers` futures; binary frames are copied out of shared wasm memory before `send` (threaded builds); nothing touches `window`, so it runs on the main thread, in Web Workers and in threaded (atomics) builds, with a `!Send` client per thread or worker.

## Backlinks

- Source anchors: [src/communication/common.rs](../../../../../src/communication/common.rs), [src/communication/client_tokio.rs](../../../../../src/communication/client_tokio.rs), [src/communication/client_wasm.rs](../../../../../src/communication/client_wasm.rs).
- 2026-10-06 current [Rustls per connection and hyper negotiate](../../../decisions/client-runtime/transport/2026-10-06-client-runtime-transport-rustls-per-connection-tls.md).
- 2026-10-06 current [Event-driven browser transport](../../../decisions/client-runtime/transport/2026-10-06-client-runtime-transport-event-driven-browser-transport.md).
