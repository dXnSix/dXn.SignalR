---
agentx:
  kind: decision
  scope: client-runtime/transport
  status: current
  created: "2026-10-09T18:00:00+02:00"
  updated: "2026-10-09T18:00:00+02:00"
  topics:
    - transports
    - websocket
    - long-polling
    - server-sent-events
    - browser
  supersedes:
    - 2026-10-06-client-runtime-transport-event-driven-browser-transport.md
  superseded_by: []
---
## Decision: Run WebSockets, Server-Sent Events and Long Polling behind one transport abstraction per target, selected as the .NET client selects them

### Context: The client accepted only WebSockets and the browser transport was a WebSocket-only event loop; the official .NET client negotiates and falls back across three transports (epic `production-grade-client`, D5, R5-R7).

### Alternatives considered: A `dyn Transport` trait object; separate connection loops per transport; HTTP transports through `reqwest` or `gloo-net`.

### Reasoning: A per-target `Transport` enum (static dispatch, no `async_trait` allocation) hands the shared connection loop a sender/receiver pair, so the handshake, liveness, stateful buffer, lifecycle and reconnect code stays transport-independent. Natively the HTTP transports use the crate's own hyper client over its TCP/TLS/proxy connector (one TLS configuration per connection, proxies for every transport); in browsers each transport is one event-driven task (`browser_socket.rs`, an `EventSource` wrapper, `fetch` with `AbortController`). Selection keeps the server's `availableTransports` order filtered by the configured set and transfer format, and falls back with a fresh negotiation per failed start.

### Trade-offs accepted: The crate owns more HTTP and browser glue. Server-Sent Events carry text only, so MessagePack never uses them. Stateful reconnect runs only over WebSockets.

### Supersedes: [Run one event-driven task per browser socket](2026-10-06-client-runtime-transport-event-driven-browser-transport.md): its WebSocket design continues as one of the browser transports; the browser negotiate `fetch` is now bounded by the connect timeout.

### Source anchors: [transport](../../../../../src/communication/transport.rs), [negotiation](../../../../../src/communication/negotiation.rs), [HTTP transports](../../../../../src/communication/http_transport_tokio.rs), [browser HTTP](../../../../../src/communication/browser_http.rs).
