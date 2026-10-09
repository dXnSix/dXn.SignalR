---
agentx:
  kind: decision
  scope: client-runtime/transport
  status: superseded
  created: "2026-10-06T21:00:00+02:00"
  updated: "2026-10-09T18:00:00+02:00"
  topics:
    - wasm
    - browser
    - websocket
    - workers
  supersedes: []
  superseded_by:
    - 2026-10-09-client-runtime-transport-one-transport-abstraction-per-target.md
---
## Decision: Run one event-driven task per browser socket over a minimal web-sys wrapper

### Context: The inherited browser transport polled with `setInterval`, leaked closures, needed `window`, and broke in Web Workers and threaded (atomics) builds.

### Alternatives considered: Keeping the polling client, `gloo-net` WebSockets (its binary send passes a view of shared memory, which browsers reject in threaded builds), and per-concern tasks.

### Reasoning: `browser_socket.rs` wraps `web_sys::WebSocket` with listeners detached on drop and copies binary frames into a non-shared buffer before sending; each socket has one `spawn_local` task that processes frames, sends keep-alive Pings and enforces the server timeout on the monotonic `performance.now()` clock, and checks the lifecycle generation so a stale socket never affects a newer connection. Nothing touches `window`, so the client works on the main thread, in workers and in threaded builds (`!Send`: one client per thread or worker).

### Trade-offs accepted: A small amount of `web-sys` glue owned by the crate; browser negotiate `fetch` is not bounded by a timeout.

### Supersedes: None.

### Source anchors: [browser socket](../../../../../src/communication/browser_socket.rs), [browser transport](../../../../../src/communication/client_wasm.rs).
