---
agentx:
  kind: decision
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-10-06T21:00:00+02:00"
  updated: "2026-10-06T21:00:00+02:00"
  topics:
    - liveness
    - lifecycle
    - pause
    - stateful
  supersedes: []
  superseded_by: []
---
## Decision: Share liveness and lifecycle state machines across targets, with platform-specific stateful pause

### Context: Mobile and browser hosts need dead-connection detection and explicit pause/resume around backgrounding; ASP.NET Core disposes a stateful connection whenever the client sends a WebSocket Close.

### Alternatives considered: Per-target ad-hoc timers and flags, relying only on server-side timeouts, and graceful closes for every pause.

### Reasoning: `liveness.rs` (keep-alive 15 s, server timeout 30 s, handshake 15 s, validated before I/O; one handshake parser) and the `Lifecycle` machine in `reconnection.rs` (Connected, Reconnecting, Paused, Disconnected, LocalClosed with a generation per run, exposed as `ConnectionStatus`) are shared by both targets. Reconnect runs are generation-checked; a local close is terminal in every state; every run that gives up ends the logical connection. Native stateful pause drops the transport without a Close so the server keeps the session; browsers cannot avoid the closing handshake, so a browser stateful pause ends the logical connection and `resume` negotiates a fresh one. `DefaultRetryPolicy` (0, 2, 10, 30 s) is opt-in.

### Trade-offs accepted: Browser stateful pause loses pending work; native resume works only while the server holds the disconnected connection.

### Supersedes: None.

### Source anchors: [liveness](../../../../../src/communication/liveness.rs), [lifecycle and policies](../../../../../src/communication/reconnection.rs), [native transport](../../../../../src/communication/client_tokio.rs), [browser transport](../../../../../src/communication/client_wasm.rs).
