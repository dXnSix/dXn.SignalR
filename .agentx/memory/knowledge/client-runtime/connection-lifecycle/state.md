---
agentx:
  kind: state
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T16:00:00+02:00"
  topics:
    - connect
    - disconnect
    - reconnection
    - tokio
    - wasm
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

Connection setup negotiates first, opens a WebSocket, completes the SignalR handshake, and exposes transport-owned storage. Liveness (`src/communication/liveness.rs`, shared): both targets read the handshake response within `handshake_timeout` (default 15 s; native skips WebSocket Ping/Pong frames while waiting) and validate it with one parser (an `error` field, a hub message, an empty or malformed response fail `connect`; hub messages sent in the same frame are kept for the receiver); a keep-alive Ping (JSON `{"type":6}` record, MessagePack framed `[6]`, never tracked for stateful replay) goes out after `keep_alive_interval` (default 15 s) without outbound frames, including during native stateful backpressure; silence for `server_timeout` (default 30 s) is a dropped transport (reconnect allowed, policy or manual handler), never a local close. Natively the writer task sends Pings and the receiver waits with `timeout_at` on a shared tracker clocked by `tokio::time::Instant`; in the browser the per-socket receiver task also drives Pings and the server timeout on the monotonic `performance.now()` clock (one task per socket instead of a separate liveness task). Invalid durations (zero, or server timeout not greater than keep-alive) fail `connect` before I/O. Natively every connection is bound to a Tokio runtime (`with_runtime_handle`, else the ambient runtime at `connect`, else an error before I/O); negotiate, connect, reconnects, policy delays and dropped-stream cancellation run on it, each physical connection has a runtime-owned writer task (owns the sink, stateful tracking/backpressure, Acks on a priority channel) and receiver task, and the public futures only wait on runtime-agnostic primitives, so they can be awaited from any executor. The stateful buffer's transport generation stops writers of a lost transport from tracking frames after a resume. Native Tokio and browser WASM share one lifecycle machine (`Lifecycle` in `reconnection.rs`: Connected, Reconnecting, Paused, Disconnected, LocalClosed with a generation per reconnect; native wraps it in `ReconnectCoordinator`, the browser in a `Cell`), exposed as public `ConnectionStatus` via `SignalRClient::status()`, plus no-retry/constant/linear/exponential/`DefaultRetryPolicy` (0, 2, 10, 30 s) policies, automatic reconnect, and manual handler control; automatic, manual and resume runs share one generation-checked attempt loop (`FirstAttempt` decides whether the first attempt waits for the policy). `pause()` supersedes any running reconnect and ends the transport (stopping keep-alive and automatic reconnect); sends fail while paused; `resume()` from Paused or Disconnected attempts immediately, then follows the policy; repeated pause/resume are no-ops and `Closed` rejects both. Ordinary pause closes with a WebSocket Close and fails pending work with "The connection is paused". Stateful pause: native drops the transport without a Close (the writer stops cooperatively, so a frame already in the replay buffer is never reported failed) and the server keeps the logical connection while it holds the disconnected connection; browsers cannot avoid the closing handshake, so a browser stateful pause ends the logical connection (all pending work fails). On both targets a disposed stateful buffer (browser pause, terminal close, exhausted automatic or resume attempts) makes the next attempt negotiate a fresh stateful connection (`StatefulMessageBuffer::renew`, new transport URL); a resume run that gives up ends the logical connection like exhausted automatic reconnect. Native lifecycle transitions and connection-state writes happen under the state lock, a dropped transport is ignored unless its connection generation is current, and reconnect runs re-check generation ownership after every delay and before each attempt. Ordinary reconnect performs fresh negotiation plus one handshake and fails pending work without replay. Opt-in stateful reconnect resumes the same logical connection token without a second handshake, replays bounded serialized frames, and suppresses duplicates. Local close and terminal server close suppress reconnect. A server Close without reconnect permission fails pending work, moves the coordinator to `Disconnected` (`remote_close`, invalidating in-flight generations) and, in manual mode, still calls the disconnection handler with `ReconnectionHandler::server_allows_reconnect() == false`, whose `reconnect`/`reconnect_with_policy` then refuse. In manual mode a reconnectable ordinary drop fails pending work before the handler runs (stateful pending work waits for resume). Pending invocations always complete: `fail_pending` and action disposal complete them with an error and end open streams.

## Backlinks

- Source anchors: [src/client/client.rs](../../../../../src/client/client.rs), [src/communication/reconnection.rs](../../../../../src/communication/reconnection.rs), [src/communication/client_tokio.rs](../../../../../src/communication/client_tokio.rs), [src/communication/client_wasm.rs](../../../../../src/communication/client_wasm.rs).
