---
agentx:
  kind: knowledge
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - reconnection
    - ownership
  supersedes: []
  superseded_by: []
---
# Knowledge

- Automatic reconnection runs only when no user disconnection handler is installed; native and WASM user handlers receive manual reconnection control.
- The default reconnection policy is `NoReconnectPolicy`.
- Ordinary reconnect reuses callbacks, endpoint, protocol, and action storage, but terminates pending invocations/streams and never replays them.
- Stateful reconnect preserves the negotiated connection token, sends Sequence before replay, and blocks new tracked sends until ordered replay completes.
- Explicit local close and `Close { allowReconnect: false }` suppress reconnect on both targets.
- Native and WASM disconnect decisions depend on shared-reference counts.
