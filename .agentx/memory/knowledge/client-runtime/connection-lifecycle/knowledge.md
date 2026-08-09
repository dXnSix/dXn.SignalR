---
agentx:
  kind: knowledge
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - reconnection
    - ownership
  supersedes: []
  superseded_by: []
---
# Knowledge

- Native automatic reconnection runs only when no user disconnection handler is installed; a user handler receives manual reconnection control.
- The default reconnection policy is `NoReconnectPolicy`.
- Native reconnect reuses the action storage, endpoint, and selected protocol; explicit local close suppresses reconnect.
- Native and WASM disconnect decisions depend on shared-reference counts.
