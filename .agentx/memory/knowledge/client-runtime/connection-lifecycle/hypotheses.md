---
agentx:
  kind: hypotheses
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-07T18:00:00+02:00"
  topics:
    - wasm
    - reconnection
  supersedes: []
  superseded_by: []
---
# Hypotheses

- Open (P1 review, 2026-10-07; addressed in code by P2): a client configured for stateful reconnect whose fresh negotiation the server answers without stateful reconnect used to diverge between targets. Since P2 both targets treat a disposed buffer as "no stateful logical connection" (created disposed, renewed only when the negotiation accepts stateful reconnect), so the connection stays ordinary. No test against a server that declines stateful reconnect proves it yet; close this with such a test in the stateful-reconnect stability work.
