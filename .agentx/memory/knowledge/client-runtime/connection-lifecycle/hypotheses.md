---
agentx:
  kind: hypotheses
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-07T15:00:00+02:00"
  topics:
    - wasm
    - reconnection
  supersedes: []
  superseded_by: []
---
# Hypotheses

- Open (P1 review, 2026-10-07): a client configured for stateful reconnect whose fresh renegotiation the server answers without stateful reconnect diverges between targets: in the browser the replay buffer stays disposed and later sends fail with `Canceled`; natively the buffer is renewed and tracks frames the server never acknowledges. Not yet reproduced; belongs to the stateful-reconnect stability work.
