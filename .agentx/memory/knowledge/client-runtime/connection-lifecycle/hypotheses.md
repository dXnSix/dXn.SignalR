---
agentx:
  kind: hypotheses
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - wasm
    - reconnection
  supersedes: []
  superseded_by: []
---
# Hypotheses

- WASM reconnection may need a transport-specific state machine rather than a direct port of the Tokio task/lock design; browser runtime evidence is required.
