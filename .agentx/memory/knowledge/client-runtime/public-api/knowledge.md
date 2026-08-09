---
agentx:
  kind: knowledge
  scope: client-runtime/public-api
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - invocation
    - streaming
    - callbacks
  supersedes: []
  superseded_by: []
---
# Knowledge

- Invocation and streaming methods allocate action keys from the target name plus a monotonically increasing index.
- Callback registration returns a handler whose consuming `unregister` removes the target from shared action storage.
- `SignalRClient` clones share the transport state and action storage.
