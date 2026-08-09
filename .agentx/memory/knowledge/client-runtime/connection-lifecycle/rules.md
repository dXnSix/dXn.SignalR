---
agentx:
  kind: rules
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - reconnection
    - cross-platform
  supersedes: []
  superseded_by: []
---
# Rules

- State native and WASM lifecycle support separately; reconnection is not currently cross-platform.
- Preserve the distinction between local close, remote close, reconnecting, and never-opened native states.
- Test manual-handler and automatic-policy modes independently when changing native reconnection.
