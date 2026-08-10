---
agentx:
  kind: rules
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - reconnection
    - cross-platform
  supersedes: []
  superseded_by: []
---
# Rules

- Preserve the distinction between local close, terminal remote close, reconnecting, connected, and never-opened states on both native and WASM.
- Test manual-handler and automatic-policy modes independently on both targets.
- Prove ordinary and stateful reconnect separately: ordinary reconnect negotiates and handshakes afresh with no invocation replay; stateful reconnect keeps the logical token, skips the second handshake, and replays only buffered serialized frames.
- Keep reconnect completion generation-checked so stale attempts cannot replace a newer connection.
