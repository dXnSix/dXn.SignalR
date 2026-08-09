---
agentx:
  kind: knowledge
  scope: client-runtime
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - architecture
    - cross-platform
  supersedes: []
  superseded_by: []
---
# Knowledge

- Platform-specific ownership is isolated primarily in communication clients and action storage; protocol and high-level client behavior are shared.
- `MaybeSend` and `MaybeSendSync` enforce native thread bounds while permitting single-threaded browser values.
