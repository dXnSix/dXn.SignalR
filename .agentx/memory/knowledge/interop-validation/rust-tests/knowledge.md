---
agentx:
  kind: knowledge
  scope: interop-validation/rust-tests
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - test-setup
    - messagepack
  supersedes: []
  superseded_by: []
---
# Knowledge

- Networked native tests assume the service listens on port 5220 with the `test` hub.
- MessagePack integration code is compiled only with the `messagepack` Cargo feature.
- The browser entry runs the same broad scenario but currently lacks assertion reporting through a dedicated browser test runner.
