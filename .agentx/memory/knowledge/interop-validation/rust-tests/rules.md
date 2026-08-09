---
agentx:
  kind: rules
  scope: interop-validation/rust-tests
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - testing
    - runtime
  supersedes: []
  superseded_by: []
---
# Rules

- Start and verify the .NET service before interpreting failures from networked Rust tests.
- Enable `messagepack` when validating MessagePack code or tests.
- Treat the WASM start program as manual/browser runtime coverage until a real browser test harness records results.
