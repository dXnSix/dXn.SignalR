---
agentx:
  kind: rules
  scope: interop-validation/rust-tests
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - testing
    - runtime
  supersedes: []
  superseded_by: []
---
# Rules

- Start and verify the .NET service before interpreting failures from networked Rust tests.
- Enable `messagepack` when validating MessagePack code or tests.
- Treat WASM reconnect as runtime-proven only when the repository-local Playwright Chromium test passes; WASM compilation remains a separate quality signal.
- Keep ordinary and stateful reconnect test names independently selectable for both JSON and MessagePack.
