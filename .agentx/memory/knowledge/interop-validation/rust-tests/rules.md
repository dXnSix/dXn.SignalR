---
agentx:
  kind: rules
  scope: interop-validation/rust-tests
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T00:30:00+02:00"
  topics:
    - testing
    - runtime
  supersedes: []
  superseded_by: []
---
# Rules

- Interpret networked scenario failures from the cell diagnostics (`artifacts-temp/e2e/<cell>.json`, including AppHost resource logs); scenarios run only through the Aspire end-to-end project.
- Enable `messagepack` when validating MessagePack code or tests.
- Treat WASM reconnect as runtime-proven only when the repository-local Playwright Chromium test passes; WASM compilation remains a separate quality signal.
- Keep ordinary and stateful reconnect test names independently selectable for both JSON and MessagePack.
- Keep the short-stream-chaining scenario WASM-only, independently selectable, dual-codec, and fatal on browser page errors.
