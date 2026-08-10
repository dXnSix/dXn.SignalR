---
agentx:
  kind: knowledge
  scope: interop-validation
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - interop
    - runtime-proof
  supersedes: []
  superseded_by: []
---
# Knowledge

- Rust compile/unit evidence and live client-to-ASP.NET interoperability are different proof types.
- JSON, MessagePack, native, and browser paths have distinct setup and feature requirements.
- The production reconnect matrix has eight logical cells: native/WASM × JSON/MessagePack × ordinary/stateful reconnect.
- Browser runtime proof is produced by repository-local Playwright 1.59.1 driving Chromium, not by WebDriver or compile-only WASM checks.
- Failure artifacts are bounded, credential-redacted, ignored locally, and uploaded by CI only when a matrix job fails.
