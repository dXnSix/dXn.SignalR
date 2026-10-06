---
agentx:
  kind: knowledge
  scope: interop-validation/rust-tests
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T19:00:00+02:00"
  topics:
    - test-setup
    - messagepack
  supersedes: []
  superseded_by: []
---
# Knowledge

- Networked native tests assume the service listens on port 5220 with the `test` hub.
- MessagePack integration code is compiled only with the `messagepack` Cargo feature.
- Playwright test titles encode the selected reconnect scenario and protocol, allowing the harness to require exactly one real browser test per cell in each Playwright project (Chromium, Firefox, WebKit).
- Stateful browser assertions prove token continuity, no second handshake, ordered replay, duplicate suppression, and backpressure; ordinary assertions prove fresh negotiation/handshake and no invocation replay.
- Short-stream chaining is a focused browser regression for action completion/open re-entry; run it in both JSON and MessagePack and fail on any Playwright `pageerror`.
