---
agentx:
  kind: decision
  scope: interop-validation/rust-tests
  status: current
  created: "2026-10-06T21:00:00+02:00"
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - browser-testing
    - playwright
    - wasm
  supersedes: 
    - 2026-08-10-interop-validation-rust-tests-use-playwright-for-browser-runtime.md
  superseded_by: []
---
## Decision: Prove browser behavior with repository-local Playwright on Chromium, Firefox and WebKit, launched by the Aspire runner

### Context: The earlier decision used repository-local Playwright with Chromium only, started by scripts. Browser support now claims Chromium, Firefox and WebKit, Web Workers and threaded (cross-origin isolated) WASM, and the Aspire runner owns orchestration.

### Alternatives considered: Keeping Chromium-only proof, WebDriver/Selenium, and `wasm-bindgen-test`.

### Reasoning: Pinned Playwright 1.59.1 under `tools-node` (browsers in `node_modules`, `PLAYWRIGHT_BROWSERS_PATH=0`) has `chromium`, `firefox` and `webkit` projects; the Aspire runner launches it per cell with one exact title against `fixture-host` (no Playwright `webServer`), so every browser scenario, including `worker-transport` and `threaded-wasm`, runs on all three engines in CI.

### Trade-offs accepted: Three browser installations and three CI jobs; engine differences surface as catalog exclusions that must be documented (none are needed today).

### Supersedes: [Use repository-local Playwright for browser runtime validation](2026-08-10-interop-validation-rust-tests-use-playwright-for-browser-runtime.md) (2026-08-10).

### Source anchors: [Playwright config](../../../../../tools-node/playwright.config.ts), [browser specs](../../../../../tools-node/tests), [browser cell runner](../../../../../dotnet/dXn.SignalR.E2E.Tests/src/Scenarios/CellRunners.cs).
