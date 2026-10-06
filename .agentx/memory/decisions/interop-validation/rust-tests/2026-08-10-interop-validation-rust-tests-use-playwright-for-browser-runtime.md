---
agentx:
  kind: decision
  scope: interop-validation/rust-tests
  status: superseded
  created: "2026-08-10T03:15:00+02:00"
  updated: "2026-10-06T21:00:00+02:00"
  topics:
    - browser-testing
    - playwright
    - wasm
  supersedes: []
  superseded_by:
    - 2026-10-06-interop-validation-rust-tests-playwright-browser-matrix.md
---
## Decision: Use repository-local Playwright for browser runtime validation

### Context: RayX.SignalR needs repeatable real-browser proof for native/WASM, JSON/MessagePack, and ordinary/stateful reconnect. The repository should follow the established RayX.GameX browser-validation pattern, and the user explicitly rejected WebDriver installed outside the repository.

### Alternatives considered: Selenium/WebDriver with ChromeDriver, manual browser execution, and compile-only or `wasm-bindgen-test` coverage without the existing application fixture.

### Reasoning: A pinned repository-local Playwright 1.59.1 dependency manages Chromium consistently, selects each scenario by test title, produces useful traces/screenshots/videos on failure, and matches the RayX.GameX tooling approach. It avoids separately managed browser-driver binaries and version skew while proving the actual browser application path.

### Trade-offs accepted: CI and local browser proof require Node, pinned pnpm dependencies, a Playwright-managed Chromium installation, and browser startup overhead.

### Supersedes: None.

### Source anchors: [Playwright package](../../../../../tools-node/package.json), [Playwright tests](../../../../../tools-node/tests), [browser cell runner](../../../../../dotnet/RayX.SignalR.E2E.Tests/Scenarios/CellRunners.cs), and [end-to-end workflow](../../../../../.github/workflows/e2e.yml).
