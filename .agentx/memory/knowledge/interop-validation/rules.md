---
agentx:
  kind: rules
  scope: interop-validation
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - validation
    - evidence
  supersedes: []
  superseded_by: []
---
# Rules

- Report which target, protocol feature, service dependency, and runtime environment each validation actually exercised.
- Do not call a browser or cross-language path runtime-proven from compilation alone.
- Run selected interop cells through `scripts/ci/run-interop.ps1`; it enforces service health, 60-second scenario bounds, one executed test per cell, and process-tree cleanup.
- Keep browser runtime validation on repository-local Playwright/Chromium; do not introduce WebDriver, ChromeDriver, or Selenium tooling.
