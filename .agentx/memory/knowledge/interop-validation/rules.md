---
agentx:
  kind: rules
  scope: interop-validation
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T19:00:00+02:00"
  topics:
    - validation
    - evidence
  supersedes: []
  superseded_by: []
---
# Rules

- Report which target, protocol feature, service dependency, and runtime environment each validation actually exercised.
- Do not call a browser or cross-language path runtime-proven from compilation alone.
- Run service-dependent cells only through the Aspire end-to-end project (`dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E` with `RAYX_SIGNALR_E2E_*` selection); it starts the AppHost on dynamic ports and enforces service health, 60-second cell bounds, exactly one passed test per cell, process-tree cleanup, and redacted diagnostics. Never hand-start the service or use fixed ports.
- Keep browser runtime validation on repository-local Playwright (Chromium, Firefox, WebKit) launched by the Aspire runner; do not introduce WebDriver, ChromeDriver, or Selenium tooling.
