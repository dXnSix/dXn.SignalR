---
agentx:
  kind: rules
  scope: interop-validation/dotnet-test-service
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - interop
    - fixtures
  supersedes: []
  superseded_by: []
---
# Rules

- Keep hub method names, routes, payload shapes, and enabled protocols synchronized with the Rust integration scenarios.
- Treat CORS and open local endpoints as test-service configuration, not production defaults.
- Validate both ends when updating the ASP.NET MessagePack dependency or DTO shape.
- Keep transport-abort and synthetic sequence controls test-only; production transport code must not expose fault injection.
