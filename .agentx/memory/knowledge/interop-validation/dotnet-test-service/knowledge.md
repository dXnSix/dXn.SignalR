---
agentx:
  kind: knowledge
  scope: interop-validation/dotnet-test-service
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - fixtures
    - messagepack
  supersedes: []
  superseded_by: []
---
# Knowledge

- The service's `TestEntity` exposes PascalCase .NET properties while Rust tests deserialize corresponding `text` and `number` fields through Serde conventions.
- The MessagePack protocol dependency is a fork-required compatibility surface and must remain aligned with the Rust feature tests.
- `MillionEntities` currently yields 100,000 items despite its name.
- `AbortPhysicalTransport` aborts the underlying HTTP connection after the hub invocation returns, allowing native and browser clients to observe a physical break and exercise ordinary or stateful recovery.
