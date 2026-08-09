---
agentx:
  kind: state
  scope: interop-validation/dotnet-test-service
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - aspnet
    - signalr
    - messagepack
    - test-hub
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

`SignalRTestService` targets .NET 10, enables SignalR MessagePack, permits broad CORS for local testing, and maps `TestHub` at `/test`. The hub provides unary calls, server streams, server-to-client callbacks, and client-result callbacks used by the Rust scenarios.

## Backlinks

- Source anchors: [Program.cs](../../../../../dotnet/SignalRTestService/Program.cs), [TestHub.cs](../../../../../dotnet/SignalRTestService/TestHub.cs), [project](../../../../../dotnet/SignalRTestService/SignalRTestService.csproj).
