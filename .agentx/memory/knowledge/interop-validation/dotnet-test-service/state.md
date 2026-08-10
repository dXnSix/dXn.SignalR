---
agentx:
  kind: state
  scope: interop-validation/dotnet-test-service
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
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

`SignalRTestService` targets .NET 10, enables SignalR stateful reconnect and MessagePack, permits broad CORS for local testing, maps `TestHub` at `/test`, and exposes a health endpoint for deterministic startup. The hub provides unary calls, server streams, callbacks, ordered sequence/replay probes, and a test-only `AbortPhysicalTransport` method used to force real reconnects without a production fault endpoint.

## Backlinks

- Source anchors: [Program.cs](../../../../../dotnet/SignalRTestService/Program.cs), [TestHub.cs](../../../../../dotnet/SignalRTestService/TestHub.cs), [project](../../../../../dotnet/SignalRTestService/SignalRTestService.csproj).
