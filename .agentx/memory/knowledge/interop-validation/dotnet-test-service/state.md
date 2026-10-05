---
agentx:
  kind: state
  scope: interop-validation/dotnet-test-service
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T00:30:00+02:00"
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

`SignalRTestService` targets .NET 10, enables SignalR stateful reconnect and MessagePack, permits broad CORS for local testing, maps `TestHub` at `/test`, and exposes a health endpoint for deterministic startup. The hub provides unary calls, server streams, callbacks, ordered sequence/replay probes, a per-connection infinite-stream cancellation counter, a test-only `AbortPhysicalTransport` method (with a per-connection `TransportAbortCount` probe) used to force real reconnects without a production fault endpoint, and `DelayedConnectionIdentity` for invocations pending across a transport drop. It has no launch profile; the Aspire AppHost allocates its port.

## Backlinks

- Source anchors: [Program.cs](../../../../../dotnet/SignalRTestService/Program.cs), [TestHub.cs](../../../../../dotnet/SignalRTestService/TestHub.cs), [project](../../../../../dotnet/SignalRTestService/SignalRTestService.csproj).
