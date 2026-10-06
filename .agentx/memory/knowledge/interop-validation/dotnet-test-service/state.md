---
agentx:
  kind: state
  scope: interop-validation/dotnet-test-service
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T21:10:00+02:00"
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

`RayX.SignalR.TestService` targets .NET 10, enables SignalR stateful reconnect and MessagePack, permits broad CORS for local testing, maps `TestHub` at `/test`, `/test-stateful`, `/test-exhaust`, `/test-tls` (served over its HTTPS endpoint with the run's test CA) and `/test-direct` (skip-negotiation cells), `AuthHub` at `/test-auth`, and a test-only negotiate redirect at `/redirect/negotiate` (Azure-shaped `url` plus `accessToken`), and exposes a health endpoint plus a per-cell `POST /e2e/reset`. A test-only authentication scheme accepts Basic `e2e-user:e2e-password` and Bearer `e2e-token` (header or `access_token`) and authenticates browser WebSockets that carry no credentials through the user who negotiated their connection token; `AuthenticationSource` reports which path applied. Test controls (`TestControls`) count negotiates per hub path, reject negotiation for a bounded window, record sends, count disconnects, collect client results through `IHubContext`, and send server-initiated Close messages through a tracking `HubLifetimeManager`. Hub invocations stay sequential per connection; `TestEntity` and `ComplexEntity` declare MessagePack keys matching the Rust Serde names. The hub provides unary calls, server streams, callbacks, ordered sequence/replay probes, a per-connection infinite-stream cancellation counter, a test-only `AbortPhysicalTransport` method (with a per-connection `TransportAbortCount` probe) used to force real reconnects without a production fault endpoint, and `DelayedConnectionIdentity` for invocations pending across a transport drop. Liveness fixtures (`LivenessHubs.cs`): `KeepAliveHub` at `/test-keepalive` (1 s server pings, 3 s client timeout), `SilentHub` at `/test-silent` (server pings every 10 minutes) and `/silent-handshake`, which accepts the WebSocket and never answers the SignalR handshake. It has no launch profile; the Aspire AppHost allocates its port.

## Backlinks

- Source anchors: [Program.cs](../../../../../dotnet/RayX.SignalR.TestService/src/Program.cs), [TestHub.cs](../../../../../dotnet/RayX.SignalR.TestService/src/TestHub.cs), [project](../../../../../dotnet/RayX.SignalR.TestService/RayX.SignalR.TestService.csproj).
