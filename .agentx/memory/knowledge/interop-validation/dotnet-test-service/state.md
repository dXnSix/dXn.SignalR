---
agentx:
  kind: state
  scope: interop-validation/dotnet-test-service
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-09T03:00:00+02:00"
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

`RayX.SignalR.TestService` targets .NET 10, enables SignalR stateful reconnect and MessagePack, permits broad CORS for local testing, maps `TestHub` at `/test`, `/test-stateful`, `/test-exhaust`, `/test-tls` (served over its HTTPS endpoint with the run's test CA) and `/test-direct` (skip-negotiation cells), `AuthHub` at `/test-auth`, `ConnectedHub` at `/test-connected` (greets the caller with its connection id from `OnConnectedAsync`), a test-only negotiate redirect at `/redirect/negotiate` (Azure-shaped `url` plus `accessToken`), and `/test-hang/negotiate`, which accepts a negotiate and never answers (held until the client aborts), and exposes a health endpoint plus a per-cell `POST /e2e/reset`. A test-only authentication scheme accepts Basic `e2e-user:e2e-password` and Bearer `e2e-token` (header or `access_token`) and authenticates browser WebSockets that carry no credentials through the user who negotiated their connection token; `AuthenticationSource` reports which path applied. Test controls (`TestControls`) count negotiates per hub path, reject negotiation for a bounded window, hold the next negotiate of a hub path (`DelayNextNegotiation`, so an attempt stays in flight), record each connection's `OnDisconnectedAsync` exception (`DisconnectError`, empty when graceful), count the SignalR Close messages clients send (both protocols are wrapped in `CloseCountingHubProtocol`; `ClientCloseMessages`, reset per cell; cells run sequentially), record sends, count disconnects, collect client results through `IHubContext`, and send server-initiated Close messages through a tracking `HubLifetimeManager`. Hub invocations stay sequential per connection; `TestEntity` and `ComplexEntity` declare MessagePack keys matching the Rust Serde names. The hub provides unary calls, server streams, callbacks, ordered sequence/replay probes, a per-connection infinite-stream cancellation counter, a test-only `AbortPhysicalTransport` method (with a per-connection `TransportAbortCount` probe) used to force real reconnects without a production fault endpoint, and `DelayedConnectionIdentity` for invocations pending across a transport drop. Liveness fixtures (`LivenessHubs.cs`): `KeepAliveHub` at `/test-keepalive` (1 s server pings, 3 s client timeout), `SilentHub` at `/test-silent` (server pings every 10 minutes) and `/silent-handshake`, which accepts the WebSocket and never answers the SignalR handshake. `PerfHub` at `/perf` serves the performance workloads without artificial delays: `Echo(payload)`, `Stream(count, size)` of `PerfItem { sequence, data }`, and `Sink(payload)` counted per connection with `SinkCount()`. Since P3 the negotiate middleware records each negotiate request (`TestControls.RecordNegotiate`, `DescribeRequest`) and `TestHub` exposes `NegotiateRequest(hubPath)` and `TransportRequest()` (the headers and query of the negotiate and WebSocket requests) for the `request-headers` scenario. Since P4 `AbortPhysicalTransportAndRejectResume` aborts the transport and makes a middleware answer the client's next WebSocket request for that connection token with 404 (`TestControls.RejectNextResume`/`TakeResumeRejection`, counted by `ResumeRejections()`), as ASP.NET Core answers a resume of a disposed connection. Since S4 `ChaosHubs.cs` maps `ChaosHub` (`/test-chaos`) and `StatefulChaosHub` (`/test-chaos-stateful`, stateful reconnect allowed; both 8 parallel invocations per client): `Twice`, counted sends (`Count`/`SendStats` distinct and duplicated), `Sequence(count, delayMs)`, ordered `TriggerCallbacks` (client `chaosCallback`), `ClientResult` (client `chaosResult`), the test-only `ChaosControl.For(hub)` (`StartChaos(min, max, seed)`/`StopChaos()` aborts the current HTTP transport of every tracked connection at seeded random intervals) and `AbortTransport`/`TransportAborts` (abort right after returning, counted per connection); `TestControls.Reset` resets them. Since S5 `TokenAuthentication.cs` adds `RotatingTokens` (`/e2e/token`, `/e2e/rotate-token`, hub methods `TestHub.IssueToken`/`RotateTokens`; reset per cell), the `E2ERotatingToken` scheme (current-generation tokens from `Authorization: Bearer` or `access_token`) and `TokenHub` (`/test-token`: `TokenGeneration`, `Abort` severs the HTTP transport). Since S6 `/test-longpolling` (Long Polling only) and `/test-no-websockets` (Server-Sent Events and Long Polling) map `TestHub` with restricted `HttpConnectionDispatcherOptions.Transports`. It has no launch profile; the Aspire AppHost allocates its port.

Since S9 `RayX.SignalR.TestProxy` (Aspire resource `test-proxy`) is a raw Kestrel connection-middleware HTTP CONNECT proxy:

- Basic `rayx:<password>` is required, with the password passed by the runner as the secret parameter `test-proxy-password`.
- `denied.invalid:80` answers 403, and `10.0.2.2` maps to loopback for Android.
- Tunnels to the service's allocated ports are routed to the ports the service really listens on (`/e2e/listening`, looked up without a proxy; a failed lookup answers 502), past Aspire's endpoint proxy, so `TestHub.PeerPort()` sees the tunnel's local port.
- `/health` and `/stats` report `{tunnels, refused, ports}`.

Cells get `RAYX_SIGNALR_E2E_PROXY_URL`/`_CREDENTIALS`. Native cells and performance binaries run with `ProcessCommand.IsolateFromProxy` (no machine proxy variables), and `Redaction` hides `*_CREDENTIALS=` values and URL user info.

Since S11 `TestCertificates` also issues a server certificate from a second, untrusted CA (`untrusted-server.pem/key`). `UntrustedTlsListener`, a test-only raw `SslStream` listener on loopback port 0, serves it; `/e2e/untrusted-tls` waits for the bind and returns `{port}`. The runner passes `RAYX_SIGNALR_E2E_UNTRUSTED_TLS_URL` (`AspireEnvironment.UntrustedTlsUrl`).

Since P6:

- `AffinityCookies` on `/test-affinity`: issues a `RayXAffinity` cookie on negotiate and answers 403 to later requests without an issued one, counting `AffinityRejections`; reset per cell. `TestHub.AffinityCookie` exposes it.
- HTTPS client certificates: the HTTPS defaults allow client certificates (`ClientCertificateMode.AllowCertificate`, validated by `ClientCertificates.IssuedBy`, a custom-root chain to the test CA with the clientAuth EKU). `/test-mtls` refuses requests without a certificate with 403. `TestHub.ClientCertificateSubject` reports the subject.
- Certificates: `TestCertificates` issues `client.pem`/`client.key` (CN=RayX.SignalR E2E Client). The AppHost passes `TestTls__CaPath`; the runner passes `RAYX_SIGNALR_E2E_CLIENT_CERT_PEM_BASE64` and `_CLIENT_KEY_PEM_BASE64`, which are redacted.

Since P7:

- `.AddNegotiate()` (`Microsoft.AspNetCore.Authentication.Negotiate` 10.0.12; 10.0.9 had an advisory) protects `WindowsAuthHub` at `/test-windows` (`Identity`, `AuthenticationType`, `Echo`).
- `Scenario.WindowsHostOnlyReason` (`WindowsHostOnly` is derived from it) makes `E2ESelection.Cells(windowsHost)` select `windows-auth` only on Windows hosts. Its leg with a rejected `with_credentials` uses a non-existent account, so no real account's lockout counter moves.

Since S12 the test proxy also:

- Answers SOCKS5 on the same port (first byte 5; RFC 1929 with the run's credentials; reply 2 for denied targets; `socks`/`socksAddressTypes` counters).
- Serves `/proxy.pac?http=..&https=..` scripts (proxy, socks5, direct, dead, throw; `pacScripts`) whose routes use a credential-free loopback listener (`openPort`).
- Runs a TLS listener with the run's server certificate (`TestProxy__CertificatePath`/`KeyPath`; `tlsPort`, `tlsSessions`) for https:// proxies.
- Routes the alias `rayx-os-proxy.test` to the service for `os-proxy`.

The E2E runner's `SystemProxy` sets and restores the emulator's global proxy (its previous value) around `os-proxy` android-app cells, and the Windows WinINet or macOS networksetup proxy only when `HostAllowed()` (`RAYX_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES=1` on Windows or macOS). E2E builds include the `proxy-pac` feature everywhere; `proxy-pac` excludes iOS.

## Backlinks

- Source anchors: [Program.cs](../../../../../dotnet/RayX.SignalR.TestService/src/Program.cs), [TestHub.cs](../../../../../dotnet/RayX.SignalR.TestService/src/TestHub.cs), [project](../../../../../dotnet/RayX.SignalR.TestService/RayX.SignalR.TestService.csproj).
