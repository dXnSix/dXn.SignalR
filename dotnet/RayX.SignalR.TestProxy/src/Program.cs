using RayX.SignalR.TestProxy;

// Test-only HTTP CONNECT proxy for the `proxy` end-to-end scenario: every connection to this
// Aspire-allocated port is handled raw (no ASP.NET Core pipeline), so it can open tunnels; it also
// answers `GET /health` and `GET /stats` (the tunnel counter and the local ports of its tunnels,
// which the service then sees as peers).
WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
using ProxyState state = ProxyState.From(builder.Configuration);
builder.WebHost.ConfigureKestrel(
  kestrel => kestrel.ConfigureEndpointDefaults(
    listen => listen.Use(_ => connection => ProxyConnection.HandleAsync(connection, state))));
WebApplication app = builder.Build();
await app.RunAsync();
