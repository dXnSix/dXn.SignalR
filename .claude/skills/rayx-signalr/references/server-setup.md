# Server setup

Full chapter: [Server setup](../../../../docs/10-server-setup.md). ASP.NET Core 8+ `Program.cs`
fragments.

| Client feature | Server requirement |
|---|---|
| any connection | `builder.Services.AddSignalR(); app.MapHub<THub>("/path");` with WebSockets allowed (the only client transport); proxies must forward upgrades |
| `skip_negotiation()` | hub accepts WebSocket-only connections: `options.Transports = HttpTransportType.WebSockets` |
| `with_messagepack_protocol()` | package `Microsoft.AspNetCore.SignalR.Protocols.MessagePack`, `.AddMessagePackProtocol()`, DTOs `[MessagePackObject]` + string `[Key("serdeName")]` |
| `with_stateful_reconnect` | `app.MapHub<THub>("/path", o => o.AllowStatefulReconnects = true)`; optional `StatefulReconnectBufferSize`; otherwise silent fallback to ordinary reconnect |
| Bearer in browsers | read `access_token` from the query for hub paths (JWT `OnMessageReceived` sets `context.Token`) |
| Basic | native clients only on a standard server |
| browser pages on another origin | CORS policy allowing the page origin (negotiate `POST`; Long Polling also `GET` and `DELETE`) and `app.UseCors()` |
| client results (`complete`) | `Clients.Client(id).InvokeAsync<T>(...)` (ASP.NET Core 7+); awaiting inside a hub method needs `MaximumParallelInvocationsPerClient > 1` |
| readable `invoke` errors | `options.EnableDetailedErrors = true` (development) |
| large arguments | raise `MaximumReceiveMessageSize` (32 KB default) |
| Azure SignalR Service | default negotiation (redirects with access tokens are followed); not `skip_negotiation()` |

## Timers

Server defaults: `KeepAliveInterval` 15 s, `ClientTimeoutInterval` 30 s. Client defaults: keep-alive
15 s, server timeout 30 s. Keep each side's timeout ≥ 2 × the other side's ping interval:

```csharp
builder.Services.AddSignalR(options =>
{
    options.KeepAliveInterval = TimeSpan.FromSeconds(10);     // client with_server_timeout >= 20 s
    options.ClientTimeoutInterval = TimeSpan.FromSeconds(60); // client with_keep_alive_interval <= 30 s
});
```

## JWT for browsers

```csharp
options.Events = new JwtBearerEvents
{
    OnMessageReceived = context =>
    {
        var token = context.Request.Query["access_token"];
        if (!string.IsNullOrEmpty(token) && context.HttpContext.Request.Path.StartsWithSegments("/chat"))
            context.Token = token;
        return Task.CompletedTask;
    }
};
```

The repository's test service (`dotnet/RayX.SignalR.TestService`) is a working reference for every
feature above, including stateful hubs, MessagePack DTOs, test authentication and liveness hubs. It
is test-only: never copy its fault controls or connection-token-to-user mapping into production.
