# Server setup

The client talks to standard ASP.NET Core SignalR. This chapter lists what the server needs for
each client feature. Snippets are `Program.cs` fragments for ASP.NET Core 8 or later.

## Minimal hub

```csharp
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSignalR();

var app = builder.Build();
app.MapHub<ChatHub>("/chat");
app.Run();

public sealed class ChatHub : Hub
{
    public Task Send(string user, string text) =>
        Clients.All.SendAsync("ReceiveMessage", user, text);
}
```

The client connects with `SignalRClient::connect_with("host", "chat", ...)`.

## WebSockets

The client only implements the WebSockets transport. ASP.NET Core enables all transports by
default; if you restrict them, keep `HttpTransportType.WebSockets`. With `skip_negotiation()`, the
hub must accept WebSocket-only connections:

```csharp
app.MapHub<ChatHub>("/chat", options =>
    options.Transports = HttpTransportType.WebSockets);
```

Reverse proxies and load balancers in front of the server must forward WebSocket upgrades.

## MessagePack

Install `Microsoft.AspNetCore.SignalR.Protocols.MessagePack` and add the protocol:

```csharp
builder.Services.AddSignalR().AddMessagePackProtocol();
```

Annotate shared DTOs with `[MessagePackObject]` and string `[Key("...")]` names that match the Rust
Serde names (see [Hub protocols](05-hub-protocols.md#messagepack)).

## Stateful reconnect

Enable it per hub (ASP.NET Core 8+), and optionally size the server's buffer:

```csharp
app.MapHub<ChatHub>("/chat", options => options.AllowStatefulReconnects = true);

builder.Services.AddSignalR(options =>
    options.StatefulReconnectBufferSize = 100_000);
```

Without `AllowStatefulReconnects`, a client configured with `with_stateful_reconnect` falls back to
ordinary reconnect.

## Authentication

Native clients send `Authorization` on negotiate and on the WebSocket request. Browsers send it on
negotiate only, and pass Bearer tokens as the `access_token` query parameter. For JWT Bearer, read
the query parameter for hub requests:

```csharp
builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.Events = new JwtBearerEvents
        {
            OnMessageReceived = context =>
            {
                var token = context.Request.Query["access_token"];
                if (!string.IsNullOrEmpty(token) && context.HttpContext.Request.Path.StartsWithSegments("/chat"))
                {
                    context.Token = token;
                }
                return Task.CompletedTask;
            }
        };
    });
```

Basic authentication works for native clients, and for browser clients over Long Polling. Browser
clients cannot authenticate a WebSocket with Basic credentials on a standard server; give them
Bearer tokens.

## CORS

Browser pages on another origin need CORS for the negotiate request and, over Long Polling, for
the `GET` polls, `POST` sends and the closing `DELETE`:

```csharp
builder.Services.AddCors(cors => cors.AddDefaultPolicy(policy =>
    policy.WithOrigins("https://app.example.com")
          .AllowAnyHeader()
          .WithMethods("GET", "POST", "DELETE")
          .AllowCredentials()));

app.UseCors();
```

Native clients are not subject to CORS.

## Keep-alive and timeouts

The client's defaults match ASP.NET Core's: the server pings every 15 s (`KeepAliveInterval`) and
drops clients silent for 30 s (`ClientTimeoutInterval`); the client pings after 15 s of outbound
silence and drops the server after 30 s of inbound silence. When you change one side, keep the
other side's timeout at least twice its partner's ping interval:

```csharp
builder.Services.AddSignalR(options =>
{
    options.KeepAliveInterval = TimeSpan.FromSeconds(10);    // client: with_server_timeout >= 20 s
    options.ClientTimeoutInterval = TimeSpan.FromSeconds(60); // client: with_keep_alive_interval <= 30 s
});
```

## Message size

`MaximumReceiveMessageSize` (32 KB by default) limits messages from the client. Raise it for large
arguments:

```csharp
builder.Services.AddSignalR(options => options.MaximumReceiveMessageSize = 4 * 1024 * 1024);
```

## Client results

Hub code can wait for a value from one client (ASP.NET Core 7+). The client answers with
`InvocationContext::complete` (see [Client results](04-server-callbacks.md#client-results)):

```csharp
public async Task<bool> Checkout(string connectionId, decimal amount)
{
    return await Clients.Client(connectionId)
        .InvokeAsync<bool>("ConfirmPurchase", amount, CancellationToken.None);
}
```

Awaiting `InvokeAsync` inside a hub method requires `MaximumParallelInvocationsPerClient` greater
than 1; otherwise the waiting hub call blocks the connection that has to deliver the result.

## Detailed errors

By default the server hides exception messages from clients, so `invoke` errors say only that an
error occurred. During development, enable details:

```csharp
builder.Services.AddSignalR(options => options.EnableDetailedErrors = true);
```

## Azure SignalR Service

The client follows negotiate redirects with access tokens, which is how Azure SignalR Service
hands clients to the service endpoint. Use the default negotiation (not `skip_negotiation()`).
