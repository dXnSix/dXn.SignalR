---
title: Server setup
description: What the ASP.NET Core application must enable for each client feature - transports, MessagePack, stateful reconnect, authentication, CORS, timeouts, message size and client results.
group: Guides
order: 170
---

The client talks to standard ASP.NET Core SignalR. This page lists what the server needs for each client feature. Snippets are `Program.cs` fragments for ASP.NET Core 8 or later.

| Client feature | Server needs |
|---|---|
| A plain connection | `AddSignalR()` and `MapHub<T>(path)` |
| `with_messagepack_protocol()` | the MessagePack package and `.AddMessagePackProtocol()` |
| `with_stateful_reconnect(...)` | ASP.NET Core 8+ and `AllowStatefulReconnects = true` on the hub |
| Bearer tokens in browsers | the `access_token` query parameter read for hub requests |
| Browser pages on another origin | a CORS policy |
| Client results (`complete`) | ASP.NET Core 7+ and `MaximumParallelInvocationsPerClient` above 1 for hub-initiated waits |
| Large arguments | a larger `MaximumReceiveMessageSize` |

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

## Transports

ASP.NET Core enables all three transports by default, and the client tries those the server offers in the server's order. If you restrict the server's transports, keep at least one that the client's `with_transports` set allows and that carries the hub protocol (Server-Sent Events carry JSON only). Stateful reconnect needs WebSockets. With `skip_negotiation()`, the hub must accept WebSocket-only connections:

```csharp
app.MapHub<ChatHub>("/chat", options =>
    options.Transports = HttpTransportType.WebSockets);
```

Reverse proxies and load balancers in front of the server must forward WebSocket upgrades for the WebSockets transport.

## MessagePack

Install `Microsoft.AspNetCore.SignalR.Protocols.MessagePack` and add the protocol:

```csharp
builder.Services.AddSignalR().AddMessagePackProtocol();
```

Annotate shared DTOs with `[MessagePackObject]` and string `[Key("...")]` names that match the Rust Serde names. See [Hub protocols](./hub-protocols.md#messagepack).

## Stateful reconnect

Enable it per hub (ASP.NET Core 8+), and optionally size the server's buffer:

```csharp
app.MapHub<ChatHub>("/chat", options => options.AllowStatefulReconnects = true);

builder.Services.AddSignalR(options =>
    options.StatefulReconnectBufferSize = 100_000);
```

Without `AllowStatefulReconnects`, a client configured with `with_stateful_reconnect` silently falls back to ordinary reconnect.

## Authentication

Native clients send `Authorization` on negotiate and on every transport request. Browsers send it on negotiate and on `fetch` requests, and pass Bearer tokens as the `access_token` query parameter on the WebSocket and event-stream URLs. For JWT Bearer, read the query parameter for hub requests:

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

Basic authentication works for native clients and for browser clients over Long Polling. Browser clients cannot authenticate a WebSocket with Basic credentials on a standard server; give them Bearer tokens. To make the server end a connection when its token expires, set `CloseOnAuthenticationExpiration = true` in the `MapHub` options.

## CORS

Browser pages on another origin need CORS for the negotiate request and, over Long Polling, the `GET` polls, `POST` sends and the closing `DELETE` (over Server-Sent Events, the stream's `GET` and the `POST` sends):

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

The client's defaults match ASP.NET Core's: the server pings every 15 s (`KeepAliveInterval`) and drops clients silent for 30 s (`ClientTimeoutInterval`); the client pings after 15 s of outbound silence and drops the server after 30 s of inbound silence. When you change one side, keep the other side's timeout at least twice its partner's ping interval:

```csharp
builder.Services.AddSignalR(options =>
{
    options.KeepAliveInterval = TimeSpan.FromSeconds(10);    // client: with_server_timeout >= 20 s
    options.ClientTimeoutInterval = TimeSpan.FromSeconds(60); // client: with_keep_alive_interval <= 30 s
});
```

## Message size

`MaximumReceiveMessageSize` (32 KB by default) limits messages from the client. A larger message closes the connection. Raise it for large arguments:

```csharp
builder.Services.AddSignalR(options => options.MaximumReceiveMessageSize = 4 * 1024 * 1024);
```

## Client results

Hub code can wait for a value from one client (ASP.NET Core 7+). The client answers with `InvocationContext::complete` (see [Client results](./server-callbacks.md#client-results)):

```csharp
public async Task<bool> Checkout(string connectionId, decimal amount)
{
    return await Clients.Client(connectionId)
        .InvokeAsync<bool>("ConfirmPurchase", amount, CancellationToken.None);
}
```

Awaiting `InvokeAsync` inside a hub method requires `MaximumParallelInvocationsPerClient` greater than 1; otherwise the waiting hub call blocks the connection that has to deliver the result.

## Detailed errors

By default the server hides exception messages from clients, so `invoke` errors say only that an error occurred. During development, enable details; a `HubException` message always reaches the client.

```csharp
builder.Services.AddSignalR(options => options.EnableDetailedErrors = true);
```

## Azure SignalR Service

The client follows negotiate redirects with access tokens, which is how Azure SignalR Service hands clients to the service endpoint. Use the default negotiation, not `skip_negotiation()`.

## Next

[Troubleshooting](./troubleshooting.md) maps client errors back to these settings.
