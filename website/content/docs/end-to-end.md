---
title: A complete example
description: One ASP.NET Core hub and one Rust client that exercise every call style, with the lifecycle hooks and a reconnection policy.
group: Start
order: 20
---

This page pairs a small ASP.NET Core hub with a Rust client that uses every call style once: a server push, fire-and-forget, a request/response call, a server stream, a client stream and a hub error. The rest of the documentation takes each piece apart.

## The hub

`Program.cs` for ASP.NET Core 8 or later. SignalR is part of the shared framework, so there is no package to install.

```csharp
using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSignalR();

var app = builder.Build();
app.MapHub<ChatHub>("/chat");
app.Run();

public sealed record ChatMessage(string User, string Text, DateTimeOffset SentAt);

public sealed class ChatHub : Hub
{
    // Pushed to the caller as soon as it connects.
    public override async Task OnConnectedAsync()
    {
        await Clients.Caller.SendAsync("Welcome", $"you are {Context.ConnectionId}");
        await base.OnConnectedAsync();
    }

    // Fire-and-forget from the client; fans out to every client.
    public Task Send(string user, string text) =>
        Clients.All.SendAsync("ReceiveMessage", new ChatMessage(user, text, DateTimeOffset.UtcNow));

    // Request/response.
    public int Add(int a, int b) => a + b;

    // Server-to-client stream; the token fires when the client drops the stream.
    public async IAsyncEnumerable<int> Countdown(
        int from, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var i = from; i >= 0; i--)
        {
            yield return i;
            await Task.Delay(500, cancellationToken);
        }
    }

    // Client-to-server stream.
    public async Task<int> Sum(IAsyncEnumerable<int> numbers)
    {
        var total = 0;
        await foreach (var n in numbers) total += n;
        return total;
    }

    // A HubException's message always reaches the client, inside a standard prefix.
    public int Fail() => throw new HubException("nope");
}
```

`System.Text.Json` writes property names in camelCase, so `ChatMessage` travels as `{"user":...,"text":...,"sentAt":...}`.

## The client

```rust
use std::sync::Arc;

use dxn_signalr::{DefaultRetryPolicy, ErrorKind, ReconnectionConfig, SignalRClient};
use futures::{stream, StreamExt};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChatMessage {
    user: String,
    text: String,
    sent_at: String,
}

#[tokio::main]
async fn main() -> Result<(), dxn_signalr::Error> {
    // `new` does no I/O, so the callbacks below exist before the hub's OnConnectedAsync pushes.
    let mut client = SignalRClient::new("localhost", "chat", |c| {
        c.unsecure();
        c.with_port(5000);
        c.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(DefaultRetryPolicy),
        });
    })?;

    let _welcome = client.register_typed("Welcome", |_ctx, (text,): (String,)| {
        println!("server says: {text}");
    });
    let _messages = client.register_typed("ReceiveMessage", |_ctx, (m,): (ChatMessage,)| {
        println!("[{}] {}: {}", m.sent_at, m.user, m.text);
    });
    let _reconnecting = client.on_reconnecting(|reason| eprintln!("reconnecting: {reason}"));
    let _reconnected = client.on_reconnected(|id| eprintln!("reconnected as {id:?}"));
    let _closed = client.on_closed(|error| eprintln!("closed: {error:?}"));

    client.start().await?;

    // Fire and forget.
    client
        .send_with_args("Send".to_string(), |args| {
            args.argument("rust").argument("hello");
        })
        .await?;

    // Request/response.
    let sum: i32 = client
        .invoke_with_args("Add".to_string(), |args| {
            args.argument(2).argument(3);
        })
        .await?;
    println!("2 + 3 = {sum}");

    // Server stream: ends when the hub method returns.
    let mut countdown = client
        .enumerate_with_args::<i32, _>("Countdown".to_string(), |args| {
            args.argument(3);
        })
        .await;
    while let Some(n) = countdown.next().await {
        println!("countdown: {n}");
    }
    if let Some(error) = countdown.error() {
        eprintln!("the stream failed: {error}");
    }

    // Client stream: any `futures::Stream` of serializable items.
    let total: i32 = client
        .invoke_with_args("Sum".to_string(), |args| {
            args.stream(stream::iter(1..=10));
        })
        .await?;
    println!("1 + ... + 10 = {total}");

    // A hub error is an `ErrorKind::Hub` error that carries the server's text.
    match client.invoke::<i32>("Fail".to_string()).await {
        Err(error) if error.kind() == ErrorKind::Hub => {
            println!("hub error: {}", error.server_message().unwrap_or(""));
        }
        other => println!("unexpected: {other:?}"),
    }

    client.stop().await;
    Ok(())
}
```

Run the hub with `dotnet run --urls http://localhost:5000`, then the client with `cargo run`. The output, with this run's connection id and timestamp:

```text
server says: you are zyKFYG6f1c0bb2kMHr6yig
[2026-10-25T00:22:14.312745+00:00] rust: hello
2 + 3 = 5
countdown: 3
countdown: 2
countdown: 1
countdown: 0
1 + ... + 10 = 55
hub error: An unexpected error occurred invoking 'Fail' on the server. HubException: nope
closed: None
```

`closed: None` is the `on_closed` hook reporting the graceful `stop`. ASP.NET Core wraps a `HubException` message in the `An unexpected error occurred invoking...` prefix; the exception's own text is at the end.

### What to notice

- `SignalRClient::new` plus `start` is `connect_with` split in two. Use the split form whenever the hub pushes from `OnConnectedAsync`, otherwise `Welcome` can arrive before the callback exists.
- Method names are plain `String`s and arguments are positional. Their order must match the hub method's parameters.
- The reconnection policy is opt-in. Without `with_reconnection_policy`, a dropped connection ends the client. See [Reconnection](./reconnection.md).
- `stop().await` closes gracefully, so the hub's `OnDisconnectedAsync` sees no error.

## Next

[Calling the hub](./calling-the-hub.md) covers arguments, return types and cancellation. [Server-to-client calls](./server-callbacks.md) covers callbacks and client results.
