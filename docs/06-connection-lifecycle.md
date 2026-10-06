# Connection lifecycle

## Status

`SignalRClient::status()` returns the connection's `ConnectionStatus`, shared by every clone:

| Status | Meaning | What works |
|---|---|---|
| `Connected` | A transport is open and the handshake completed. | Everything. |
| `Reconnecting` | An automatic, manual or `resume` reconnect is running. | Registered callbacks wait; calls fail (stateful pending calls wait). |
| `Paused` | The application called `pause()`. No transport, keep-alive or automatic reconnect. | `resume()`. Calls fail. |
| `Disconnected` | The transport is gone and nothing is reconnecting: a drop in manual mode, exhausted reconnect attempts, or a server close. | `resume()` or a manual reconnect. Calls fail. |
| `Closed` | Closed locally by `disconnect` or by dropping the last handle. Terminal. | Nothing; `pause`/`resume` return an error. |

`status()` is synchronous and cheap, so a UI can poll it. There is no status-change event; watch
for drops with a [disconnection handler](07-reconnection.md#manual-reconnection) or poll.

## Liveness

Three timers keep a connection honest. Set them in `connect_with`:

```rust
use std::time::Duration;

let client = rayx_signalr::SignalRClient::connect_with("localhost", "chat", |c| {
    c.with_keep_alive_interval(Duration::from_secs(10));
    c.with_server_timeout(Duration::from_secs(40));
    c.with_handshake_timeout(Duration::from_secs(5));
})
.await?;
```

| Option | Default | Behavior |
|---|---|---|
| `with_keep_alive_interval` | 15 s | After this long without any outbound message, the client sends a SignalR `Ping`. The server's `ClientTimeoutInterval` (30 s by default) drops clients that stay silent longer. |
| `with_server_timeout` | 30 s | After this long without any inbound message, the transport counts as dropped and the reconnect path runs. The server pings every `KeepAliveInterval` (15 s by default). |
| `with_handshake_timeout` | 15 s | `connect` (and each reconnect attempt) fails when the server does not answer the handshake in time. |

Rules, checked before any I/O: every duration must be greater than zero, and the server timeout
must be greater than the keep-alive interval. Keep the client's server timeout at least twice the
server's `KeepAliveInterval`, and the client's keep-alive interval at most half the server's
`ClientTimeoutInterval`, as the official clients do.

A handshake answer is validated: an `error` field, an empty or malformed response, or a hub message
in place of the answer fails `connect`. Hub messages that arrive in the same frame after a valid
answer are kept.

Pings are never buffered for stateful replay, and keep-alive continues while a stateful send waits
for buffer space.

## Pausing and resuming

`pause()` and `resume()` let the application control the connection, for example when a mobile app
moves to the background and back:

```rust
// Going to the background: end the transport, stop keep-alive and automatic reconnects.
client.pause().await?;
assert_eq!(client.status(), rayx_signalr::ConnectionStatus::Paused);

// Back in the foreground: reconnect now, then follow the reconnection policy.
client.resume().await?;
```

`pause()`:

- ends the transport, stops keep-alive, and stops or supersedes any running reconnect;
- makes calls fail while paused; ordinary pending invocations and streams fail with
  "The connection is paused";
- does nothing on a paused client, and returns an error on a closed one.

`resume()`:

- starts a reconnect from `Paused` or `Disconnected` and returns once connected; the first attempt
  starts immediately, and later attempts follow the
  [reconnection policy](07-reconnection.md) (`NoReconnectPolicy` allows only the first attempt);
- negotiates a new logical connection, unless a native stateful pause can resume the old one (see
  below);
- if every attempt fails, leaves the client `Disconnected` and returns the error; a later `resume`
  tries again with a fresh negotiation;
- does nothing on a connected or reconnecting client, and returns an error on a closed one.

### Pause with stateful reconnect

With [stateful reconnect](07-reconnection.md#stateful-reconnect) the two runtimes differ, because
ASP.NET Core disposes a stateful connection as soon as the client sends a WebSocket Close:

- **Native** pause drops the transport *without* a Close. The server keeps the logical connection
  for its stateful reconnect window, pending work stays pending, and `resume` continues the same
  connection and replays unacknowledged messages. After the server gives up on the connection,
  resume negotiates a fresh one.
- **Browser** pause cannot avoid the closing handshake, so it ends the logical connection: pending
  work fails, and `resume` negotiates a fresh stateful connection with a new connection id.

## Clones and ownership

`SignalRClient` is `Clone`. Clones share the connection, the callbacks, the lifecycle and the
status. Use them to make concurrent calls or to hand the client to other parts of the application.

The connection closes when the **last** handle closes it:

- `disconnect(self)` consumes a handle. If other handles still exist it only drops this one; the
  last `disconnect` closes the connection.
- Dropping a handle does the same as `disconnect`.
- Each registered callback holds a handle (see
  [Server-to-client calls](04-server-callbacks.md#registrations-keep-the-connection-alive)), and
  so does every `InvocationContext` while it lives.

Closing locally is terminal: it supersedes a running reconnect or pause, fails pending invocations
and streams with "Client was locally disconnected", disposes the stateful buffer, and sets the
status to `Closed`. A closed client cannot reconnect; connect a new one.

`disconnect` and drop are synchronous. They briefly block the calling thread to update the shared
connection state.

## Server-initiated close

A server can close a connection (`Context.Abort()`, shutdown, or a hub filter). The SignalR `Close`
message says whether the client may reconnect:

- **Reconnect allowed**: treated like a dropped transport; the reconnection policy or the
  disconnection handler runs.
- **Reconnect not allowed**: pending work fails with "Server closed the connection without allowing
  reconnect", the status becomes `Disconnected`, and nothing reconnects automatically. A
  disconnection handler still runs, with `server_allows_reconnect() == false`.
