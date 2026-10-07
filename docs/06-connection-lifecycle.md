# Connection lifecycle

The lifecycle follows the official .NET client's `HubConnection`: build a client, register
callbacks and lifecycle hooks, `start` it, `stop` it, and start it again when needed.

## Building and starting

`SignalRClient::new` builds a client that is not started, as `HubConnectionBuilder.Build()` does:
it validates the configuration (natively it also resolves the Tokio runtime and the TLS
configuration) and does no I/O. `start().await` negotiates, connects and completes the handshake,
as `StartAsync`:

```rust
let mut client = rayx_signalr::SignalRClient::new("localhost", "chat", |c| {
    c.with_port(5000);
})?;

// Registered before `start`, so messages the hub sends from `OnConnectedAsync` arrive here.
let _welcome = client.register_typed("Welcome", |_context, (text,): (String,)| {
    println!("{text}");
});
let _closed = client.on_closed(|error| println!("closed: {error:?}"));

client.start().await?;
```

`SignalRClient::connect` and `connect_with` are `new` followed by `start`. Use `new` whenever the
server sends right after connecting: a callback registered after `connect_with` returned can miss
those messages.

`start`:

- fails with `ErrorKind::InvalidState` when the client is starting, connected, reconnecting or
  paused, and leaves it unchanged;
- starts again a client that `disconnect`/`stop` stopped, that the server closed, or whose
  reconnect attempts were exhausted, always with a fresh negotiation (a new connection id and,
  with stateful reconnect, a new logical connection); registered callbacks and hooks stay;
- leaves the client `Disconnected` when it fails, and may be called again;
- is canceled at once by `disconnect` or `stop` on any handle, and then fails with
  `ErrorKind::ConnectionClosed` (`CloseReason::Local`).

A call on a client that was never started (or whose start failed) fails with
`ErrorKind::InvalidState`, as .NET's `InvalidOperationException`.

## Status

`SignalRClient::status()` returns the connection's `ConnectionStatus`, shared by every clone:

| Status | Meaning | What works |
|---|---|---|
| `Connecting` | `start` is negotiating and connecting. | Calls wait for the start's outcome. |
| `Connected` | A transport is open and the handshake completed. | Everything. |
| `Reconnecting` | An automatic, manual or `resume` reconnect is running. | Calls made while an attempt is in flight wait for it; during the policy delay they fail at once (stateful pending calls wait). |
| `Paused` | The application called `pause()`. No transport, keep-alive or automatic reconnect. | `resume()`. Calls fail. |
| `Disconnected` | Not started yet, or the transport is gone and nothing is reconnecting: a failed start, a drop in manual mode, exhausted reconnect attempts, or a server close. | `start()`, `resume()` (after a connection) or a manual reconnect. Calls fail. |
| `Closed` | Stopped locally by `disconnect`/`stop` (on any handle) or by dropping the last handle. | `start()` connects again. |

`status()` is synchronous and cheap. To react to changes, use the [lifecycle events](#lifecycle-events).

`connection_id()` returns the negotiated connection id (`Context.ConnectionId` on the hub), as
`HubConnection.ConnectionId`: a new id after every fresh connection, the same id after a stateful
resume, and `None` with `skip_negotiation` and while the client is not connected.

## Lifecycle events

Hooks for the .NET client's `Closed`, `Reconnecting` and `Reconnected` events can be added before
`start` and stay across restarts:

```rust
let _reconnecting = client.on_reconnecting(|reason| eprintln!("reconnecting: {reason}"));
let _reconnected = client.on_reconnected(|connection_id| {
    println!("reconnected as {connection_id:?}");
});
let _closed = client.on_closed(|error| match error {
    Some(error) => eprintln!("connection lost: {error}"),
    None => println!("stopped"),
});
```

| Hook | Raised |
|---|---|
| `on_reconnecting(&Error)` | An automatic reconnect run begins after the transport dropped, before its first delay; ordinary pending work has already failed. Not raised when the reconnection policy allows no attempt, in manual mode, or for `resume`. |
| `on_reconnected(Option<&str>)` | An automatic reconnect run succeeded; the argument is the connection id (new after an ordinary reconnect, unchanged after a stateful resume). |
| `on_closed(Option<&Error>)` | The connection ended and nothing reconnects automatically: `None` after `disconnect`/`stop`, or an `ErrorKind::Canceled` error when the stop interrupted a reconnect (as .NET's `OperationCanceledException`); otherwise a server close without reconnect permission, a drop the policy allows no attempt for, a drop in manual mode (before the disconnection handler runs), or exhausted attempts. Once per established connection; a failed `start` raises none. |

Several hooks per event run in registration order. Events run one at a time in the order the
connection raised them (natively they are raised under the connection's state lock, so a
concurrent stop or a new drop never reorders them), on a task of their own: natively on the Tokio
blocking pool, in the browser on the event loop. A slow hook therefore never blocks the
connection. Natively hooks are `Send + Sync`, and a hook that panics is caught and logged. Each
method returns a handle whose `unregister` removes the hook.

A hook that needs the client should capture a `WeakSignalRClient` from `client.downgrade()`: a
captured `SignalRClient` forms a reference cycle, so the client would never stop when its last
handle is dropped. Restarting after a server close, as .NET applications do in `Closed`:

```rust
let weak = client.downgrade();
let _restart = client.on_closed(move |error| {
    // `None`, or a cancellation during a reconnect: stopped by the application.
    let Some(error) = error else { return };
    if error.kind() == rayx_signalr::ErrorKind::Canceled {
        return;
    }
    if let Some(client) = weak.upgrade() {
        tokio::spawn(async move {
            let _ = client.start().await;
        });
    }
});
```

## Stopping

`stop().await` stops the client as `StopAsync` does and returns once the transport closed:
messages already sent go out, then a SignalR `Close` message, then the WebSocket closes, so the
hub's `OnDisconnectedAsync` sees no error. It waits at most the close timeout
(`with_close_timeout`, 5 s by default, as .NET's `CloseTimeout`), then drops the transport.
`disconnect()` is the synchronous form: it stops the same way and lets the transport close in the
background, bounded by the same close timeout.

Stopping on any handle:

- supersedes and cancels a running start, reconnect or pause;
- fails pending invocations and open streams with `ErrorKind::ConnectionClosed`
  (`CloseReason::Local`), and later calls with the same error;
- disposes the stateful buffer and ends the callback dispatch task; registered callbacks and hooks
  stay registered for a later `start`;
- raises `on_closed` (`None`, or a canceled error when it interrupted a reconnect) and sets the
  status to `Closed`.

A `ReconnectionHandler` kept from an earlier drop fails with the same error. Stopping a stopped
client does nothing.

## Calls during a connection attempt

As the .NET client's connection lock does, a call made while a start or reconnect attempt is in
flight waits for that attempt, then runs on the new connection or fails with the attempt's error.
During a reconnect policy's delay, calls fail at once with `ErrorKind::Transport`. Server-to-client
replies (client results) never wait: they belong to the connection the call arrived on.

## Liveness

Three timers keep a connection honest, a fourth bounds connection setup and a fifth bounds `stop`.
Set them when building the client:

```rust
use std::time::Duration;

let client = rayx_signalr::SignalRClient::new("localhost", "chat", |c| {
    c.with_keep_alive_interval(Duration::from_secs(10));
    c.with_server_timeout(Duration::from_secs(40));
    c.with_handshake_timeout(Duration::from_secs(5));
    c.with_connect_timeout(Duration::from_secs(10));
    c.with_close_timeout(Duration::from_secs(2));
})?;
```

| Option | Default | Behavior |
|---|---|---|
| `with_keep_alive_interval` | 15 s | After this long without any outbound message, the client sends a SignalR `Ping`. The server's `ClientTimeoutInterval` (30 s by default) drops clients that stay silent longer. |
| `with_server_timeout` | 30 s | After this long without any inbound message, the transport counts as dropped and the reconnect path runs. The server pings every `KeepAliveInterval` (15 s by default). |
| `with_handshake_timeout` | 15 s | `start` (and each reconnect attempt) fails when the server does not answer the handshake in time. |
| `with_connect_timeout` | 120 s (the official .NET client's bound) | Bounds each negotiate request and opening the transport (natively TCP, TLS and the WebSocket upgrade; in browsers opening the WebSocket). Browsers abort a negotiate `fetch` that exceeds it. `start` (and each reconnect attempt) then fails with `ErrorKind::Timeout` naming `Negotiate` or `Connect`. |
| `with_close_timeout` | 5 s | How long `stop` waits for the transport to close. |

As in the .NET client, every duration is accepted. Values that cannot work are logged as warnings
when the client is built: a zero keep-alive interval or server timeout (which then act as one
second, the .NET client's timer tick), a zero handshake or connect timeout (every attempt then
times out), and a server timeout not above the keep-alive interval (the connection is then
considered lost whenever the server is idle). Keep the client's server timeout at least twice the
server's `KeepAliveInterval`, and the client's keep-alive interval at most half the server's
`ClientTimeoutInterval`, as the official clients do.

A handshake answer is validated: an `error` field, an empty or malformed response, or a hub message
in place of the answer fails the start. Hub messages that arrive in the same frame after a valid
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

- ends the transport, stops keep-alive, and stops or supersedes any running start or reconnect;
- makes calls fail while paused; ordinary pending invocations and streams fail with
  `ErrorKind::Paused`;
- does nothing on a paused client, returns `ErrorKind::ConnectionClosed` on a stopped one and
  `ErrorKind::InvalidState` on one that was never started.

`resume()`:

- starts a reconnect from `Paused` or `Disconnected` and returns once connected; the first attempt
  starts immediately, and later attempts follow the
  [reconnection policy](07-reconnection.md) (`NoReconnectPolicy` allows only the first attempt);
- negotiates a new logical connection, unless a native stateful pause can resume the old one (see
  below);
- if every attempt fails, leaves the client `Disconnected` and returns the error; a later `resume`
  tries again with a fresh negotiation;
- does nothing on a starting, connected or reconnecting client, returns
  `ErrorKind::ConnectionClosed` on a stopped one (use `start`) and `ErrorKind::InvalidState` on one
  that was never started.

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

`SignalRClient` is `Clone`. Clones share the connection, the callbacks, the hooks, the lifecycle
and the status. Use them to make concurrent calls or to hand the client to other parts of the
application.

- `disconnect`/`stop` stop the connection for **every** handle, as `HubConnection.StopAsync` does:
  other clones and live `InvocationContext`s do not keep it open.
- Dropping a handle stops the connection only when it is the **last** one. Registered callbacks
  and lifecycle hooks never count as handles (see
  [Server-to-client calls](04-server-callbacks.md#registrations-do-not-keep-a-client-alive)); an
  `InvocationContext` holds one while it lives. Call `disconnect` or `stop` when you are done.
- `downgrade()` returns a `WeakSignalRClient`, which does not keep the client alive; `upgrade()`
  returns the client while any handle exists. Capture weak handles in callbacks and hooks.

`disconnect` and drop are synchronous. They briefly block the calling thread to update the shared
connection state.

## Server-initiated close

A server can close a connection (`Context.Abort()`, shutdown, or a hub filter). The SignalR `Close`
message says whether the client may reconnect:

- **Reconnect allowed**: treated like a dropped transport; the reconnection policy or the
  disconnection handler runs.
- **Reconnect not allowed**: pending work fails with `ErrorKind::ConnectionClosed`
  (`CloseReason::Server`, the Close message's error in `server_message()`, and
  `server_allows_reconnect() == Some(false)`), the status becomes `Disconnected`, `on_closed`
  runs, and nothing reconnects automatically; `start` connects again. A disconnection handler
  still runs, with `server_allows_reconnect() == false` and the same error in `reason()`.
