---
title: Lifecycle
description: Build, start, stop and restart a client, read its status, react to lifecycle events, tune keep-alive and timeouts, and pause and resume it.
group: Connection
order: 100
---

The lifecycle follows the .NET client's `HubConnection`: build a client, register callbacks and hooks, `start` it, `stop` it, and start it again when needed.

## Building and starting

`SignalRClient::new` builds a client that is not started and does no I/O. `start().await` negotiates, connects and completes the handshake.

```rust
use dxn_signalr::SignalRClient;

async fn run() -> Result<(), dxn_signalr::Error> {
    let mut client = SignalRClient::new("localhost", "chat", |c| {
        c.with_port(5000);
    })?;

    // Registered before `start`, so messages the hub sends from OnConnectedAsync arrive here.
    let _welcome = client.register_typed("Welcome", |_context, (text,): (String,)| {
        println!("{text}");
    });
    let _closed = client.on_closed(|error| println!("closed: {error:?}"));

    client.start().await
}
```

`start`:

- fails with `ErrorKind::InvalidState` when the client is starting, connected, reconnecting or paused, and leaves it unchanged;
- starts again a client that `disconnect` or `stop` stopped, that the server closed, or whose reconnect attempts were exhausted, always with a fresh negotiation (a new connection id); registered callbacks and hooks stay;
- leaves the client `Disconnected` when it fails, and may be called again;
- is canceled at once by `disconnect` or `stop` on any handle, and then fails with `ErrorKind::ConnectionClosed` (`CloseReason::Local`).

A call on a client that was never started, or whose start failed, fails with `ErrorKind::InvalidState`.

## Status

`SignalRClient::status()` returns the connection's `ConnectionStatus`, shared by every clone. It is synchronous and cheap.

| Status | Meaning | What works |
|---|---|---|
| `Connecting` | `start` is negotiating and connecting. | Calls wait for the start's outcome. |
| `Connected` | A transport is open and the handshake completed, or a stateful connection is transparently resuming. | Everything; during a resume, sends go into the replay buffer. |
| `Reconnecting` | An automatic, manual or `resume` reconnect is running. | Calls made while an attempt is in flight wait for it; during the policy delay they fail at once. |
| `Paused` | The application called `pause()`. | `resume()`. Calls fail. |
| `Disconnected` | Not started yet, or the transport is gone and nothing is reconnecting: a failed start, a drop in manual mode, exhausted attempts, or a server close. | `start()`, `resume()` or a manual reconnect. Calls fail. |
| `Closed` | Stopped locally by `disconnect` or `stop` (on any handle) or by dropping the last handle. | `start()` connects again. |

`connection_id()` returns the negotiated id (`Context.ConnectionId` on the hub): new after every fresh connection, the same after a stateful resume, and `None` with `skip_negotiation` and while not connected.

## Lifecycle events

Hooks for the .NET client's `Closed`, `Reconnecting` and `Reconnected` events can be added before `start` and stay across restarts. Each returns a handle with `unregister()`.

```rust
use dxn_signalr::{CallbackHandler, SignalRClient};

fn hooks(client: &SignalRClient) {
    let _reconnecting = client.on_reconnecting(|reason| eprintln!("reconnecting: {reason}"));
    let _reconnected = client.on_reconnected(|connection_id| {
        println!("reconnected as {connection_id:?}");
    });
    let closed = client.on_closed(|error| match error {
        Some(error) => eprintln!("connection lost: {error}"),
        None => println!("stopped"),
    });
    closed.unregister();
}
```

| Hook | Raised |
|---|---|
| `on_reconnecting(&Error)` | An automatic reconnect run begins after the transport dropped, before its first delay; ordinary pending work has already failed. Not raised when the policy allows no attempt, in manual mode, or for `resume`. |
| `on_reconnected(Option<&str>)` | An automatic reconnect run succeeded; the argument is the new connection id. A transparent stateful resume raises no hook. |
| `on_closed(Option<&Error>)` | The connection ended and nothing reconnects automatically: `None` after `disconnect` or `stop`, an `ErrorKind::Canceled` error when the stop interrupted a reconnect; otherwise a server close without reconnect permission, a drop the policy allows no attempt for, a drop in manual mode, or exhausted attempts. Once per established connection; a failed `start` raises none. |

Several hooks per event run in registration order, one at a time, on a task of their own (natively on the Tokio blocking pool, in the browser on the event loop), so a slow hook never blocks the connection. Natively hooks are `Send + Sync`, and one that panics is caught and logged.

A hook that needs the client should capture a `WeakSignalRClient` from `client.downgrade()`, not a clone. This restarts the client after a server close, as .NET applications do in `Closed`:

```rust
use dxn_signalr::{CallbackHandler, ErrorKind, SignalRClient};

fn restart_on_close(client: &SignalRClient) {
    let weak = client.downgrade();
    let _restart = client.on_closed(move |error| {
        // `None`, or a cancellation during a reconnect: stopped by the application.
        let Some(error) = error else { return };
        if error.kind() == ErrorKind::Canceled {
            return;
        }
        if let Some(client) = weak.upgrade() {
            tokio::spawn(async move {
                let _ = client.start().await;
            });
        }
    });
}
```

## Stopping

`stop().await` stops the client as `StopAsync` does and returns once the transport closed: messages already sent go out, then a SignalR `Close` message, then the transport closes, so the hub's `OnDisconnectedAsync` sees no error. It waits at most `with_close_timeout` (5 s), then drops the transport. `disconnect()` is the synchronous form; it stops the same way and lets the transport close in the background.

Stopping on any handle supersedes a running start, reconnect or pause, fails pending invocations and open streams with `ErrorKind::ConnectionClosed` (`CloseReason::Local`), raises `on_closed`, and sets the status to `Closed`. Callbacks and hooks stay registered for a later `start`. Stopping a stopped client does nothing.

A call made while a start or reconnect attempt is in flight waits for that attempt, then runs on the new connection or fails with the attempt's error. During a reconnect policy's delay, calls fail at once with `ErrorKind::Transport`.

## Liveness

Three timers keep a connection honest, a fourth bounds connection setup and a fifth bounds `stop`.

```rust
use std::time::Duration;
use dxn_signalr::SignalRClient;

fn timers() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("localhost", "chat", |c| {
        c.with_keep_alive_interval(Duration::from_secs(10));
        c.with_server_timeout(Duration::from_secs(40));
        c.with_handshake_timeout(Duration::from_secs(5));
        c.with_connect_timeout(Duration::from_secs(10));
        c.with_close_timeout(Duration::from_secs(2));
    })
}
```

| Option | Default | Behavior |
|---|---|---|
| `with_keep_alive_interval` | 15 s | After this long without any outbound message the client sends a `Ping`; it also sends one right after every handshake. The server's `ClientTimeoutInterval` (30 s by default) drops clients that stay silent longer. |
| `with_server_timeout` | 30 s | After this long without any inbound message the transport counts as dropped and the reconnect path runs. The server pings every `KeepAliveInterval` (15 s by default). |
| `with_handshake_timeout` | 15 s | `start` (and each reconnect attempt) fails when the server does not answer the handshake in time. |
| `with_connect_timeout` | 120 s | Bounds each negotiate request and starting the transport. Fails with `ErrorKind::Timeout` naming `Negotiate` or `Connect`. |
| `with_close_timeout` | 5 s | How long `stop` waits for the transport to close. |

:::warning Keep the two sides' timers in proportion
As the official clients advise, keep the client's server timeout at least twice the server's `KeepAliveInterval`, and the client's keep-alive interval at most half the server's `ClientTimeoutInterval`. Every duration is accepted, but values that cannot work are logged as warnings when the client is built: a zero keep-alive or server timeout (which acts as one second), a zero handshake or connect timeout (every attempt then times out), and a server timeout not above the keep-alive interval.
:::

The keep-alive interval and server timeout apply to WebSockets and Server-Sent Events. Long Polling keeps itself alive and runs neither.

### Suspension detection

An app can be suspended without being paused: the OS stops the process, the browser freezes the page, or the device sleeps. Its timers then fire late, all at once. The client checks its timers at least once per second on a clock that includes the time the device slept. When a check comes more than one keep-alive interval late, the app was suspended:

- If a message arrived within the server timeout, the client sends a `Ping` at once so the server hears from it before its own timeout.
- Otherwise the server timeout has passed: the connection counts as lost at once (`ErrorKind::Timeout` with `TimeoutOperation::ServerTimeout`) and the reconnection policy runs. As in the .NET client, a server timeout is not resumed, stateful or not.

After a suspension longer than the server's client timeout, ASP.NET Core ends the hub connection, so even a stateful client reconnects on a new connection. When the app knows it is going away, pause instead.

## Pausing and resuming

`pause()` and `resume()` let the application control the connection, for example when a mobile app moves to the background:

```rust
use dxn_signalr::{ConnectionStatus, SignalRClient};

async fn background_and_back(client: &SignalRClient) -> Result<(), dxn_signalr::Error> {
    // Going to the background: end the transport, stop keep-alive and automatic reconnects.
    client.pause().await?;
    assert_eq!(client.status(), ConnectionStatus::Paused);

    // Back in the foreground: reconnect now, then follow the reconnection policy.
    client.resume().await
}
```

`pause()` ends the transport, stops keep-alive and supersedes any running start or reconnect. Calls fail while paused, and ordinary pending invocations and streams fail with `ErrorKind::Paused`. It does nothing on a paused client, returns `ErrorKind::ConnectionClosed` on a stopped one and `ErrorKind::InvalidState` on one never started.

`resume()` starts a reconnect from `Paused` or `Disconnected` and returns once connected. The first attempt starts immediately and later attempts follow the reconnection policy (`NoReconnectPolicy` allows only the first). If every attempt fails the client is `Disconnected` and `resume` returns the error; a later `resume` tries again with a fresh negotiation.

With [stateful reconnect](./reconnection.md#stateful-reconnect) the runtimes differ, because ASP.NET Core disposes a stateful connection as soon as the client sends a WebSocket Close:

- **Native** pause drops the transport without a Close. The server keeps the logical connection for its stateful window, pending work stays pending, and `resume` continues the same connection. After the server gives up, `resume` negotiates a fresh one.
- **Browser** pause cannot avoid the closing handshake, so it ends the logical connection: pending work fails and `resume` negotiates a new one.

The [cookbook](./cookbook.md#pause-and-resume-with-the-app-lifecycle) wires `pause` and `resume` to iOS, Android and browser lifecycle events.

## Clones and ownership

`SignalRClient` is `Clone`. Clones share the connection, callbacks, hooks, lifecycle and status.

- `disconnect` and `stop` stop the connection for **every** handle, as `HubConnection.StopAsync` does. Other clones and live `InvocationContext`s do not keep it open.
- Dropping a handle stops the connection only when it is the **last** one. Registered callbacks and lifecycle hooks never count as handles; an `InvocationContext` holds one while it lives.
- `downgrade()` returns a `WeakSignalRClient` that does not keep the client alive; `upgrade()` returns the client while any handle exists.

`disconnect` and drop are synchronous and briefly block the calling thread to update the shared connection state.

## Server-initiated close

A server can close a connection (`Context.Abort()`, shutdown, a hub filter). The `Close` message says whether the client may reconnect.

- **Reconnect allowed:** treated like a dropped transport; the reconnection policy or disconnection handler runs.
- **Reconnect not allowed:** pending work fails with `ErrorKind::ConnectionClosed` (`CloseReason::Server`, the server's error in `server_message()`, `server_allows_reconnect() == Some(false)`), the status becomes `Disconnected`, `on_closed` runs, and nothing reconnects automatically. `start` connects again.

## Next

[Reconnection](./reconnection.md) covers policies and stateful reconnect.
