# Reconnection

A transport can drop: the network goes away, the server restarts, or the
[server timeout](06-connection-lifecycle.md#liveness) expires. What happens next depends on two
choices: who drives the reconnect (a policy, or your own handler), and whether the logical
connection survives the drop (ordinary or stateful reconnect).

## Automatic reconnection

By default the client never reconnects: `ReconnectionConfig::default()` holds `NoReconnectPolicy`.
As in the .NET client, a drop the policy allows no attempt for ends the connection at once: pending
work fails with `ErrorKind::Transport`, `on_closed` runs with the drop reason, and the client is
`Disconnected` (`start` connects again). Opt in with a policy:

```rust
use std::sync::Arc;
use rayx_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient};

let client = SignalRClient::connect_with("localhost", "chat", |c| {
    c.with_reconnection_policy(ReconnectionConfig {
        policy: Arc::new(DefaultRetryPolicy),
    });
})
.await?;
```

On a drop the client fails ordinary pending work, moves to `Reconnecting`, raises
[`on_reconnecting`](06-connection-lifecycle.md#lifecycle-events), waits for the policy's first
delay, renegotiates and handshakes. On success it is `Connected` again and raises `on_reconnected`
with the connection id; when the policy returns `None` it gives up, becomes `Disconnected` and
raises `on_closed`. Calls made while an attempt is in flight wait for it; during a delay they fail
at once.

### Built-in policies

| Policy | Delays |
|---|---|
| `NoReconnectPolicy` | never retries (the default) |
| `DefaultRetryPolicy` | 0, 2, 10 and 30 seconds, then stops, like the official JavaScript and Swift clients' `withAutomaticReconnect()` |
| `ConstantDelayPolicy::new(delay, max_attempts)` | `delay` every time, up to `max_attempts` (`None`: forever) |
| `LinearBackoffPolicy::new(initial, increment, max_delay, max_attempts)` | `initial + increment × n`, capped at `max_delay` |
| `ExponentialBackoffPolicy::new(initial, factor, max_delay, max_attempts)` | `initial × factor^n`, capped at `max_delay` |
| `DelayListPolicy::new(delays)` | each delay of the list in turn, then stops, like the .NET client's `WithAutomaticReconnect(TimeSpan[])` |

`n` is the zero-based retry count.

```rust
use std::{sync::Arc, time::Duration};
use rayx_signalr::{ExponentialBackoffPolicy, ReconnectionConfig};

let config = ReconnectionConfig {
    policy: Arc::new(ExponentialBackoffPolicy::new(
        Duration::from_millis(500), // first delay
        2.0,                        // factor
        Some(Duration::from_secs(30)),
        Some(10),                   // give up after 10 attempts
    )),
};
```

### Custom policies

Implement `ReconnectionPolicy` (it must be `Send + Sync` natively). `next_retry_delay_with`
receives a `RetryContext`, as the .NET client's `IRetryPolicy.NextRetryDelay`:

```rust
use std::time::Duration;
use rayx_signalr::{ErrorKind, ReconnectionPolicy, RetryContext};

/// Retries every 5 seconds for at most two minutes, but not after a failed authentication.
struct ForTwoMinutes;

impl ReconnectionPolicy for ForTwoMinutes {
    fn next_retry_delay_with(&self, context: &RetryContext) -> Option<Duration> {
        if context.retry_reason.kind() == ErrorKind::Authentication {
            return None;
        }
        (context.elapsed < Duration::from_secs(120)).then(|| Duration::from_secs(5))
    }
}
```

`previous_retry_count` starts at 0; `elapsed` is the time since the reconnect run began;
`retry_reason` is why the transport ended before the first attempt, then the last attempt's error.
Returning `None` ends the run. Policies written for the older
`next_retry_delay(retry_count, elapsed_milliseconds)` keep working: `next_retry_delay_with` calls it
by default. A policy that panics stops the run, which then ends as if it had returned `None`.
`RetryContext::new` builds a context, for example to unit-test a policy.

## Manual reconnection

A `DisconnectionHandler` takes over: with one configured, the client never reconnects on its own,
and calls the handler whenever the transport drops or the server closes the connection.

```rust
use std::sync::Arc;
use rayx_signalr::{
    DefaultRetryPolicy, DisconnectionHandler, ReconnectionConfig, ReconnectionHandler, SignalRClient,
};

struct Reconnector;

impl DisconnectionHandler for Reconnector {
    fn on_disconnected(&self, reconnection: ReconnectionHandler) {
        if !reconnection.server_allows_reconnect() {
            eprintln!("{} closed the connection for good", reconnection.endpoint());
            return;
        }
        tokio::spawn(async move {
            // Uses the configured ReconnectionConfig; the first attempt starts immediately.
            if let Err(error) = reconnection.reconnect_with_policy().await {
                eprintln!("giving up: {error}");
            }
        });
    }
}

let client = SignalRClient::connect_with("localhost", "chat", |c| {
    // The policy only paces reconnect_with_policy here; the handler decides when to reconnect.
    c.with_reconnection_policy(ReconnectionConfig {
        policy: Arc::new(DefaultRetryPolicy),
    });
    c.with_disconnection_handler(Reconnector);
})
.await?;
```

`ReconnectionHandler`:

| Method | Description |
|---|---|
| `server_allows_reconnect()` | `false` after a server Close without reconnect permission; the reconnect methods then fail with that close error. |
| `reason()` | Why the transport ended: a dropped transport (`ErrorKind::Transport`), the server timeout (`ErrorKind::Timeout`), a server Close (`ErrorKind::ConnectionClosed` with the server's message), or a protocol violation (`ErrorKind::Protocol`). |
| `reconnect().await` | One attempt; fails with the attempt's error, `Paused` or `ConnectionClosed` when `pause` or a local close superseded it, or `Canceled` while another reconnect runs. |
| `reconnect_with_policy().await` | Attempts until the configured policy gives up, then fails with `ConnectionClosed` (`CloseReason::ReconnectExhausted`, the last attempt's error as its `source()`). The first attempt starts immediately when the policy allows any attempt (with `NoReconnectPolicy` there is none). |
| `is_connected().await` | Whether the client is connected. |
| `endpoint()` | The hub endpoint, for logging. |

The handler runs on the connection's runtime natively (so `tokio::spawn` works inside it; the
handler must be `Send + Sync`) and on the event loop in the browser (use
`wasm_bindgen_futures::spawn_local`). Pending ordinary invocations have already failed when the
handler runs; stateful pending work waits for the resume. `on_closed` has been raised for the drop
(manual mode has no automatic reconnect), and a manual reconnect raises no lifecycle event.

`resume()` is the simpler alternative when you only need "reconnect now":
[Pausing and resuming](06-connection-lifecycle.md#pausing-and-resuming).

## Ordinary reconnect

Without stateful reconnect, every reconnect creates a **new logical connection**: a fresh
negotiation, a new connection id, and a new handshake. Consequences:

- pending invocations and open streams fail with `ErrorKind::Transport` when the transport drops;
  nothing is replayed;
- messages the server sent while the client was away are lost;
- server-side state keyed by connection id (groups, `Context.Items`) is gone: rejoin groups after
  reconnecting;
- registered callbacks stay registered.

## Stateful reconnect

ASP.NET Core 8 added [stateful reconnect](https://learn.microsoft.com/aspnet/core/signalr/configuration#configure-stateful-reconnect):
the client and server buffer unacknowledged messages, and after a short drop the client resumes the
**same logical connection** and both sides replay what the other missed.

```rust
use std::sync::Arc;
use rayx_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient, StatefulReconnectConfig};

let stateful = StatefulReconnectConfig::new(256 * 1024)?; // replay buffer in bytes
let client = SignalRClient::connect_with("localhost", "chat", move |c| {
    c.with_stateful_reconnect(stateful);
    c.with_reconnection_policy(ReconnectionConfig {
        policy: Arc::new(DefaultRetryPolicy),
    });
})
.await?;
```

`StatefulReconnectConfig::default()` uses a 100,000-byte buffer; `new(0)` is a `Configuration`
error.

How it works:

- Negotiation adds `useStatefulReconnect=true`. If the server does not enable stateful reconnect
  for the hub, the connection silently falls back to ordinary reconnect.
- Every application message the client sends is kept, serialized, until the server acknowledges it
  (`Ack`). As in the .NET client's `MessageBuffer`, a send waits only while the buffer holds more
  than its size: a message larger than the buffer is still sent, and the next send waits until
  acknowledgements free space.
- The client acknowledges what it received at most once per second, and only when something new
  arrived (the last messages of a burst are acknowledged once the connection is quiet), as the .NET
  client's Ack timer does.
- A protocol violation ends the transport with `ErrorKind::Protocol` and is never resumed, since
  the server would replay the offending message: a malformed, unexpected or oversized message, a
  WebSocket message of the wrong kind, or a violation of the stateful protocol (for example a
  message before the `Sequence` after a resume). The reconnect then negotiates a fresh stateful
  connection. A failed send, including an Ack, is an ordinary loss and is resumed; a message is
  always dispatched before its Ack is sent.
- On a drop, the client reconnects to the same connection token without negotiating or
  handshaking again, exchanges `Sequence` messages, replays unacknowledged messages, and discards
  duplicates the server replays.
- Pending invocations and streams stay pending across the drop and complete after the resume.
- The server keeps a disconnected stateful connection only for a limited time. When a resume fails
  because the server gave up, or the policy is exhausted, the logical connection ends: pending work
  fails and the next attempt negotiates a fresh stateful connection.

Stateful reconnect needs negotiation, so `skip_negotiation()` disables it (with a warning). It
needs a reconnection policy, a disconnection handler or `resume()` to actually reconnect. The
server side must opt in with `AllowStatefulReconnects = true` on the hub (see
[Server setup](10-server-setup.md#stateful-reconnect)).

## Which combination to use

| Need | Configuration |
|---|---|
| Fail fast, let the app decide | default (`NoReconnectPolicy`), check `status()` |
| Survive restarts and network changes | `DefaultRetryPolicy` or a backoff policy |
| Never lose an in-flight call across short drops | stateful reconnect plus a policy |
| Mobile background/foreground | a policy, plus `pause()`/`resume()` on lifecycle events |
| Custom UI ("Reconnecting…", retry button) | a `DisconnectionHandler`, or poll `status()` and call `resume()` |
