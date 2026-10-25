---
title: Reconnection
description: Retry policies, custom policies, manual reconnection with a disconnection handler, and stateful reconnect with replay of unacknowledged messages.
group: Connection
order: 110
---

A transport can drop: the network goes away, the server restarts, or the server timeout expires. What happens next depends on two choices: who drives the reconnect (a policy, or your own handler), and whether the logical connection survives the drop (ordinary or stateful reconnect).

## Automatic reconnection

By default the client never reconnects: `ReconnectionConfig::default()` holds `NoReconnectPolicy`. As in the .NET client, a drop the policy allows no attempt for ends the connection at once. Pending work fails with `ErrorKind::Transport`, `on_closed` runs with the drop reason, and the client is `Disconnected`. Opt in with a policy:

```rust
use std::sync::Arc;
use dxn_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient};

async fn reconnecting() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::connect_with("localhost", "chat", |c| {
        c.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(DefaultRetryPolicy),
        });
    })
    .await
}
```

On a drop the client fails ordinary pending work, moves to `Reconnecting`, raises [`on_reconnecting`](./lifecycle.md#lifecycle-events), waits for the policy's first delay, renegotiates and handshakes. On success it is `Connected` again and raises `on_reconnected` with the connection id. When the policy returns `None` it gives up, becomes `Disconnected` and raises `on_closed`.

### Built-in policies

| Policy | Delays |
|---|---|
| `NoReconnectPolicy` | never retries (the default) |
| `DefaultRetryPolicy` | 0, 2, 10 and 30 seconds, then stops, like the JavaScript and Swift clients' `withAutomaticReconnect()` |
| `ConstantDelayPolicy::new(delay, max_attempts)` | `delay` every time, up to `max_attempts` (`None`: forever) |
| `LinearBackoffPolicy::new(initial, increment, max_delay, max_attempts)` | `initial + increment × n`, capped at `max_delay` |
| `ExponentialBackoffPolicy::new(initial, factor, max_delay, max_attempts)` | `initial × factor^n`, capped at `max_delay` |
| `DelayListPolicy::new(delays)` | each delay of the list in turn, then stops, like the .NET client's `WithAutomaticReconnect(TimeSpan[])` |

`n` is the zero-based retry count. `max_delay` and `max_attempts` are `Option`s.

```rust
use std::{sync::Arc, time::Duration};
use dxn_signalr::{ExponentialBackoffPolicy, ReconnectionConfig};

fn backoff() -> ReconnectionConfig {
    ReconnectionConfig {
        policy: Arc::new(ExponentialBackoffPolicy::new(
            Duration::from_millis(500), // first delay
            2.0,                        // factor
            Some(Duration::from_secs(30)),
            Some(10), // give up after 10 attempts
        )),
    }
}
```

### Custom policies

Implement `ReconnectionPolicy` (it must be `Send + Sync` natively). `next_retry_delay_with` receives a `RetryContext`, like the .NET client's `IRetryPolicy.NextRetryDelay`. `previous_retry_count` starts at 0, `elapsed` is the time since the run began, and `retry_reason` is why the transport ended, then the last attempt's error. Returning `None` ends the run.

```rust
use std::time::Duration;
use dxn_signalr::{ErrorKind, ReconnectionPolicy, RetryContext};

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

A policy that panics stops the run as if it had returned `None`. `RetryContext::new` builds a context, for example to unit-test a policy.

## Manual reconnection

A `DisconnectionHandler` takes over: with one configured, the client never reconnects on its own and calls the handler whenever the transport drops or the server closes the connection.

```rust
use std::sync::Arc;
use dxn_signalr::{
    DefaultRetryPolicy, DisconnectionHandler, ReconnectionConfig, ReconnectionHandler,
    SignalRClient,
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

async fn manual() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::connect_with("localhost", "chat", |c| {
        // The policy only paces reconnect_with_policy here; the handler decides when to reconnect.
        c.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(DefaultRetryPolicy),
        });
        c.with_disconnection_handler(Reconnector);
    })
    .await
}
```

| `ReconnectionHandler` method | Description |
|---|---|
| `server_allows_reconnect()` | `false` after a server Close without reconnect permission; the reconnect methods then fail with that close error. |
| `reason()` | Why the transport ended: a dropped transport (`Transport`), the server timeout (`Timeout`), a server Close (`ConnectionClosed`) or a protocol violation (`Protocol`). |
| `reconnect().await` | One attempt; fails with the attempt's error, `Paused` or `ConnectionClosed` when superseded, or `Canceled` while another reconnect runs. |
| `reconnect_with_policy().await` | Attempts until the configured policy gives up, then fails with `ConnectionClosed` (`CloseReason::ReconnectExhausted`). |
| `is_connected().await` | Whether the client is connected. |
| `endpoint()` | The hub endpoint, for logging. |

The handler runs on the connection's runtime natively (so `tokio::spawn` works inside it; it must be `Send + Sync`) and on the event loop in the browser (use `wasm_bindgen_futures::spawn_local`). Pending invocations have already failed when it runs. `on_closed` has been raised for the drop, and a manual reconnect raises no lifecycle event.

`resume()` is the simpler alternative when you only need "reconnect now": see [Pausing and resuming](./lifecycle.md#pausing-and-resuming).

## Ordinary reconnect

Without stateful reconnect, every reconnect creates a **new logical connection**: a fresh negotiation, a new connection id and a new handshake.

- Pending invocations and open streams fail with `ErrorKind::Transport`. Nothing is replayed.
- Messages the server sent while the client was away are lost.
- Server-side state keyed by connection id (groups, `Context.Items`) is gone, so rejoin groups after reconnecting.
- Registered callbacks stay registered.

## Stateful reconnect

ASP.NET Core 8 added [stateful reconnect](https://learn.microsoft.com/aspnet/core/signalr/configuration#configure-stateful-reconnect): client and server buffer unacknowledged messages, and after a short drop the client resumes the **same logical connection** while both sides replay what the other missed.

```rust
use std::sync::Arc;
use dxn_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient, StatefulReconnectConfig};

async fn stateful() -> Result<SignalRClient, dxn_signalr::Error> {
    let stateful = StatefulReconnectConfig::new(256 * 1024)?; // replay buffer in bytes
    SignalRClient::connect_with("localhost", "chat", move |c| {
        c.with_stateful_reconnect(stateful);
        c.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(DefaultRetryPolicy),
        });
    })
    .await
}
```

`StatefulReconnectConfig::default()` uses a 100,000-byte buffer; `new(0)` is a `Configuration` error. The hub must opt in with `AllowStatefulReconnects = true` (see [Server setup](./server-setup.md#stateful-reconnect)). If it does not, the connection silently falls back to ordinary reconnect.

How it works:

- Every application message the client sends is kept, serialized, until the server acknowledges it. A send waits only while the buffer holds more than its size; a message larger than the buffer is still sent, and the next send waits.
- The client acknowledges what it received at most once per second, and only when something new arrived.
- When the transport is lost ungracefully, the client resumes it **once, at once and transparently**, whatever the reconnection policy (also with `NoReconnectPolicy`): it reconnects to the same connection token without negotiating or handshaking again, exchanges `Sequence` messages, replays unacknowledged messages and discards duplicates. Meanwhile `status()` stays `Connected`, no lifecycle hook runs, the connection id is kept, and sends go into the replay buffer.
- Pending invocations and streams stay pending across the drop and complete after the resume.
- When the resume fails (the server no longer holds the connection and answers 404), or after a server Close, a server timeout or a protocol violation, the logical connection ends. Pending work fails, and the connection continues as after an ordinary drop: the policy or handler reconnects with a fresh negotiation and a new connection id, or `on_closed` runs.

Stateful reconnect runs only over WebSockets and needs negotiation, so `skip_negotiation()` disables it (with a warning).

## Which combination

| Need | Configuration |
|---|---|
| Fail fast, let the app decide | the default (`NoReconnectPolicy`); check `status()` |
| Survive restarts and network changes | `DefaultRetryPolicy` or a backoff policy |
| Never lose an in-flight call across short drops | stateful reconnect, plus a policy for longer outages |
| Mobile background and foreground | a policy, plus `pause()` and `resume()` on lifecycle events |
| Custom UI ("Reconnecting...", a retry button) | lifecycle hooks, or a `DisconnectionHandler`; see the [cookbook](./cookbook.md#show-reconnect-state-in-a-ui) |

## Next

[The cookbook](./cookbook.md) has reconnect UX and mobile pause recipes. [Errors](./errors.md) lists what pending calls fail with.
