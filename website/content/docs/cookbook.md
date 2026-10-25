---
title: Cookbook
description: Recipes for token refresh, streaming into a channel, client results, reconnect state in a UI, rejoining groups, and pausing with the app lifecycle on mobile and in the browser.
group: Guides
order: 160
---

Self-contained recipes for problems that come up once the basics work. Native snippets use Tokio; browser snippets are marked.

## Refresh an access token

A JWT that expires while the app runs must be replaced on every reconnect. `with_access_token_factory` is asked for a token before every negotiate and every WebSocket open, so it can read from a shared store that refreshes lazily.

```rust
use std::{sync::Arc, time::{Duration, Instant}};

use dxn_signalr::SignalRClient;
use tokio::sync::RwLock;

struct Token {
    value: String,
    expires_at: Instant,
}

#[derive(Clone)]
struct TokenStore {
    inner: Arc<RwLock<Token>>,
}

impl TokenStore {
    fn fresh(token: &Token) -> bool {
        token.expires_at > Instant::now() + Duration::from_secs(60)
    }

    async fn access_token(&self) -> Result<String, String> {
        {
            let token = self.inner.read().await;
            if Self::fresh(&token) {
                return Ok(token.value.clone());
            }
        }
        let mut token = self.inner.write().await;
        // Another task may have refreshed while this one waited for the write lock.
        if !Self::fresh(&token) {
            *token = request_new_token().await?;
        }
        Ok(token.value.clone())
    }
}

async fn request_new_token() -> Result<Token, String> {
    // Call your identity provider here; an `Err` fails the connection attempt.
    Ok(Token {
        value: "<access token>".to_string(),
        expires_at: Instant::now() + Duration::from_secs(3600),
    })
}

fn orders_client(store: TokenStore) -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("api.example.com", "hubs/orders", move |c| {
        c.with_access_token_factory(move || {
            let store = store.clone();
            async move { store.access_token().await }
        });
    })
}
```

An `Err` from the factory fails that attempt with `ErrorKind::Authentication`, and a reconnection policy tries again. The factory cannot be combined with `authenticate_bearer` or `authenticate_basic`.

:::warning Open connections keep their old token
ASP.NET Core checks the token when the connection is established, not afterwards. For the server to end a connection when its token expires, so the client reconnects with a fresh one, set `CloseOnAuthenticationExpiration`:
:::

```csharp
app.MapHub<OrdersHub>("/hubs/orders", options => options.CloseOnAuthenticationExpiration = true);
```

Browsers pass the token as the `access_token` query parameter on the WebSocket; the server must read it. See [Server setup](./server-setup.md#authentication).

## Stream into a channel, with a deadline

A server stream is a `futures::Stream`, so it composes with `select!`. Dropping it cancels the server-side enumeration.

```rust
use std::time::Duration;

use dxn_signalr::SignalRClient;
use futures::StreamExt;
use tokio::sync::mpsc;

async fn watch_prices(mut client: SignalRClient, tx: mpsc::Sender<f64>) {
    let mut prices = client.enumerate::<f64>("Prices".to_string()).await;
    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            item = prices.next() => match item {
                // The receiver is gone: leave the loop; dropping `prices` cancels the stream.
                Some(price) => if tx.send(price).await.is_err() { break },
                None => break,
            },
            _ = &mut deadline => break,
        }
    }

    if let Some(error) = prices.error() {
        eprintln!("the stream failed: {error}");
    }
}
```

`error()` is `None` while the stream is open, so a stream cut short by the deadline reports nothing. The matching hub method honors its `CancellationToken`, which fires on the drop:

```csharp
public async IAsyncEnumerable<double> Prices([EnumeratorCancellation] CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        yield return NextPrice();
        await Task.Delay(250, cancellationToken);
    }
}
```

## Answer a client result

The hub waits for a value from one client; the client asks the user and answers. A `register_async` callback can await the answer without blocking other callbacks' completions.

```csharp
public async Task<bool> RequestApproval(string connectionId, string question)
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    return await Clients.Client(connectionId).InvokeAsync<bool>("Confirm", question, timeout.Token);
}
```

```rust
use dxn_signalr::SignalRClient;
use tokio::sync::{mpsc, oneshot};

type Prompt = (String, oneshot::Sender<bool>);

fn answer_confirmations(client: &mut SignalRClient, prompts: mpsc::Sender<Prompt>) {
    let _confirm = client.register_async("Confirm".to_string(), move |mut ctx| {
        let prompts = prompts.clone();
        async move {
            let question: String = ctx.argument(0).unwrap_or_default();
            let (answer_tx, answer_rx) = oneshot::channel();
            let approved = match prompts.send((question, answer_tx)).await {
                Ok(()) => answer_rx.await.unwrap_or(false),
                Err(_) => false,
            };
            if let Err(error) = ctx.complete(approved).await {
                eprintln!("cannot answer the server: {error}");
            }
        }
    });
}
```

The UI task receives `(question, answer_tx)` from the channel and sends the user's choice back. Awaiting `InvokeAsync` inside a hub method requires `MaximumParallelInvocationsPerClient` greater than 1, otherwise the waiting hub call blocks the connection that has to deliver the answer. If the user takes longer than the server's token allows, `complete` returns an error because the server has forgotten the call.

## Show reconnect state in a UI

Three lifecycle hooks cover every transition a UI needs. Fold them into a `watch` channel that the UI task reads.

```rust
use dxn_signalr::{ConnectionStatus, SignalRClient};
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq)]
enum Link {
    Online,
    Reconnecting(String),
    Offline(Option<String>),
}

fn track(client: &SignalRClient) -> watch::Receiver<Link> {
    let (tx, rx) = watch::channel(Link::Online);

    let reconnecting = tx.clone();
    let _ = client.on_reconnecting(move |reason| {
        reconnecting.send_replace(Link::Reconnecting(reason.to_string()));
    });
    let reconnected = tx.clone();
    let _ = client.on_reconnected(move |_id| {
        reconnected.send_replace(Link::Online);
    });
    let _ = client.on_closed(move |error| {
        tx.send_replace(Link::Offline(error.map(|e| e.to_string())));
    });
    rx
}

/// The "Retry" button: restart a stopped client, or reconnect one that gave up.
async fn retry(client: &SignalRClient) -> Result<(), dxn_signalr::Error> {
    match client.status() {
        ConnectionStatus::Closed => client.start().await,
        _ => client.resume().await,
    }
}
```

Call `track` before `start`. A transparent stateful resume raises no hook and keeps the status `Connected`, so the UI correctly shows nothing for drops the client heals itself. For a status badge without events, poll `client.status()`.

## Rejoin groups after reconnecting

After an ordinary reconnect the connection id is new, so server-side group membership is gone. Re-join from `on_reconnected`. The hook captures a weak handle so it does not keep the client alive.

```rust
use dxn_signalr::SignalRClient;

fn rejoin_groups(client: &SignalRClient, groups: Vec<String>) {
    let weak = client.downgrade();
    let _ = client.on_reconnected(move |_id| {
        let Some(mut client) = weak.upgrade() else { return };
        let groups = groups.clone();
        tokio::spawn(async move {
            for group in groups {
                let joined = client
                    .send_with_args("JoinGroup".to_string(), |args| {
                        args.argument(&group);
                    })
                    .await;
                if let Err(error) = joined {
                    eprintln!("cannot rejoin {group}: {error}");
                }
            }
        });
    });
}
```

With stateful reconnect a resumed connection keeps its groups and the hook does not run; the hook runs only when a fresh connection replaced the old one.

## Pause and resume with the app lifecycle

Mobile operating systems suspend background apps, and the server drops a client that is silent for its client timeout. When the app knows it is going away, `pause()` ends the transport cleanly; `resume()` reconnects when it returns. Both are safe to call at any time (`resume` on a connected client does nothing).

```rust
use dxn_signalr::SignalRClient;

/// Call from the platform's lifecycle notifications, with a handle to the connection's runtime.
pub fn on_app_state_changed(runtime: &tokio::runtime::Handle, client: &SignalRClient, foreground: bool) {
    let client = client.clone();
    runtime.spawn(async move {
        let result = if foreground { client.resume().await } else { client.pause().await };
        if let Err(error) = result {
            log::warn!("lifecycle change failed: {error}");
        }
    });
}
```

- **iOS:** call it from `sceneDidEnterBackground` or `applicationDidEnterBackground` with `foreground = false`, and from `sceneDidBecomeActive` or `applicationDidBecomeActive` with `true`, through your FFI layer. iOS suspends background apps after a few seconds.
- **Android:** call it from the activity's (or `ProcessLifecycleOwner`'s) `onStop` and `onStart`, through a JNI method that drives your client. Doze and app standby defer background network access.
- **Without pausing:** suspension detection recovers as soon as the app runs again, but a suspended app receives nothing and the server ends the connection after its client timeout.

With stateful reconnect, a native pause keeps the logical connection for the server's stateful window so pending calls survive; a browser pause ends it.

### In the browser

Browsers throttle hidden tabs, so pause after the page has been hidden for a while rather than on every tab switch. This snippet needs the `web-sys` features `Window`, `Document` and `VisibilityState`, and `gloo-timers` with `futures`.

```rust
use dxn_signalr::SignalRClient;
use gloo_timers::future::TimeoutFuture;
use wasm_bindgen::{prelude::*, JsCast};

pub fn pause_when_hidden(client: SignalRClient) -> Result<(), JsValue> {
    let document = web_sys::window()
        .and_then(|window| window.document())
        .ok_or_else(|| JsValue::from_str("no document"))?;

    let watched = document.clone();
    let on_change = Closure::<dyn FnMut()>::new(move || {
        let client = client.clone();
        let document = watched.clone();
        wasm_bindgen_futures::spawn_local(async move {
            if document.visibility_state() == web_sys::VisibilityState::Hidden {
                TimeoutFuture::new(30_000).await; // hidden for 30 s
                if document.hidden() {
                    let _ = client.pause().await;
                }
            } else {
                let _ = client.resume().await;
            }
        });
    });
    document.add_event_listener_with_callback("visibilitychange", on_change.as_ref().unchecked_ref())?;
    on_change.forget();
    Ok(())
}
```

## Time out a call in the browser

`tokio::time::timeout` is not available on `wasm32-unknown-unknown`. Race the call against a `gloo-timers` future.

```rust
use dxn_signalr::SignalRClient;
use futures::future::{select, Either};
use gloo_timers::future::TimeoutFuture;

pub async fn total_within(client: &mut SignalRClient, millis: u32) -> Option<i32> {
    let call = Box::pin(client.invoke::<i32>("Total".to_string()));
    match select(call, TimeoutFuture::new(millis)).await {
        Either::Left((result, _)) => result.ok(),
        Either::Right(_) => {
            log::warn!("Total did not answer within {millis} ms");
            None
        }
    }
}
```

## Next

[Troubleshooting](./troubleshooting.md) maps symptoms to causes. [Server setup](./server-setup.md) lists the hub configuration each recipe assumes.
