---
title: Troubleshooting
description: Find the cause of a SignalR client problem from the symptom or the error you see - connection failures, silent callbacks, serialization errors, drops and reconnect surprises.
group: Reference
order: 190
---

Start from what you observe. Each entry names the `ErrorKind` where there is one and points at the fix. Turn on `RUST_LOG=dxn_signalr=debug` first when the symptom is not an error; see [Logging](./errors.md#logging).

## Connecting

### Connection refused, or it connects to the wrong place

`connect("localhost", "chat")` connects to `https://localhost/chat`. A local development server usually needs `unsecure()` and `with_port(...)`. The domain argument is a host name without scheme or path.

### `Configuration` error mentioning a Tokio runtime

A native client needs a Tokio runtime when it is built. Call `new` or `connect` inside a runtime (`#[tokio::main]`), or pass `with_runtime_handle(handle)`. The error's `setting()` is `with_runtime_handle`. See [Platforms](./platforms.md#native-runtime-ownership).

### `Configuration` error naming `TLS Cargo features`

A secure endpoint needs one verifier feature and one provider feature. You probably built with `default-features = false`. Enable `tls-platform-verifier` or `tls-webpki-roots`, and `tls-ring` or `tls-aws-lc-rs`. See [TLS](./tls.md#features).

### `Negotiate` error with status 404

The server has no hub at that path. The `hub` argument must equal the path given to `MapHub` without the leading slash (`"chat"` for `MapHub<ChatHub>("/chat")`). Check any `with_hub` override and a reverse proxy that rewrites paths.

### `Authentication` error with status 401 or 403

The server refused the credentials at negotiate. Check the token's lifetime and audience, and that the hub requires the scheme you send. For tokens that expire, use `with_access_token_factory` instead of `authenticate_bearer`. A 403 from a server that requires a client certificate means `with_client_certificate` is missing. See [Authentication](./authentication.md).

### The browser connects but the WebSocket is rejected with 401

Browsers cannot set headers on a WebSocket, so the Bearer token travels as the `access_token` query parameter. The server must read it for hub requests (`OnMessageReceived`). Basic credentials never reach a browser WebSocket. See [Server setup](./server-setup.md#authentication).

### The browser fails before any HTTP status

A failed negotiate `fetch` with no status is usually CORS: the server must allow the page's origin, the `GET`, `POST` and `DELETE` methods, and credentials. The browser reports the real reason only in its console. See [CORS](./server-setup.md#cors).

### `Tls` error

The server's certificate is not trusted by the verifier. For an internal or development CA, add it with `with_additional_root_certificates_pem`, or install it in the operating system's trust store. A name mismatch or an expired certificate fails the same way. On Android with the platform verifier, call `tls::android::init_with_env` once before the first secure connection. See [TLS](./tls.md) and [Platforms](./platforms.md#android).

### `Timeout` naming `Handshake`

The transport opened but the server did not answer the SignalR handshake within `with_handshake_timeout` (15 s). The endpoint may not be a SignalR hub, or an intermediary buffers the upgrade. Check the URL and any proxy in front of the server.

### `Handshake` error

The server answered the handshake with an error. The usual cause is a protocol the server does not offer: the client asks for `messagepack` but the server lacks `.AddMessagePackProtocol()`. See [Hub protocols](./hub-protocols.md).

### `TransportUnavailable`

No transport could start. Read `error.attempts()`: each entry says why a transport was skipped or failed. Common causes are `with_transports` excluding what the server offers, the server restricting transports, and MessagePack with only Server-Sent Events (which carry text only). See [Transports](./transports.md#selection-and-fallback).

### `Proxy` error with status 407

The proxy wants credentials the client cannot give. Put them in the proxy URL (`http://user:password@proxy:3128`). The client answers only Basic challenges; Digest, NTLM and Negotiate proxies fail. See [Proxies](./proxies.md).

### The build fails with "supports browser WASM ... as its only wasm target"

You compiled for WASI or another `wasm32` target. Build for `wasm32-unknown-unknown`. See [Browser](./browser.md#set-up-a-project).

### The wasm module fails to load with a schema or version mismatch

The `wasm-bindgen` CLI version must equal the `wasm-bindgen` crate version in `Cargo.lock`. Install the matching CLI or run `wasm-pack`.

## Calls

### `ErrorKind::InvalidState`: "the connection is not started"

You built the client with `new` and have not called `start`, or the start failed. `start` also fails with `InvalidState` when the client is already connecting, connected, reconnecting or paused.

### A callback never fires

Check these in order:

1. It was registered after `connect_with` returned, and the hub pushed from `OnConnectedAsync`. Build with `new`, register, then `start`.
2. The target name differs. Targets match exactly and case-sensitively the string passed to `SendAsync`. A call for an unregistered target is logged as a warning.
3. `register_typed` could not bind the arguments (another count, or a type mismatch). The failure is logged and the callback is not called.
4. The last client handle was dropped, which stops the connection. Keep a handle alive and call `disconnect` or `stop` when you are done.

### `send` succeeds but nothing happens on the server

`send` returns once the transport accepts the frame; it never reports what the hub method did. An exception in the hub method is invisible to it. Use `invoke` while debugging to see the `Hub` error.

### `Hub` error with an unhelpful message

By default the server hides exception messages. Enable `EnableDetailedErrors` in development, or throw a `HubException`, whose message always reaches the client. Also check the argument count and types against the hub method's parameters: arguments are positional.

### `Serialization` error

The result or an argument does not match the Rust type. Common causes: property names in the wrong case (use `#[serde(rename_all = "camelCase")]`), a MessagePack DTO without string `[Key("name")]` attributes, a method that returns `void` invoked with a non-unit `T` (use `()`), a nullable result invoked as `T` instead of `Option<T>`, and a missing field without `#[serde(default)]`. See [Hub protocols](./hub-protocols.md).

### A server stream ends at once

`next()` returned `None`; read `stream.error()`. A `Hub` error means the hub method threw. A `Transport`, `Paused` or `InvalidState` error means the invocation could not be sent, so the client was not connected. A stream that completes normally has `error() == None`.

### The connection closes when I send a large message

ASP.NET Core rejects incoming messages above `MaximumReceiveMessageSize` (32 KB by default) and closes the connection. Raise it on the server. See [Server setup](./server-setup.md#message-size).

### The hub's `InvokeAsync` to the client fails with `Client didn't provide a result.`

No registered callback answered. Register one for the target and call `ctx.complete(value)` or `ctx.fail(message)`. If the hub method awaits the result and the call never arrives, check `MaximumParallelInvocationsPerClient`. See [Client results](./server-callbacks.md#client-results).

## Drops and reconnects

### The connection drops every 30 seconds or so

The server timeout fired (`ErrorKind::Timeout` naming `ServerTimeout`): the client received nothing for `with_server_timeout`. Check that the server's `KeepAliveInterval` is at most half the client's server timeout, that a proxy or load balancer does not close idle connections sooner, and that the client is not starved of CPU. See [Lifecycle](./lifecycle.md#liveness).

### Pending calls fail with `Transport` and nothing reconnects

The default reconnection policy never retries. Pass `with_reconnection_policy`. Check `status()`: `Disconnected` after a drop means no policy allowed an attempt, and `resume()` or `start()` connects again. See [Reconnection](./reconnection.md).

### `ConnectionClosed` with `CloseReason::ReconnectExhausted`

Every attempt of the policy failed. The last attempt's error is the `source()`. Use a longer or unbounded policy, or call `resume()` when connectivity returns.

### Groups or `Context.Items` are empty after a reconnect

An ordinary reconnect is a new connection with a new connection id, so server-side state is gone. Re-join groups from `on_reconnected` (see the [cookbook](./cookbook.md#rejoin-groups-after-reconnecting)) or use stateful reconnect.

### Stateful reconnect does nothing

It silently falls back to ordinary reconnect when the hub lacks `AllowStatefulReconnects = true`, and it is disabled by `skip_negotiation()` (a warning is logged). It runs only over WebSockets, needs ASP.NET Core 8 or later, and a browser `pause()` always ends it. See [Reconnection](./reconnection.md#stateful-reconnect).

### The app loses its connection in the background

The operating system suspended it, and the server dropped the silent client. Call `pause()` when the app goes to the background and `resume()` when it returns. See [Pause and resume](./cookbook.md#pause-and-resume-with-the-app-lifecycle).

### The client stops by itself

Dropping the last `SignalRClient` handle stops the connection. A callback or hook never counts as a handle, so a client built inside a function and not returned is stopped when the function ends. If a hook captures a `SignalRClient`, use `downgrade()` instead, or the client never stops on drop.

## Next

[Errors](./errors.md) lists every kind and field. [Limitations](./limitations.md) lists what the client does not do.
