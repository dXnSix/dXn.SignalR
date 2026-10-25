---
title: Authentication
description: Bearer tokens, refreshable access-token factories, Basic credentials and integrated Windows authentication, and how each reaches the server on native and in the browser.
group: Connection
order: 80
---

A connection has one authentication mode. Use a Bearer token for anything tokens can express, an access-token factory when tokens expire, and Basic or integrated Windows authentication when the server asks for them.

| Method | Native | Browser |
|---|---|---|
| `authenticate_bearer(token)` | `Authorization: Bearer` on negotiate, the WebSocket upgrade and every Long Polling and Server-Sent Events request; never on the URL | `Authorization: Bearer` on negotiate and on `fetch` requests; `access_token` query on the WebSocket and event-stream URLs |
| `authenticate_basic(user, Some(password))` | `Authorization: Basic` on negotiate, the upgrade and every HTTP request | `Authorization: Basic` on `fetch` requests only; a WebSocket or `EventSource` cannot carry it (a warning is logged) |
| `with_access_token_factory(f)` | a fresh Bearer token from `f` on every negotiate and every WebSocket upgrade; Long Polling and Server-Sent Events keep the negotiate token until a 401 | a fresh token on every negotiate (`Authorization`) and WebSocket (`access_token`); the HTTP transports keep the negotiate token until a 401 |

`authenticate_basic(user, None)` sends `user:` with an empty password. Between Basic and Bearer the last call wins. An access-token factory cannot be combined with either: `new` then fails before any I/O with `ErrorKind::Configuration` naming `with_access_token_factory`.

```rust
use dxn_signalr::SignalRClient;

fn bearer(token: String) -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("api.example.com", "hubs/orders", |c| {
        c.authenticate_bearer(token);
    })
}
```

:::warning A Bearer token is fixed for the client's lifetime
`authenticate_bearer` reuses the same token on every reconnect. If the token expires while the client lives, the next reconnect fails with `ErrorKind::Authentication`. Use `with_access_token_factory` for expiring tokens.
:::

## Access-token factory

`with_access_token_factory` takes an async function returning `Result<String, String>`, like the .NET client's `AccessTokenProvider`. It is called before every negotiate request (the initial connect, every reconnect that negotiates, each redirect hop) and again whenever a WebSocket is opened, including every stateful resume. Long Polling keeps the token negotiate fetched and asks again only after a 401. A token that a negotiate redirect issued (Azure SignalR Service) is used instead, for every transport of that connection.

```rust
use dxn_signalr::SignalRClient;

fn refreshable() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("api.example.com", "hubs/orders", |c| {
        c.with_access_token_factory(|| async {
            // Fetch or refresh the token here.
            Ok::<_, String>(std::env::var("API_TOKEN").unwrap_or_default())
        });
    })
}
```

An `Err` fails that attempt with `ErrorKind::Authentication` and the message `access token factory failed: <message>` (anything token-shaped in the message is redacted). A reconnection policy then tries again. Natively the factory and its future must be `Send`; in the browser they need only `'static`. The [cookbook](./cookbook.md#refresh-an-access-token) shows a factory backed by a shared token store.

## Browsers

Browsers cannot set headers on a WebSocket, and a standard ASP.NET Core server authenticates the WebSocket request separately from negotiate. So in the browser:

- **Bearer** works when the server reads the `access_token` query parameter for hub requests. See [Server setup](./server-setup.md#authentication).
- **Basic** reaches only the `fetch` requests. A standard server does not authenticate the WebSocket from it. Building a browser client with Basic credentials logs a warning that names `authenticate_bearer` and `with_access_token_factory`.

## Integrated Windows authentication

`with_default_credentials()` authenticates with the current Windows account, and `with_credentials(user, password, domain)` with explicit ones, like the .NET client's `UseDefaultCredentials` and `Credentials`. Both are native only.

```rust
use dxn_signalr::SignalRClient;

fn intranet() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("intranet.example.com", "chat", |c| {
        // The signed-in Windows account (Kerberos, or NTLM where Kerberos is unavailable).
        c.with_default_credentials();
        // Or explicit credentials: c.with_credentials("user", "password", "CORP");
    })
}
```

As with `SocketsHttpHandler`, a request goes out without credentials first. When the server answers with a 401 offering `Negotiate` (preferred) or `NTLM`, the client sends it again with the account's tokens until the server accepts or rejects them. A rejection fails with `ErrorKind::Authentication` and status 401.

- On Windows the client authenticates through SSPI, so Negotiate picks Kerberos or NTLM as .NET does.
- On other native targets `with_credentials` answers with the crate's own NTLMv2. `with_default_credentials` there fails `new` with `ErrorKind::Configuration`, because the client has no GSSAPI integration.
- Integrated authentication cannot run over HTTP/2, so every request uses HTTP/1.1, as the .NET client does.
- The password never appears in logs, errors or `Debug` output. Browsers use their own integrated authentication and have neither option.

## Client certificates

Mutual TLS is configured with `with_client_certificate`; see [TLS](./tls.md#client-certificates).

## Next

[Transports](./transports.md) covers how the credentials travel on each transport. [Server setup](./server-setup.md#authentication) shows the JWT `access_token` hook.
