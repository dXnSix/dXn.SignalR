# TLS

This chapter is about **native** targets. In the browser, `https`/`wss` go through the browser's
own TLS stack and trust store, and none of the options below exist.

## How native TLS works

Native connections use [rustls](https://github.com/rustls/rustls), never OpenSSL. Each connection
builds its own rustls `ClientConfig` at `connect`, with an explicit crypto provider and certificate
verifier, and reuses it for negotiate, the WebSocket and every reconnect. The client never installs
or depends on rustls' process-wide default `CryptoProvider`, so other crates in your application can
select providers without affecting it.

A secure endpoint never falls back to plaintext or to a verifier that accepts any certificate.

## Features

| Feature | Kind | Default | Effect |
|---|---|---|---|
| `tls-platform-verifier` | verifier | on | The operating system's trust store, through `rustls-platform-verifier`. |
| `tls-webpki-roots` | verifier | off | Mozilla's root certificates compiled into the binary (`webpki-roots`). |
| `tls-ring` | provider | on | `ring` cryptography. |
| `tls-aws-lc-rs` | provider | off | `aws-lc-rs` cryptography; preferred over `ring` when both are enabled. |

A build can open secure endpoints when it has at least one verifier feature and one provider
feature. Without them it is a plaintext-only client: `http`/`ws` endpoints work, and `https`/`wss`
endpoints fail at `connect`, before any I/O, with a `Configuration` error that names the missing
features. A server certificate the verifier does not trust fails `connect` with `ErrorKind::Tls`.

```toml
# Bundled roots and aws-lc-rs instead of the defaults:
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR", default-features = false, features = ["tls-webpki-roots", "tls-aws-lc-rs"] }

# Plaintext only (for example, an embedded client on a trusted network):
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR", default-features = false }
```

`aws-lc-rs` compiles C and assembly code, so it needs a working C toolchain for every target you
build; `ring` is the simpler default.

## Verifiers

The verifier is chosen per connection:

| Configuration | Verifier |
|---|---|
| default | the platform verifier (when compiled in), else the bundled roots |
| `use_webpki_roots()` | the bundled Mozilla roots; requires `tls-webpki-roots`, otherwise `connect` fails |
| extra PEM roots, platform verifier | the platform trust store plus the extra roots |
| extra PEM roots on Android | webpki over the extra roots, plus the bundled roots when `tls-webpki-roots` is enabled |
| extra PEM roots, `use_webpki_roots()` | the bundled roots plus the extra roots |

```rust
let client = rayx_signalr::SignalRClient::connect_with("api.example.com", "hub", |c| {
    c.use_webpki_roots(); // needs the tls-webpki-roots feature
})
.await?;
```

The platform verifier uses the Windows certificate store, the macOS/iOS Security framework, the
Android trust store (after JVM initialization; see [Android](09-platforms.md#android)), and the
system CA bundle on Linux. Bundled roots behave the same everywhere and need no setup, but they do
not include enterprise or user-installed roots.

## Private certificate authorities

Trust additional roots (an internal CA, or a development CA) for one connection with
`with_additional_root_certificates_pem`:

```rust
let ca = std::fs::read("certs/internal-ca.pem")?;
let client = rayx_signalr::SignalRClient::connect_with("internal.example.com", "hub", move |c| {
    c.with_additional_root_certificates_pem(ca.clone());
})
.await?;
```

The PEM may contain one or more `CERTIFICATE` blocks. Malformed PEM, or PEM without any
certificate, fails `connect` before any I/O. The extra roots extend the selected verifier; they do
not replace it, except on Android as shown above.

For development servers, either pass the development CA's certificate here, or install it in the
operating system's trust store and keep the platform verifier.

## Client certificates

Present a client certificate to servers that ask for one (mutual TLS), as the .NET client's
`HttpConnectionOptions.ClientCertificates`, with `with_client_certificate`:

```rust
let chain = std::fs::read("certs/client.pem")?; // the certificate, then any intermediates
let key = std::fs::read("certs/client.key")?; // its private key
let client = rayx_signalr::SignalRClient::connect_with("internal.example.com", "hub", move |c| {
    c.with_client_certificate(chain.clone(), key.clone());
})
.await?;
```

Every native TLS connection of the client presents it when the server requests a certificate:
negotiate, the WebSocket, Long Polling and Server-Sent Events, also after reconnects. The chain is
one or more `CERTIFICATE` blocks with the leaf first; the key is PKCS#8, PKCS#1 or SEC1 PEM.
Malformed PEM, or a key that does not belong to the certificate, fails `new` and `connect` with
`ErrorKind::Configuration` naming `with_client_certificate`, before any I/O; errors never contain
the key. A server that requires a certificate and gets none usually refuses the negotiate request
(`ErrorKind::Authentication` with status 403 from ASP.NET Core's client-certificate checks) or the
TLS handshake (`ErrorKind::Tls`, also when, under TLS 1.3, the refusal arrives just after the
handshake). Browsers choose client certificates from their own store.

## Adjusting the rustls configuration

`with_tls_config_hook` hands each connection's rustls `ClientConfig` to the application before its
first use, as the .NET client exposes the TLS options of `SocketsHttpHandler` and `ClientWebSocket`
through `HttpMessageHandlerFactory` and `WebSocketConfiguration`. The config is the one the client
built from the features and options above. The hook runs once per connection (at `connect`, or on
a first redirect to a secure endpoint), and its result serves negotiate, every transport and every
reconnect of that connection. `rayx_signalr::tls::rustls` re-exports the rustls version the client
uses:

```rust
let client = rayx_signalr::SignalRClient::new("internal.example.com", "hub", |c| {
    c.with_tls_config_hook(|config| {
        // For example, servers addressed by a name they do not expect in SNI.
        config.enable_sni = false;
    });
})?;
```

The hook can weaken security, and doing so is the application's explicit choice. Replacing the
certificate verifier through `config.dangerous()`, or narrowing the protocol versions, changes what
the client trusts. The client offers no ALPN protocols by default and speaks HTTP/1.1, so a hook
that sets `alpn_protocols` must include `http/1.1`.

## Redirects to secure endpoints

A plaintext connection can be redirected by negotiation to an `https` endpoint (Azure SignalR
Service does this). The connection then builds its TLS configuration on first use, with the same
rules.
