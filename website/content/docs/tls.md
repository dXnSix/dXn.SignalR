---
title: TLS
description: How native clients verify certificates with rustls, which Cargo features pick the verifier and provider, private CAs, client certificates and the rustls config hook.
group: Connection
order: 120
---

This page is about **native** targets. In the browser `https` and `wss` go through the browser's own TLS stack and trust store, and none of the options below exist.

Native connections use [rustls](https://github.com/rustls/rustls), never OpenSSL. Each connection builds its own `ClientConfig` when the client is built, with an explicit crypto provider and certificate verifier, and reuses it for negotiate, every transport and every reconnect. The client never installs or depends on rustls' process-wide default `CryptoProvider`, so other crates can select providers without affecting it. A secure endpoint never falls back to plaintext or to a verifier that accepts any certificate.

## Features

| Feature | Kind | Default | Effect |
|---|---|---|---|
| `tls-platform-verifier` | verifier | on | The operating system's trust store, through `rustls-platform-verifier`. |
| `tls-webpki-roots` | verifier | off | Mozilla's root certificates compiled into the binary. |
| `tls-ring` | provider | on | `ring` cryptography. |
| `tls-aws-lc-rs` | provider | off | `aws-lc-rs` cryptography; preferred over `ring` when both are enabled. |

A build can open secure endpoints when it has at least one verifier feature and one provider feature. Without them it is a plaintext-only client: `http` and `ws` endpoints work, and `https` and `wss` endpoints fail before any I/O with a `Configuration` error that names the missing features. A certificate the verifier does not trust fails the start with `ErrorKind::Tls`.

```toml
# Bundled roots and aws-lc-rs instead of the defaults:
dxn-signalr = { git = "https://github.com/dXnSix/dXn.SignalR", tag = "v1.1.0", default-features = false, features = ["tls-webpki-roots", "tls-aws-lc-rs"] }

# Plaintext only (for example, an embedded client on a trusted network):
dxn-signalr = { git = "https://github.com/dXnSix/dXn.SignalR", tag = "v1.1.0", default-features = false }
```

`aws-lc-rs` compiles C and assembly, so it needs a working C toolchain for every target you build; `ring` is the simpler default.

## Verifiers

| Configuration | Verifier |
|---|---|
| default | the platform verifier (when compiled in), else the bundled roots |
| `use_webpki_roots()` | the bundled Mozilla roots; requires `tls-webpki-roots`, otherwise the build fails with a `Configuration` error |
| extra PEM roots, platform verifier | the platform trust store plus the extra roots |
| extra PEM roots on Android | webpki over the extra roots, plus the bundled roots when `tls-webpki-roots` is enabled |
| extra PEM roots, `use_webpki_roots()` | the bundled roots plus the extra roots |

```rust
use dxn_signalr::SignalRClient;

async fn bundled() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::connect_with("api.example.com", "hub", |c| {
        c.use_webpki_roots(); // needs the tls-webpki-roots feature
    })
    .await
}
```

The platform verifier uses the Windows certificate store, the macOS and iOS Security framework, the Android trust store (after JVM initialization, see [Platforms](./platforms.md#android)) and the system CA bundle on Linux. Bundled roots behave the same everywhere and need no setup, but they do not include enterprise or user-installed roots.

## Private certificate authorities

Trust additional roots, such as an internal or development CA, for one connection with `with_additional_root_certificates_pem`. The extra roots extend the selected verifier; they do not replace it (except on Android, as in the table).

```rust
use dxn_signalr::SignalRClient;

async fn internal() -> Result<SignalRClient, Box<dyn std::error::Error>> {
    let ca = std::fs::read("certs/internal-ca.pem")?;
    let client = SignalRClient::connect_with("internal.example.com", "hub", move |c| {
        c.with_additional_root_certificates_pem(ca.clone());
    })
    .await?;
    Ok(client)
}
```

The PEM may contain one or more `CERTIFICATE` blocks. Malformed PEM, or PEM without a certificate, fails before any I/O. This snippet reads a file and connects to a server. For a development server, either pass its CA here or install the CA in the operating system's trust store and keep the platform verifier.

## Client certificates

Present a client certificate to servers that ask for one (mutual TLS), like the .NET client's `ClientCertificates`:

```rust
use dxn_signalr::SignalRClient;

async fn mutual() -> Result<SignalRClient, Box<dyn std::error::Error>> {
    let chain = std::fs::read("certs/client.pem")?; // the certificate, then any intermediates
    let key = std::fs::read("certs/client.key")?; // its private key
    let client = SignalRClient::connect_with("internal.example.com", "hub", move |c| {
        c.with_client_certificate(chain.clone(), key.clone());
    })
    .await?;
    Ok(client)
}
```

Every native TLS connection of the client presents it when the server requests one, including after reconnects. The chain is one or more `CERTIFICATE` blocks with the leaf first; the key is PKCS#8, PKCS#1 or SEC1 PEM. Malformed PEM, or a key that does not belong to the certificate, fails with `ErrorKind::Configuration` naming `with_client_certificate`, and errors never contain the key. A server that requires a certificate and gets none usually refuses negotiate with `ErrorKind::Authentication` (status 403) or fails the TLS handshake with `ErrorKind::Tls`. Browsers choose client certificates from their own store.

## Adjusting the rustls configuration

`with_tls_config_hook` hands each connection's rustls `ClientConfig` to the application before its first use, like the .NET client's `HttpMessageHandlerFactory` and `WebSocketConfiguration` expose the TLS options of `SocketsHttpHandler` and `ClientWebSocket`. `dxn_signalr::tls::rustls` re-exports the rustls version the client uses.

```rust
use dxn_signalr::SignalRClient;

fn tuned() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("internal.example.com", "hub", |c| {
        c.with_tls_config_hook(|config| {
            // For example, servers addressed by a name they do not expect in SNI.
            config.enable_sni = false;
        });
    })
}
```

:::danger The hook can weaken security
Replacing the certificate verifier through `config.dangerous()`, or narrowing the protocol versions, changes what the client trusts. That is the application's explicit choice; the client does not guard against it.
:::

The ALPN offer of negotiate, Long Polling and Server-Sent Events comes from [`with_http_version`](./transports.md#http-versions) and replaces the hook's. The HTTP/1.1 WebSocket handshake and `Http1` requests offer the hook's `alpn_protocols` (none by default), so a hook that sets them must include `http/1.1`.

A plaintext connection can be redirected by negotiation to an `https` endpoint (Azure SignalR Service does this). The connection then builds its TLS configuration on first use, with the same rules.

## Next

[Proxies](./proxies.md) covers tunnels and PAC scripts. Android needs a one-time initialization for the platform verifier; see [Platforms](./platforms.md#android).
