# TLS

Full chapter: [TLS](../../../../docs/08-tls.md). Native only; browsers use the browser's TLS and
have none of these options.

## Model

- rustls only, never OpenSSL. One `ClientConfig` per connection, built at `connect` with an
  explicit provider and verifier, shared by negotiate, the WebSocket and every reconnect.
- Never uses or installs rustls' process-default `CryptoProvider`.
- Secure endpoints never fall back to plaintext or to an accept-any verifier.
- A plaintext connection redirected by negotiate to `https` builds its TLS config on first use.

## Features

| Feature | Kind | Default |
|---|---|---|
| `tls-platform-verifier` | verifier (OS trust store) | on |
| `tls-webpki-roots` | verifier (bundled Mozilla roots) | off |
| `tls-ring` | provider | on |
| `tls-aws-lc-rs` | provider, preferred over ring | off |

Secure endpoints need ≥1 verifier and ≥1 provider; otherwise `https`/`wss` fail at `connect` before
I/O with an error naming the features. The build script derives the internal `rayx_tls` cfg from
these features (native targets only); keep `build.rs`.

```toml
rayx-signalr = { git = "...", default-features = false, features = ["tls-webpki-roots", "tls-aws-lc-rs"] }
rayx-signalr = { git = "...", default-features = false } # plaintext-only native client
```

## Verifier selection

| Configuration | Verifier |
|---|---|
| default | platform verifier if compiled, else bundled roots |
| `use_webpki_roots()` | bundled roots; error without `tls-webpki-roots` |
| `with_additional_root_certificates_pem(pem)` + platform verifier | OS store + extra roots |
| extra roots on Android | webpki over extra roots (+ bundled roots if enabled) |
| extra roots + `use_webpki_roots()` | bundled + extra roots |

Client certificates (mutual TLS, .NET `HttpConnectionOptions.ClientCertificates`):
`with_client_certificate(chain_pem, key_pem)` (native): leaf-first `CERTIFICATE` chain plus a
PKCS#8/PKCS#1/SEC1 key, presented on every native TLS connection (negotiate, WebSocket, LP, SSE,
reconnects) when the server asks. Bad PEM or a mismatched key fails `new` with
`Configuration`/`with_client_certificate` before I/O (never echoing the key); a server requiring one
refuses a client without it (ASP.NET Core: negotiate 403 → `Authentication`, or `Tls`, also for
a TLS 1.3 refusal read after the handshake).

```rust
let ca = std::fs::read("internal-ca.pem").map_err(|e| e.to_string())?;
let client = SignalRClient::connect_with("internal.example.com", "hub", move |c| {
    c.with_additional_root_certificates_pem(ca.clone());
}).await?;
```

PEM: one or more `CERTIFICATE` blocks; malformed or empty PEM fails before I/O. Extra roots extend,
not replace, the verifier (except Android). For development CAs, pass the CA PEM or install it in the
OS store.

Android with the platform verifier needs JVM initialization: see [Platforms](platforms.md#android).

## TLS config hook

`with_tls_config_hook(|config: &mut rustls::ClientConfig| ...)` (native, TLS features; rustls
re-exported as `rayx_signalr::tls::rustls`) adjusts each connection's built config once before its
first use (at `connect`, or on a first redirect to `https`). The result is shared by negotiate,
transports and reconnects. Replacing the verifier (`dangerous()`) is the application's explicit
choice. No ALPN is offered by default; a hook that sets `alpn_protocols` must include
`http/1.1`.
