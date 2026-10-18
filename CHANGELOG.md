# Changelog

All notable changes to `rayx-signalr` are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). It is generated from
Conventional Commits by release-plz.

## [1.0.0] - 2026-10-19

The first release of `rayx-signalr`: an ASP.NET Core SignalR client for Rust on native Tokio
targets and in the browser, modelled on the official .NET client.

### Features
- Every SignalR call style: `invoke` for typed results, `send` (fire and forget), server streams
  as a `Stream` (dropping it cancels the server side), client-to-server streams, and server-to-client
  calls (`register`, `register_async`, `register_typed`) including client results.
- The JSON hub protocol and, with the `messagepack` feature, MessagePack, both carrying your Serde
  types unchanged.
- Reconnection: automatic retry policies (including the official 0/2/10/30 s schedule), manual
  reconnection through a handler, and ASP.NET Core stateful reconnect, which replays
  unacknowledged messages.
- The .NET client's lifecycle: `new`, `start` and `stop`, the `on_closed`, `on_reconnecting` and
  `on_reconnected` hooks, `status()`, `pause()`/`resume()` for mobile backgrounding, keep-alive
  pings, server timeouts and a bounded handshake.
- WebSockets, Server-Sent Events and Long Polling on every target, tried in the server's order with
  fallback, or `skip_negotiation()` for a direct WebSocket.
- Negotiation redirects with access tokens, Bearer and Basic authentication, refreshable tokens and
  integrated Windows authentication (native).
- HTTP/2 as the .NET client negotiates it and experimental HTTP/3 (`http3` feature), native.
- Proxies: the system proxy by default on native targets, explicit HTTP, HTTPS and SOCKS proxies,
  and proxy auto-config scripts (`proxy-pac` feature).
- Native TLS with rustls and no OpenSSL: the platform verifier (`tls-platform-verifier`) and the
  `ring` provider (`tls-ring`) by default, bundled roots (`tls-webpki-roots`), the `aws-lc-rs`
  provider (`tls-aws-lc-rs`), per-connection private CAs and client certificates.
- A typed `Error` with an `ErrorKind` for every failure.

### Platforms
- Windows, Linux, macOS, Android and iOS on Tokio; each native connection runs on its own runtime.
- Browser `wasm32-unknown-unknown` on the main thread, in Web Workers and in multithreaded
  (atomics) builds. WASI and other `wasm32` targets are not supported.
- Rust edition 2021, MSRV 1.92.

### Distribution
- Released as git tags `vX.Y.Z` with a GitHub Release, not on crates.io or any registry: depend on
  `rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR", tag = "v1.0.0" }`.
