# RayX.SignalR documentation

This is the technical documentation for `rayx-signalr` (imported as `rayx_signalr`), a SignalR
client for Rust. The [repository README](../README.md) gives a short overview. These chapters
explain the complete API surface, the connection and reconnection semantics, the platform
requirements, and the ASP.NET Core server setup that goes with them.

## Chapters

### Part I: Using the client

1. [Getting started](01-getting-started.md): installation, Cargo features, toolchain, and a first
   connection on native and in the browser.
2. [Connecting](02-connecting.md): endpoints, authentication, negotiation, and the complete
   `ConnectionConfiguration` reference.
3. [Calling the hub](03-calling-the-hub.md): `invoke`, `send`, server streams, arguments, return
   types, cancellation, and concurrency.
4. [Server-to-client calls](04-server-callbacks.md): `register`, `InvocationContext`, client
   results, and unregistering.
5. [Hub protocols and serialization](05-hub-protocols.md): JSON and MessagePack, and how to match
   Rust and .NET types on the wire.

### Part II: Connection management

6. [Connection lifecycle](06-connection-lifecycle.md): `ConnectionStatus`, keep-alive and
   timeouts, `pause`/`resume`, clones, and disconnecting.
7. [Reconnection](07-reconnection.md): retry policies, automatic and manual reconnection, and
   stateful reconnect with replay.
8. [TLS](08-tls.md): rustls features, certificate verifiers, crypto providers, and private CAs.

### Part III: Platforms and servers

9. [Platforms](09-platforms.md): native runtime ownership, browsers, Web Workers, multithreaded
   WASM, Android, and iOS.
10. [Server setup](10-server-setup.md): what the ASP.NET Core application needs for each client
    feature.
11. [Errors and logging](11-errors-and-logging.md): the error model, common failures, and
    diagnostics.
12. [Limitations](12-limitations.md): what the client does not do, and the risks that are not
    covered by tests.

### Part IV: Working on the library

13. [Architecture](13-architecture.md): the module map, the connection pipeline, action storage,
    and the lifecycle state machine.
14. [Testing and CI](14-testing.md): the Aspire test graph, end-to-end cells, static contract
    categories, and the CI checks.

## Conventions

- Examples import from the crate root (`use rayx_signalr::...`); every public type is exported
  there. Short snippets use `?` and `.await`, so they assume an enclosing `async fn` that returns
  `Result<_, Box<dyn std::error::Error>>`. Native code runs inside a Tokio runtime
  (`#[tokio::main]`), browser code inside `wasm_bindgen_futures::spawn_local`.
- Configuration methods take `&mut self` and return `&ConnectionConfiguration`, so call them as
  separate statements inside the `connect_with` closure instead of chaining them.
- "Native" means every non-WASM target (Windows, Linux, macOS, Android, iOS). "Browser" means
  `wasm32-unknown-unknown`, the only supported WebAssembly target.
