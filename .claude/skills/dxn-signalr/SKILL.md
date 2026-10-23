---
name: dxn-signalr
description: Use, integrate, change, and validate dxn-signalr (Rust crate `dxn_signalr`), the ASP.NET Core SignalR client for native Tokio (Windows, Linux, macOS, Android, iOS) and browser WebAssembly in this repository (dXn.SignalR). Use for adding the dependency and Cargo features, connecting (endpoints, Bearer/Basic auth, negotiation and redirects, skip_negotiation, runtime handles), invoke/send/server streams and stream cancellation, server-to-client callbacks and client results, JSON vs MessagePack and Rust/.NET DTO mapping, ConnectionStatus, keep-alive and timeouts, pause/resume, clone and disconnect semantics, retry policies, manual reconnection and stateful reconnect, rustls TLS features and private CAs, browser/Web Worker/multithreaded WASM, Android and iOS setup, ASP.NET Core server configuration, errors and logging, and for changing the crate with its Aspire end-to-end validation.
---

# dxn-signalr

Inspect the affected current source first; it is the source of truth (`src/`, re-exported from
`src/lib.rs`). Select the reference matching the task, then follow additional links only for a
concrete need. Paths below are relative to this skill; commands run from the repository root. The
long-form documentation is in [`docs/`](../../../docs/README.md). Every Rust example in the README
and `docs/` must compile; `tests/documentation_examples.rs` holds a compiled copy of each, and
`cargo test documentation_examples_` fails when a changed block has no matching copy.
Examples in these references may be fragments that assume a `client` in scope.

The package is `dxn-signalr`, imported as `dxn_signalr`; it is released as git tags `vX.Y.Z`, not
on crates.io or any registry, so consumers pin a release tag
(`dxn-signalr = { git = "https://github.com/dXnSix/dXn.SignalR", tag = "v1.1.0" }`; see
[releasing](../../../docs/releasing.md)). One API covers native Tokio targets and browser
`wasm32-unknown-unknown`; WASI and other `wasm32` targets do not compile. The transports are
WebSockets, Server-Sent Events and Long Polling. RayX (`../../RayX/RayX.Old`) wraps the crate in
`rayx_devkit::SignalRService`; compile and test `rayx_devkit` after public API changes.

## Reference Router

| Task | Reference |
|---|---|
| Add the dependency, features, connect, endpoint, auth, negotiation, configuration options | [Setup and connection](references/setup-and-connection.md) |
| `invoke`, `send`, `enumerate`, arguments, result types, stream cancellation, concurrency | [Calls and streams](references/calls-and-streams.md) |
| `register`, `InvocationContext`, client results, unregistering | [Callbacks](references/callbacks.md) |
| JSON vs MessagePack, Serde names, .NET DTO attributes, value conversion | [Protocols and serialization](references/protocols-and-serialization.md) |
| `new`/`start`/`stop`/restart, `status()`, `connection_id()`, `on_closed`/`on_reconnecting`/`on_reconnected`, timeouts, `pause`/`resume`, clones, `disconnect` | [Lifecycle](references/lifecycle.md) |
| Retry policies, `DisconnectionHandler`, ordinary vs stateful reconnect | [Reconnection](references/reconnection.md) |
| rustls features, verifiers, providers, extra PEM roots | [TLS](references/tls.md) |
| Native runtime ownership, browsers, Web Workers, threaded WASM, Android, iOS | [Platforms](references/platforms.md) |
| What the ASP.NET Core hub must enable | [Server setup](references/server-setup.md) |
| Error messages, logging, known limitations | [Troubleshooting](references/troubleshooting.md) |
| Changing the crate: architecture, invariants, tests, Aspire end-to-end cells | [Maintaining](references/maintaining.md) |

## Usage Rules

- Configuration methods return `&ConnectionConfiguration`, so they do not chain: one statement per
  option inside the `connect_with` closure.
- The default endpoint is secure (`https`/`wss`, default port). Development servers need
  `unsecure()` and `with_port(...)`.
- `ReconnectionConfig::default()` never reconnects. Opt in with
  `ReconnectionConfig { policy: Arc::new(DefaultRetryPolicy) }` or another policy.
- Native `connect` needs a Tokio runtime: call it inside one, or pass `with_runtime_handle`.
- In browsers use Bearer tokens (sent as `access_token`); Basic credentials reach only negotiate.
- Build with `SignalRClient::new`, register callbacks and lifecycle hooks, then `start().await`
  (`connect_with` is both at once and can miss messages the hub sends on connect).
- Import `CallbackHandler` to call `unregister()`. `disconnect()`/`stop().await` stop the connection
  for every handle (like .NET `StopAsync`); `start()` restarts it. Dropping the last handle closes
  it (registered callbacks and hooks do not count; an open stream does until it is dropped). Capture `client.downgrade()` in callbacks and
  hooks, never a `SignalRClient` clone (reference cycle).
- Import `futures::StreamExt` to iterate `ManualStream`; check `error()` after it ends; drop it to
  cancel the server stream.
- Match wire names on both sides: `#[serde(rename_all = "camelCase")]` in Rust, and for
  MessagePack `[MessagePackObject]` with string `[Key("camelName")]` in .NET.

## Change Constraints

- Preserve the public surface and exported names; this crate is a general network client with no
  UI framework coupling.
- Keep native and browser behavior aligned; prove behavior changes on both, with JSON and
  MessagePack.
- Test only what you change: add a focused test group beside the changed code (or a new end-to-end
  scenario), run it alone, then run the validation list in `AGENTS.md` once before committing.
- Public API or behavior changes update `docs/`, the README examples, and this skill's references
  in the same change.

## Memory

AgentX memory lives in `.agentx/memory/`: start at the
[knowledge index](../../../.agentx/memory/knowledge/INDEX.md) and
[decision index](../../../.agentx/memory/decisions/INDEX.md). Domains are `client-runtime`
(`public-api`, `hub-protocol`, `transport`, `connection-lifecycle`, `action-execution`),
`interop-validation`, and `repository-governance`. Read the touched domain's `rules.md` before
changing behavior.
