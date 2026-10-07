---
name: rayx-signalr
description: Use, integrate, change, and validate rayx-signalr (Rust crate `rayx_signalr`), the ASP.NET Core SignalR client for native Tokio (Windows, Linux, macOS, Android, iOS) and browser WebAssembly in this repository (RayX.SignalR). Use for adding the dependency and Cargo features, connecting (endpoints, Bearer/Basic auth, negotiation and redirects, skip_negotiation, runtime handles), invoke/send/server streams and stream cancellation, server-to-client callbacks and client results, JSON vs MessagePack and Rust/.NET DTO mapping, ConnectionStatus, keep-alive and timeouts, pause/resume, clone and disconnect semantics, retry policies, manual reconnection and stateful reconnect, rustls TLS features and private CAs, browser/Web Worker/multithreaded WASM, Android and iOS setup, ASP.NET Core server configuration, errors and logging, and for changing the crate with its Aspire end-to-end validation.
---

# rayx-signalr

Inspect the affected current source first; it is the source of truth (`src/`, re-exported from
`src/lib.rs`). Select the reference matching the task, then follow additional links only for a
concrete need. Paths below are relative to this skill; commands run from the repository root. The
long-form, compile-checked documentation is in [`docs/`](../../../docs/README.md).

The package is `rayx-signalr`, imported as `rayx_signalr`; it is not on crates.io (use a Git or path
dependency). One API covers native Tokio targets and browser `wasm32-unknown-unknown`; WASI and
other `wasm32` targets do not compile. The only transport is WebSockets. RayX (`../RayX`) consumes
the crate by path (`rayx-signalr = { path = "../RayX.SignalR" }`) through
`rayx_devkit::SignalRService`; compile and test `rayx_devkit` after public API changes.

## Reference Router

| Task | Reference |
|---|---|
| Add the dependency, features, connect, endpoint, auth, negotiation, configuration options | [Setup and connection](references/setup-and-connection.md) |
| `invoke`, `send`, `enumerate`, arguments, result types, stream cancellation, concurrency | [Calls and streams](references/calls-and-streams.md) |
| `register`, `InvocationContext`, client results, unregistering | [Callbacks](references/callbacks.md) |
| JSON vs MessagePack, Serde names, .NET DTO attributes, value conversion | [Protocols and serialization](references/protocols-and-serialization.md) |
| `status()`, keep-alive/server/handshake timeouts, `pause`/`resume`, clones, `disconnect` | [Lifecycle](references/lifecycle.md) |
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
- Import `CallbackHandler` to call `unregister()`. `disconnect()` stops the connection for every
  handle (like .NET `StopAsync`); dropping handles closes it only with the last one, and each
  registration holds a client clone, so call `disconnect()` when done.
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
