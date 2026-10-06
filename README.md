# RayX.SignalR

This repository is the RayX-maintained fork of
[`signalr-client`](https://github.com/danielleiszen/rust_signalr_client). It
is a hard fork with the owned Cargo package and Rust crate identity
`rayx-signalr` / `rayx_signalr`. See [FORK.md](FORK.md) for provenance, the
upstream-import policy, and the current hardening scope.

The fork targets reusable native Tokio and browser WebAssembly clients, with
both JSON and MessagePack SignalR Hub Protocol support. It is not currently
published to crates.io.

## Local path dependency

```toml
[dependencies]
rayx-signalr = {
    path = "../RayX.SignalR",
    features = ["messagepack"]
}
```

The dependency is then imported as `rayx_signalr` in Rust source.

## Status

![license](https://shields.io/badge/license-MIT-blue)

I made this client because I could not find a client that supported my requirements calling a complex backend written in SignalR from a WASM frontend. This package was designed to overcome this limitation. It supports WebAssembly targets out of the box and both JSON and MessagePack hub protocols.

 Read more about SignalR in the [`offical documentation`](https://learn.microsoft.com/en-us/aspnet/core/signalr/introduction?view=aspnetcore-9.0).

I tried to design this package to be simple and convenient to use without writing boilerpart code. Please be free to comment or post issues if you have recommendations or requirements. Contribution is also welcome.

A .NET test backend is provided for testing (see the `dotnet` folder); every service-dependent test, including Basic and Bearer authentication, runs against it through Aspire (see "Testing with Aspire").

## Platform support

| Target | Status | Proof |
| --- | --- | --- |
| Windows, Linux, macOS (Tokio) | Supported | Full end-to-end catalog through Aspire (`e2e / native (<os>)`) |
| Android (`aarch64`, `x86_64`) | Supported | Full native catalog on an x86_64 emulator (`e2e / android`); `cargo ndk` builds for arm64-v8a and x86_64 (`quality / android`). The platform verifier needs an app's JVM, so on Android the cells verify TLS with webpki (extra roots, bundled roots); the platform verifier itself is build-checked only |
| iOS (`aarch64-apple-ios`, simulator) | Supported | Full native catalog on the iOS simulator (`e2e / ios`); device target compile check (`quality / ios`) |
| Browser, main thread (`wasm32-unknown-unknown`) | Supported | Full browser catalog in Chromium, Firefox and WebKit (`e2e / browser (<engine>)`) |
| Browser, Web Worker | Supported | `worker-transport` scenario in every engine |
| Browser, multithreaded WASM (atomics) | Supported | `threaded-wasm` scenario (cross-origin isolated page, pinned nightly with `-Z build-std`) |
| WASI (`wasm32-wasi*`) | Not supported | The crate fails to compile with an explanatory error; there is no browser WebSocket there |

Emulators and simulators prove the client's networking, TLS and lifecycle code on
those operating systems; they do not exercise a real device's radio, Doze or
background suspension.

### TLS

Native connections use rustls (no OpenSSL) and never the process-wide default
`CryptoProvider`: every connection builds its own `ClientConfig` with an explicit
provider and verifier.

| Feature | Effect |
| --- | --- |
| `tls-platform-verifier` (default) | Verifies against the operating system's trust store (`rustls-platform-verifier`) |
| `tls-webpki-roots` | Bundles Mozilla's roots; select them per connection with `use_webpki_roots()` |
| `tls-ring` (default) | `ring` crypto provider |
| `tls-aws-lc-rs` | `aws-lc-rs` crypto provider; takes precedence over `tls-ring` when both are enabled |

Disabling all TLS features keeps plaintext `ws`/`http` endpoints working;
secure endpoints then fail before any I/O. Extra trust anchors (for example a
private CA) are added per connection with
`with_additional_root_certificates_pem(pem)`; invalid PEM fails `connect` before
any I/O. Secure endpoints never fall back to plaintext or to a certificate-accepting
verifier.

### Android

The default platform verifier needs the app's JVM context. Either:

1. Keep `tls-platform-verifier` and wire it up like any `rustls-platform-verifier`
   consumer: add the crate's bundled Maven repository to Gradle
   (`maven { url = uri(<path of rustls-platform-verifier-android from cargo metadata>/maven) }`),
   depend on `rustls:rustls-platform-verifier:latest.release`, keep its classes
   from shrinking (`-keep, includedescriptorclasses class org.rustls.platformverifier.** { *; }`),
   and call `rayx_signalr::tls::android::init_with_env(&mut env, context)` once
   (from `JNI_OnLoad` or the activity) before the first secure connection; or
2. Enable `tls-webpki-roots` and call `use_webpki_roots()` on the configuration,
   which needs no JVM. A connection with extra PEM roots on Android always
   verifies with webpki over those roots (plus the bundled roots when enabled).

An emulator reaches the development machine's loopback at `10.0.2.2`.

### iOS

App Transport Security governs Apple's URL loading system, not the client's own
rustls sockets, but production apps should still use `https`/`wss` endpoints;
keep plain `ws`/`http` for local development (the simulator shares the Mac's
loopback). The platform verifier uses the iOS trust store.

### Native runtime

Every native connection is owned by one Tokio runtime: the configured
`with_runtime_handle(handle)`, otherwise the ambient runtime at `connect`, otherwise
`connect` fails before any I/O. Socket I/O, timers and the connection's tasks run
on that runtime, and the client's futures only wait on runtime-agnostic
primitives, so UI frameworks can await them from their own executors (for
example `futures::executor::block_on` on a plain thread).

### Liveness and lifecycle

- `with_keep_alive_interval` (default 15 s) sends Pings when nothing else went
  out; `with_server_timeout` (default 30 s, must exceed the keep-alive interval)
  treats a silent server as a dropped transport; `with_handshake_timeout`
  (default 15 s) bounds the SignalR handshake. Invalid durations fail `connect`
  before any I/O.
- `SignalRClient::status()` reports `ConnectionStatus::{Connected, Reconnecting,
  Paused, Disconnected, Closed}`.
- `pause()` (for example when a mobile app moves to the background) ends the
  transport and stops keep-alive and automatic reconnects; `resume()` reconnects
  immediately, then follows the reconnection policy. With stateful reconnect a
  native pause keeps the logical connection while the server holds it; a browser
  pause cannot (browsers always send a WebSocket Close, after which ASP.NET Core
  disposes the connection), so it fails pending work and `resume()` negotiates a
  fresh stateful connection.
- `DefaultRetryPolicy` retries after 0, 2, 10 and 30 seconds and then stops, like
  the official JavaScript and Swift clients. `ReconnectionConfig::default()`
  still never reconnects; opt in with
  `ReconnectionConfig { policy: Arc::new(DefaultRetryPolicy) }`.

### Negotiation

Negotiation follows redirects (up to 100, with the redirect's access token) and
`skip_negotiation()` connects the WebSocket directly, as the official clients'
`skipNegotiation` does; the server must allow WebSockets-only connections, and
it cannot be combined with stateful reconnect.

### Browsers, workers and threads

The browser transport is event-driven and touches neither `window` nor polling
timers, so it runs on the main thread and in Web Workers. The client is `!Send`:
use one client per thread or worker. Multithreaded (atomics) builds work too:
they need a cross-origin isolated page (`Cross-Origin-Opener-Policy: same-origin`
and `Cross-Origin-Embedder-Policy: require-corp`), a nightly toolchain with
`rust-src`, `-C target-feature=+atomics,+bulk-memory,+mutable-globals` and
`-Z build-std=std,panic_abort`; binary frames are copied out of shared memory
before sending.

## Testing with Aspire

Service-dependent tests run only through .NET Aspire. The AppHost
(`dotnet/RayX.SignalR.AppHost`, selected by the root `aspire.config.json`)
composes the ASP.NET Core SignalR test service (HTTP and HTTPS with a per-run
test CA), the pinned `wasm-bindgen` CLI provisioning, the browser fixture builds
and the `fixture-host` static host, all on dynamically allocated ports. The
end-to-end project (`dotnet/RayX.SignalR.E2E.Tests`, xUnit through `dotnet test`)
starts the same AppHost through `Aspire.Hosting.Testing` and is the single entry
point for those tests, locally and in GitHub Actions. No test scripts remain.

Prerequisites:

- .NET SDK 10.0.100 (pinned by `global.json`) and the Aspire CLI
  (`dotnet tool install --global Aspire.Cli --version 13.5.3`);
- Rust 1.92.0 (with `wasm32-unknown-unknown` for browser cells);
- browser cells: Node 24 with pnpm and the repository-local Playwright browsers
  (`pnpm --dir tools-node install --frozen-lockfile`, then
  `pnpm --dir tools-node exec playwright install chromium firefox webkit`); the
  `threaded-wasm` cells also need `nightly-2026-06-23` with `rust-src`. The
  AppHost installs the fixture's exact `wasm-bindgen` CLI under
  `artifacts-temp/tools/wasm-bindgen/<version>` and never uses a machine-wide CLI;
- Android cells: the Android SDK (`ANDROID_HOME`) with an NDK, `cargo install cargo-ndk`,
  `rustup target add x86_64-linux-android`, and one running x86_64 emulator
  (`emulator -avd <name>`), or `RAYX_SIGNALR_E2E_ANDROID_SERIAL` naming the device;
- iOS cells (macOS only): Xcode with an iOS simulator runtime and
  `rustup target add aarch64-apple-ios-sim`; `RAYX_SIGNALR_E2E_IOS_DEVICE` picks a
  simulator by name or UDID (default: the newest available iPhone).

Start the graph with the Aspire dashboard for interactive debugging:

```shell
aspire run
```

Run end-to-end cells. `RAYX_SIGNALR_E2E_TARGETS` (`native`, `chromium`,
`firefox`, `webkit`, `android`, `ios`; default `native`),
`RAYX_SIGNALR_E2E_PROTOCOLS` and `RAYX_SIGNALR_E2E_SCENARIOS` (default `all`)
take comma lists or `all`; a selection that matches no cataloged cell fails.
The C# scenario catalog (`dotnet/RayX.SignalR.E2E.Tests/Scenarios/ScenarioCatalog.cs`)
decides which targets run which scenarios.

```shell
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
RAYX_SIGNALR_E2E_TARGETS=chromium,firefox,webkit dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
RAYX_SIGNALR_E2E_TARGETS=android dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
```

Each cell runs exactly one scenario (the Rust `e2e` test binary with
`--exact <scenario>::<protocol>` on the host, the Android emulator or the iOS
simulator, or Playwright with one exact title), is bounded to 60 seconds with
process cleanup, and on failure writes bounded, token-redacted output plus Aspire
resource logs (and an Android logcat excerpt) to `artifacts-temp/e2e/<cell>.json`.
Android cells push the binary built by `cargo ndk` to `/data/local/tmp` and reach
the service at `10.0.2.2`; iOS cells run it with `xcrun simctl spawn`. Device-side
artifacts are removed after the run. The Rust scenarios live in `tests/e2e`
(`test = false`, so plain `cargo test` never needs the service).

Static checks are .NET test categories, run the same way locally and in CI:

```shell
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter "Category=WorkflowContract|Category=Repository|Category=Catalog|Category=Toolchain"
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=Orchestration
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=Platform
cargo install cargo-audit --locked
dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=DependencySecurity
```

The least-privilege GitHub Actions workflows run on pull requests to `master`
and pushes to `master` (end-to-end can also be started manually). They do not
publish packages, require secrets, or change repository settings.

Stable quality check names are:

- `quality / rustfmt`
- `quality / clippy`
- `quality / native (<os>, Rust <toolchain>)`
- `quality / wasm (<feature mode>)`
- `quality / unit-contract (<protocol>)`
- `quality / android (cargo ndk)`
- `quality / ios (<target>)`
- `quality / threaded-wasm`
- `quality / platform`
- `quality / rustdoc`
- `quality / dependency-security`
- `quality / contracts`

Stable end-to-end check names are:

- `e2e / native (<os>)`
- `e2e / browser (<engine>)` for `chromium`, `firefox` and `webkit`
- `e2e / android`
- `e2e / ios`

RayX.GameX's dependency-conformance gate replaces its former
`scripts/ci/run-interop.ps1` call with:

```shell
RAYX_SIGNALR_E2E_TARGETS=chromium RAYX_SIGNALR_E2E_SCENARIOS=ordinary-reconnect,stateful-reconnect dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E
```

## Package Overview
`rayx-signalr` is a Rust library designed to call SignalR hubs from cross-platform Rust applications. It supports Tokio-based native runtimes and browser WASM clients.

## Installation Instructions
To use this package, add the following to your Cargo.toml:

```toml
[dependencies]
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR.git" }
```

## MessagePack Protocol

The library supports the SignalR MessagePack hub protocol as an optional feature. MessagePack uses a binary encoding format, which results in smaller messages and can improve throughput compared to JSON.

To enable MessagePack support, add the `messagepack` feature:

```toml
[dependencies]
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR.git", features = ["messagepack"] }
```

Then use the `with_messagepack_protocol()` builder method when connecting:

```rust
let mut client = SignalRClient::connect_with("localhost", "hub", |c| {
    c.with_port(5000);
    c.with_messagepack_protocol();
}).await.unwrap();
```

All other API calls (`invoke`, `enumerate`, `send`, `register`) work exactly the same regardless of protocol. The server must also have MessagePack protocol enabled (e.g., `.AddMessagePackProtocol()` in ASP.NET Core).

## Usage Examples

Here is a complex test scenario demonstrating how to use the `rayx-signalr` package:

The `SignalRClient` can be used to invoke methods on the hub, send messages, and register callbacks.
The client can be cloned and used freely across different parts of your application.

```
// Connect to the SignalR server with custom configuration
let mut client = SignalRClient::connect_with("localhost", "test", |c| {
    c.with_port(5220); // Set the port to 5220
    c.unsecure(); // Use an unsecure (HTTP) connection
}).await.unwrap();

// Invoke the "SingleEntity" method and assert the result
let re = client.invoke::<TestEntity>("SingleEntity".to_string()).await;
assert!(re.is_ok());

// Unwrap the result and assert the entity's text
let entity = re.unwrap();
assert_eq!(entity.text, "test".to_string());

// Log the entity's details
info!("Entity {}, {}", entity.text, entity.number);

// Enumerate "HundredEntities" and log each entity
let mut he = client.enumerate::<TestEntity>("HundredEntities".to_string()).await;
while let Some(item) = he.next().await {
    info!("Entity {}, {}", item.text, item.number);
}

info!("Finished fetching entities, calling pushes");

// Invoke the "PushEntity" method with arguments and assert the result
let push1 = client.invoke_with_args::<bool, _>("PushEntity".to_string(), |c| {
    c.argument(TestEntity {
        text: "push1".to_string(),
        number: 100,
    });
}).await;
assert!(push1.unwrap());

// Clone the client and invoke the "PushTwoEntities" method with arguments
let mut secondclient = client.clone();
let push2 = secondclient.invoke_with_args::<TestEntity, _>("PushTwoEntities".to_string(), |c| {
    c.argument(TestEntity {
        text: "entity1".to_string(),
        number: 200,
    }).argument(TestEntity {
        text: "entity2".to_string(),
        number: 300,
    });
}).await;
assert!(push2.is_ok());

// Unwrap the result and assert the merged entity's number
let entity = push2.unwrap();
assert_eq!(entity.number, 500);
info!("Merged Entity {}, {}", entity.text, entity.number);

// Drop the second client
drop(secondclient);

// Register callbacks for "callback1" and "callback2"
let c1 = client.register("callback1".to_string(), |ctx| {
    let result = ctx.argument::<TestEntity>(0);
    if result.is_ok() {
        let entity = result.unwrap();
        info!("Callback results entity: {}, {}", entity.text, entity.number);
    }
});

let c2 = client.register("callback2".to_string(), |mut ctx| {
    let result = ctx.argument::<TestEntity>(0);
    if result.is_ok() {
        let entity = result.unwrap();
        info!("Callback2 results entity: {}, {}", entity.text, entity.number);
        let e2 = entity.clone();
        spawn(async move {
            info!("Completing callback2");
            let _ = ctx.complete(e2).await;
        });
    }
});

// Trigger the callbacks
info!("Calling callback1");
_ = client.send_with_args("TriggerEntityCallback".to_string(), |c| {
    c.argument("callback1".to_string());
}).await;

info!("Calling callback2");
let succ = client.invoke_with_args::<bool, _>("TriggerEntityResponse".to_string(), |c| {
    c.argument("callback2".to_string());
}).await;
assert!(succ.unwrap());

// Measure the time taken to fetch a million entities
let now = Instant::now();
{
    let mut me = client.enumerate::<TestEntity>("MillionEntities".to_string()).await;
    while let Some(_) = me.next().await {}
}
let elapsed = now.elapsed();
info!("1 million entities fetched in: {:.2?}", elapsed);

// Unregister the callbacks and disconnect the client
c1.unregister();
c2.unregister();
client.disconnect();
```

## Acknowledgements

Special thanks to the [`maintainer of the signalrs package`](https://github.com/szarykott) for his invaluable inspiration and work in the first SignalR client. Their efforts have significantly contributed to the development of this package.

## Contributing
Contributions are welcome! Please fork the repository and submit pull requests along with an issue or some explanation. Ensure your code follows the existing style and includes tests for any new functionality. 

## License
This project is licensed under the MIT License. See the LICENSE file for more details.
