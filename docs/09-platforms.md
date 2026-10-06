# Platforms

## Support matrix

| Target | Status | Proof |
|---|---|---|
| Windows, Linux, macOS (Tokio) | Supported | Full end-to-end catalog in CI (`e2e / native (<os>)`) |
| Android (`aarch64`, `x86_64`) | Supported | Full catalog on an x86_64 emulator (`e2e / android`); arm64-v8a is compile-checked (`quality / android`) |
| iOS (`aarch64-apple-ios`, simulator) | Supported | Full catalog on the iOS simulator (`e2e / ios`); the device target is compile-checked (`quality / ios`) |
| Browser, main thread (`wasm32-unknown-unknown`) | Supported | Full catalog in Chromium, Firefox and WebKit (`e2e / browser (<engine>)`) |
| Browser, Web Worker | Supported | `worker-transport` scenario in every engine |
| Browser, multithreaded WASM (atomics) | Supported | `threaded-wasm` scenario on a cross-origin isolated page |
| WASI and other `wasm32` targets | Not supported | Compilation fails with an explanatory error |

Every scenario runs with both JSON and MessagePack. Emulators and simulators prove the client's
networking, TLS and lifecycle code on those operating systems; they do not exercise a real
device's radio, Doze mode or background suspension. See [Limitations](12-limitations.md).

## Native runtime ownership

Natively, every connection belongs to exactly one Tokio runtime:

1. the handle given to `with_runtime_handle(handle)`, otherwise
2. the runtime that `connect` is called from, otherwise
3. `connect` fails before any I/O, with an error that names both options.

Negotiation, the WebSocket, keep-alive and timeout timers, reconnect delays, stateful backpressure,
stream cancellation and the connection's writer and receiver tasks all run on that runtime. The
futures the client returns only wait on runtime-agnostic primitives, so they can be awaited from any
executor: a UI framework's executor, `futures::executor::block_on` on a plain thread, or another
Tokio runtime.

```rust
use futures::executor::block_on;
use rayx_signalr::SignalRClient;

// A small runtime that owns the network work.
let runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(1)
    .enable_all()
    .build()
    .map_err(|error| error.to_string())?;
let handle = runtime.handle().clone();

// Awaited from a thread with no Tokio context.
let mut client = block_on(SignalRClient::connect_with("localhost", "chat", move |c| {
    c.unsecure();
    c.with_port(5000);
    c.with_runtime_handle(handle.clone());
}))?;
let answer: String = block_on(client.invoke("Echo".to_string()))?;
```

The runtime must have the IO and time drivers enabled (`enable_all()`), and must outlive the client.
The crate itself only enables Tokio's `rt`, `net`, `time` and `sync` features; your application
chooses the scheduler.

Callbacks and disconnection handlers run on the connection's runtime, so `tokio::spawn` works inside
them.

## Browsers

The browser client uses `fetch` for negotiation and the browser's `WebSocket` for the transport.
It is event-driven: socket events feed one receiver task per socket, timers are cancellable
`gloo-timers` futures, and nothing touches `window`.

- **`!Send`.** The client belongs to the thread or worker that created it and runs on that event
  loop. Use one client per thread or worker.
- **Spawning.** Drive it from `wasm_bindgen_futures::spawn_local`. `InvocationContext::spawn` maps
  to `spawn_local` too.
- **CORS.** The page and the hub usually have different origins, so the server must allow the
  page's origin (see [Server setup](10-server-setup.md#cors)).
- **Authentication.** Browsers cannot set WebSocket headers. Bearer tokens go on the WebSocket URL
  as `access_token`; Basic credentials reach only negotiate. Use Bearer tokens in browsers.
- **TLS.** The browser's TLS and trust store apply; the TLS features and options do not exist.
- **Pause.** A browser `pause()` always sends a WebSocket Close, so it ends a stateful logical
  connection (see [Pause with stateful reconnect](06-connection-lifecycle.md#pause-with-stateful-reconnect)).
- **Background tabs.** Browsers throttle timers in hidden tabs. Keep-alive pings and the server
  timeout can then fire late; consider `pause()` on `visibilitychange` for long-hidden tabs.

### Web Workers

The same code runs in a dedicated Web Worker: start the worker with your wasm-bindgen module and
connect from inside it. Nothing in the client needs the DOM.

### Multithreaded WASM

Builds with WebAssembly threads (atomics) work too. They need:

- a cross-origin isolated page: `Cross-Origin-Opener-Policy: same-origin` and
  `Cross-Origin-Embedder-Policy: require-corp`;
- a nightly toolchain with `rust-src` and `-Z build-std=std,panic_abort`;
- shared, imported memory and the thread-local-storage exports that wasm-bindgen's thread support
  expects. The tested build (the same flags as RayX) uses:

  ```text
  RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals
    -C link-arg=--shared-memory -C link-arg=--import-memory -C link-arg=--max-memory=2147483648
    -C link-arg=--export=__heap_base -C link-arg=--export=__wasm_init_tls
    -C link-arg=--export=__tls_size -C link-arg=--export=__tls_align -C link-arg=--export=__tls_base"
  cargo +nightly build --target wasm32-unknown-unknown -Z build-std=std,panic_abort
  ```

Binary frames are copied out of shared memory before they are sent, because `WebSocket.send` does
not accept views of a `SharedArrayBuffer`. Keep each client on the thread that connected it.

## Android

Android builds natively with the NDK (for example through `cargo ndk`). TLS needs one decision,
because the default platform verifier reads the Android trust store through the app's JVM:

**Option 1: the platform verifier** (the default `tls-platform-verifier` feature). Follow
`rustls-platform-verifier`'s Android setup (0.7.x):

1. Add its Maven repository to Gradle:
   `maven { url = "https://github.com/rustls/rustls-platform-verifier/raw/maven-archive/android-release-support/maven/" }`.
2. Depend on `rustls:rustls-platform-verifier` at the version of the
   `rustls-platform-verifier-android` crate in your `Cargo.lock` (its documentation shows a Gradle
   snippet that reads it; a mismatched version can fail at runtime).
3. Keep its classes from shrinking:
   `-keep, includedescriptorclasses class org.rustls.platformverifier.** { *; }`.
4. Before the first secure connection, call
   `rayx_signalr::tls::android::init_with_env(env, context)` once with the JNI environment and an
   Android `Context`, for example from a native method that your `Application` or `Activity` calls
   at start-up (`JNI_OnLoad` has no `Context`). `init_with_refs` and `init_with_runtime` are
   re-exported too.

**Option 2: bundled roots.** Enable `tls-webpki-roots` and call `use_webpki_roots()` on the
configuration. This needs no JVM, but ignores user-installed and enterprise roots.

Connections with extra PEM roots always verify with webpki on Android, because the platform
verifier cannot add roots there.

The emulator reaches the development machine's loopback at `10.0.2.2`.

## iOS

The client runs in iOS apps and on the simulator; the platform verifier uses the iOS trust store.

- App Transport Security governs Apple's URL loading system, not the client's own rustls sockets.
  Production apps should still use `https`/`wss`; keep plain `http`/`ws` for local development
  (the simulator shares the Mac's loopback).
- iOS throttles and coalesces wakeups of work that is not in the foreground, so keep-alive and
  server-timeout timers can fire late in the background. Call `pause()` when the app moves to the
  background and `resume()` when it returns.

## Desktop

Windows, Linux and macOS need nothing beyond a Tokio runtime. The platform verifier uses the Windows
certificate store, the macOS keychain, or the Linux system CA bundle.
