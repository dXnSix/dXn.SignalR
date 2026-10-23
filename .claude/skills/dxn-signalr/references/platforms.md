# Platforms

Full chapter: [Platforms](../../../../docs/09-platforms.md).

## Support

Windows, Linux, macOS, Android (`aarch64`, `x86_64`), iOS (device, simulator), browser
`wasm32-unknown-unknown` (main thread, Web Worker, multithreaded atomics). WASI and other `wasm32`
targets fail to compile on purpose. Every end-to-end scenario runs with JSON and MessagePack on
desktop OSes, Chromium/Firefox/WebKit, the Android x86_64 emulator and the iOS simulator.

## Native runtime ownership

Each connection belongs to one Tokio runtime: `with_runtime_handle(handle)`, else the runtime
`connect` is called from, else `connect` fails before I/O naming both options. All I/O, timers,
reconnect delays, stream cancellation and the writer/receiver tasks run there. Public futures only
wait on runtime-agnostic primitives, so any executor can await them:

```rust
let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build()
    .map_err(|e| e.to_string())?;
let handle = runtime.handle().clone();
let mut client = futures::executor::block_on(SignalRClient::connect_with("localhost", "chat", move |c| {
    c.with_runtime_handle(handle.clone());
}))?;
```

The runtime needs IO and time drivers and must outlive the client. The crate enables only Tokio
`rt`, `net`, `time`, `sync`. Callbacks and disconnection handlers run on that runtime.

## Browser

- `fetch` for negotiate, Long Polling and Server-Sent Events sends, `EventSource` for the
  Server-Sent Events stream, `WebSocket` for WebSockets; event-driven, no `window` access.
- Client is `!Send`: one per thread or worker; drive with `wasm_bindgen_futures::spawn_local`.
- Server must allow the page origin (CORS).
- Bearer tokens or an access-token factory (sent as `access_token` on the WebSocket and the
  `EventSource` URL); Basic reaches only negotiate there (with a warning). Long Polling and
  Server-Sent Events `fetch` requests carry either in `Authorization`.
- Browser TLS; no TLS options.
- `pause()` always sends a Close, so it ends stateful logical connections.
- Hidden tabs throttle timers and pages can be frozen; suspension detection recovers once the page
  runs, and `pause()` on long `visibilitychange` hides and `freeze` avoids the loss.
- Web Workers: same code inside the worker.
- Multithreaded WASM: cross-origin isolated page (`Cross-Origin-Opener-Policy: same-origin`,
  `Cross-Origin-Embedder-Policy: require-corp`), nightly with `rust-src`,
  `-Z build-std=std,panic_abort`, and RUSTFLAGS with `+atomics,+bulk-memory,+mutable-globals`,
  shared imported memory and the TLS exports (the exact tested set is `ThreadedWasm.RustFlags` in
  `dotnet/dXn.SignalR.AppHost/src/ThreadedWasm.cs`, mirroring RayX's xtask). Binary frames are
  copied out of shared memory before send. Keep the client on the thread that connected.

## Android

Pick one:

1. Platform verifier (default feature; proven inside `tests/android-app`): add the Maven repo
   `https://github.com/rustls/rustls-platform-verifier/raw/maven-archive/android-release-support/maven/`,
   depend on `org.rustls:rustls-platform-verifier` at the `rustls-platform-verifier-android`
   version in `Cargo.lock` (docs/09 has the Gradle snippet), keep
   `-keep, includedescriptorclasses class org.rustls.platformverifier.** { *; }`, and call
   `dxn_signalr::tls::android::init_with_env(env, context)` once with the JNI env (`jni` 0.22) and
   an Android `Context` before the first secure connection (`JNI_OnLoad` has no `Context`;
   `init_with_refs`/`init_with_runtime` are also re-exported). It trusts system and user CAs
   (`AndroidCAStore`), **not** the app's network security config: install a private CA on the
   device (settings or device policy) or use extra PEM roots.
2. Bundled roots: feature `tls-webpki-roots` + `use_webpki_roots()`; no JVM, no user/enterprise roots.

Extra PEM roots always use webpki on Android. Emulator → host loopback is `10.0.2.2`. Build with
`cargo ndk`.

## iOS

Platform verifier uses the iOS trust store. ATS does not govern the client's rustls sockets, but use
`https`/`wss` in production. Background wakeups are throttled and background apps suspended: `pause()` on
background, `resume()` on foreground; suspension detection recovers apps that did not pause. The simulator shares the Mac's loopback.
