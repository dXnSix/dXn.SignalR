# Platforms

## Support matrix

| Target | Status | Proof |
|---|---|---|
| Windows, Linux, macOS (Tokio) | Supported | Full end-to-end catalog in CI (`e2e / native (<os>)`) |
| Android (`aarch64`, `x86_64`) | Supported | Full catalog on an x86_64 emulator (`e2e / android`), TLS and proxy cells inside the test app (`e2e / android-app`); arm64-v8a is compile-checked (`quality / android`) |
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
3. `connect` fails before any I/O, with an `ErrorKind::Configuration` error (`setting()` is
   `with_runtime_handle`) that names both options.

Negotiation, the WebSocket, keep-alive and timeout timers, reconnect delays, stateful backpressure,
stream cancellation, the connection's writer and receiver tasks and the callback dispatch task
all run on that runtime. The
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
    .build()?;
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

The browser client uses `fetch` for negotiation and Long Polling, and the browser's `WebSocket` for
the WebSockets transport. It is event-driven: transport events feed one receiver task per
transport, timers are cancellable `gloo-timers` futures, and nothing touches `window`.

- **`!Send`.** The client belongs to the thread or worker that created it and runs on that event
  loop. Use one client per thread or worker.
- **Spawning.** Drive it from `wasm_bindgen_futures::spawn_local`. `InvocationContext::spawn` maps
  to `spawn_local` too.
- **CORS.** The page and the hub usually have different origins, so the server must allow the
  page's origin (see [Server setup](10-server-setup.md#cors)).
- **Authentication.** Browsers cannot set WebSocket headers. Bearer tokens go on the WebSocket URL
  and the Server-Sent Events stream URL as `access_token`; Basic credentials reach only negotiate
  there. Long Polling and Server-Sent Events sends are `fetch` requests, so they carry Basic or
  Bearer credentials in the `Authorization` header. Use Bearer tokens in browsers.
- **TLS.** The browser's TLS and trust store apply; the TLS features and options do not exist.
- **Pause.** A browser `pause()` always sends a WebSocket Close, so it ends a stateful logical
  connection (see [Pause with stateful reconnect](06-connection-lifecycle.md#pause-with-stateful-reconnect)).
- **Background tabs and frozen pages.** Browsers throttle timers in hidden tabs and freeze or
  discard pages. Suspension detection notices late timers once the page runs again (see
  [Suspension detection](06-connection-lifecycle.md#suspension-detection)); pause on
  `visibilitychange` and `freeze` as in
  [App lifecycle integration](06-connection-lifecycle.md#app-lifecycle-integration).

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

**Option 1: the platform verifier** (the default `tls-platform-verifier` feature). This is the
setup of the repository's test app (`tests/android-app`), which the `android-app` and
`android-app-release` end-to-end targets run (`rustls-platform-verifier` 0.7.x):

1. Add its Maven repository to Gradle (`settings.gradle.kts`):

   ```kotlin
   dependencyResolutionManagement {
       repositories {
           google()
           mavenCentral()
           maven {
               url = uri("https://github.com/rustls/rustls-platform-verifier/raw/maven-archive/android-release-support/maven/")
           }
       }
   }
   ```

2. Depend on `org.rustls:rustls-platform-verifier` at the version of the
   `rustls-platform-verifier-android` crate in your `Cargo.lock`; a mismatched version can fail at
   runtime. The test app reads it from the lockfile (`app/build.gradle.kts`):

   ```kotlin
   abstract class RustlsVersion : ValueSource<String, RustlsVersion.Params> {
       interface Params : ValueSourceParameters {
           val lockFile: RegularFileProperty
       }

       override fun obtain(): String {
           val lines = parameters.lockFile.get().asFile.readLines()
           val name = lines.indexOfFirst { it.trim() == "name = \"rustls-platform-verifier-android\"" }
           return lines.drop(name + 1)
               .firstOrNull { name >= 0 && it.trimStart().startsWith("version = ") }
               ?.substringAfter('"', "")
               ?.substringBefore('"', "")
               ?.takeIf { it.isNotEmpty() }
               ?: error("rustls-platform-verifier-android not found in Cargo.lock")
       }
   }

   val rustlsPlatformVerifierVersion = providers.of(RustlsVersion::class.java) {
       parameters.lockFile.set(rootProject.layout.projectDirectory.file("../Cargo.lock"))
   }

   dependencies {
       implementation("org.rustls:rustls-platform-verifier:${rustlsPlatformVerifierVersion.get()}")
   }
   ```

3. Keep its classes from shrinking in release builds (`proguard-rules.pro`; the test app's
   minified release APK proves it):

   ```text
   -keep, includedescriptorclasses class org.rustls.platformverifier.** { *; }
   ```

4. Before the first secure connection, call
   `rayx_signalr::tls::android::init_with_env(env, context)` once with the JNI environment and an
   Android `Context`, for example from a native method that your `Application` or `Activity` calls
   at start-up (`JNI_OnLoad` has no `Context`). It also keeps the Java VM, so the default system
   proxy follows the device's proxy (`java.net.ProxySelector`). The test app does it from a JNI
   method that its Kotlin code calls with the `Context`:

   ```rust
   /// `static native void initialize(Context context)` of `com.example.Native`.
   #[no_mangle]
   pub extern "system" fn Java_com_example_Native_initialize<'local>(
       mut unowned: jni::EnvUnowned<'local>,
       _class: jni::objects::JClass<'local>,
       context: jni::objects::JObject<'local>,
   ) {
       unowned
           .with_env(|env| -> Result<(), jni::errors::Error> {
               // `with_env` catches the panic; the policy throws it to Kotlin as a
               // RuntimeException.
               if let Err(error) = rayx_signalr::tls::android::init_with_env(env, context) {
                   panic!("cannot initialize the platform verifier: {error}");
               }
               Ok(())
           })
           .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
   }
   ```

   Use the `jni` version `rayx-signalr` depends on (0.22). `init_with_refs` and
   `init_with_runtime` are re-exported too.

The platform verifier trusts the device's CA store: the system CAs and user-installed CAs (it
verifies against Android's `AndroidCAStore`). It does **not** read the app's network security
config, so a `<trust-anchors>` entry there does not make a private CA trusted. To trust a private
CA, install it on the device (a user CA from the settings, or a device policy such as an MDM's
`installCaCert`, which is how the test app trusts its per-run CA), or use extra PEM roots (below).

**Option 2: bundled roots.** Enable `tls-webpki-roots` and call `use_webpki_roots()` on the
configuration. This needs no JVM, but ignores user-installed and enterprise roots.

Connections with extra PEM roots always verify with webpki on Android, because the platform
verifier cannot add roots there.

The emulator reaches the development machine's loopback at `10.0.2.2`.

Android stops and defers background apps (Doze, app standby). Pause the client in `onStop` and
resume it in `onStart` (see
[App lifecycle integration](06-connection-lifecycle.md#app-lifecycle-integration)); a stopped or
sleeping app that did not pause is recovered by suspension detection once it runs again.

## iOS

The client runs in iOS apps and on the simulator; the platform verifier uses the iOS trust store.

- App Transport Security governs Apple's URL loading system, not the client's own rustls sockets.
  Production apps should still use `https`/`wss`; keep plain `http`/`ws` for local development
  (the simulator shares the Mac's loopback).
- iOS throttles and coalesces wakeups of work that is not in the foreground and suspends
  background apps, so keep-alive and server-timeout timers fire late or not at all. Call `pause()`
  when the app enters the background and `resume()` when it becomes active (see
  [App lifecycle integration](06-connection-lifecycle.md#app-lifecycle-integration)); suspension
  detection recovers an app that did not pause once it runs again.

## Desktop

Windows, Linux and macOS need nothing beyond a Tokio runtime. The platform verifier uses the Windows
certificate store, the macOS keychain, or the Linux system CA bundle.
