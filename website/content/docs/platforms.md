---
title: Platforms
description: The support matrix, how a native connection owns a Tokio runtime, and the Android and iOS setup.
group: Platforms
order: 140
---

One API runs on native Tokio targets and in the browser. "Native" means every non-WASM target; "browser" means `wasm32-unknown-unknown`, the only supported WebAssembly target.

## Support matrix

| Target | Status | Proof |
|---|---|---|
| Windows, Linux, macOS (Tokio) | Supported | Full end-to-end catalog in CI |
| Android (`aarch64`, `x86_64`) | Supported | Full catalog on an x86_64 emulator, TLS and proxy cells inside a test app; arm64-v8a is compile-checked |
| iOS (`aarch64-apple-ios`, simulator) | Supported | Full catalog on the iOS simulator; the device target is compile-checked |
| Browser, main thread | Supported | Full catalog in Chromium, Firefox and WebKit |
| Browser, Web Worker | Supported | `worker-transport` scenario in every engine |
| Browser, multithreaded WASM (atomics) | Supported | `threaded-wasm` scenario on a cross-origin isolated page |
| WASI and other `wasm32` targets | Not supported | Compilation fails with an explanatory error |

Every scenario runs with both JSON and MessagePack. Emulators and simulators prove the client's networking, TLS and lifecycle code on those operating systems; they do not exercise a real device's radio, Doze mode or background suspension. See [Testing](./testing.md) and [Limitations](./limitations.md).

## Native runtime ownership

Natively, every connection belongs to exactly one Tokio runtime:

1. the handle given to `with_runtime_handle(handle)`, otherwise
2. the runtime that `new` or `connect` is called from, otherwise
3. the call fails before any I/O with an `ErrorKind::Configuration` error (`setting()` is `with_runtime_handle`) that names both options.

Negotiation, the transport, timers, reconnect delays, stream cancellation, the writer and receiver tasks and the callback dispatch task all run on that runtime. The futures the client returns wait only on runtime-agnostic primitives, so they can be awaited from any executor: a UI framework's executor, `futures::executor::block_on` on a plain thread, or another Tokio runtime.

```rust
use dxn_signalr::SignalRClient;
use futures::executor::block_on;

fn blocking_client() -> Result<(), Box<dyn std::error::Error>> {
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
    println!("{answer}");
    Ok(())
}
```

The runtime needs the IO and time drivers (`enable_all()`) and must outlive the client. The crate itself enables only Tokio's `rt`, `net`, `time` and `sync` features; your application chooses the scheduler. Callbacks and disconnection handlers run on the connection's runtime, so `tokio::spawn` works inside them.

## Android

Android builds natively with the NDK, for example through `cargo ndk`. TLS needs one decision, because the default platform verifier reads the Android trust store through the app's JVM.

**Option 1: the platform verifier** (the default `tls-platform-verifier`). This is the setup of the repository's test app (`tests/android-app`), with `rustls-platform-verifier` 0.7.x:

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

2. Depend on `org.rustls:rustls-platform-verifier` at the version of the `rustls-platform-verifier-android` crate in your `Cargo.lock`; a mismatched version can fail at runtime. The test app reads it from the lockfile in its `app/build.gradle.kts`.

3. Keep its classes from shrinking in release builds (`proguard-rules.pro`):

   ```text
   -keep, includedescriptorclasses class org.rustls.platformverifier.** { *; }
   ```

4. Before the first secure connection, call `dxn_signalr::tls::android::init_with_env(env, context)` once with the JNI environment and an Android `Context`, for example from a native method that your `Application` or `Activity` calls at start-up (`JNI_OnLoad` has no `Context`). It also keeps the Java VM, so the default system proxy follows the device's proxy.

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
               if let Err(error) = dxn_signalr::tls::android::init_with_env(env, context) {
                   panic!("cannot initialize the platform verifier: {error}");
               }
               Ok(())
           })
           .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
   }
   ```

   Use the `jni` version `dxn-signalr` depends on (0.22). `init_with_refs` and `init_with_runtime` are re-exported too.

The platform verifier trusts the device's system and user CAs (Android's `AndroidCAStore`). It does **not** read the app's network security config, so a `<trust-anchors>` entry there does not make a private CA trusted. Install the CA on the device (a user CA, or a device policy such as an MDM's `installCaCert`), or use extra PEM roots.

**Option 2: bundled roots.** Enable `tls-webpki-roots` and call `use_webpki_roots()`. This needs no JVM, but ignores user-installed and enterprise roots. Connections with extra PEM roots always verify with webpki on Android.

The emulator reaches the development machine's loopback at `10.0.2.2`. Android defers background work (Doze, app standby), so pause in `onStop` and resume in `onStart`; see the [cookbook](./cookbook.md#pause-and-resume-with-the-app-lifecycle).

## iOS

The client runs in iOS apps and on the simulator; the platform verifier uses the iOS trust store.

- App Transport Security governs Apple's URL loading system, not the client's own rustls sockets. Production apps should still use `https` and `wss`; keep plain `http` and `ws` for local development (the simulator shares the Mac's loopback).
- iOS suspends background apps after a few seconds, so keep-alive and server-timeout timers fire late or not at all. Call `pause()` when the app enters the background and `resume()` when it becomes active.
- `with_proxy_auto_config` is not built for iOS; the system's auto-config scripts still work.

## Desktop

Windows, Linux and macOS need nothing beyond a Tokio runtime. The platform verifier uses the Windows certificate store, the macOS keychain, or the Linux system CA bundle.

## Next

[Browser and WebAssembly](./browser.md) covers the browser build. The [cookbook](./cookbook.md) has mobile lifecycle recipes.
