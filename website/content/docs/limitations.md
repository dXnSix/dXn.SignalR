---
title: Limitations
description: What the client does not implement, behavior with caveats, and the parts the test suite does not cover.
group: Reference
order: 200
---

What the client does not do, where it behaves differently from what you might expect, and what its tests do not prove. Each item says what to do instead where there is an alternative.

## Not implemented

- **Browser credentials mode.** Browsers send `fetch` requests (negotiate, Long Polling, Server-Sent Events sends) with the default `same-origin` credentials mode and open `EventSource` without `withCredentials`, as the .NET client's browser `HttpClient` does by default. There is no option to change it, so cookies such as a load balancer's sticky-session cookie are not sent to a hub on another origin.
- **HTTP/3 is experimental.** It needs the `http3` feature. WebSockets never run over HTTP/3 because servers such as Kestrel do not bootstrap them (RFC 9220). QUIC never goes through proxies. A host's addresses are tried one after another, so an unreachable first address costs its QUIC handshake timeout. There is no 0-RTT. The .NET SignalR client has no HTTP/3.
- **`with_proxy_auto_config` on iOS.** The `proxy-pac` feature is not built for iOS because the Boa engine's `time` dependency does not compile there. The system's own auto-config scripts still work: CFNetwork runs them.
- **System proxies on Android need the Java VM.** `java.net.ProxySelector` is read only after `tls::android::init_with_env` has stored the Java VM; before that, and in processes without a Java VM, the environment variables are the system's settings.
- **Proxy authentication schemes.** Proxies authenticate with Basic only. Digest, NTLM and Negotiate challenges fail with `ErrorKind::Proxy` (407).
- **Invocation timeout.** There is no per-call timeout; wrap calls in your runtime's timeout.
- **Status-change callback.** As in the .NET client, there is no hook for every status change. `on_closed`, `on_reconnecting` and `on_reconnected` report the `Closed`, `Reconnecting` and `Reconnected` events, a disconnection handler sees drops in manual mode, and `status()` returns the current status. Other transitions, such as a completed `start` or a `pause`, show only in `status()`.
- **WASI and other non-browser WebAssembly targets.** There is no WebSocket implementation there; compilation fails on purpose.

## Behavior with caveats

- **Browser Basic authentication.** Browsers send Basic credentials on every `fetch` request and log a warning: a WebSocket or an `EventSource` cannot carry them, so a standard ASP.NET Core server does not authenticate those from them. Use Bearer tokens or an access-token factory.
- **Integrated authentication outside Windows.** `with_default_credentials` needs Windows SSPI; elsewhere it fails `new` with `ErrorKind::Configuration`, where the .NET client would use the system GSSAPI library. `with_credentials` works everywhere natively, through SSPI on Windows and NTLMv2 elsewhere (no Kerberos outside Windows). Outside Windows, the NTLM target name uses the host as given, not its canonical DNS name.
- **Android trust with the platform verifier.** It trusts the device's system and user CAs, not the app's network security config. See [Platforms](./platforms.md#android).
- **Browser pause with stateful reconnect.** Browsers always send a WebSocket Close, which ends the stateful logical connection; `resume` negotiates a new one.
- **Backgrounded apps and tabs.** Operating systems and browsers delay or stop background work. Suspension detection pings or ends the transport as soon as the app runs again, but a suspended app receives nothing, and after the server's client timeout (30 s by default) ASP.NET Core ends the hub connection, so even a stateful client reconnects on a new connection. Call `pause()` and `resume()` around backgrounding.
- **MessagePack timestamps.** The client has no date type of its own: a `DateTime` arrives as MessagePack extension type -1 and decodes through `rmp_serde`'s extension convention. See [Hub protocols](./hub-protocols.md#how-values-cross-the-wire).
- **The crate is not on crates.io.** It is consumed as a git dependency on a release tag, so there is no docs.rs page; build the API reference with `cargo doc --open`.

## Not covered by tests

The end-to-end suite runs its catalog on Windows, Linux and macOS, in Chromium, Firefox and WebKit, on an Android x86_64 emulator, inside an Android test app and on the iOS simulator. Some scenarios are excluded from some targets, each with its reason (see [Testing](./testing.md)): `http3` runs only where Kestrel serves HTTP/3, so not on macOS, iOS or in browsers; `windows-auth` runs on Windows hosts only; the soak and stress scenarios run on native, Chromium and Android.

These parts are not exercised:

- **Physical devices.** Emulators and simulators do not cover real radios, Doze mode or iOS background suspension. Android arm64-v8a and the iOS device target are compile-checked only.
- **Kerberos.** The `windows-auth` scenario authenticates the current account with NTLM through SSPI. Kerberos needs a domain, which the test host does not have.
- **NTLMv2 outside Windows.** Unit tests prove it with the MS-NLMP test vectors and scripted exchanges, but no end-to-end cell authenticates with it.
- **HTTP/3 on Apple targets.** The QUIC client is compile-checked for macOS and iOS only, because Kestrel cannot serve HTTP/3 there.
- **Performance on one host.** The measured comparison with the .NET client comes from one Windows machine. See [Performance](./performance.md).
- **OS proxy settings on Windows and macOS hosts.** Only the `e2e / os-proxy` CI jobs change them, on ephemeral runners.
- **iOS background timing.** Spawned simulator processes are not foreground apps.
- **Two unexplained flakes.** A WebKit Long Polling cell on Linux CI occasionally hung while connecting in dispatched runs in October 2026, and one `callbacks` cell on Windows CI once missed a server-to-client call. No cause is confirmed for either; the library's [testing notes](https://github.com/dXnSix/dXn.SignalR/blob/master/docs/12-limitations.md#not-covered-by-tests) list the details.

## Next

[Troubleshooting](./troubleshooting.md) covers fixing problems you can fix. [Testing](./testing.md) explains how the suite is built.
