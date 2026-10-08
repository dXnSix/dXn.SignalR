# Limitations

This chapter lists what the client does not do, and the behavior that the test suite does not
prove. Each item says what to do instead where there is an alternative.

## Not implemented

- **Browser credentials mode.** Browsers send `fetch` requests (negotiate, Long Polling, Server-Sent
  Events sends) with the default `same-origin` credentials mode and open `EventSource` without
  `withCredentials`, as the .NET client's browser `HttpClient` does by default; there is no option
  to change it, so cookies (for example a load balancer's sticky-session cookie) are not sent to
  a hub on another origin.
- **HTTP/3 is experimental.** It needs the `http3` feature. WebSockets never run over HTTP/3,
  because servers such as Kestrel do not bootstrap them (RFC 9220). QUIC never goes through
  proxies. A host's addresses are tried one after another, so an unreachable first address costs
  its QUIC handshake timeout. There is no 0-RTT. The .NET SignalR client has no HTTP/3 at all.
- **`with_proxy_auto_config` on iOS.** The `proxy-pac` feature is not built for iOS (the Boa
  engine's `time` dependency does not compile there), so an explicit script is unavailable there.
  The system's own auto-config scripts still work: CFNetwork runs them.
- **System proxies on Android need the Java VM.** `java.net.ProxySelector` is read only after
  `tls::android::init_with_env` (the platform verifier's initialization, which needs the
  `tls-platform-verifier` feature) has stored the Java VM; before that, and in processes without
  a Java VM, the environment variables are the system's settings.
- **Proxy authentication schemes.** Proxies authenticate with Basic only, sent after the proxy's
  407 challenge offers it; Digest, NTLM and Negotiate challenges fail with `ErrorKind::Proxy`
  (407).
- **Invocation timeout.** There is no per-call timeout; wrap calls in your runtime's timeout.
- **Status events.** There is no status-change callback; use a disconnection handler or poll
  `status()`.
- **WASI and other non-browser WebAssembly targets.** There is no WebSocket implementation there;
  compilation fails on purpose.

## Behavior with caveats

- **Browser Basic authentication.** Browsers send Basic credentials on every `fetch` request
  (negotiate, Long Polling, Server-Sent Events sends), as the .NET client does, and log a warning:
  a WebSocket or an `EventSource` cannot carry them, so a standard ASP.NET Core server does not
  authenticate those from them. Use Bearer tokens or an access-token factory in browsers.
- **Integrated authentication outside Windows.** `with_default_credentials` (the current
  account) needs Windows SSPI; elsewhere it fails `new` with `ErrorKind::Configuration`, where the
  .NET client would use the system GSSAPI library. `with_credentials` works everywhere natively,
  through SSPI on Windows and NTLMv2 elsewhere (no Kerberos outside Windows). Outside Windows, the
  NTLM target name uses the host as given, not its canonical DNS name.
- **Android trust with the platform verifier.** The platform verifier trusts the device's system
  and user CAs (Android's `AndroidCAStore`), not the app's network security config: a
  `<trust-anchors>` entry there does not make a private CA trusted. Install the CA on the device
  (settings or a device policy) or use extra PEM roots, which verify with webpki. See
  [Android](09-platforms.md#android).
- **Browser pause with stateful reconnect.** Browsers always send a WebSocket Close, which ends the
  stateful logical connection; `resume` negotiates a new one.
- **Backgrounded apps and tabs.** Operating systems and browsers delay or stop background work.
  Suspension detection pings or ends the transport as soon as the app runs again, but a suspended
  app receives nothing, and after the server's client timeout (30 s by default) ASP.NET Core ends
  the hub connection, so even a stateful client reconnects on a new connection. Call `pause()` when
  going to the background and `resume()` on return (see
  [App lifecycle integration](06-connection-lifecycle.md#app-lifecycle-integration)).
- **MessagePack timestamps.** The client has no date type of its own: a `DateTime` argument or
  result arrives as MessagePack extension type -1 and decodes through `rmp_serde`'s extension
  convention. See [Hub protocols](05-hub-protocols.md#inbound-server-to-client).

## Not covered by tests

The end-to-end suite runs every scenario on Windows, Linux and macOS, in Chromium, Firefox and
WebKit, on an Android x86_64 emulator and on the iOS simulator. These parts are not exercised:

- **Physical devices.** Emulators and simulators do not cover real radios, Doze mode, or iOS
  background suspension. Android arm64-v8a and the iOS device target are compile-checked only.
  The Android platform verifier runs inside the test app on the emulator (`android-app`,
  `android-app-release`), and `aws-lc-rs` runs the TLS-dependent scenarios on native and Android
  (`RAYX_SIGNALR_E2E_TLS_PROVIDER=aws-lc-rs`).
- **Kerberos.** The `windows-auth` scenario authenticates the current Windows account with NTLM
  through SSPI. Kerberos needs a domain, which the test host does not have.
- **OS proxy settings on Windows and macOS hosts.** Only the `e2e / os-proxy` CI jobs change
  them, on ephemeral runners. Developer machines run `os-proxy` only inside the Android test app.
- **iOS background timing.** Spawned simulator processes are not foreground apps. The iOS runner
  raises the scenario thread's QoS class and warms the simulator up before timed cells.
- **A once-seen stall, retired.** After a third stateful resume on the Android emulator, one
  invocation once did not complete although the transport stayed open. The `stateful-resume-stress`
  scenario (500 transport aborts per cell, each followed by an invocation and a stream, with the
  stateful sequence trace on) and the `soak-chaos` scenario pass on native, Chromium and Android
  for JSON and MessagePack without reproducing it; if it recurs, the stress cell's trace names the
  lost sequence range.
