# Limitations

This chapter lists what the client does not do, and the behavior that the test suite does not
prove. Each item says what to do instead where there is an alternative.

## Not implemented

- **Browser credentials mode.** Browsers send `fetch` requests (negotiate, Long Polling, Server-Sent
  Events sends) with the default `same-origin` credentials mode and open `EventSource` without
  `withCredentials`, as the .NET client's browser `HttpClient` does by default; there is no option
  to change it, so cookies (for example a load balancer's sticky-session cookie) are not sent to
  a hub on another origin.
- **Operating-system proxy settings, HTTPS and SOCKS proxies.** Natively the system proxy comes
  from the proxy environment variables; the operating system's own proxy settings (and proxy
  auto-configuration) are not read yet, and only `http://` proxies are supported: an `https://`
  or `socks` proxy URL in the environment is skipped, and `with_proxy` refuses one. Use
  `with_proxy` or the environment variables. Browsers use the browser's proxy configuration.
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

- **The Android platform verifier inside a real app.** The Android cells run as a bare binary
  through `adb shell`, without an app's JVM, so they verify TLS with webpki. Trust-store
  verification after `rayx_signalr::tls::android::init_with_env` is only build-checked.
- **Physical devices.** Emulators and simulators do not cover real radios, Doze mode, or iOS
  background suspension. Android arm64-v8a and the iOS device target are compile-checked only.
- **`aws-lc-rs`.** The end-to-end cells use the default `ring` provider; `aws-lc-rs` is covered by
  builds and unit tests.
- **iOS background timing.** Spawned simulator processes are not foreground apps. The iOS runner
  raises the scenario thread's QoS class and warms the simulator up before timed cells.
- **A once-seen stall, retired.** After a third stateful resume on the Android emulator, one
  invocation once did not complete although the transport stayed open. The `stateful-resume-stress`
  scenario (500 transport aborts per cell, each followed by an invocation and a stream, with the
  stateful sequence trace on) and the `soak-chaos` scenario pass on native, Chromium and Android
  for JSON and MessagePack without reproducing it; if it recurs, the stress cell's trace names the
  lost sequence range.
