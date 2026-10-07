# Limitations

This chapter lists what the client does not do, and the behavior that the test suite does not
prove. Each item says what to do instead where there is an alternative.

## Not implemented

- **Transports other than WebSockets.** Server-Sent Events and Long Polling are not implemented.
  The server must allow WebSockets, and every network path between client and server must carry
  WebSocket upgrades.
- **Proxies.** Native clients connect directly; HTTP `CONNECT` proxies and system proxy settings
  are not used. Browsers use the browser's proxy configuration.
- **Client-to-server streaming.** Hub methods with `IAsyncEnumerable<T>` or `ChannelReader<T>`
  parameters cannot be called.
- **Token refresh.** Bearer tokens are fixed strings for the lifetime of the client, and every
  reconnect reuses them. There is no access-token factory; reconnect with a new client when a token
  expires.
- **Invocation timeout.** There is no per-call timeout; wrap calls in your runtime's timeout.
- **Status events.** There is no status-change callback; use a disconnection handler or poll
  `status()`.
- **WASI and other non-browser WebAssembly targets.** There is no WebSocket implementation there;
  compilation fails on purpose.

## Behavior with caveats

- **Browser Basic authentication.** Browsers send Basic credentials only with negotiate; a standard
  ASP.NET Core server does not authenticate the WebSocket from them. Use Bearer tokens in browsers.
- **Browser pause with stateful reconnect.** Browsers always send a WebSocket Close, which ends the
  stateful logical connection; `resume` negotiates a new one.
- **Backgrounded apps and tabs.** Operating systems and browsers delay timers of background work.
  Keep-alive and server-timeout timers can fire late, and a dead transport is then noticed only after
  a further server timeout. Call `pause()` when going to the background and `resume()` on return.
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
