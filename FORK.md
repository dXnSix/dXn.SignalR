# RayX.SignalR fork notes

## Provenance

- Upstream repository: <https://github.com/danielleiszen/rust_signalr_client.git>
- RayX repository: <https://github.com/rayx-ui/RayX.SignalR>
- Fork baseline: `d32bd04` (`v0.3.2`)
- Upstream package: `signalr-client`
- RayX package / crate: `rayx-signalr` / `rayx_signalr`
- Initial RayX version: `0.1.0`
- License: MIT

The full upstream Git history is preserved. `origin` is the only persistent
remote and points to the RayX-owned hard fork. Future upstream imports fetch an
explicit commit directly from the upstream repository URL without configuring
an `upstream` remote. The RayX default and integration branch is `master`; the
source repository's default branch remains independently named `main`.

## Hard-fork policy

RayX owns package identity, release/version policy, API direction, platform
support, and local patches. Upstream changes are imported deliberately; the
fork is not automatically rebased, reset, or mirrored. Each import records the
exact upstream commit and preserves or adapts the RayX patch surface in
reviewable commits. See `.codex/skills/rayx-signalr/` for the maintenance
workflow.

Publishing remains disabled until a RayX registry and release policy are
explicitly established.

## Initial fork corrections

- Preserve application-defined Serde field names in outbound MessagePack maps.
  The transport must not recursively rewrite DTO keys to PascalCase.
- Use a patched ASP.NET Core SignalR MessagePack test dependency.
- Remove the upstream case-colliding `CLAUDE.md`/`claude.md` pair so Windows
  checkouts are stable.
- Maintain the owned `rayx-signalr` package identity and RayX repository
  metadata across upstream imports.

## Production hardening baseline

The fork now has explicit ordinary and stateful reconnect lifecycles on native
Tokio and browser WASM, JSON and MessagePack Ack/Sequence compatibility, bounded
replay and duplicate suppression, and runtime interoperability coverage against
the checked-in ASP.NET Core service. Browser proof uses the repository-local
Playwright 1.59.1 harness (Chromium, Firefox and WebKit) following the RayX.GameX
validation pattern.

The production gate baseline pins Rust 1.92, denies native/WASM/rustdoc warnings,
checks pinned and stable Rust across Windows/Linux/macOS, runs service-independent
unit and contract tests, executes every native/browser reconnect matrix cell,
and scans RustSec and NuGet transitive vulnerabilities. Workflows use
`contents: read`, cancel superseded pull-request runs, require no secrets, and
upload bounded redacted diagnostics only after failure. Publishing and branch
protection remain separate, explicitly authorized repository-owner actions.

Replacing the remaining inherited panic/`unwrap` network and protocol paths
with a fully typed public error model remains follow-up hardening and is not
claimed complete by this reconnect-and-production-gates epic.

## Cross-platform support

The cross-platform-support epic made the crate build and work on every platform
RayX targets and moved all service-dependent validation to .NET Aspire. It proved,
through Aspire-hosted end-to-end cells for JSON and MessagePack:

- the functional baseline (negotiation, authentication, invoke/send/streams,
  callbacks, clones, large payloads, server close, reconnect exhaustion, ordinary
  and stateful reconnect, stream cancellation) on Windows, Linux and macOS,
  Chromium, Firefox and WebKit, an Android x86_64 emulator and the iOS simulator;
- rustls TLS with an explicit provider per connection (platform verifier or
  bundled roots, extra PEM roots), with no OpenSSL anywhere in the graph;
- negotiate redirects with access tokens and `skip_negotiation`;
- runtime ownership: native futures can be awaited from foreign executors;
- an event-driven browser transport on the main thread, in Web Workers and in
  multithreaded (atomics) builds;
- connection liveness (keep-alive Pings, server timeout, bounded and validated
  handshake) and application lifecycle controls (`pause`, `resume`, `status`,
  `DefaultRetryPolicy`).

Compile-checked only: Android arm64-v8a (`quality / android`) and the iOS device target
`aarch64-apple-ios` (`quality / ios`); the end-to-end catalog runs on the x86_64 Android
emulator and the iOS simulator.

Residual risks, not claimed by that epic:

- The Android platform verifier is only build-checked: the Android cells run as
  a bare binary through `adb shell` without an app's JVM, so they verify TLS with
  webpki (extra roots use webpki on Android, and the untrusted case uses the
  bundled roots). Trust-store verification inside a real APK after
  `rayx_signalr::tls::android::init_with_env` is not exercised by CI. Emulators and simulators do not cover
  physical radios, Doze or iOS background suspension.
- Proxies (HTTP CONNECT, system proxy settings) are not supported.
- The end-to-end cells use the default `ring` provider; `aws-lc-rs` is covered by builds
  and unit tests only.
- iOS throttles the wakeups of processes that are not in the foreground: the iOS cells
  raise the scenario thread's QoS class, warm the simulator up once untimed, and accept a
  passed test whose `simctl spawn` launcher does not exit (recorded in
  `artifacts-temp/e2e/runner-notes.log`). Apps that keep connections in the background
  can see late keep-alive and timeout timers; `pause()` on backgrounding avoids that.
- Browsers send Basic credentials only with the negotiate request; a standard ASP.NET
  Core server does not authenticate the WebSocket from them (use Bearer tokens).
- Server-Sent Events and Long Polling transports are not implemented; servers
  must allow WebSockets.
- A browser `pause()` cannot keep a stateful logical connection, because browsers
  always send a WebSocket Close (see the README's lifecycle notes).
- The inherited untyped `String` errors remain; a typed public error model is
  still follow-up hardening.
