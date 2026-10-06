---
agentx:
  kind: decision
  scope: client-runtime/transport
  status: current
  created: "2026-10-06T21:00:00+02:00"
  updated: "2026-10-06T21:00:00+02:00"
  topics:
    - tls
    - rustls
    - negotiate
    - native
  supersedes: []
  superseded_by: []
---
## Decision: Use rustls with an explicit provider per connection and hyper for native negotiate

### Context: Consumers such as RayX unify rustls features across crates, mobile targets must not need OpenSSL, and negotiate and the WebSocket must trust the same roots.

### Alternatives considered: native-tls/OpenSSL, the process-default rustls `CryptoProvider`, and keeping `ehttp`/`ureq` for negotiate.

### Reasoning: Each secure native connection builds one `rustls::ClientConfig` with an explicit provider (`tls-aws-lc-rs` over `tls-ring`) and verifier (`tls-platform-verifier` by default, `tls-webpki-roots` on request, webpki over extra roots on Android) and shares it with every negotiate (one hyper HTTP/1.1 request) and WebSocket connect; both open their own TCP stream with `TCP_NODELAY`, try every resolved address and are bounded by a 15 s setup timeout. Builds without a verifier and a provider feature reject secure endpoints before I/O; nothing falls back to plaintext or to an accept-all verifier.

### Trade-offs accepted: Feature combinations to document; the Android platform verifier needs app-side JVM initialization.

### Supersedes: None.

### Source anchors: [TLS context](../../../../../src/communication/tls.rs), [native negotiate](../../../../../src/communication/negotiate_http.rs), [native transport](../../../../../src/communication/client_tokio.rs), [build script](../../../../../build.rs).
