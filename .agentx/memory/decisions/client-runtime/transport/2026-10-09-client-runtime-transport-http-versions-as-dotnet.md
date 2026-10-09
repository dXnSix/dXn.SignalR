---
agentx:
  kind: decision
  scope: client-runtime/transport
  status: current
  created: "2026-10-09T18:00:00+02:00"
  updated: "2026-10-09T18:00:00+02:00"
  topics:
    - http2
    - http3
    - alpn
  supersedes: []
  superseded_by: []
---
## Decision: Choose HTTP versions as the .NET client does: HTTP/2 over TLS by default for HTTP requests, opt-in HTTP/2 WebSockets, experimental HTTP/3 behind a feature

### Context: The .NET client wraps its handler to request HTTP/2 with fallback unless credentials are set; it never uses HTTP/3 (`production-grade-client` D14, D15, R14, R15).

### Alternatives considered: HTTP/1.1 only; HTTP/2 opt-in only; HTTP/3 by default.

### Reasoning: `HttpVersion::Auto` (default) offers ALPN `h2, http/1.1` for negotiate, Long Polling and Server-Sent Events and falls back to HTTP/1.1; WebSockets stay HTTP/1.1 unless `Http2` asks for RFC 8441 extended CONNECT. The HTTP/2 connections of one connect attempt are pooled and multiplexed, each attempt opens new ones, and only requests the server never processed are retried. Integrated Windows authentication keeps HTTP/1.1, as .NET does. HTTP/3 (quinn and h3, feature `http3`) uses the connection's rustls configuration; WebSockets are skipped over HTTP/3 because Kestrel offers no RFC 9220.

### Trade-offs accepted: More connection states; HTTP/3 stays experimental and is not a parity feature; Apple TLS answers an h2-only ALPN mismatch with `protocol_version` (reported as `Tls`).

### Supersedes: None.

### Source anchors: [HTTP/2](../../../../../src/communication/http2.rs), [HTTP/3](../../../../../src/communication/http3.rs), [negotiate](../../../../../src/communication/negotiate_http.rs).
