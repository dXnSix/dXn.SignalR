---
agentx:
  kind: decision
  scope: client-runtime/transport
  status: current
  created: "2026-10-09T18:00:00+02:00"
  updated: "2026-10-09T18:00:00+02:00"
  topics:
    - proxy
    - socks
    - pac
    - os-proxy
  supersedes: []
  superseded_by: []
---
## Decision: Resolve proxies per target URL into ordered routes in front of one tunnel connector, using the system proxy by default

### Context: The official .NET client uses the system proxy (environment, WinHTTP, CFNetwork, Android `ProxySelector`, including PAC) by default; the crate had no proxy support (`production-grade-client` D8, D13, R8, R13).

### Alternatives considered: Only explicit proxies; reimplementing WPAD/PAC discovery; a third-party proxy crate per platform.

### Reasoning: `ProxyConfig` (`System` default, `Explicit` http/https/socks4/4a/5/5h, `Os`, `AutoConfig`, `Disabled`) resolves to an ordered `Vec<Route>`; the shared connector tries each route's TCP in order and fails over only when a proxy is unreachable, so a 407/403 stays final as in .NET. OS settings come from the OS APIs (WinHTTP, CFNetwork, JNI `ProxySelector`), so auto-detection and PAC behave as in other apps; explicit PAC scripts run sandboxed in the pure-Rust Boa engine with time, instruction and DNS bounds. TLS to an `https://` proxy uses the connection's own rustls configuration; credentials are masked in logs and errors.

### Trade-offs accepted: Platform readers per OS; PAC is not built for iOS (Boa's time dependency); proxies authenticate with Basic only; QUIC never uses a proxy.

### Supersedes: None.

### Source anchors: [proxy](../../../../../src/communication/proxy.rs), [SOCKS](../../../../../src/communication/socks.rs), [PAC](../../../../../src/communication/pac.rs), [OS proxy](../../../../../src/communication/os_proxy.rs).
