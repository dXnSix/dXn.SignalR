---
title: Proxies
description: How native clients pick up the system proxy, and how to configure HTTP, HTTPS, SOCKS and PAC proxies explicitly.
group: Connection
order: 130
---

Native clients use the system's proxy settings by default, like the .NET client's default proxy, read each time a connection is opened. Browsers always use the browser's proxy settings and have none of these options.

When a proxy environment variable names a proxy, the environment decides. Otherwise the operating system's settings do:

| Platform | System settings |
|---|---|
| Windows | The current user's settings through WinHTTP (manual proxy and bypass list, automatic detection, auto-config script). Loopback hosts are never proxied. |
| macOS, iOS | CFNetwork's system settings: HTTP, HTTPS and SOCKS proxies and auto-config scripts, which CFNetwork runs. |
| Android | `java.net.ProxySelector`, once `tls::android::init_with_env` has stored the Java VM; otherwise the environment variables. |
| Linux and others | The environment variables. |

The variables are read as .NET reads them: `https_proxy` then `HTTPS_PROXY` for secure endpoints, `http_proxy` then `HTTP_PROXY` for plain ones, and `all_proxy` then `ALL_PROXY` when neither applies. A variable that is empty or not a supported proxy URL (`http://`, `https://`, `socks4://`, `socks4a://`, `socks5://`, `socks5h://`) is skipped. `no_proxy` (else `NO_PROXY`) exempts hosts with curl's rules: host names with their subdomains, IP addresses, CIDR ranges, entries with a port, and `*`.

`with_os_proxy()` uses the operating system's settings alone, ignoring the environment variables. Settings that cannot be read connect directly, as .NET does.

## Explicit proxies

```rust
use dxn_signalr::SignalRClient;

fn through_proxy() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("hub.example.com", "chat", |c| {
        // An explicit proxy instead of the system's; credentials are optional.
        c.with_proxy("http://user:password@proxy.example.com:3128");
        // Or a SOCKS proxy: c.with_proxy("socks5://user:password@proxy.example.com:1080");
        // Or connect directly with c.without_proxy(), or return to the default with c.with_system_proxy().
    })
}
```

Every native connection (negotiate, the WebSocket, Long Polling and Server-Sent Events) to a proxied endpoint opens a tunnel through the proxy and runs TLS with the connection's own configuration inside it, so the proxy never sees the traffic or terminates TLS. As in .NET, the URL's credentials (percent-encode `:` and `@` in them) go out only when the proxy asks: after a 407 that offers Basic, the tunnel request is sent again with Basic `Proxy-Authorization`.

A tunnel that fails fails the connection with `ErrorKind::Proxy`, with `status()` when the proxy answered. 407 and 403 are not transient; an unreachable proxy (no status) and 5xx are. Proxy credentials never appear in logs or errors. A proxy URL that is not `scheme://[user:password@]host[:port]` with one of the schemes above fails `new` with `ErrorKind::Configuration` naming `with_proxy`.

An `https://` proxy (port 443 by default) is reached over TLS, as in .NET 8 and later. The client verifies the proxy's certificate with the connection's own TLS configuration (its trust roots, extra roots included; never an accept-all verifier) and runs the endpoint's TLS inside the tunnel.

### SOCKS

| Scheme | Destination | Credentials |
|---|---|---|
| `socks5://`, `socks5h://` | a host name goes to the proxy, which resolves it; an IP literal goes as an address | user name and password (RFC 1929), offered alongside no authentication when the URL has them |
| `socks4a://` | resolved by the proxy | the user name as the SOCKS4 user id |
| `socks4://` | resolved locally to IPv4 | the user name as the SOCKS4 user id |

The default port is 1080. A SOCKS failure is `ErrorKind::Proxy` and its message names the proxy's reply code. Refused or missing credentials report status 407 and a forbidden destination 403; neither is transient.

:::info Proxy authentication is Basic only
Digest, NTLM and Negotiate challenges from a proxy fail with `ErrorKind::Proxy` (407).
:::

## Proxy auto-config

With the `proxy-pac` feature, `with_proxy_auto_config(url)` chooses proxies with a PAC script, as WinHTTP and browsers do. It is not built for iOS, where the system's own auto-config scripts still work through CFNetwork.

```rust
use dxn_signalr::SignalRClient;

fn pac() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("hub.example.com", "chat", |c| {
        c.with_proxy_auto_config("http://wpad.example.com/proxy.pac");
    })
}
```

The script comes from an `http`, `https` or `file` URL. It is loaded directly (never through a proxy) at each `start`, within the connect timeout. For each connection, `FindProxyForURL(url, host)` receives the endpoint and returns routes to try in order: `PROXY` and `HTTP`, `HTTPS`, `SOCKS` (SOCKS4), `SOCKS4`, `SOCKS5` and `DIRECT`. A route that cannot be reached passes to the next. The standard helpers (`isPlainHostName`, `dnsDomainIs`, `isInNet`, `dnsResolve`, `shExpMatch`, `weekdayRange` and the rest) are available.

The script runs in the pure-Rust Boa JavaScript engine on a blocking thread, with no network or file access beyond the helpers' DNS lookups. One evaluation is bounded at 5 seconds, a lookup at 2 seconds, and at most eight evaluations run at once. A script that cannot be loaded, has no `FindProxyForURL`, throws or runs too long fails the connection with `ErrorKind::Proxy`. PAC routes carry no credentials, so their proxies must not require authentication.

## Next

[TLS](./tls.md) covers what runs inside the tunnel. [Troubleshooting](./troubleshooting.md) maps proxy errors to causes.
