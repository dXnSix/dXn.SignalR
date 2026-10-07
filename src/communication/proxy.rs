//! Native HTTP proxies, as the .NET client's default `HttpClient`/`ClientWebSocket` proxy: every
//! native connection (negotiate, WebSocket, Long Polling, Server-Sent Events) to a proxied
//! endpoint opens an HTTP `CONNECT` tunnel through the proxy, then runs TLS with the connection's
//! own rustls configuration inside it for secure endpoints, so the proxy never sees the traffic.
//! The proxy comes from [`ProxyConfig`]: an explicit URL, none, or the system's (the
//! `HTTPS_PROXY`/`HTTP_PROXY`/`ALL_PROXY` and `NO_PROXY` environment variables, read at connect
//! time). Credentials in a proxy URL go out as Basic `Proxy-Authorization` and never appear in a
//! log, an error or a `Debug`/`Display` text.

// Proxy code must not panic on peer or environment input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::{fmt, net::IpAddr};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::error::Error;

/// The largest CONNECT response head accepted.
const RESPONSE_LIMIT: usize = 16 * 1024;

/// Which proxy native connections use.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum ProxyConfig {
    /// Connect directly (`without_proxy`).
    Disabled,
    /// Always this proxy (`with_proxy`).
    Explicit(ProxyUrl),
    /// The system's proxy settings, resolved at connect time (`with_system_proxy`, the default).
    #[default]
    System,
}

/// An `http://[user:password@]host[:port]` proxy. Its credentials are never shown.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ProxyUrl {
    host: String,
    port: u16,
    credentials: Option<(String, String)>,
}

impl ProxyUrl {
    /// Parses a proxy URL; a missing scheme means `http`, a missing port 80. The error never
    /// repeats the URL, which can carry credentials.
    pub(crate) fn parse(url: &str) -> Result<Self, Error> {
        let invalid = |reason: &str| {
            Error::configuration("with_proxy", format!("the proxy URL is invalid: {reason}"))
        };
        let url = url.trim();
        let rest = match url.split_once("://") {
            Some((scheme, rest)) if scheme.eq_ignore_ascii_case("http") => rest,
            Some(_) => return Err(invalid("only http:// proxies are supported")),
            None => url,
        };
        // Path, query and fragment carry nothing for a proxy.
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        let (userinfo, hostport) = match authority.rsplit_once('@') {
            Some((userinfo, hostport)) => (Some(userinfo), hostport),
            None => (None, authority),
        };
        let (host, port) = if let Some(bracketed) = hostport.strip_prefix('[') {
            let (host, after) = bracketed
                .split_once(']')
                .ok_or_else(|| invalid("an IPv6 address is missing its closing bracket"))?;
            let port = match after.strip_prefix(':') {
                Some(port) => Some(port),
                None if after.is_empty() => None,
                None => return Err(invalid("unexpected text after the host")),
            };
            (host, port)
        } else {
            match hostport.rsplit_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (hostport, None),
            }
        };
        if host.is_empty() {
            return Err(invalid("the host is missing"));
        }
        let port = match port {
            Some(port) => port
                .parse::<u16>()
                .ok()
                .filter(|port| *port != 0)
                .ok_or_else(|| invalid("the port is not a number from 1 to 65535"))?,
            None => 80,
        };
        let credentials = match userinfo {
            Some(userinfo) => {
                let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
                Some((percent_decode(user), percent_decode(password)))
            }
            None => None,
        };
        Ok(Self {
            host: host.to_string(),
            port,
            credentials,
        })
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    /// The `Proxy-Authorization` value of the URL's credentials.
    fn authorization(&self) -> Option<String> {
        use base64::{engine::general_purpose, Engine};
        self.credentials.as_ref().map(|(user, password)| {
            format!(
                "Basic {}",
                general_purpose::STANDARD.encode(format!("{user}:{password}"))
            )
        })
    }
}

impl fmt::Display for ProxyUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let credentials = if self.credentials.is_some() {
            "***@"
        } else {
            ""
        };
        if self.host.contains(':') {
            write!(
                formatter,
                "http://{credentials}[{}]:{}",
                self.host, self.port
            )
        } else {
            write!(formatter, "http://{credentials}{}:{}", self.host, self.port)
        }
    }
}

impl fmt::Debug for ProxyUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ProxyUrl({self})")
    }
}

/// Decodes `%XX` escapes (a userinfo may escape `:` and `@`); anything else is kept as is.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        let escaped = (byte == b'%')
            .then(|| bytes.get(index + 1..index + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(value) => {
                decoded.push(value);
                index += 3;
            }
            None => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// The proxy a connection to `host:port` uses under `config`, reading the environment through
/// `environment` for [`ProxyConfig::System`] as the .NET client's `HttpEnvironmentProxy` does:
/// `http_proxy`, then `HTTP_PROXY` (ignored when `GATEWAY_INTERFACE` is set, so a CGI request's
/// `Proxy:` header cannot choose the proxy), for plain endpoints; `https_proxy`, then
/// `HTTPS_PROXY`, for secure ones; `all_proxy`, then `ALL_PROXY`, when neither applies. A name
/// whose value is empty or not a usable proxy URL is skipped. `no_proxy` (else `NO_PROXY`)
/// exempts hosts.
pub(crate) fn resolve(
    config: &ProxyConfig,
    secure: bool,
    host: &str,
    port: u16,
    environment: impl Fn(&str) -> Option<String>,
) -> Option<ProxyUrl> {
    match config {
        ProxyConfig::Disabled => None,
        ProxyConfig::Explicit(proxy) => Some(proxy.clone()),
        ProxyConfig::System => {
            let usable = |name: &str| {
                let value = environment(name)?;
                if value.trim().is_empty() {
                    return None;
                }
                match ProxyUrl::parse(&value) {
                    Ok(proxy) => Some(proxy),
                    Err(_) => {
                        log::warn!(
                            "{name} is not an http:// proxy URL this client can use; it is skipped"
                        );
                        None
                    }
                }
            };
            let scheme_proxy = if secure {
                usable("https_proxy").or_else(|| usable("HTTPS_PROXY"))
            } else {
                usable("http_proxy").or_else(|| {
                    environment("GATEWAY_INTERFACE")
                        .is_none()
                        .then(|| usable("HTTP_PROXY"))
                        .flatten()
                })
            };
            let proxy =
                scheme_proxy.or_else(|| usable("all_proxy").or_else(|| usable("ALL_PROXY")))?;
            let no_proxy = environment("no_proxy").or_else(|| environment("NO_PROXY"));
            if no_proxy.is_some_and(|no_proxy| bypasses(&no_proxy, host, port)) {
                return None;
            }
            Some(proxy)
        }
    }
}

/// Whether `NO_PROXY` (curl semantics) exempts `host:port`: `*` exempts everything; otherwise
/// comma-separated entries match a host name and its subdomains (a leading dot is optional), an
/// IP address, or a CIDR range, each optionally restricted to a port (`name:port`,
/// `[v6]:port`).
pub(crate) fn bypasses(no_proxy: &str, host: &str, port: u16) -> bool {
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let address: Option<IpAddr> = host.parse().ok();
    no_proxy
        .split([',', ' ', '\t'])
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .any(|entry| {
            if entry == "*" {
                return true;
            }
            let (pattern, entry_port) = split_port(entry);
            if entry_port.is_some_and(|entry_port| entry_port != port) {
                return false;
            }
            let pattern = pattern.to_ascii_lowercase();
            if let Some((network, prefix)) = pattern.split_once('/') {
                return match (network.parse::<IpAddr>(), prefix.parse::<u8>(), address) {
                    (Ok(network), Ok(prefix), Some(address)) => in_range(address, network, prefix),
                    _ => false,
                };
            }
            if let Ok(pattern) = pattern.parse::<IpAddr>() {
                return address == Some(pattern);
            }
            let pattern = pattern.trim_start_matches('.').trim_end_matches('.');
            !pattern.is_empty()
                && (host == pattern
                    || host
                        .strip_suffix(pattern)
                        .is_some_and(|prefix| prefix.ends_with('.')))
        })
}

/// Splits `entry` into its host pattern and an optional port (`[v6]:port`, `name:port`; a bare
/// IPv6 address has no port).
fn split_port(entry: &str) -> (&str, Option<u16>) {
    if let Some(bracketed) = entry.strip_prefix('[') {
        return match bracketed.split_once(']') {
            Some((host, after)) => (host, after.strip_prefix(':').and_then(|p| p.parse().ok())),
            None => (entry, None),
        };
    }
    match entry.rsplit_once(':') {
        // More than one colon: an IPv6 address (possibly with a prefix length).
        Some((host, _)) if host.contains(':') => (entry, None),
        Some((host, port)) => match port.parse() {
            Ok(port) => (host, Some(port)),
            Err(_) => (entry, None),
        },
        None => (entry, None),
    }
}

/// Whether `address` lies in `network/prefix` (both of the same family).
fn in_range(address: IpAddr, network: IpAddr, prefix: u8) -> bool {
    match (address, network) {
        (IpAddr::V4(address), IpAddr::V4(network)) if prefix <= 32 => {
            let mask = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
            u32::from(address) & mask == u32::from(network) & mask
        }
        (IpAddr::V6(address), IpAddr::V6(network)) if prefix <= 128 => {
            let mask = u128::MAX.checked_shl(128 - u32::from(prefix)).unwrap_or(0);
            u128::from(address) & mask == u128::from(network) & mask
        }
        _ => false,
    }
}

/// The CONNECT request for `host:port`, with the proxy's Basic credentials when `authorize`.
pub(crate) fn connect_request(proxy: &ProxyUrl, host: &str, port: u16, authorize: bool) -> Vec<u8> {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let target = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let mut request = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n");
    if let Some(authorization) = proxy.authorization().filter(|_| authorize) {
        request.push_str("Proxy-Authorization: ");
        request.push_str(&authorization);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.into_bytes()
}

/// A CONNECT response that is not a success.
#[derive(Debug)]
pub(crate) enum Refusal {
    /// 407: the proxy asks for credentials; `basic` when it offers the Basic scheme.
    Challenge { basic: bool },
    /// Any other failure.
    Failed(Error),
}

/// Checks a CONNECT response head (status line and headers, up to the blank line).
pub(crate) fn check_response(head: &[u8], proxy: &ProxyUrl) -> Result<(), Refusal> {
    let text = String::from_utf8_lossy(head);
    let mut lines = text.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let mut parts = status_line.splitn(3, ' ');
    let version = parts.next().unwrap_or_default();
    let status = parts
        .next()
        .and_then(|status| status.parse::<u16>().ok())
        .filter(|_| version.starts_with("HTTP/1."));
    match status {
        Some(status) if (200..300).contains(&status) => Ok(()),
        Some(407) => {
            let basic = lines
                .filter_map(|line| line.split_once(':'))
                .filter(|(name, _)| name.trim().eq_ignore_ascii_case("proxy-authenticate"))
                .flat_map(|(_, value)| value.split(','))
                .any(|challenge| {
                    challenge
                        .trim()
                        .split(' ')
                        .next()
                        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("basic"))
                });
            Err(Refusal::Challenge { basic })
        }
        Some(status) => Err(Refusal::Failed(Error::proxy(
            Some(status),
            format!("the proxy {proxy} refused the tunnel with status {status}"),
        ))),
        None => Err(Refusal::Failed(Error::proxy(
            None,
            format!("the proxy {proxy} answered the tunnel request with an invalid response"),
        ))),
    }
}

/// Opens a CONNECT tunnel to `host:port` through `proxy`. As the .NET client, credentials go out
/// only when the proxy asks for them: after a 407 that offers the Basic scheme the request is
/// sent again, on a new connection, with Basic `Proxy-Authorization`.
pub(crate) async fn tunnel(proxy: &ProxyUrl, host: &str, port: u16) -> Result<TcpStream, Error> {
    log::debug!("Connecting to {host}:{port} through the proxy {proxy}");
    match tunnel_once(proxy, host, port, false).await {
        Ok(stream) => Ok(stream),
        Err(Refusal::Challenge { basic: true }) if proxy.credentials.is_some() => {
            match tunnel_once(proxy, host, port, true).await {
                Ok(stream) => Ok(stream),
                Err(Refusal::Challenge { .. }) => Err(Error::proxy(
                    Some(407),
                    format!(
                    "the proxy {proxy} refused the credentials (407 Proxy Authentication Required)"
                ),
                )),
                Err(Refusal::Failed(error)) => Err(error),
            }
        }
        Err(Refusal::Challenge { basic }) => {
            let reason = if proxy.credentials.is_none() {
                "no credentials are configured"
            } else if !basic {
                "it offers no Basic scheme, the only one this client supports"
            } else {
                "it refused them"
            };
            Err(Error::proxy(
                Some(407),
                format!(
                    "the proxy {proxy} requires authentication (407 Proxy Authentication Required): {reason}"
                ),
            ))
        }
        Err(Refusal::Failed(error)) => Err(error),
    }
}

/// One CONNECT exchange on a new connection to the proxy.
async fn tunnel_once(
    proxy: &ProxyUrl,
    host: &str,
    port: u16,
    authorize: bool,
) -> Result<TcpStream, Refusal> {
    let mut stream = super::negotiate_http::connect_tcp(proxy.host(), proxy.port())
        .await
        .map_err(|error| {
            Refusal::Failed(Error::proxy(
                None,
                format!("cannot connect to the proxy {proxy}: {error}"),
            ))
        })?;
    let failed = |error: std::io::Error| {
        Refusal::Failed(
            Error::proxy(
                None,
                format!("the tunnel through the proxy {proxy} failed: {error}"),
            )
            .with_source(error),
        )
    };
    stream
        .write_all(&connect_request(proxy, host, port, authorize))
        .await
        .map_err(failed)?;
    let head = read_head(&mut stream, proxy).await?;
    check_response(&head, proxy)?;
    Ok(stream)
}

/// Reads a CONNECT response head without reading past it: nothing may follow before the client
/// speaks (TLS, the WebSocket upgrade or HTTP), so a byte past the blank line is an error.
async fn read_head<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
    proxy: &ProxyUrl,
) -> Result<Vec<u8>, Refusal> {
    let mut head = Vec::with_capacity(256);
    let mut chunk = [0_u8; 512];
    loop {
        let read = stream.read(&mut chunk).await.map_err(|error| {
            Refusal::Failed(
                Error::proxy(
                    None,
                    format!("the tunnel through the proxy {proxy} failed: {error}"),
                )
                .with_source(error),
            )
        })?;
        if read == 0 {
            return Err(Refusal::Failed(Error::proxy(
                None,
                format!("the proxy {proxy} closed the connection during the tunnel request"),
            )));
        }
        head.extend_from_slice(chunk.get(..read).unwrap_or_default());
        if let Some(end) = head.windows(4).position(|window| window == b"\r\n\r\n") {
            if end + 4 != head.len() {
                return Err(Refusal::Failed(Error::proxy(
                    None,
                    format!("the proxy {proxy} sent data before the tunnel was used"),
                )));
            }
            head.truncate(end);
            return Ok(head);
        }
        if head.len() > RESPONSE_LIMIT {
            return Err(Refusal::Failed(Error::proxy(
                None,
                format!("the proxy {proxy} sent a tunnel response over {RESPONSE_LIMIT} bytes"),
            )));
        }
    }
}

/// Opens the TCP stream of a connection to `host:port`: directly, or through the proxy `config`
/// resolves for it. TLS for a secure endpoint follows inside the returned stream.
pub(crate) async fn connect(
    config: &ProxyConfig,
    secure: bool,
    host: &str,
    port: u16,
) -> Result<TcpStream, Error> {
    match resolve(config, secure, host, port, |name| std::env::var(name).ok()) {
        Some(proxy) => tunnel(&proxy, host, port).await,
        None => super::negotiate_http::connect_tcp(host, port).await,
    }
}

#[cfg(test)]
mod proxy_tests {
    use std::collections::HashMap;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{
        bypasses, check_response, connect_request, read_head, resolve, tunnel, ProxyConfig,
        ProxyUrl, Refusal, RESPONSE_LIMIT,
    };
    use crate::ErrorKind;

    fn environment(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let values: HashMap<String, String> = pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        move |name| values.get(name).cloned()
    }

    fn system(pairs: &[(&str, &str)], secure: bool, host: &str, port: u16) -> Option<String> {
        resolve(&ProxyConfig::System, secure, host, port, environment(pairs))
            .map(|proxy| format!("{}:{}", proxy.host(), proxy.port()))
    }

    #[test]
    fn proxy_urls_parse_with_and_without_credentials() {
        let plain = ProxyUrl::parse("http://proxy.example:3128").unwrap();
        assert_eq!((plain.host(), plain.port()), ("proxy.example", 3128));
        assert_eq!(plain.authorization(), None);

        let bare = ProxyUrl::parse("proxy.example").unwrap();
        assert_eq!(
            (bare.host(), bare.port()),
            ("proxy.example", 80),
            "http and port 80"
        );

        let credentials = ProxyUrl::parse("http://us%40er:p%3Ass@10.0.0.1:8080/").unwrap();
        assert_eq!((credentials.host(), credentials.port()), ("10.0.0.1", 8080));
        // base64("us@er:p:ss")
        assert_eq!(
            credentials.authorization().as_deref(),
            Some("Basic dXNAZXI6cDpzcw==")
        );

        let v6 = ProxyUrl::parse("HTTP://[::1]:9000").unwrap();
        assert_eq!((v6.host(), v6.port()), ("::1", 9000));
    }

    #[test]
    fn proxy_invalid_urls_fail_without_repeating_them() {
        for url in [
            "https://user:secret@proxy:443",
            "http://user:secret@:8080",
            "http://user:secret@proxy:99999",
            "http://user:secret@proxy:0",
            "http://user:secret@[::1",
            "socks5://user:secret@proxy:1080",
        ] {
            let error = ProxyUrl::parse(url).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Configuration, "{url}");
            assert_eq!(error.setting(), Some("with_proxy"));
            assert!(!error.to_string().contains("secret"), "{error}");
            assert!(!format!("{error:?}").contains("secret"), "{error:?}");
        }
    }

    #[test]
    fn proxy_urls_never_show_their_credentials() {
        let proxy = ProxyUrl::parse("http://user:secret@proxy.example:3128").unwrap();
        assert_eq!(proxy.to_string(), "http://***@proxy.example:3128");
        assert!(!format!("{proxy:?}").contains("secret"));
        assert!(!format!("{:?}", ProxyConfig::Explicit(proxy)).contains("secret"));
        let v6 = ProxyUrl::parse("http://[::1]:9000").unwrap();
        assert_eq!(v6.to_string(), "http://[::1]:9000");
    }

    #[test]
    fn proxy_environment_follows_the_endpoint_scheme() {
        let variables = [
            ("HTTPS_PROXY", "https-proxy:1"),
            ("HTTP_PROXY", "http-proxy:2"),
        ];
        assert_eq!(
            system(&variables, true, "hub", 443).as_deref(),
            Some("https-proxy:1")
        );
        assert_eq!(
            system(&variables, false, "hub", 80).as_deref(),
            Some("http-proxy:2")
        );
        // ALL_PROXY when the scheme's own variables are missing or empty.
        let fallback = [("HTTPS_PROXY", " "), ("ALL_PROXY", "all-proxy:3")];
        assert_eq!(
            system(&fallback, true, "hub", 443).as_deref(),
            Some("all-proxy:3")
        );
        assert_eq!(
            system(&fallback, false, "hub", 80).as_deref(),
            Some("all-proxy:3")
        );
        assert_eq!(system(&[], true, "hub", 443), None);
    }

    #[test]
    fn proxy_environment_skips_empty_and_unusable_names_as_dotnet_does() {
        // Lower case first.
        let both = [("https_proxy", "lower:1"), ("HTTPS_PROXY", "upper:2")];
        assert_eq!(system(&both, true, "hub", 443).as_deref(), Some("lower:1"));
        // An empty or unusable lower-case value falls through to the next name.
        let empty = [("https_proxy", ""), ("HTTPS_PROXY", "upper:2")];
        assert_eq!(system(&empty, true, "hub", 443).as_deref(), Some("upper:2"));
        let unusable = [
            ("https_proxy", "socks5://x:1"),
            ("HTTPS_PROXY", "https://y:2"),
            ("all_proxy", "all:3"),
        ];
        assert_eq!(
            system(&unusable, true, "hub", 443).as_deref(),
            Some("all:3")
        );
        assert_eq!(
            system(&[("HTTPS_PROXY", "socks5://x:1")], true, "hub", 443),
            None
        );
        // no_proxy (else NO_PROXY) applies.
        let upper = [("HTTPS_PROXY", "upper:2"), ("NO_PROXY", "hub")];
        assert_eq!(system(&upper, true, "hub", 443), None);
    }

    #[test]
    fn proxy_cgi_processes_ignore_upper_case_http_proxy() {
        // httpoxy: a CGI request's `Proxy:` header becomes HTTP_PROXY.
        let cgi = [
            ("GATEWAY_INTERFACE", "CGI/1.1"),
            ("HTTP_PROXY", "attacker:1"),
        ];
        assert_eq!(system(&cgi, false, "hub", 80), None);
        let lower = [("GATEWAY_INTERFACE", "CGI/1.1"), ("http_proxy", "chosen:2")];
        assert_eq!(
            system(&lower, false, "hub", 80).as_deref(),
            Some("chosen:2")
        );
        assert_eq!(
            system(&[("HTTP_PROXY", "chosen:3")], false, "hub", 80).as_deref(),
            Some("chosen:3")
        );
    }

    #[test]
    fn proxy_explicit_and_disabled_ignore_the_environment() {
        let variables = environment(&[("HTTPS_PROXY", "env:1"), ("NO_PROXY", "*")]);
        let explicit = ProxyConfig::Explicit(ProxyUrl::parse("http://chosen:8080").unwrap());
        let resolved = resolve(&explicit, true, "hub", 443, &variables).unwrap();
        assert_eq!((resolved.host(), resolved.port()), ("chosen", 8080));
        assert!(resolve(&ProxyConfig::Disabled, true, "hub", 443, &variables).is_none());
    }

    #[test]
    fn proxy_no_proxy_matches_host_names_and_their_subdomains() {
        let list = "example.com, .internal ,LOCALHOST";
        for host in [
            "example.com",
            "api.example.com",
            "Example.COM",
            "a.b.internal",
            "localhost",
        ] {
            assert!(bypasses(list, host, 443), "{host}");
        }
        for host in [
            "notexample.com",
            "example.co",
            "internal.net",
            "localhost.example",
        ] {
            assert!(!bypasses(list, host, 443), "{host}");
        }
    }

    #[test]
    fn proxy_no_proxy_matches_addresses_and_cidr_ranges() {
        let list = "10.0.0.0/8,192.168.1.7,::1,fd00::/8";
        assert!(bypasses(list, "10.20.30.40", 80));
        assert!(bypasses(list, "192.168.1.7", 80));
        assert!(!bypasses(list, "192.168.1.8", 80));
        assert!(!bypasses(list, "11.0.0.1", 80));
        assert!(bypasses(list, "[::1]", 80));
        assert!(bypasses(list, "fd12:3456::1", 80));
        assert!(!bypasses(list, "fe80::1", 80));
        // A host name never matches an address entry.
        assert!(!bypasses(list, "ten.example", 80));
    }

    #[test]
    fn proxy_no_proxy_entries_may_name_a_port() {
        let list = "example.com:8443,[::1]:9000,10.0.0.1:80";
        assert!(bypasses(list, "example.com", 8443));
        assert!(!bypasses(list, "example.com", 443));
        assert!(bypasses(list, "::1", 9000));
        assert!(!bypasses(list, "::1", 9001));
        assert!(bypasses(list, "10.0.0.1", 80));
        assert!(!bypasses(list, "10.0.0.1", 81));
    }

    #[test]
    fn proxy_no_proxy_star_exempts_every_host() {
        assert!(bypasses("*", "anything.example", 1));
        assert!(bypasses(" * ", "10.0.0.1", 2));
        assert!(!bypasses("", "anything.example", 1));
        assert!(!bypasses(" , ", "anything.example", 1));
    }

    #[test]
    fn proxy_connect_requests_name_the_target_and_credentials_only_when_asked() {
        let plain = ProxyUrl::parse("http://proxy:3128").unwrap();
        assert_eq!(
            connect_request(&plain, "hub.example", 443, true),
            b"CONNECT hub.example:443 HTTP/1.1\r\nHost: hub.example:443\r\n\r\n"
        );
        let authenticated = ProxyUrl::parse("http://user:pass@proxy:3128").unwrap();
        assert_eq!(
            connect_request(&authenticated, "[::1]", 5000, false),
            b"CONNECT [::1]:5000 HTTP/1.1\r\nHost: [::1]:5000\r\n\r\n"
        );
        assert_eq!(
            connect_request(&authenticated, "[::1]", 5000, true),
            b"CONNECT [::1]:5000 HTTP/1.1\r\nHost: [::1]:5000\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n\r\n"
        );
    }

    #[test]
    fn proxy_connect_responses_are_checked() {
        let proxy = ProxyUrl::parse("http://user:secret@proxy:3128").unwrap();
        assert!(check_response(b"HTTP/1.1 200 Connection established", &proxy).is_ok());
        assert!(check_response(b"HTTP/1.0 204 OK", &proxy).is_ok());
        assert!(matches!(
            check_response(
                b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Negotiate\r\nproxy-authenticate: basic realm=\"x\"",
                &proxy
            ),
            Err(Refusal::Challenge { basic: true })
        ));
        assert!(matches!(
            check_response(
                b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: NTLM",
                &proxy
            ),
            Err(Refusal::Challenge { basic: false })
        ));
        for (head, status, transient) in [
            (&b"HTTP/1.1 403 Forbidden"[..], Some(403), false),
            (&b"HTTP/1.1 502 Bad Gateway"[..], Some(502), true),
            (&b"SSH-2.0-OpenSSH"[..], None, true),
        ] {
            let Err(Refusal::Failed(error)) = check_response(head, &proxy) else {
                panic!("a failure was expected");
            };
            assert_eq!(error.kind(), ErrorKind::Proxy);
            assert_eq!(error.status(), status);
            assert_eq!(error.is_transient(), transient);
            assert!(!error.to_string().contains("secret"), "{error}");
        }
    }

    #[tokio::test]
    async fn proxy_response_heads_are_bounded_and_never_overread() {
        let proxy = ProxyUrl::parse("http://proxy:3128").unwrap();
        let failure = |refusal: Refusal| match refusal {
            Refusal::Failed(error) => error,
            Refusal::Challenge { .. } => panic!("a failure was expected"),
        };
        // Over the limit without an end.
        let endless = vec![b'x'; RESPONSE_LIMIT + 1024];
        let error = failure(read_head(&mut &endless[..], &proxy).await.unwrap_err());
        assert_eq!(error.kind(), ErrorKind::Proxy);
        assert!(error.to_string().contains("bytes"), "{error}");
        // Bytes after the head.
        let early = b"HTTP/1.1 200 OK\r\n\r\nextra";
        let error = failure(read_head(&mut &early[..], &proxy).await.unwrap_err());
        assert!(error.to_string().contains("sent data"), "{error}");
        // The proxy closes before the head ends.
        let closed = b"HTTP/1.1 200 OK\r\n";
        let error = failure(read_head(&mut &closed[..], &proxy).await.unwrap_err());
        assert!(error.to_string().contains("closed"), "{error}");
        // A complete head.
        let head = read_head(&mut &b"HTTP/1.1 200 OK\r\n\r\n"[..], &proxy)
            .await
            .unwrap();
        assert_eq!(head, b"HTTP/1.1 200 OK");
    }

    /// A proxy on a local port that answers its connections, in order, with `answers` (and
    /// echoes in a tunnel after a 200); reports each request head.
    async fn local_proxy(
        answers: Vec<&'static [u8]>,
    ) -> (u16, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (heads, received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            for answer in answers {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut chunk = [0_u8; 256];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut chunk).await.unwrap();
                    request.extend_from_slice(&chunk[..read]);
                }
                let _ = heads.send(String::from_utf8(request).unwrap());
                stream.write_all(answer).await.unwrap();
                if answer.starts_with(b"HTTP/1.1 200") {
                    let read = stream.read(&mut chunk).await.unwrap_or(0);
                    let _ = stream.write_all(&chunk[..read]).await;
                }
            }
        });
        (port, received)
    }

    const CHALLENGE: &[u8] =
        b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"x\"\r\nContent-Length: 0\r\n\r\n";
    const ESTABLISHED: &[u8] = b"HTTP/1.1 200 Connection established\r\n\r\n";

    #[tokio::test]
    async fn proxy_tunnels_send_credentials_only_after_a_basic_challenge() {
        let (port, mut heads) = local_proxy(vec![CHALLENGE, ESTABLISHED]).await;
        let proxy = ProxyUrl::parse(&format!("http://user:pass@127.0.0.1:{port}")).unwrap();
        let mut stream = tunnel(&proxy, "hub.example", 443).await.unwrap();
        assert_eq!(
            heads.recv().await.unwrap(),
            "CONNECT hub.example:443 HTTP/1.1\r\nHost: hub.example:443\r\n\r\n",
            "no credentials before the challenge"
        );
        assert_eq!(
            heads.recv().await.unwrap(),
            "CONNECT hub.example:443 HTTP/1.1\r\nHost: hub.example:443\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n\r\n"
        );
        stream.write_all(b"ping").await.unwrap();
        let mut echoed = [0_u8; 4];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"ping");
    }

    #[tokio::test]
    async fn proxy_tunnels_without_a_challenge_never_send_credentials() {
        let (port, mut heads) = local_proxy(vec![ESTABLISHED]).await;
        let proxy = ProxyUrl::parse(&format!("http://user:pass@127.0.0.1:{port}")).unwrap();
        tunnel(&proxy, "hub.example", 443).await.unwrap();
        assert!(!heads.recv().await.unwrap().contains("Proxy-Authorization"));
    }

    #[tokio::test]
    async fn proxy_refused_credentials_fail_with_407_and_never_show_them() {
        let (port, _heads) = local_proxy(vec![CHALLENGE, CHALLENGE]).await;
        let proxy = ProxyUrl::parse(&format!("http://user:secret@127.0.0.1:{port}")).unwrap();
        let error = tunnel(&proxy, "hub.example", 443).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Proxy);
        assert_eq!(error.status(), Some(407));
        assert!(
            !error.is_transient(),
            "retrying refused credentials cannot help"
        );
        assert!(!error.to_string().contains("secret"), "{error}");
        assert!(!format!("{error:?}").contains("secret"), "{error:?}");

        // Without credentials, the challenge is the error at once.
        let (port, _heads) = local_proxy(vec![CHALLENGE]).await;
        let anonymous = ProxyUrl::parse(&format!("http://127.0.0.1:{port}")).unwrap();
        let error = tunnel(&anonymous, "hub.example", 443).await.unwrap_err();
        assert_eq!(
            (error.kind(), error.status()),
            (ErrorKind::Proxy, Some(407))
        );
        assert!(error.to_string().contains("no credentials"), "{error}");
    }

    #[tokio::test]
    async fn proxy_an_unreachable_proxy_is_a_transient_proxy_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let proxy = ProxyUrl::parse(&format!("http://user:secret@127.0.0.1:{port}")).unwrap();
        let error = tunnel(&proxy, "hub.example", 443).await.unwrap_err();
        assert_eq!((error.kind(), error.status()), (ErrorKind::Proxy, None));
        assert!(error.is_transient());
        assert!(!error.to_string().contains("secret"), "{error}");
    }

    #[test]
    fn proxy_configuration_defaults_to_the_system_proxy() {
        use crate::ConnectionConfiguration;
        let mut configuration = ConnectionConfiguration::new("localhost".into(), "hub".into());
        assert_eq!(
            configuration.connection_recipe().proxy(),
            &ProxyConfig::System
        );
        configuration.without_proxy();
        assert_eq!(
            configuration.connection_recipe().proxy(),
            &ProxyConfig::Disabled
        );
        configuration.with_proxy("http://proxy:3128");
        assert!(matches!(
            configuration.connection_recipe().proxy(),
            ProxyConfig::Explicit(proxy) if proxy.port() == 3128
        ));
        configuration.with_system_proxy();
        assert_eq!(
            configuration.connection_recipe().proxy(),
            &ProxyConfig::System
        );
    }

    #[test]
    fn proxy_invalid_configuration_fails_new_before_io() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        let error = crate::SignalRClient::new("localhost", "hub", |configuration| {
            configuration.with_proxy("https://user:secret@proxy:443");
        })
        .err()
        .expect("an https proxy URL is refused");
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.setting(), Some("with_proxy"));
        assert!(!error.to_string().contains("secret"));
    }
}
