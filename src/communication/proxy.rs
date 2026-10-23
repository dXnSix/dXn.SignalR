//! Native proxies, as the .NET client's default `HttpClient`/`ClientWebSocket` proxy: every
//! native connection (negotiate, WebSocket, Long Polling, Server-Sent Events) to a proxied
//! endpoint opens a tunnel through the proxy (an HTTP `CONNECT`, or a SOCKS handshake, see
//! `socks`), then runs TLS with the connection's own rustls configuration inside it for secure
//! endpoints, so the proxy never sees the traffic.
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

use std::{
    fmt,
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};

use crate::error::Error;

/// A native connection's stream: TCP, directly or through an HTTP or SOCKS proxy's tunnel, or the
/// TLS session to an HTTPS proxy that carries its tunnel.
pub(crate) enum Stream {
    Tcp(TcpStream),
    #[cfg(dxn_tls)]
    ProxyTls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl fmt::Debug for Stream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Stream::Tcp(_) => "Stream::Tcp",
            #[cfg(dxn_tls)]
            Stream::ProxyTls(_) => "Stream::ProxyTls",
        })
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_read(context, buffer),
            #[cfg(dxn_tls)]
            Stream::ProxyTls(stream) => Pin::new(stream.as_mut()).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_write(context, buffer),
            #[cfg(dxn_tls)]
            Stream::ProxyTls(stream) => Pin::new(stream.as_mut()).poll_write(context, buffer),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_write_vectored(context, buffers),
            #[cfg(dxn_tls)]
            Stream::ProxyTls(stream) => {
                Pin::new(stream.as_mut()).poll_write_vectored(context, buffers)
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Stream::Tcp(stream) => stream.is_write_vectored(),
            #[cfg(dxn_tls)]
            Stream::ProxyTls(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_flush(context),
            #[cfg(dxn_tls)]
            Stream::ProxyTls(stream) => Pin::new(stream.as_mut()).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_shutdown(context),
            #[cfg(dxn_tls)]
            Stream::ProxyTls(stream) => Pin::new(stream.as_mut()).poll_shutdown(context),
        }
    }
}

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
    /// The operating system's settings only (`with_os_proxy`): WinHTTP, CFNetwork, Android's
    /// `ProxySelector`, or the environment variables where they are the system's settings.
    Os,
    /// A proxy auto-config script (`with_proxy_auto_config`).
    #[cfg(dxn_pac)]
    AutoConfig(super::pac::PacConfig),
}

/// Where a connection goes: directly, or through a proxy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    Direct,
    Proxy(ProxyUrl),
}

/// The URL a proxy resolver receives for an endpoint: its scheme, host and port (non-default
/// only), as `http(s)://host[:port]/`.
pub(crate) fn target_url(secure: bool, host: &str, port: u16) -> String {
    let (scheme, default) = if secure { ("https", 443) } else { ("http", 80) };
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    if port == default {
        format!("{scheme}://{host}/")
    } else {
        format!("{scheme}://{host}:{port}/")
    }
}

impl ProxyConfig {
    /// Starts a connect (`start`): an auto-config script is loaded again.
    pub(crate) fn begin_connect(&self) {
        #[cfg(dxn_pac)]
        if let ProxyConfig::AutoConfig(pac) = self {
            pac.begin_connect();
        }
    }
}

/// The kinds of proxy, by URL scheme, as .NET's `HttpEnvironmentProxy` accepts them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProxyScheme {
    /// `http://`: an HTTP `CONNECT` tunnel.
    Http,
    /// `https://`: an HTTP `CONNECT` tunnel inside TLS to the proxy (.NET 8 and later).
    Https,
    /// `socks4://`: the destination resolved locally to IPv4.
    Socks4,
    /// `socks4a://`: the destination resolved by the proxy.
    Socks4a,
    /// `socks5://`: domain names resolved by the proxy, as .NET.
    Socks5,
    /// `socks5h://`: the same as `socks5://`.
    Socks5h,
}

impl ProxyScheme {
    fn parse(scheme: &str) -> Option<Self> {
        Some(match scheme.to_ascii_lowercase().as_str() {
            "http" => ProxyScheme::Http,
            "https" => ProxyScheme::Https,
            "socks4" => ProxyScheme::Socks4,
            "socks4a" => ProxyScheme::Socks4a,
            "socks5" => ProxyScheme::Socks5,
            "socks5h" => ProxyScheme::Socks5h,
            _ => return None,
        })
    }

    fn name(self) -> &'static str {
        match self {
            ProxyScheme::Http => "http",
            ProxyScheme::Https => "https",
            ProxyScheme::Socks4 => "socks4",
            ProxyScheme::Socks4a => "socks4a",
            ProxyScheme::Socks5 => "socks5",
            ProxyScheme::Socks5h => "socks5h",
        }
    }

    fn default_port(self) -> u16 {
        match self {
            ProxyScheme::Http => 80,
            ProxyScheme::Https => 443,
            _ => 1080,
        }
    }
}

/// A `scheme://[user:password@]host[:port]` proxy (`http`, `socks4`, `socks4a`, `socks5`,
/// `socks5h`). Its credentials are never shown.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ProxyUrl {
    scheme: ProxyScheme,
    host: String,
    port: u16,
    credentials: Option<(String, String)>,
}

impl ProxyUrl {
    /// Parses a proxy URL; a missing scheme means `http`, a missing port 80 (1080 for SOCKS). The
    /// error never repeats the URL, which can carry credentials.
    pub(crate) fn parse(url: &str) -> Result<Self, Error> {
        let invalid = |reason: &str| {
            Error::configuration("with_proxy", format!("the proxy URL is invalid: {reason}"))
        };
        let url = url.trim();
        let (scheme, rest) = match url.split_once("://") {
            Some((scheme, rest)) => (
                ProxyScheme::parse(scheme).ok_or_else(|| {
                    invalid("the scheme is not http, https, socks4, socks4a, socks5 or socks5h")
                })?,
                rest,
            ),
            None => (ProxyScheme::Http, url),
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
            None => scheme.default_port(),
        };
        let credentials = match userinfo {
            Some(userinfo) => {
                let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
                Some((percent_decode(user), percent_decode(password)))
            }
            None => None,
        };
        Ok(Self {
            scheme,
            host: host.to_string(),
            port,
            credentials,
        })
    }

    pub(crate) fn scheme(&self) -> ProxyScheme {
        self.scheme
    }

    /// The URL's user name and password, if any.
    pub(crate) fn credentials(&self) -> Option<(&str, &str)> {
        self.credentials
            .as_ref()
            .map(|(user, password)| (user.as_str(), password.as_str()))
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
        let scheme = self.scheme.name();
        if self.host.contains(':') {
            write!(
                formatter,
                "{scheme}://{credentials}[{}]:{}",
                self.host, self.port
            )
        } else {
            write!(
                formatter,
                "{scheme}://{credentials}{}:{}",
                self.host, self.port
            )
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

/// Whether the environment names a proxy (`http_proxy`, `https_proxy` or `all_proxy`, in either
/// case, usable), so the system proxy is the environment's, as .NET's `HttpEnvironmentProxy`
/// takes precedence over the operating system's settings.
pub(crate) fn environment_configured(environment: impl Fn(&str) -> Option<String>) -> bool {
    let gateway = environment("GATEWAY_INTERFACE").is_some();
    [
        "http_proxy",
        "HTTP_PROXY",
        "https_proxy",
        "HTTPS_PROXY",
        "all_proxy",
        "ALL_PROXY",
    ]
    .into_iter()
    .filter(|name| !(gateway && *name == "HTTP_PROXY"))
    .filter_map(&environment)
    .any(|value| !value.trim().is_empty() && ProxyUrl::parse(&value).is_ok())
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
        // Routes come from the script or the system (`routes`).
        #[cfg(dxn_pac)]
        ProxyConfig::AutoConfig(_) => None,
        ProxyConfig::Os => None,
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
                        log::warn!("{name} is not a proxy URL this client can use; it is skipped");
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

/// Opens a CONNECT tunnel to `host:port` through `proxy`, inside TLS to the proxy for an
/// `https://` proxy (with the connection's TLS configuration `tls`). As the .NET client,
/// credentials go out only when the proxy asks for them: after a 407 that offers the Basic scheme
/// the request is sent again, on a new connection, with Basic `Proxy-Authorization`.
pub(crate) async fn tunnel(
    proxy: &ProxyUrl,
    tls: &super::tls::TlsContext,
    host: &str,
    port: u16,
    first: Option<TcpStream>,
) -> Result<Stream, Error> {
    log::debug!("Connecting to {host}:{port} through the proxy {proxy}");
    match tunnel_once(proxy, tls, host, port, false, first).await {
        Ok(stream) => Ok(stream),
        Err(Refusal::Challenge { basic: true }) if proxy.credentials.is_some() => {
            match tunnel_once(proxy, tls, host, port, true, None).await {
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
    tls: &super::tls::TlsContext,
    host: &str,
    port: u16,
    authorize: bool,
    connection: Option<TcpStream>,
) -> Result<Stream, Refusal> {
    let tcp = match connection {
        Some(tcp) => tcp,
        None => connect_proxy(proxy).await.map_err(Refusal::Failed)?,
    };
    let mut stream = if proxy.scheme() == ProxyScheme::Https {
        proxy_tls(proxy, tls, tcp).await.map_err(Refusal::Failed)?
    } else {
        Stream::Tcp(tcp)
    };
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
/// resolves for it; an auto-config script's routes are tried in order, and the last failure is
/// reported. TLS for a secure endpoint follows inside the returned stream (`tls` also loads an
/// `https` auto-config script).
pub(crate) async fn connect(
    config: &ProxyConfig,
    tls: &super::tls::TlsContext,
    secure: bool,
    host: &str,
    port: u16,
) -> Result<Stream, Error> {
    // As .NET, the next route is tried only when a route cannot be reached; a proxy that was
    // reached decides, so its refusal (a 407, a denied target) is the connection's error.
    let mut failure = None;
    for route in routes(config, tls, secure, host, port).await? {
        match &route {
            Route::Direct => match super::negotiate_http::connect_tcp(host, port).await {
                Ok(stream) => return Ok(Stream::Tcp(stream)),
                Err(error) => failure = Some(error),
            },
            Route::Proxy(proxy) => match connect_proxy(proxy).await {
                Ok(tcp) => return through(proxy, tls, host, port, tcp).await,
                Err(error) => failure = Some(error),
            },
        }
        log::debug!("The proxy route {route:?} cannot be reached; trying the next one");
    }
    Err(failure.unwrap_or_else(|| Error::proxy(None, "no proxy route to try")))
}

/// A TCP connection to `proxy`, or the proxy error that it cannot be reached.
async fn connect_proxy(proxy: &ProxyUrl) -> Result<TcpStream, Error> {
    super::negotiate_http::connect_tcp(proxy.host(), proxy.port())
        .await
        .map_err(|error| {
            Error::proxy(
                None,
                format!("cannot connect to the proxy {proxy}: {error}"),
            )
        })
}

/// The routes a connection to `host:port` tries, in order, under `config`: the system proxy is
/// the environment's when it names one, else the operating system's (as .NET's default proxy).
pub(crate) async fn routes(
    config: &ProxyConfig,
    tls: &super::tls::TlsContext,
    secure: bool,
    host: &str,
    port: u16,
) -> Result<Vec<Route>, Error> {
    #[cfg(not(dxn_pac))]
    let _ = tls;
    let environment = |name: &str| std::env::var(name).ok();
    let from_environment = || {
        vec![
            match resolve(&ProxyConfig::System, secure, host, port, environment) {
                Some(proxy) => Route::Proxy(proxy),
                None => Route::Direct,
            },
        ]
    };
    match config {
        #[cfg(dxn_pac)]
        ProxyConfig::AutoConfig(pac) => pac.routes(tls, secure, host, port).await,
        ProxyConfig::System if environment_configured(environment) => Ok(from_environment()),
        ProxyConfig::System | ProxyConfig::Os => match super::os_proxy::platform() {
            Some(reader) => Ok(super::os_proxy::routes(reader, secure, host, port).await),
            None => Ok(from_environment()),
        },
        ProxyConfig::Disabled | ProxyConfig::Explicit(_) => {
            Ok(vec![
                match resolve(config, secure, host, port, environment) {
                    Some(proxy) => Route::Proxy(proxy),
                    None => Route::Direct,
                },
            ])
        }
    }
}

/// A tunnel to `host:port` through `proxy`.
/// A tunnel to `host:port` through `proxy`, starting on `tcp`, a connection to it.
async fn through(
    proxy: &ProxyUrl,
    tls: &super::tls::TlsContext,
    host: &str,
    port: u16,
    tcp: TcpStream,
) -> Result<Stream, Error> {
    match proxy.scheme() {
        ProxyScheme::Http | ProxyScheme::Https => tunnel(proxy, tls, host, port, Some(tcp)).await,
        _ => super::socks::tunnel_on(tcp, proxy, host, port)
            .await
            .map(Stream::Tcp),
    }
}

/// TLS to an `https://` proxy with the connection's TLS configuration (its trust roots and
/// provider, never an accept-all verifier). A failed handshake, such as a proxy certificate that
/// does not verify, is a proxy error naming the proxy.
async fn proxy_tls(
    proxy: &ProxyUrl,
    tls: &super::tls::TlsContext,
    tcp: TcpStream,
) -> Result<Stream, Error> {
    #[cfg(dxn_tls)]
    {
        // The connection's configuration without ALPN: the proxy speaks HTTP/1.1 `CONNECT`
        // whatever the endpoint negotiates, as .NET clears the application protocols for it.
        let mut config = (*tls.config()?).clone();
        config.alpn_protocols.clear();
        let name =
            rustls_pki_types::ServerName::try_from(proxy.host().to_string()).map_err(|error| {
                Error::configuration(
                    "with_proxy",
                    format!("the proxy host is not a valid TLS server name: {error}"),
                )
            })?;
        tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
            .connect(name, tcp)
            .await
            .map(|stream| Stream::ProxyTls(Box::new(stream)))
            .map_err(|error| {
                Error::proxy(
                    None,
                    format!("the TLS connection to the proxy {proxy} failed: {error}"),
                )
                .with_source(error)
            })
    }
    #[cfg(not(dxn_tls))]
    {
        let _ = (proxy, tls, tcp);
        Err(super::tls::tls_disabled())
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
    fn socks_urls_parse_with_their_schemes_and_default_port() {
        for (url, scheme) in [
            ("socks4://proxy", super::ProxyScheme::Socks4),
            ("SOCKS4A://proxy", super::ProxyScheme::Socks4a),
            ("socks5://proxy", super::ProxyScheme::Socks5),
            ("socks5h://proxy", super::ProxyScheme::Socks5h),
        ] {
            let proxy = ProxyUrl::parse(url).unwrap();
            assert_eq!((proxy.scheme(), proxy.port()), (scheme, 1080), "{url}");
        }
        let proxy = ProxyUrl::parse("socks5h://us%3Aer:s3cret@[::1]:9050").unwrap();
        assert_eq!(proxy.credentials(), Some(("us:er", "s3cret")));
        assert_eq!(proxy.to_string(), "socks5h://***@[::1]:9050");
        assert!(!format!("{proxy:?}").contains("s3cret"));
        // The environment accepts SOCKS proxies too, as .NET's `HttpEnvironmentProxy`.
        assert_eq!(
            resolve(
                &ProxyConfig::System,
                true,
                "hub",
                443,
                environment(&[("all_proxy", "socks5://socks:1080")]),
            )
            .map(|proxy| proxy.to_string())
            .as_deref(),
            Some("socks5://socks:1080")
        );
    }

    #[test]
    fn proxy_invalid_urls_fail_without_repeating_them() {
        for url in [
            "gopher://user:secret@proxy:443",
            "http://user:secret@:8080",
            "http://user:secret@proxy:99999",
            "http://user:secret@proxy:0",
            "http://user:secret@[::1",
            "ftp://user:secret@proxy:21",
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
            ("https_proxy", "ftp://x:1"),
            ("HTTPS_PROXY", "gopher://y:2"),
            ("all_proxy", "all:3"),
        ];
        assert_eq!(
            system(&unusable, true, "hub", 443).as_deref(),
            Some("all:3")
        );
        assert_eq!(
            system(&[("HTTPS_PROXY", "ftp://x:1")], true, "hub", 443),
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
        let mut stream = tunnel(
            &proxy,
            &crate::communication::tls::TlsContext::default(),
            "hub.example",
            443,
            None,
        )
        .await
        .unwrap();
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
        tunnel(
            &proxy,
            &crate::communication::tls::TlsContext::default(),
            "hub.example",
            443,
            None,
        )
        .await
        .unwrap();
        assert!(!heads.recv().await.unwrap().contains("Proxy-Authorization"));
    }

    #[tokio::test]
    async fn proxy_refused_credentials_fail_with_407_and_never_show_them() {
        let (port, _heads) = local_proxy(vec![CHALLENGE, CHALLENGE]).await;
        let proxy = ProxyUrl::parse(&format!("http://user:secret@127.0.0.1:{port}")).unwrap();
        let error = tunnel(
            &proxy,
            &crate::communication::tls::TlsContext::default(),
            "hub.example",
            443,
            None,
        )
        .await
        .unwrap_err();
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
        let error = tunnel(
            &anonymous,
            &crate::communication::tls::TlsContext::default(),
            "hub.example",
            443,
            None,
        )
        .await
        .unwrap_err();
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
        let error = tunnel(
            &proxy,
            &crate::communication::tls::TlsContext::default(),
            "hub.example",
            443,
            None,
        )
        .await
        .unwrap_err();
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
            configuration.with_proxy("ftp://user:secret@proxy:21");
        })
        .err()
        .expect("an ftp proxy URL is refused");
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.setting(), Some("with_proxy"));
        assert!(!error.to_string().contains("secret"));
    }
}

#[cfg(all(test, dxn_tls))]
mod https_proxy_tests {
    use std::sync::Arc;

    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
    use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{resolve, tunnel, ProxyConfig, ProxyScheme, ProxyUrl};
    use crate::{
        communication::tls::{crypto_provider, CompiledTls, TlsContext, TlsSettings},
        ErrorKind,
    };

    /// A test CA and a `localhost` server certificate it issued: (CA PEM, certificate PEM, key PEM).
    fn certificates() -> (String, String, String) {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "https_proxy_ test CA");
        let key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap().pem();
        let issuer = Issuer::new(params, key);
        let server_key = KeyPair::generate().unwrap();
        let server = CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .signed_by(&server_key, &issuer)
            .unwrap();
        (ca, server.pem(), server_key.serialize_pem())
    }

    fn trusting(ca: Option<&str>) -> TlsContext {
        TlsContext::for_connection(
            true,
            &TlsSettings {
                extra_roots_pem: ca.map(|ca| ca.as_bytes().to_vec()),
                ..TlsSettings::default()
            },
        )
        .unwrap()
    }

    /// A context trusting `ca` whose hook offers HTTP/2 through ALPN, as a later endpoint might.
    fn offering_h2(ca: &str) -> TlsContext {
        TlsContext::for_connection(
            true,
            &TlsSettings {
                extra_roots_pem: Some(ca.as_bytes().to_vec()),
                config_hook: Some(crate::communication::tls::TlsConfigHook(Arc::new(
                    |config: &mut rustls::ClientConfig| {
                        config.alpn_protocols = vec![b"h2".to_vec()]
                    },
                ))),
                ..TlsSettings::default()
            },
        )
        .unwrap()
    }

    /// Reads a request head (up to the blank line).
    async fn head<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> String {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8; 1];
            if stream.read_exact(&mut byte).await.is_err() {
                break;
            }
            head.push(byte[0]);
        }
        String::from_utf8(head).unwrap()
    }

    /// A TLS proxy on loopback: each connection's CONNECT gets 407 (Basic) without credentials and
    /// 200 with them; after a 200 it echoes what the tunnel carries. Returns the port and the
    /// heads it saw.
    async fn tls_proxy(
        certificate: String,
        key: String,
    ) -> (u16, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let provider = crypto_provider(CompiledTls::CURRENT.provider().unwrap());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                CertificateDer::pem_slice_iter(certificate.as_bytes())
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
            )
            .unwrap();
        // A proxy that would accept HTTP/2: the client must offer no ALPN protocol to it.
        let mut config = config;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (seen, heads) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let (acceptor, seen) = (acceptor.clone(), seen.clone());
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let alpn = tls.get_ref().1.alpn_protocol().map(<[u8]>::to_vec);
                    let request = format!("{}alpn: {alpn:?}\r\n", head(&mut tls).await);
                    let authorized = request.contains("Proxy-Authorization: Basic ");
                    let _ = seen.send(request);
                    if !authorized {
                        let _ = tls
                            .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"x\"\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        let _ = tls.shutdown().await;
                        return;
                    }
                    tls.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                        .await
                        .unwrap();
                    let mut buffer = [0u8; 5];
                    if tls.read_exact(&mut buffer).await.is_ok() {
                        let _ = tls.write_all(&buffer).await;
                    }
                });
            }
        });
        (port, heads)
    }

    #[test]
    fn https_proxy_urls_parse_with_their_default_port_and_hide_credentials() {
        let proxy = ProxyUrl::parse("https://user:s3cret@proxy.example").unwrap();
        assert_eq!(proxy.scheme(), ProxyScheme::Https);
        assert_eq!((proxy.host(), proxy.port()), ("proxy.example", 443));
        assert_eq!(proxy.to_string(), "https://***@proxy.example:443");
        assert!(!format!("{proxy:?}").contains("s3cret"));
        let explicit = ProxyUrl::parse("HTTPS://[::1]:8443").unwrap();
        assert_eq!((explicit.host(), explicit.port()), ("::1", 8443));
    }

    #[test]
    fn https_proxy_environment_variables_accept_https_proxies() {
        let environment =
            |name: &str| (name == "HTTPS_PROXY").then(|| "https://proxy:8443".to_string());
        let proxy = resolve(&ProxyConfig::System, true, "hub", 443, environment).unwrap();
        assert_eq!(proxy.to_string(), "https://proxy:8443");
        assert!(super::environment_configured(environment));
    }

    #[tokio::test]
    async fn https_proxy_tunnels_inside_tls_with_the_basic_challenge() {
        let (ca, certificate, key) = certificates();
        let (port, mut heads) = tls_proxy(certificate, key).await;
        let proxy = ProxyUrl::parse(&format!("https://user:pass@localhost:{port}")).unwrap();
        let mut stream = tunnel(&proxy, &offering_h2(&ca), "hub.example", 443, None)
            .await
            .unwrap();
        // The challenge, then the authorized CONNECT, each inside its own TLS session.
        let first = heads.recv().await.unwrap();
        assert!(
            first.starts_with("CONNECT hub.example:443 HTTP/1.1\r\n"),
            "{first}"
        );
        assert!(!first.contains("Proxy-Authorization"), "{first}");
        let second = heads.recv().await.unwrap();
        // base64("user:pass")
        assert!(
            second.contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"),
            "{second}"
        );
        // The connection's ALPN list (here `h2`, from a hook) is not offered to the proxy.
        assert!(second.ends_with("alpn: None\r\n"), "{second}");
        // The tunnel carries the endpoint's bytes.
        stream.write_all(b"hello").await.unwrap();
        let mut echoed = [0u8; 5];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"hello");
    }

    #[tokio::test]
    async fn https_proxy_certificates_must_verify() {
        let (_, certificate, key) = certificates();
        let (port, mut heads) = tls_proxy(certificate, key).await;
        let proxy = ProxyUrl::parse(&format!("https://user:s3cret@localhost:{port}")).unwrap();
        // The CA is not trusted: the handshake fails and no CONNECT is sent.
        let error = match tunnel(&proxy, &trusting(None), "hub.example", 443, None).await {
            Ok(_) => panic!("an unverified proxy certificate was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");
        assert!(
            error.to_string().contains(&format!("localhost:{port}")),
            "{error}"
        );
        assert!(!error.to_string().contains("s3cret"), "{error}");
        assert!(!format!("{error:?}").contains("s3cret"), "{error:?}");
        assert!(heads.try_recv().is_err(), "nothing was sent to the proxy");
    }

    #[tokio::test]
    async fn https_proxy_refusals_keep_the_http_proxy_statuses() {
        let (ca, certificate, key) = certificates();
        let (port, _heads) = tls_proxy(certificate, key).await;
        // No credentials: the 407 is reported, as for http:// proxies.
        let proxy = ProxyUrl::parse(&format!("https://localhost:{port}")).unwrap();
        let error = match tunnel(&proxy, &trusting(Some(&ca)), "hub.example", 443, None).await {
            Ok(_) => panic!("the proxy requires credentials"),
            Err(error) => error,
        };
        assert_eq!(
            (error.kind(), error.status()),
            (ErrorKind::Proxy, Some(407))
        );
        assert!(!error.is_transient());
    }
}
