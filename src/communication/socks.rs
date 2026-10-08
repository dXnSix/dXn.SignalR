//! SOCKS proxies, as the .NET client's `SocksHelper`: SOCKS5 (`socks5://` and `socks5h://`, RFC
//! 1928) with username/password authentication (RFC 1929), and SOCKS4 (`socks4://`, the
//! destination resolved locally to IPv4) and SOCKS4a (`socks4a://`, resolved by the proxy). As in
//! .NET, SOCKS5 sends a destination's domain name for the proxy to resolve and IP literals as IPv4
//! or IPv6 addresses. TLS for a secure endpoint runs inside the tunnel. Credentials never appear in
//! an error.

// Proxy code must not panic on peer input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::net::{IpAddr, Ipv4Addr};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

use super::proxy::{ProxyScheme, ProxyUrl};
use crate::error::Error;

const VERSION5: u8 = 5;
const NO_AUTHENTICATION: u8 = 0;
const USERNAME_PASSWORD: u8 = 2;
const NO_ACCEPTABLE_METHOD: u8 = 0xff;
const SUBNEGOTIATION_VERSION: u8 = 1;
const CONNECT: u8 = 1;
const ATYP_IPV4: u8 = 1;
const ATYP_DOMAIN: u8 = 3;
const ATYP_IPV6: u8 = 4;
const VERSION4: u8 = 4;
const SOCKS4_GRANTED: u8 = 0x5a;

/// Opens a tunnel to `host:port` through the SOCKS `proxy` on `stream`, a connection to it.
pub(crate) async fn tunnel_on(
    mut stream: TcpStream,
    proxy: &ProxyUrl,
    host: &str,
    port: u16,
) -> Result<TcpStream, Error> {
    log::debug!("Connecting to {host}:{port} through the SOCKS proxy {proxy}");
    handshake(&mut stream, proxy, host, port).await?;
    Ok(stream)
}

/// The SOCKS handshake for `host:port` on `stream`, a connection to `proxy`.
pub(crate) async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    proxy: &ProxyUrl,
    host: &str,
    port: u16,
) -> Result<(), Error> {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match proxy.scheme() {
        ProxyScheme::Socks4 | ProxyScheme::Socks4a => socks4(stream, proxy, host, port).await,
        _ => socks5(stream, proxy, host, port).await,
    }
}

fn failure(proxy: &ProxyUrl, message: impl std::fmt::Display) -> Error {
    Error::proxy(None, format!("the SOCKS proxy {proxy} {message}"))
}

fn io_failure(proxy: &ProxyUrl, error: std::io::Error) -> Error {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        failure(proxy, "closed the connection before its reply was complete")
    } else {
        Error::proxy(
            None,
            format!("the tunnel through the SOCKS proxy {proxy} failed: {error}"),
        )
        .with_source(error)
    }
}

/// The SOCKS5 greeting: no authentication, and username/password when credentials are set.
pub(crate) fn greeting(with_credentials: bool) -> Vec<u8> {
    if with_credentials {
        vec![VERSION5, 2, NO_AUTHENTICATION, USERNAME_PASSWORD]
    } else {
        vec![VERSION5, 1, NO_AUTHENTICATION]
    }
}

/// A length-prefixed SOCKS string of at most 255 UTF-8 bytes.
fn short_string(text: &str, what: &str, proxy: &ProxyUrl) -> Result<Vec<u8>, Error> {
    let length = u8::try_from(text.len())
        .map_err(|_| failure(proxy, format!("cannot be sent a {what} over 255 bytes")))?;
    let mut bytes = Vec::with_capacity(text.len() + 1);
    bytes.push(length);
    bytes.extend_from_slice(text.as_bytes());
    Ok(bytes)
}

/// The RFC 1929 username/password request.
pub(crate) fn authentication(
    user: &str,
    password: &str,
    proxy: &ProxyUrl,
) -> Result<Vec<u8>, Error> {
    let mut request = vec![SUBNEGOTIATION_VERSION];
    request.extend(short_string(user, "user name", proxy)?);
    request.extend(short_string(password, "password", proxy)?);
    Ok(request)
}

/// The SOCKS5 CONNECT request: an IP literal as IPv4 or IPv6, anything else as a domain name.
pub(crate) fn connect_request(host: &str, port: u16, proxy: &ProxyUrl) -> Result<Vec<u8>, Error> {
    let mut request = vec![VERSION5, CONNECT, 0];
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(address)) => {
            request.push(ATYP_IPV4);
            request.extend_from_slice(&address.octets());
        }
        Ok(IpAddr::V6(address)) => {
            request.push(ATYP_IPV6);
            request.extend_from_slice(&address.octets());
        }
        Err(_) => {
            request.push(ATYP_DOMAIN);
            request.extend(short_string(host, "host name", proxy)?);
        }
    }
    request.extend_from_slice(&port.to_be_bytes());
    Ok(request)
}

/// What a SOCKS5 reply code means (RFC 1928 6).
pub(crate) fn reply_meaning(code: u8) -> &'static str {
    match code {
        1 => "general SOCKS server failure",
        2 => "connection not allowed by ruleset",
        3 => "network unreachable",
        4 => "host unreachable",
        5 => "connection refused",
        6 => "TTL expired",
        7 => "command not supported",
        8 => "address type not supported",
        _ => "unassigned reply code",
    }
}

async fn socks5<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    proxy: &ProxyUrl,
    host: &str,
    port: u16,
) -> Result<(), Error> {
    let credentials = proxy.credentials();
    let io = |error| io_failure(proxy, error);
    stream
        .write_all(&greeting(credentials.is_some()))
        .await
        .map_err(io)?;
    let mut choice = [0u8; 2];
    stream.read_exact(&mut choice).await.map_err(io)?;
    let [version, method] = choice;
    if version != VERSION5 {
        return Err(failure(proxy, "did not answer as a SOCKS5 proxy"));
    }
    match (method, credentials) {
        (NO_AUTHENTICATION, _) => {}
        (USERNAME_PASSWORD, Some((user, password))) => {
            stream
                .write_all(&authentication(user, password, proxy)?)
                .await
                .map_err(io)?;
            let mut status = [0u8; 2];
            stream.read_exact(&mut status).await.map_err(io)?;
            if status != [SUBNEGOTIATION_VERSION, 0] {
                return Err(Error::proxy(
                    Some(407),
                    format!("the SOCKS proxy {proxy} refused the credentials"),
                ));
            }
        }
        (USERNAME_PASSWORD, None) | (NO_ACCEPTABLE_METHOD, None) => {
            return Err(Error::proxy(
                Some(407),
                format!("the SOCKS proxy {proxy} requires authentication: no credentials are configured"),
            ));
        }
        (NO_ACCEPTABLE_METHOD, Some(_)) => {
            return Err(Error::proxy(
                Some(407),
                format!(
                    "the SOCKS proxy {proxy} accepts none of the offered authentication methods"
                ),
            ));
        }
        (other, _) => {
            return Err(failure(
                proxy,
                format!("chose the authentication method {other:#04x}, which was not offered"),
            ));
        }
    }
    stream
        .write_all(&connect_request(host, port, proxy)?)
        .await
        .map_err(io)?;
    let mut reply = [0u8; 5];
    stream.read_exact(&mut reply).await.map_err(io)?;
    let [version, code, _, address_type, first] = reply;
    if version != VERSION5 {
        return Err(failure(proxy, "did not answer as a SOCKS5 proxy"));
    }
    if code != 0 {
        // As HTTP's 403, a destination the ruleset forbids is not worth retrying.
        let status = (code == 2).then_some(403);
        return Err(Error::proxy(
            status,
            format!(
                "the SOCKS proxy {proxy} could not connect to {host}:{port}: reply {code:#04x} ({})",
                reply_meaning(code)
            ),
        ));
    }
    // The rest of the bound address (one byte of it was read) and its port.
    let remaining = match address_type {
        ATYP_IPV4 => 3 + 2,
        ATYP_IPV6 => 15 + 2,
        ATYP_DOMAIN => usize::from(first) + 2,
        _ => return Err(failure(proxy, "answered with an unknown address type")),
    };
    let mut bound = vec![0u8; remaining];
    stream.read_exact(&mut bound).await.map_err(io)?;
    Ok(())
}

/// The SOCKS4 CONNECT request; SOCKS4a passes the domain name as `0.0.0.1` plus the name.
pub(crate) fn socks4_request(
    address: Option<Ipv4Addr>,
    host: &str,
    port: u16,
    user: &str,
) -> Vec<u8> {
    let mut request = vec![VERSION4, CONNECT];
    request.extend_from_slice(&port.to_be_bytes());
    request.extend_from_slice(&address.unwrap_or(Ipv4Addr::new(0, 0, 0, 1)).octets());
    request.extend_from_slice(user.as_bytes());
    request.push(0);
    if address.is_none() {
        request.extend_from_slice(host.as_bytes());
        request.push(0);
    }
    request
}

async fn socks4<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    proxy: &ProxyUrl,
    host: &str,
    port: u16,
) -> Result<(), Error> {
    let io = |error| io_failure(proxy, error);
    let address = match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(address)) => Some(address),
        // An IPv4-mapped IPv6 address is its IPv4 address, as .NET maps it.
        Ok(IpAddr::V6(address)) if address.to_ipv4_mapped().is_some() => address.to_ipv4_mapped(),
        Ok(IpAddr::V6(_)) => {
            return Err(failure(
                proxy,
                "cannot reach an IPv6 address: SOCKS4 is IPv4 only",
            ));
        }
        Err(_) if proxy.scheme() == ProxyScheme::Socks4a => None,
        // SOCKS4 needs the destination's IPv4 address, resolved here.
        Err(_) => Some(
            tokio::net::lookup_host((host, port))
                .await
                .map_err(|error| {
                    failure(
                        proxy,
                        format!("cannot be used: {host} has no IPv4 address ({error})"),
                    )
                })?
                .find_map(|address| match address.ip() {
                    IpAddr::V4(address) => Some(address),
                    IpAddr::V6(_) => None,
                })
                .ok_or_else(|| {
                    failure(proxy, format!("cannot be used: {host} has no IPv4 address"))
                })?,
        ),
    };
    if host.len() > 255 {
        return Err(failure(proxy, "cannot be sent a host name over 255 bytes"));
    }
    let user = proxy.credentials().map_or("", |(user, _)| user);
    if user.len() > 255 {
        return Err(failure(proxy, "cannot be sent a user name over 255 bytes"));
    }
    stream
        .write_all(&socks4_request(address, host, port, user))
        .await
        .map_err(io)?;
    let mut reply = [0u8; 8];
    stream.read_exact(&mut reply).await.map_err(io)?;
    let [_, code, ..] = reply;
    if code != SOCKS4_GRANTED {
        let reason = match code {
            0x5b => "rejected or failed",
            0x5c => "the proxy cannot reach the client's identd",
            0x5d => "identd could not confirm the user id",
            _ => "unknown reply code",
        };
        return Err(Error::proxy(
            None,
            format!("the SOCKS proxy {proxy} could not connect to {host}:{port}: reply {code:#04x} ({reason})"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod socks_tests {
    use std::net::Ipv4Addr;

    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt, DuplexStream};

    use super::{
        authentication, connect_request, greeting, handshake, reply_meaning, socks4_request,
    };
    use crate::{communication::proxy::ProxyUrl, ErrorKind};

    fn proxy(url: &str) -> ProxyUrl {
        ProxyUrl::parse(url).unwrap()
    }

    /// Runs the client handshake against a scripted server: `server` reads what it expects and
    /// answers; returns the client's result.
    async fn exchange<F, Fut>(
        url: &str,
        host: &str,
        port: u16,
        server: F,
    ) -> Result<(), crate::Error>
    where
        F: FnOnce(DuplexStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let (mut client, server_side) = duplex(4096);
        let script = tokio::spawn(server(server_side));
        let result = handshake(&mut client, &proxy(url), host, port).await;
        drop(client);
        script.await.unwrap();
        result
    }

    async fn expect(stream: &mut DuplexStream, bytes: &[u8]) {
        let mut read = vec![0u8; bytes.len()];
        stream.read_exact(&mut read).await.unwrap();
        assert_eq!(read, bytes);
    }

    #[test]
    fn socks_greeting_offers_username_password_only_with_credentials() {
        assert_eq!(greeting(false), [5, 1, 0]);
        assert_eq!(greeting(true), [5, 2, 0, 2]);
    }

    #[test]
    fn socks_requests_encode_ipv4_ipv6_and_domain_names() {
        let proxy = proxy("socks5://proxy:1080");
        assert_eq!(
            connect_request("10.1.2.3", 443, &proxy).unwrap(),
            [5, 1, 0, 1, 10, 1, 2, 3, 0x01, 0xbb]
        );
        let mut v6 = vec![5, 1, 0, 4];
        v6.extend(std::net::Ipv6Addr::LOCALHOST.octets());
        v6.extend([0, 80]);
        assert_eq!(connect_request("::1", 80, &proxy).unwrap(), v6);
        let mut domain = vec![5, 1, 0, 3, 11];
        domain.extend(b"example.com");
        domain.extend([0x1f, 0x90]);
        assert_eq!(
            connect_request("example.com", 8080, &proxy).unwrap(),
            domain,
            "socks5 sends the name for the proxy to resolve, as .NET"
        );
        let long = "a".repeat(256);
        let error = connect_request(&long, 80, &proxy).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Proxy);
    }

    #[test]
    fn socks_authentication_follows_rfc_1929() {
        let proxy = proxy("socks5://u:p@proxy");
        assert_eq!(
            authentication("user", "pw", &proxy).unwrap(),
            [1, 4, b'u', b's', b'e', b'r', 2, b'p', b'w']
        );
        let error = authentication("user", &"x".repeat(256), &proxy).unwrap_err();
        assert!(!error.to_string().contains("xxxx"), "{error}");
    }

    #[test]
    fn socks4_requests_carry_the_address_or_the_4a_domain() {
        assert_eq!(
            socks4_request(Some(Ipv4Addr::new(10, 0, 0, 2)), "10.0.0.2", 80, "u"),
            [4, 1, 0, 80, 10, 0, 0, 2, b'u', 0]
        );
        let mut socks4a = vec![4, 1, 0x01, 0xbb, 0, 0, 0, 1, 0];
        socks4a.extend(b"hub.example");
        socks4a.push(0);
        assert_eq!(socks4_request(None, "hub.example", 443, ""), socks4a);
        assert_eq!(reply_meaning(5), "connection refused");
    }

    #[tokio::test]
    async fn socks_handshake_authenticates_and_connects() {
        exchange(
            "socks5h://user:pa%3Ass@proxy",
            "hub.example",
            443,
            |mut server| async move {
                expect(&mut server, &[5, 2, 0, 2]).await;
                server.write_all(&[5, 2]).await.unwrap();
                expect(
                    &mut server,
                    &[
                        1, 4, b'u', b's', b'e', b'r', 5, b'p', b'a', b':', b's', b's',
                    ],
                )
                .await;
                server.write_all(&[1, 0]).await.unwrap();
                let mut request = vec![5, 1, 0, 3, 11];
                request.extend(b"hub.example");
                request.extend([1, 0xbb]);
                expect(&mut server, &request).await;
                // A domain-name bound address.
                server
                    .write_all(&[5, 0, 0, 3, 4, b'h', b'o', b's', b't', 0, 1])
                    .await
                    .unwrap();
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn socks_handshake_without_authentication_reads_an_ipv6_bound_address() {
        exchange("socks5://proxy", "127.0.0.1", 80, |mut server| async move {
            expect(&mut server, &[5, 1, 0]).await;
            server.write_all(&[5, 0]).await.unwrap();
            expect(&mut server, &[5, 1, 0, 1, 127, 0, 0, 1, 0, 80]).await;
            let mut reply = vec![5, 0, 0, 4];
            reply.extend([0u8; 16]);
            reply.extend([0, 1]);
            server.write_all(&reply).await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn socks_reply_codes_and_refusals_are_proxy_errors_without_credentials() {
        // A refused connection names the reply code.
        let error = exchange("socks5://proxy", "hub", 80, |mut server| async move {
            expect(&mut server, &[5, 1, 0]).await;
            server.write_all(&[5, 0]).await.unwrap();
            let mut request = vec![0u8; 5 + 3 + 2];
            server.read_exact(&mut request).await.unwrap();
            server
                .write_all(&[5, 5, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        })
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Proxy);
        assert!(
            error
                .to_string()
                .contains("reply 0x05 (connection refused)"),
            "{error}"
        );
        assert!(error.is_transient());

        // The ruleset forbids the destination: 403, not transient.
        let error = exchange("socks5://proxy", "hub", 80, |mut server| async move {
            expect(&mut server, &[5, 1, 0]).await;
            server.write_all(&[5, 0]).await.unwrap();
            let mut request = vec![0u8; 10];
            server.read_exact(&mut request).await.unwrap();
            server
                .write_all(&[5, 2, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        })
        .await
        .unwrap_err();
        assert_eq!(error.status(), Some(403));
        assert!(!error.is_transient());

        // Refused credentials: 407, never shown.
        let error = exchange(
            "socks5://user:s3cret@proxy",
            "hub",
            80,
            |mut server| async move {
                expect(&mut server, &[5, 2, 0, 2]).await;
                server.write_all(&[5, 2]).await.unwrap();
                let mut request = vec![0u8; 1 + 5 + 7];
                server.read_exact(&mut request).await.unwrap();
                server.write_all(&[1, 1]).await.unwrap();
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            (error.kind(), error.status()),
            (ErrorKind::Proxy, Some(407))
        );
        assert!(!error.to_string().contains("s3cret"), "{error}");
        assert!(!format!("{error:?}").contains("s3cret"), "{error:?}");

        // The proxy requires authentication the client has no credentials for.
        let error = exchange("socks5://proxy", "hub", 80, |mut server| async move {
            expect(&mut server, &[5, 1, 0]).await;
            server.write_all(&[5, 0xff]).await.unwrap();
        })
        .await
        .unwrap_err();
        assert_eq!(error.status(), Some(407));
        assert!(error.to_string().contains("no credentials"), "{error}");
    }

    #[tokio::test]
    async fn socks_truncated_or_foreign_replies_fail() {
        // The proxy closes mid-reply.
        let error = exchange("socks5://proxy", "hub", 80, |mut server| async move {
            expect(&mut server, &[5, 1, 0]).await;
            server.write_all(&[5]).await.unwrap();
        })
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Proxy);
        assert!(
            error.to_string().contains("before its reply was complete"),
            "{error}"
        );

        // A truncated bound address.
        let error = exchange("socks5://proxy", "hub", 80, |mut server| async move {
            expect(&mut server, &[5, 1, 0]).await;
            server.write_all(&[5, 0]).await.unwrap();
            let mut request = vec![0u8; 10];
            server.read_exact(&mut request).await.unwrap();
            server.write_all(&[5, 0, 0, 1, 0, 0]).await.unwrap();
        })
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("before its reply was complete"),
            "{error}"
        );

        // An HTTP proxy answering a SOCKS greeting.
        let error = exchange("socks5://proxy", "hub", 80, |mut server| async move {
            expect(&mut server, &[5, 1, 0]).await;
            server
                .write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                .await
                .unwrap();
        })
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("did not answer as a SOCKS5 proxy"),
            "{error}"
        );

        // An unknown bound address type.
        let error = exchange("socks5://proxy", "hub", 80, |mut server| async move {
            expect(&mut server, &[5, 1, 0]).await;
            server.write_all(&[5, 0]).await.unwrap();
            let mut request = vec![0u8; 10];
            server.read_exact(&mut request).await.unwrap();
            server.write_all(&[5, 0, 0, 9, 0]).await.unwrap();
        })
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("unknown address type"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn socks4_and_4a_handshakes_follow_the_protocol() {
        // SOCKS4a: the proxy resolves the name; the user id comes from the URL.
        exchange(
            "socks4a://ident@proxy",
            "hub.example",
            80,
            |mut server| async move {
                expect(
                    &mut server,
                    &socks4_request(None, "hub.example", 80, "ident"),
                )
                .await;
                server
                    .write_all(&[0, 0x5a, 0, 0, 0, 0, 0, 0])
                    .await
                    .unwrap();
            },
        )
        .await
        .unwrap();
        // SOCKS4 resolves locally to IPv4.
        exchange("socks4://proxy", "localhost", 80, |mut server| async move {
            expect(
                &mut server,
                &socks4_request(Some(Ipv4Addr::LOCALHOST), "localhost", 80, ""),
            )
            .await;
            server
                .write_all(&[0, 0x5a, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        })
        .await
        .unwrap();
        // A rejection.
        let error = exchange("socks4://proxy", "10.0.0.1", 80, |mut server| async move {
            let mut request = vec![0u8; 9];
            server.read_exact(&mut request).await.unwrap();
            server
                .write_all(&[0, 0x5b, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("reply 0x5b"), "{error}");
        // An IPv4-mapped IPv6 literal goes as its IPv4 address.
        exchange(
            "socks4://proxy",
            "[::ffff:10.0.0.3]",
            80,
            |mut server| async move {
                expect(
                    &mut server,
                    &socks4_request(Some(Ipv4Addr::new(10, 0, 0, 3)), "::ffff:10.0.0.3", 80, ""),
                )
                .await;
                server
                    .write_all(&[0, 0x5a, 0, 0, 0, 0, 0, 0])
                    .await
                    .unwrap();
            },
        )
        .await
        .unwrap();
        // IPv6 destinations are not possible over SOCKS4.
        let error = exchange("socks4://proxy", "[::1]", 80, |_| async {})
            .await
            .unwrap_err();
        assert!(error.to_string().contains("IPv4 only"), "{error}");
    }
}
