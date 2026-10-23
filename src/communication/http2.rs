//! HTTP/2 for native connections, as the .NET client uses it: negotiate, Long Polling and
//! Server-Sent Events ask for HTTP/2 over TLS through ALPN (`h2, http/1.1`) and fall back to
//! HTTP/1.1 (`HttpVersion::Auto`, the default; plaintext stays HTTP/1.1), or use HTTP/2 only
//! (`HttpVersion::Http2`, with h2c prior knowledge for plaintext and WebSockets over RFC 8441
//! extended CONNECT). Integrated Windows authentication, which HTTP/2 cannot carry, keeps every
//! request on HTTP/1.1, as the .NET client turns HTTP/2 off with credentials. The HTTP/2
//! connections of one logical connection are shared by its requests ([`Pool`]).

// Transport code must not panic on peer input; tests may.
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
    collections::HashMap,
    sync::{Arc, Mutex},
};

use http_body_util::Full;
use hyper::body::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{error::Error, platform::LockExt};

/// The HTTP version of native connections
/// ([`ConnectionConfiguration::with_http_version`](crate::ConnectionConfiguration::with_http_version)).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HttpVersion {
    /// HTTP/1.1 only.
    Http1,
    /// HTTP/2 for negotiate, Long Polling and Server-Sent Events when TLS negotiates it (ALPN
    /// `h2, http/1.1`), else HTTP/1.1; plaintext endpoints and WebSockets use HTTP/1.1. The
    /// default, as the .NET client.
    #[default]
    Auto,
    /// HTTP/2 only: TLS must negotiate `h2`, plaintext endpoints use h2c with prior knowledge, and
    /// WebSockets use RFC 8441 extended CONNECT. An endpoint without HTTP/2 fails with
    /// [`ErrorKind::Transport`](crate::ErrorKind::Transport).
    Http2,
    /// Experimental (feature `http3`): HTTP/3 only, over QUIC with the connection's TLS
    /// configuration, for negotiate, Long Polling and Server-Sent Events. WebSockets are
    /// unavailable over HTTP/3 (servers such as ASP.NET Core do not bootstrap them, RFC 9220), so
    /// transport selection skips them. A plaintext or proxied endpoint, or one QUIC cannot reach,
    /// fails with [`ErrorKind::Transport`](crate::ErrorKind::Transport).
    #[cfg(feature = "http3")]
    Http3,
    /// Experimental (feature `http3`): HTTP/3 where QUIC connects within half of the connect
    /// timeout (once per connect attempt and endpoint), else TCP as
    /// [`Auto`](HttpVersion::Auto); WebSockets use TCP as with `Auto`.
    #[cfg(feature = "http3")]
    Http3Preferred,
}

/// The ALPN protocols a TLS connection offers for `version` (`None`: the connection's own).
#[cfg_attr(not(dxn_tls), allow(dead_code))]
pub(crate) fn alpn(version: HttpVersion) -> Option<&'static [&'static [u8]]> {
    match version {
        HttpVersion::Http1 => None,
        HttpVersion::Auto => Some(&[b"h2", b"http/1.1"]),
        HttpVersion::Http2 => Some(&[b"h2"]),
        // Over TCP, HTTP/3 preferences offer what `Auto` offers.
        #[cfg(feature = "http3")]
        HttpVersion::Http3 | HttpVersion::Http3Preferred => alpn(HttpVersion::Auto),
    }
}

/// The body of HTTP/2 requests.
pub(crate) type Body = Full<Bytes>;

/// A sender on an HTTP/2 connection; clones multiplex streams on the same connection.
pub(crate) type Sender = hyper::client::conn::http2::SendRequest<Body>;

/// An endpoint an HTTP/2 connection serves: host, port and whether it is secure.
type Key = (String, u16, bool);

/// The HTTP/2 connections of one connect attempt, by endpoint: negotiate, Long Polling, the
/// Server-Sent Events stream and an extended-CONNECT WebSocket share them, as the .NET client's
/// `HttpClient` multiplexes its requests. Each start and reconnect attempt, and a local close,
/// forgets them, as the .NET client opens a new `HttpConnection` (and `HttpClient`) for each. The
/// lock is never held across an `.await`.
#[derive(Clone, Default)]
pub(crate) struct Pool(Arc<Mutex<Connections>>);

/// The connections a [`Pool`] keeps.
#[derive(Default)]
struct Connections {
    http2: HashMap<Key, Sender>,
    /// HTTP/3 connections by host and port (always secure).
    #[cfg(dxn_http3)]
    http3: HashMap<(String, u16), super::http3::Connection>,
    /// Endpoints `Http3Preferred` reaches over TCP for the rest of the attempt.
    #[cfg(dxn_http3)]
    tcp: std::collections::HashSet<(String, u16)>,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Pool")
    }
}

impl Pool {
    /// Forgets the connections (each connect attempt opens its own, as a new .NET
    /// `HttpConnection`); their streams in use run to their end.
    pub(crate) fn begin_connect(&self) {
        *self.0.locked() = Connections::default();
    }

    /// The open connection to `host:port`, if any.
    pub(crate) fn get(&self, host: &str, port: u16, secure: bool) -> Option<Sender> {
        let mut connections = self.0.locked();
        let key = (host.to_string(), port, secure);
        match connections.http2.get(&key) {
            Some(sender) if !sender.is_closed() => Some(sender.clone()),
            Some(_) => {
                connections.http2.remove(&key);
                None
            }
            None => None,
        }
    }

    /// Keeps `sender` for `host:port`.
    pub(crate) fn insert(&self, host: &str, port: u16, secure: bool, sender: Sender) {
        self.0
            .locked()
            .http2
            .insert((host.to_string(), port, secure), sender);
    }

    /// Forgets the connection to `host:port` after it failed.
    pub(crate) fn remove(&self, host: &str, port: u16, secure: bool) {
        self.0
            .locked()
            .http2
            .remove(&(host.to_string(), port, secure));
    }

    /// The open HTTP/3 connection to `host:port`, if any.
    #[cfg(dxn_http3)]
    pub(crate) fn http3(&self, host: &str, port: u16) -> Option<super::http3::Connection> {
        let mut connections = self.0.locked();
        let key = (host.to_string(), port);
        match connections.http3.get(&key) {
            Some(connection) if !connection.is_closed() => Some(connection.clone()),
            Some(_) => {
                connections.http3.remove(&key);
                None
            }
            None => None,
        }
    }

    /// Keeps the HTTP/3 `connection` for `host:port`.
    #[cfg(dxn_http3)]
    pub(crate) fn insert_http3(&self, host: &str, port: u16, connection: super::http3::Connection) {
        self.0
            .locked()
            .http3
            .insert((host.to_string(), port), connection);
    }

    /// Forgets the HTTP/3 connection to `host:port` after it failed.
    #[cfg(dxn_http3)]
    pub(crate) fn remove_http3(&self, host: &str, port: u16) {
        self.0.locked().http3.remove(&(host.to_string(), port));
    }

    /// Remembers that `host:port` goes over TCP for the rest of the attempt (`Http3Preferred`).
    #[cfg(dxn_http3)]
    pub(crate) fn use_tcp(&self, host: &str, port: u16) {
        self.0.locked().tcp.insert((host.to_string(), port));
    }

    /// Whether `host:port` goes over TCP for the rest of the attempt.
    #[cfg(dxn_http3)]
    pub(crate) fn uses_tcp(&self, host: &str, port: u16) -> bool {
        self.0.locked().tcp.contains(&(host.to_string(), port))
    }
}

/// Starts HTTP/2 on `stream` (after ALPN `h2`, or h2c with prior knowledge), running the
/// connection on its own task.
pub(crate) async fn handshake<S>(
    stream: S,
    handshake_error: impl FnOnce(hyper::Error) -> Error,
) -> Result<Sender, Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (sender, connection) =
        hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .map_err(handshake_error)?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            log::debug!("An HTTP/2 connection ended: {error}");
        }
    });
    Ok(sender)
}

/// An HTTP/2 request that failed. `retry` when the server never processed it, so it can go out
/// again on another connection without running twice: it was never sent (the connection was
/// closing), the server refused its stream (`REFUSED_STREAM`), or a graceful `GOAWAY` came before
/// the server took it, as the .NET client retries such requests.
#[derive(Debug)]
pub(crate) struct SendFailure {
    pub(crate) error: hyper::Error,
    pub(crate) retry: bool,
}

/// Sends `request` on `sender` once it can take a stream.
pub(crate) async fn send(
    sender: &mut Sender,
    request: http::Request<Body>,
) -> Result<http::Response<hyper::body::Incoming>, SendFailure> {
    if let Err(error) = sender.ready().await {
        return Err(SendFailure { error, retry: true });
    }
    sender
        .try_send_request(request)
        .await
        .map_err(|mut failure| {
            let unsent = failure.take_message().is_some();
            let error = failure.into_error();
            let retry = unsent || unprocessed(&error);
            SendFailure { error, retry }
        })
}

/// The HTTP/2 error behind `error`, if any.
fn h2_error(error: &hyper::Error) -> Option<&h2::Error> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        if let Some(h2) = current.downcast_ref::<h2::Error>() {
            return Some(h2);
        }
        source = current.source();
    }
    None
}

/// Whether the server reports it never processed the stream: `REFUSED_STREAM`, or a graceful
/// (`NO_ERROR`) `GOAWAY` whose last stream came before it.
fn unprocessed(error: &hyper::Error) -> bool {
    h2_error(error).is_some_and(|h2| {
        h2.reason() == Some(h2::Reason::REFUSED_STREAM)
            || (h2.is_go_away() && h2.is_remote() && h2.reason() == Some(h2::Reason::NO_ERROR))
    })
}

/// Whether an extended CONNECT's error means the server does not enable RFC 8441: the client
/// refuses to send it after the server's SETTINGS left it off, or the server resets the stream
/// with `PROTOCOL_ERROR`, as RFC 8441 servers without it do.
pub(crate) fn extended_connect_refused(error: &hyper::Error) -> bool {
    if h2_error(error).is_some_and(|h2| h2.reason() == Some(h2::Reason::PROTOCOL_ERROR)) {
        return true;
    }
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        let text = current.to_string().to_ascii_lowercase();
        if text.contains("extended connect") || text.contains("connect protocol") {
            return true;
        }
        source = current.source();
    }
    false
}

#[cfg(test)]
mod http2_tests {
    use std::{
        convert::Infallible,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    use futures::{SinkExt, StreamExt};
    use http_body_util::Full;
    use hyper::{body::Bytes, service::service_fn, Request, Response};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{alpn, HttpVersion, Pool};
    use crate::{
        client::ConnectionRecipe,
        communication::{
            client_tokio::CommunicationClient,
            http_transport_tokio::{Lane, Target},
            negotiate_http::{post, Options},
            proxy::{ProxyConfig, ProxyUrl},
            tls::TlsContext,
        },
        ConnectionConfiguration, ErrorKind,
    };

    /// What a test server saw: the HTTP version of each request, and its connections.
    #[derive(Clone, Default)]
    struct Seen {
        versions: Arc<Mutex<Vec<http::Version>>>,
        connections: Arc<AtomicUsize>,
    }

    async fn answer(
        seen: Seen,
        request: Request<hyper::body::Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        seen.versions.lock().unwrap().push(request.version());
        if request.method() == http::Method::CONNECT {
            // RFC 8441: the WebSocket protocol and version, then a WebSocket that echoes one
            // message; a path with `deny` is refused with 401.
            assert_eq!(
                request
                    .extensions()
                    .get::<hyper::ext::Protocol>()
                    .map(hyper::ext::Protocol::as_str),
                Some("websocket")
            );
            assert_eq!(
                request.headers().get("sec-websocket-version").unwrap(),
                "13"
            );
            if request.uri().path().contains("deny") {
                let mut refused = Response::new(Full::new(Bytes::new()));
                *refused.status_mut() = http::StatusCode::UNAUTHORIZED;
                return Ok(refused);
            }
            tokio::spawn(async move {
                let upgraded = hyper::upgrade::on(request).await.unwrap();
                let mut socket =
                    tokio_websockets::ServerBuilder::new().serve(TokioIo::new(upgraded));
                if let Some(Ok(message)) = socket.next().await {
                    socket.send(message).await.unwrap();
                }
            });
            return Ok(Response::new(Full::new(Bytes::new())));
        }
        Ok(Response::new(Full::new(Bytes::from_static(
            b"{\"negotiateVersion\":1}",
        ))))
    }

    /// A plaintext HTTP/2 (h2c prior knowledge) server; `connect` enables extended CONNECT.
    async fn h2c_server(connect: bool) -> (u16, Seen) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Seen::default();
        let state = seen.clone();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                state.connections.fetch_add(1, Ordering::SeqCst);
                let seen = state.clone();
                tokio::spawn(async move {
                    let mut builder =
                        hyper::server::conn::http2::Builder::new(TokioExecutor::new());
                    if connect {
                        builder.enable_connect_protocol();
                    }
                    let _ = builder
                        .serve_connection(
                            TokioIo::new(tcp),
                            service_fn(move |request| answer(seen.clone(), request)),
                        )
                        .await;
                });
            }
        });
        (port, seen)
    }

    /// A plaintext HTTP/1.1 server.
    async fn http1_server() -> (u16, Seen) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Seen::default();
        let state = seen.clone();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                state.connections.fetch_add(1, Ordering::SeqCst);
                let seen = state.clone();
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(tcp),
                            service_fn(move |request| answer(seen.clone(), request)),
                        )
                        .await;
                });
            }
        });
        (port, seen)
    }

    /// A TLS server for `localhost` that offers `protocols` through ALPN and serves HTTP/2 when
    /// it is negotiated, else HTTP/1.1. Returns its port, the CA to trust and what it saw.
    #[cfg(dxn_tls)]
    async fn tls_server(protocols: &[&[u8]]) -> (u16, String, Seen) {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
        use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};

        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "http2_ test CA");
        let key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap().pem();
        let issuer = Issuer::new(params, key);
        let server_key = KeyPair::generate().unwrap();
        let server = CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .signed_by(&server_key, &issuer)
            .unwrap()
            .pem();
        let provider = crate::communication::tls::crypto_provider(
            crate::communication::tls::CompiledTls::CURRENT
                .provider()
                .unwrap(),
        );
        let mut config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                CertificateDer::pem_slice_iter(server.as_bytes())
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                PrivateKeyDer::from_pem_slice(server_key.serialize_pem().as_bytes()).unwrap(),
            )
            .unwrap();
        config.alpn_protocols = protocols.iter().map(|protocol| protocol.to_vec()).collect();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Seen::default();
        let state = seen.clone();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                state.connections.fetch_add(1, Ordering::SeqCst);
                let (acceptor, seen) = (acceptor.clone(), state.clone());
                tokio::spawn(async move {
                    let Ok(tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2");
                    let service = service_fn(move |request| answer(seen.clone(), request));
                    if h2 {
                        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                            .serve_connection(TokioIo::new(tls), service)
                            .await;
                    } else {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(tls), service)
                            .await;
                    }
                });
            }
        });
        (port, ca, seen)
    }

    /// An h2c server (with extended CONNECT) that resets its first stream with `reason` and
    /// answers the others with 200. Returns its port and what it saw.
    async fn resetting_server(reason: h2::Reason) -> (u16, Seen) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Seen::default();
        let state = seen.clone();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                state.connections.fetch_add(1, Ordering::SeqCst);
                let seen = state.clone();
                tokio::spawn(async move {
                    let Ok(mut connection) = h2::server::Builder::new()
                        .enable_connect_protocol()
                        .handshake::<_, Bytes>(tcp)
                        .await
                    else {
                        return;
                    };
                    let mut open = Vec::new();
                    while let Some(Ok((request, mut respond))) = connection.accept().await {
                        let first = {
                            let mut versions = seen.versions.lock().unwrap();
                            versions.push(request.version());
                            versions.len() == 1
                        };
                        let connect = request.method() == http::Method::CONNECT;
                        if first {
                            respond.send_reset(reason);
                        } else if let Ok(stream) =
                            respond.send_response(Response::new(()), !connect)
                        {
                            // Kept open: an upgraded stream stays usable.
                            open.push(stream);
                        }
                    }
                });
            }
        });
        (port, seen)
    }

    /// An HTTP CONNECT proxy that tunnels to `127.0.0.1` at the requested port and reports each
    /// request line.
    async fn tunnelling_proxy() -> (u16, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let lines = Arc::new(Mutex::new(Vec::new()));
        let seen = lines.clone();
        tokio::spawn(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                let seen = seen.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        if client.read_exact(&mut byte).await.is_err() {
                            return;
                        }
                        head.push(byte[0]);
                    }
                    let head = String::from_utf8(head).unwrap();
                    let line = head.lines().next().unwrap().to_string();
                    let target = line.split(' ').nth(1).unwrap().to_string();
                    let port: u16 = target.rsplit(':').next().unwrap().parse().unwrap();
                    seen.lock().unwrap().push(line);
                    let mut server = tokio::net::TcpStream::connect(("127.0.0.1", port))
                        .await
                        .unwrap();
                    client
                        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                        .await
                        .unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                });
            }
        });
        (port, lines)
    }

    fn options<'a>(
        tls: &'a TlsContext,
        cookies: &'a crate::CookieJar,
        version: HttpVersion,
        pool: &'a Pool,
    ) -> Options<'a> {
        Options {
            headers: &[],
            tls,
            proxy: &ProxyConfig::Disabled,
            cookies,
            integrated: None,
            hook: None,
            version,
            http2: Some(pool),
        }
    }

    async fn negotiate(
        scheme: &str,
        port: u16,
        tls: &TlsContext,
        version: HttpVersion,
        pool: &Pool,
    ) -> Result<String, crate::Error> {
        negotiate_through(scheme, port, tls, version, pool, &ProxyConfig::Disabled).await
    }

    async fn negotiate_through(
        scheme: &str,
        port: u16,
        tls: &TlsContext,
        version: HttpVersion,
        pool: &Pool,
        proxy: &ProxyConfig,
    ) -> Result<String, crate::Error> {
        let cookies = crate::CookieJar::new();
        post(
            &format!("{scheme}://localhost:{port}/hub/negotiate?negotiateVersion=1"),
            None,
            Options {
                proxy,
                ..options(tls, &cookies, version, pool)
            },
            std::time::Duration::from_secs(10),
        )
        .await
    }

    /// The recipe of an `Http2` connection to the h2c server on `port`.
    fn h2c_recipe(port: u16) -> ConnectionRecipe {
        let mut configuration =
            ConnectionConfiguration::new("127.0.0.1".to_string(), "hub".to_string());
        configuration.unsecure();
        configuration.with_port(i32::from(port));
        configuration.with_http_version(HttpVersion::Http2);
        configuration.connection_recipe()
    }

    /// A Long Polling lane to the h2c server on `port`.
    fn lane(recipe: &ConnectionRecipe, port: u16) -> Lane {
        let target = Target::from_url(
            &format!("http://127.0.0.1:{port}/hub?id=1").parse().unwrap(),
            crate::TransportKind::LongPolling,
        )
        .unwrap();
        Lane::new(crate::TransportKind::LongPolling, target, recipe.clone())
    }

    /// The WebSocket over HTTP/2 to `path` on the h2c server on `port`.
    async fn upgrade(
        recipe: &ConnectionRecipe,
        port: u16,
        path: &str,
    ) -> Result<crate::communication::transport_tokio::WebSocket, crate::Error> {
        let endpoint: http::Uri = format!("ws://127.0.0.1:{port}/{path}?id=1")
            .parse()
            .unwrap();
        CommunicationClient::upgrade_http2(&endpoint, recipe, None).await
    }

    #[test]
    fn http2_versions_offer_their_alpn_protocols() {
        assert_eq!(alpn(HttpVersion::Http1), None);
        assert_eq!(
            alpn(HttpVersion::Auto),
            Some(&[&b"h2"[..], &b"http/1.1"[..]][..])
        );
        assert_eq!(alpn(HttpVersion::Http2), Some(&[&b"h2"[..]][..]));
        assert_eq!(
            HttpVersion::default(),
            HttpVersion::Auto,
            "as the .NET client"
        );
    }

    #[test]
    fn http2_integrated_authentication_keeps_http1() {
        let mut configuration =
            ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        configuration.with_credentials("user", "password", "DOMAIN");
        let recipe = configuration
            .connection_recipe()
            .with_integrated(configuration.integrated_provider().unwrap());
        assert_eq!(
            recipe.http_version(),
            HttpVersion::Http1,
            "Auto falls back to HTTP/1.1, as .NET turns HTTP/2 off with credentials"
        );
        // Http2 is lowered the same way, as .NET lowers it, and the client builds.
        configuration.with_http_version(HttpVersion::Http2);
        let recipe = configuration
            .connection_recipe()
            .with_integrated(configuration.integrated_provider().unwrap());
        assert_eq!(recipe.http_version(), HttpVersion::Http1);
        #[cfg(windows)]
        {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _entered = runtime.enter();
            crate::SignalRClient::new("localhost", "hub", |c| {
                c.unsecure();
                c.with_credentials("user", "password", "DOMAIN");
                c.with_http_version(HttpVersion::Http2);
            })
            .unwrap();
        }

        let mut plain = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        plain.with_http_version(HttpVersion::Http2);
        assert_eq!(plain.connection_recipe().http_version(), HttpVersion::Http2);
    }

    #[tokio::test]
    async fn http2_h2c_prior_knowledge_shares_one_connection() {
        let (port, seen) = h2c_server(false).await;
        let tls = TlsContext::default();
        let pool = Pool::default();
        for _ in 0..3 {
            let body = negotiate("http", port, &tls, HttpVersion::Http2, &pool)
                .await
                .unwrap();
            assert_eq!(body, "{\"negotiateVersion\":1}");
        }
        assert_eq!(
            *seen.versions.lock().unwrap(),
            vec![http::Version::HTTP_2; 3]
        );
        assert_eq!(
            seen.connections.load(Ordering::SeqCst),
            1,
            "the requests multiplex on one connection"
        );
        assert!(pool.get("localhost", port, false).is_some());
        // A new start opens a new connection.
        pool.begin_connect();
        assert!(pool.get("localhost", port, false).is_none());
        negotiate("http", port, &tls, HttpVersion::Http2, &pool)
            .await
            .unwrap();
        assert_eq!(seen.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn http2_plaintext_auto_stays_on_http1() {
        let (port, seen) = http1_server().await;
        let tls = TlsContext::default();
        let pool = Pool::default();
        negotiate("http", port, &tls, HttpVersion::Auto, &pool)
            .await
            .unwrap();
        assert_eq!(*seen.versions.lock().unwrap(), vec![http::Version::HTTP_11]);
        assert!(pool.get("localhost", port, false).is_none());
        // Http2 against an HTTP/1.1 server fails (h2c prior knowledge is refused).
        let error = negotiate("http", port, &tls, HttpVersion::Http2, &pool)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
    }

    #[cfg(dxn_tls)]
    fn trusting(ca: &str) -> TlsContext {
        TlsContext::for_connection(
            true,
            &crate::communication::tls::TlsSettings {
                extra_roots_pem: Some(ca.as_bytes().to_vec()),
                ..crate::communication::tls::TlsSettings::default()
            },
        )
        .unwrap()
    }

    #[cfg(dxn_tls)]
    #[tokio::test]
    async fn http2_auto_negotiates_h2_through_alpn() {
        let (port, ca, seen) = tls_server(&[b"h2", b"http/1.1"]).await;
        let tls = trusting(&ca);
        let pool = Pool::default();
        for _ in 0..2 {
            negotiate("https", port, &tls, HttpVersion::Auto, &pool)
                .await
                .unwrap();
        }
        assert_eq!(
            *seen.versions.lock().unwrap(),
            vec![http::Version::HTTP_2; 2]
        );
        assert_eq!(seen.connections.load(Ordering::SeqCst), 1);
        // Http1 offers no ALPN: the server stays on HTTP/1.1.
        negotiate("https", port, &tls, HttpVersion::Http1, &Pool::default())
            .await
            .unwrap();
        assert_eq!(
            seen.versions.lock().unwrap().last(),
            Some(&http::Version::HTTP_11)
        );
    }

    #[cfg(dxn_tls)]
    #[tokio::test]
    async fn http2_auto_falls_back_to_http1_and_http2_fails() {
        let (port, ca, seen) = tls_server(&[b"http/1.1"]).await;
        let tls = trusting(&ca);
        let pool = Pool::default();
        negotiate("https", port, &tls, HttpVersion::Auto, &pool)
            .await
            .unwrap();
        assert_eq!(*seen.versions.lock().unwrap(), vec![http::Version::HTTP_11]);
        assert!(pool.get("localhost", port, true).is_none());
        let error = negotiate("https", port, &tls, HttpVersion::Http2, &pool)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        assert!(
            error.to_string().contains("did not negotiate HTTP/2"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn http2_websockets_open_through_extended_connect() {
        let (port, seen) = h2c_server(true).await;
        let recipe = h2c_recipe(port);
        for text in ["one", "two"] {
            let mut socket = upgrade(&recipe, port, "hub").await.unwrap();
            socket
                .send(tokio_websockets::Message::text(text))
                .await
                .unwrap();
            let echoed = socket.next().await.unwrap().unwrap();
            assert_eq!(echoed.as_text(), Some(text));
        }
        assert_eq!(
            *seen.versions.lock().unwrap(),
            vec![http::Version::HTTP_2; 2]
        );
        assert_eq!(
            seen.connections.load(Ordering::SeqCst),
            1,
            "the WebSockets are streams of the attempt's HTTP/2 connection"
        );
    }

    #[tokio::test]
    async fn http2_websockets_without_extended_connect_fail_with_transport() {
        let (port, _seen) = h2c_server(false).await;
        let Err(error) = upgrade(&h2c_recipe(port), port, "hub").await else {
            panic!("the upgrade succeeded");
        };
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        assert_eq!(error.transport(), Some(crate::TransportKind::WebSockets));
        assert!(error.to_string().contains("extended CONNECT"), "{error}");
    }

    #[tokio::test]
    async fn http2_websockets_refused_with_401_fail_with_authentication() {
        let (port, _seen) = h2c_server(true).await;
        let Err(error) = upgrade(&h2c_recipe(port), port, "deny").await else {
            panic!("the upgrade succeeded");
        };
        assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
        assert_eq!(error.status(), Some(401));
    }

    #[tokio::test]
    async fn http2_unprocessed_requests_are_sent_again_on_a_new_connection() {
        // A refused stream: negotiate, Long Polling and the WebSocket each go out once more.
        let (port, seen) = resetting_server(h2::Reason::REFUSED_STREAM).await;
        negotiate(
            "http",
            port,
            &TlsContext::default(),
            HttpVersion::Http2,
            &Pool::default(),
        )
        .await
        .unwrap();
        assert_eq!(seen.versions.lock().unwrap().len(), 2);
        assert_eq!(seen.connections.load(Ordering::SeqCst), 2);

        let (port, seen) = resetting_server(h2::Reason::REFUSED_STREAM).await;
        let response = lane(&h2c_recipe(port), port)
            .request(
                http::Method::POST,
                Bytes::from_static(b"{}"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(response.status, http::StatusCode::OK);
        assert_eq!(seen.versions.lock().unwrap().len(), 2);
        assert_eq!(seen.connections.load(Ordering::SeqCst), 2);

        let (port, seen) = resetting_server(h2::Reason::REFUSED_STREAM).await;
        upgrade(&h2c_recipe(port), port, "hub").await.unwrap();
        assert_eq!(seen.versions.lock().unwrap().len(), 2);
        assert_eq!(seen.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn http2_processed_requests_are_never_sent_twice() {
        // A stream the server reset after taking it may have run: the send fails, once.
        let (port, seen) = resetting_server(h2::Reason::INTERNAL_ERROR).await;
        let Err(error) = lane(&h2c_recipe(port), port)
            .request(
                http::Method::POST,
                Bytes::from_static(b"{}"),
                Some("text/plain"),
                None,
            )
            .await
        else {
            panic!("the request was sent again");
        };
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        assert_eq!(seen.versions.lock().unwrap().len(), 1);

        let (port, seen) = resetting_server(h2::Reason::INTERNAL_ERROR).await;
        let error = negotiate(
            "http",
            port,
            &TlsContext::default(),
            HttpVersion::Http2,
            &Pool::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        assert_eq!(seen.versions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn http2_runs_through_proxy_tunnels() {
        let (port, seen) = h2c_server(false).await;
        let (proxy_port, lines) = tunnelling_proxy().await;
        let proxy = ProxyConfig::Explicit(
            ProxyUrl::parse(&format!("http://127.0.0.1:{proxy_port}")).unwrap(),
        );
        let pool = Pool::default();
        for _ in 0..2 {
            negotiate_through(
                "http",
                port,
                &TlsContext::default(),
                HttpVersion::Http2,
                &pool,
                &proxy,
            )
            .await
            .unwrap();
        }
        assert_eq!(
            *seen.versions.lock().unwrap(),
            vec![http::Version::HTTP_2; 2]
        );
        assert_eq!(
            *lines.lock().unwrap(),
            vec![format!("CONNECT localhost:{port} HTTP/1.1")],
            "one tunnel carries the multiplexed connection"
        );
    }

    #[cfg(dxn_tls)]
    #[tokio::test]
    async fn http2_negotiates_h2_inside_proxy_tunnels() {
        let (port, ca, seen) = tls_server(&[b"h2", b"http/1.1"]).await;
        let (proxy_port, lines) = tunnelling_proxy().await;
        let proxy = ProxyConfig::Explicit(
            ProxyUrl::parse(&format!("http://127.0.0.1:{proxy_port}")).unwrap(),
        );
        negotiate_through(
            "https",
            port,
            &trusting(&ca),
            HttpVersion::Auto,
            &Pool::default(),
            &proxy,
        )
        .await
        .unwrap();
        assert_eq!(*seen.versions.lock().unwrap(), vec![http::Version::HTTP_2]);
        assert_eq!(lines.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn http2_lanes_multiplex_on_the_shared_connection() {
        let (port, seen) = h2c_server(false).await;
        let recipe = h2c_recipe(port);
        let mut polls = lane(&recipe, port);
        let mut sends = lane(&recipe, port);
        let poll = polls
            .request(http::Method::GET, Bytes::new(), None, None)
            .await
            .unwrap();
        assert_eq!(poll.status, http::StatusCode::OK);
        sends
            .request(
                http::Method::POST,
                Bytes::from_static(b"{}"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            *seen.versions.lock().unwrap(),
            vec![http::Version::HTTP_2; 2]
        );
        assert_eq!(
            seen.connections.load(Ordering::SeqCst),
            1,
            "both lanes share the connection's HTTP/2 connection"
        );
    }
}
