//! Experimental HTTP/3 for native connections (feature `http3`): negotiate, Long Polling and
//! Server-Sent Events over QUIC (quinn) and h3, with the connection's own rustls configuration
//! (its provider, verifier and roots, offering ALPN `h3`). `HttpVersion::Http3` requires HTTP/3;
//! `HttpVersion::Http3Preferred` tries QUIC within half of the connect timeout once per connect
//! attempt and endpoint, then uses TCP (HTTP/2 or HTTP/1.1, as `Auto`). QUIC runs only to secure
//! endpoints reached directly: a proxy (TCP only) or a plaintext endpoint means TCP. WebSockets
//! never run over HTTP/3: no server the client targets bootstraps them (RFC 9220; ASP.NET Core
//! 10's Kestrel does not advertise it), so strict `Http3` skips them in transport selection.

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
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use bytes::{Buf, Bytes, BytesMut};
use log::debug;

use super::{
    http2::{HttpVersion, Pool},
    negotiate_http::Endpoint,
    proxy::Route,
    tls::TlsContext,
};
use crate::error::{Error, TimeoutOperation};

/// The ALPN protocol of HTTP/3.
const ALPN: &[&[u8]] = &[b"h3"];

/// How often a QUIC connection pings the server, so long polls and event streams with no traffic
/// outlive QUIC's idle timeout.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

type Sender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
type Stream = h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

/// An HTTP/3 connection: clones send requests as streams of the same QUIC connection.
#[derive(Clone)]
pub(crate) struct Connection {
    sender: Sender,
    quic: quinn::Connection,
}

impl Connection {
    /// Whether the QUIC connection has closed.
    pub(crate) fn is_closed(&self) -> bool {
        self.quic.close_reason().is_some()
    }
}

/// The body of an HTTP/3 response, read as it arrives.
pub(crate) struct Body(Stream);

impl Body {
    /// The next data of the body; `None` at its end.
    pub(crate) async fn chunk(&mut self) -> Option<Result<Bytes, h3::error::StreamError>> {
        match self.0.recv_data().await {
            Ok(Some(mut data)) => Some(Ok(data.copy_to_bytes(data.remaining()))),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }

    /// The whole body.
    pub(crate) async fn collect(mut self) -> Result<Bytes, h3::error::StreamError> {
        let mut body = BytesMut::new();
        while let Some(chunk) = self.chunk().await {
            body.extend_from_slice(&chunk?);
        }
        Ok(body.freeze())
    }
}

/// The HTTP/3 connection a request to `endpoint` uses under `version`, from `pool` or opened;
/// `None` when the request goes over TCP. `Http3` fails where HTTP/3 cannot run (a plaintext or
/// proxied endpoint, or no QUIC connection); `Http3Preferred` then uses TCP, after a QUIC attempt
/// bounded by half of `bound`, and remembers that for the endpoint in `pool`.
pub(crate) async fn connection(
    endpoint: &Endpoint<'_>,
    version: HttpVersion,
    pool: Option<&Pool>,
    bound: Duration,
) -> Result<Option<Connection>, Error> {
    let required = match version {
        HttpVersion::Http3 => true,
        HttpVersion::Http3Preferred => false,
        _ => return Ok(None),
    };
    let (host, port) = (endpoint.host, endpoint.port);
    let unavailable = |reason: String| {
        if required {
            Err(Error::transport_failure(
                None,
                format!("HTTP/3, which with_http_version(Http3) requires, {reason}"),
            ))
        } else {
            if let Some(pool) = pool {
                pool.use_tcp(host, port);
            }
            Ok(None)
        }
    };
    if !endpoint.secure {
        return unavailable(format!(
            "needs a secure endpoint, and {host}:{port} is plaintext"
        ));
    }
    if let Some(pool) = pool {
        if pool.uses_tcp(host, port) {
            return Ok(None);
        }
        if let Some(connection) = pool.http3(host, port) {
            return Ok(Some(connection));
        }
    }
    let routes = super::proxy::routes(endpoint.proxy, endpoint.tls, true, host, port).await?;
    if !matches!(routes.first(), None | Some(Route::Direct)) {
        return unavailable(format!(
            "cannot run through the proxy that {host}:{port} is reached through"
        ));
    }
    let opened = if required {
        connect(endpoint.tls, host, port).await
    } else {
        let half = bound / 2;
        tokio::time::timeout(half, connect(endpoint.tls, host, port))
            .await
            .unwrap_or_else(|_| Err(Error::timed_out(TimeoutOperation::Connect, half)))
    };
    match opened {
        Ok(connection) => {
            if let Some(pool) = pool {
                pool.insert_http3(host, port, connection.clone());
            }
            Ok(Some(connection))
        }
        Err(error) if required => Err(error),
        Err(error) => {
            debug!("HTTP/3 to {host}:{port} is unavailable ({error}); using TCP");
            unavailable(String::new())
        }
    }
}

/// Opens a QUIC connection to `host:port` with the connection's TLS configuration offering `h3`,
/// trying each resolved address in turn, and starts HTTP/3 on it.
async fn connect(tls: &TlsContext, host: &str, port: u16) -> Result<Connection, Error> {
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls.config_with_alpn(Some(ALPN))?)
            .map_err(|error| {
                Error::transport_failure(
                    None,
                    format!("the TLS configuration cannot run QUIC (TLS 1.3): {error}"),
                )
            })?;
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(KEEP_ALIVE));
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport));
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| {
            Error::transport_failure(None, format!("cannot resolve {host}:{port}: {error}"))
                .with_source(error)
        })?;
    let mut last_error = None;
    for address in addresses {
        match open(&config, address, host).await {
            Ok(connection) => return Ok(connection),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        Error::transport_failure(None, format!("cannot resolve {host}:{port}: no addresses"))
    }))
}

/// A QUIC connection to `address` (TLS for `host`) running HTTP/3, its driver on its own task.
async fn open(
    config: &quinn::ClientConfig,
    address: SocketAddr,
    host: &str,
) -> Result<Connection, Error> {
    let local = if address.is_ipv6() {
        SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))
    } else {
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
    };
    let failed = |message: String| Error::transport_failure(None, message);
    let endpoint = quinn::Endpoint::client(local).map_err(|error| {
        failed(format!("cannot open a QUIC socket: {error}")).with_source(error)
    })?;
    let quic = endpoint
        .connect_with(config.clone(), address, host)
        .map_err(|error| {
            failed(format!(
                "cannot connect to {host} ({address}) over QUIC: {error}"
            ))
            .with_source(error)
        })?
        .await
        .map_err(|error| handshake_error(host, address, error))?;
    let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(quic.clone()))
        .await
        .map_err(|error| {
            failed(format!("the HTTP/3 connection to {host} failed: {error}")).with_source(error)
        })?;
    tokio::spawn(async move {
        let error = std::future::poll_fn(|context| driver.poll_close(context)).await;
        if !error.is_h3_no_error() {
            debug!("An HTTP/3 connection ended: {error}");
        }
    });
    Ok(Connection { sender, quic })
}

/// A failed QUIC handshake: TLS errors (a certificate that does not verify) are
/// `ErrorKind::Tls`, others `ErrorKind::Transport`.
fn handshake_error(host: &str, address: SocketAddr, error: quinn::ConnectionError) -> Error {
    let crypto = matches!(
        &error,
        quinn::ConnectionError::TransportError(transport)
            if (0x100..0x200).contains(&u64::from(transport.code))
    );
    let message = format!("the QUIC connection to {host} ({address}) failed: {error}");
    if crypto {
        Error::tls(format!("{message}; the TLS connection failed")).with_source(error)
    } else {
        Error::transport_failure(None, message).with_source(error)
    }
}

/// An HTTP/3 request that failed; `retry` when it never left the client (its stream could not
/// be opened), so it can be sent on another connection without running twice.
pub(crate) struct SendFailure {
    pub(crate) error: h3::error::StreamError,
    pub(crate) retry: bool,
}

/// Sends `request` with `body` on `connection` and returns the response head and its body.
pub(crate) async fn send(
    connection: &Connection,
    request: http::Request<()>,
    body: Bytes,
) -> Result<(http::Response<()>, Body), SendFailure> {
    let mut sender = connection.sender.clone();
    let mut stream = sender
        .send_request(request)
        .await
        .map_err(|error| SendFailure { error, retry: true })?;
    let sent = async {
        if !body.is_empty() {
            stream.send_data(body).await?;
        }
        stream.finish().await?;
        stream.recv_response().await
    };
    let response = sent.await.map_err(|error| SendFailure {
        error,
        retry: false,
    })?;
    Ok((response, Body(stream)))
}

/// Sends a request over HTTP/3 when `version` uses it for `endpoint` (`None`: it goes over TCP).
/// `request` builds it (again for the one retry of a request that never left the client); a
/// failed connection is forgotten; `failed` describes a failure.
pub(crate) async fn exchange(
    endpoint: &Endpoint<'_>,
    version: HttpVersion,
    pool: Option<&Pool>,
    bound: Duration,
    request: impl Fn() -> Result<http::Request<()>, Error>,
    body: &Bytes,
    failed: impl Fn(String) -> Error,
) -> Result<Option<(http::Response<()>, Body)>, Error> {
    let mut retried = false;
    loop {
        let Some(connection) = connection(endpoint, version, pool, bound).await? else {
            return Ok(None);
        };
        match send(&connection, request()?, body.clone()).await {
            Ok(exchanged) => return Ok(Some(exchanged)),
            Err(failure) => {
                if let Some(pool) = pool {
                    pool.remove_http3(endpoint.host, endpoint.port);
                }
                if failure.retry && !retried {
                    debug!(
                        "An HTTP/3 request never left the client ({}); sending it on another \
                         connection",
                        failure.error
                    );
                    retried = true;
                    continue;
                }
                return Err(failed(failure.error.to_string()).with_source(failure.error));
            }
        }
    }
}

/// An HTTP/3 request for `url` (absolute) with `headers`; `Content-Length` for a body.
pub(crate) fn request(
    method: http::Method,
    url: &str,
    headers: &[(String, String)],
    length: usize,
) -> Result<http::Request<()>, Error> {
    let mut request = http::Request::builder().method(method.clone()).uri(url);
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    if length > 0 || method == http::Method::POST {
        request = request.header(http::header::CONTENT_LENGTH, length);
    }
    request.body(()).map_err(|_| {
        Error::configuration(
            "authentication",
            "the credentials are not a valid Authorization header value",
        )
    })
}

#[cfg(test)]
mod http3_tests {
    use std::{
        convert::Infallible,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    };

    use bytes::{Buf, Bytes};
    use http_body_util::Full;
    use hyper::{service::service_fn, Response};
    use hyper_util::rt::TokioIo;
    use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};

    use super::super::{
        http2::{HttpVersion, Pool},
        http_transport_tokio::{Lane, Target},
        negotiate_http::{post, Options},
        negotiation::TransportSelection,
        proxy::{ProxyConfig, ProxyUrl},
        tls::{TlsContext, TlsSettings},
    };
    use crate::{client::ConnectionRecipe, ConnectionConfiguration, ErrorKind, TransportKind};

    const NEGOTIATED: &[u8] = b"{\"negotiateVersion\":1}";

    /// A CA (its PEM) and a server certificate it issued for `localhost` and `127.0.0.1`.
    fn certificates() -> (String, Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};

        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "http3_ test CA");
        let key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap().pem();
        let issuer = Issuer::new(params, key);
        let server_key = KeyPair::generate().unwrap();
        let server = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap()
            .signed_by(&server_key, &issuer)
            .unwrap()
            .pem();
        (
            ca,
            CertificateDer::pem_slice_iter(server.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            PrivateKeyDer::from_pem_slice(server_key.serialize_pem().as_bytes()).unwrap(),
        )
    }

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        crate::communication::tls::crypto_provider(
            crate::communication::tls::CompiledTls::CURRENT
                .provider()
                .unwrap(),
        )
    }

    /// What a server saw: each request's method and path, and its connections.
    #[derive(Clone, Default)]
    struct Seen {
        requests: Arc<Mutex<Vec<(http::Method, String)>>>,
        connections: Arc<AtomicUsize>,
    }

    impl Seen {
        fn record(&self, method: &http::Method, path: &str) {
            self.requests
                .lock()
                .unwrap()
                .push((method.clone(), path.to_string()));
        }
    }

    /// An HTTP/3 server on a loopback UDP port: a GET gets an event stream of two events, a POST
    /// to a negotiate path the negotiate body, other POSTs an empty 200. Returns its port, the CA
    /// that issued its certificate and what it saw.
    async fn h3_server() -> (u16, String, Seen) {
        let (ca, chain, key) = certificates();
        let mut config = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
        config.alpn_protocols = vec![b"h3".to_vec()];
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(config).unwrap();
        let endpoint = quinn::Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(crypto)),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let port = endpoint.local_addr().unwrap().port();
        let seen = Seen::default();
        let state = seen.clone();
        tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                state.connections.fetch_add(1, Ordering::SeqCst);
                let seen = state.clone();
                tokio::spawn(async move {
                    let Ok(quic) = incoming.await else {
                        return;
                    };
                    let mut connection =
                        h3::server::Connection::<_, Bytes>::new(h3_quinn::Connection::new(quic))
                            .await
                            .unwrap();
                    while let Ok(Some(resolver)) = connection.accept().await {
                        let seen = seen.clone();
                        tokio::spawn(async move {
                            let (request, mut stream) = resolver.resolve_request().await.unwrap();
                            seen.record(request.method(), request.uri().path());
                            // A request to /reset is reset after the server took it.
                            if request.uri().path() == "/reset" {
                                stream.stop_stream(h3::error::Code::H3_INTERNAL_ERROR);
                                return;
                            }
                            while let Ok(Some(mut data)) = stream.recv_data().await {
                                data.advance(data.remaining());
                            }
                            stream.send_response(http::Response::new(())).await.unwrap();
                            if request.method() == http::Method::GET {
                                for event in ["data: one\r\n\r\n", "data: two\n\n"] {
                                    stream
                                        .send_data(Bytes::from_static(event.as_bytes()))
                                        .await
                                        .unwrap();
                                }
                            } else if request.uri().path().ends_with("/negotiate") {
                                stream
                                    .send_data(Bytes::from_static(NEGOTIATED))
                                    .await
                                    .unwrap();
                            }
                            let _ = stream.finish().await;
                        });
                    }
                });
            }
        });
        (port, ca, seen)
    }

    /// A TCP-only HTTPS server (HTTP/1.1, no UDP listener): what HTTP/3 falls back to.
    async fn tcp_server() -> (u16, String, Seen) {
        let (ca, chain, key) = certificates();
        let config = rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
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
                    let service =
                        service_fn(move |request: http::Request<hyper::body::Incoming>| {
                            seen.record(request.method(), request.uri().path());
                            async move {
                                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                                    NEGOTIATED,
                                ))))
                            }
                        });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(tls), service)
                        .await;
                });
            }
        });
        (port, ca, seen)
    }

    fn trusting(ca: &str) -> TlsContext {
        TlsContext::for_connection(
            true,
            &TlsSettings {
                extra_roots_pem: Some(ca.as_bytes().to_vec()),
                ..TlsSettings::default()
            },
        )
        .unwrap()
    }

    async fn negotiate(
        url: &str,
        tls: &TlsContext,
        version: HttpVersion,
        pool: &Pool,
        proxy: &ProxyConfig,
        bound: Duration,
    ) -> Result<String, crate::Error> {
        let cookies = crate::CookieJar::new();
        post(
            url,
            None,
            Options {
                headers: &[],
                tls,
                proxy,
                cookies: &cookies,
                integrated: None,
                hook: None,
                version,
                http2: Some(pool),
            },
            bound,
        )
        .await
    }

    /// The recipe of a connection to `port` with `version`, trusting `ca`.
    fn recipe(port: u16, ca: &str, version: HttpVersion) -> ConnectionRecipe {
        let mut configuration =
            ConnectionConfiguration::new("127.0.0.1".to_string(), "hub".to_string());
        configuration.with_port(i32::from(port));
        configuration.with_http_version(version);
        configuration.without_proxy();
        configuration.with_additional_root_certificates_pem(ca.as_bytes().to_vec());
        configuration
            .connection_recipe()
            .with_tls(TlsContext::for_connection(true, configuration.tls_settings()).unwrap())
    }

    fn lane(recipe: &ConnectionRecipe, port: u16, kind: TransportKind) -> Lane {
        lane_to(recipe, port, "hub", kind)
    }

    fn lane_to(recipe: &ConnectionRecipe, port: u16, path: &str, kind: TransportKind) -> Lane {
        let target = Target::from_url(
            &format!("https://127.0.0.1:{port}/{path}?id=1")
                .parse()
                .unwrap(),
            kind,
        )
        .unwrap();
        Lane::new(kind, target, recipe.clone())
    }

    #[tokio::test]
    async fn http3_requests_the_server_took_are_never_sent_twice() {
        let (port, ca, seen) = h3_server().await;
        let recipe = recipe(port, &ca, HttpVersion::Http3);
        let Err(error) = lane_to(&recipe, port, "reset", TransportKind::LongPolling)
            .request(
                http::Method::POST,
                Bytes::from_static(b"{}"),
                Some("text/plain"),
                None,
            )
            .await
        else {
            panic!("the reset request succeeded");
        };
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        assert_eq!(
            *seen.requests.lock().unwrap(),
            vec![(http::Method::POST, "/reset".to_string())],
            "the request was sent once"
        );
        // The failed connection was forgotten; the next request opens another.
        lane(&recipe, port, TransportKind::LongPolling)
            .request(
                http::Method::POST,
                Bytes::from_static(b"{}"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(seen.connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn http3_requests_share_one_quic_connection() {
        let (port, ca, seen) = h3_server().await;
        let tls = trusting(&ca);
        let pool = Pool::default();
        let url = format!("https://127.0.0.1:{port}/hub/negotiate?negotiateVersion=1");
        for _ in 0..2 {
            let body = negotiate(
                &url,
                &tls,
                HttpVersion::Http3,
                &pool,
                &ProxyConfig::Disabled,
                Duration::from_secs(10),
            )
            .await
            .unwrap();
            assert_eq!(body.as_bytes(), NEGOTIATED);
        }
        assert_eq!(seen.requests.lock().unwrap().len(), 2);
        assert_eq!(
            seen.connections.load(Ordering::SeqCst),
            1,
            "the requests are streams of one QUIC connection"
        );

        // Long Polling over HTTP/3.
        let recipe = recipe(port, &ca, HttpVersion::Http3);
        let response = lane(&recipe, port, TransportKind::LongPolling)
            .request(
                http::Method::POST,
                Bytes::from_static(b"{}"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(response.status, http::StatusCode::OK);
        assert_eq!(
            seen.requests.lock().unwrap().last(),
            Some(&(http::Method::POST, "/hub".to_string()))
        );
    }

    #[tokio::test]
    async fn http3_event_streams_arrive_over_http3() {
        let (port, ca, seen) = h3_server().await;
        let recipe = recipe(port, &ca, HttpVersion::Http3);
        let response = lane(&recipe, port, TransportKind::ServerSentEvents)
            .open_stream(&[("Accept", "text/event-stream")], None)
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        let mut body = response.into_body();
        let mut received = Vec::new();
        while let Some(chunk) = body.chunk().await {
            received.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(received, b"data: one\r\n\r\ndata: two\n\n");
        assert_eq!(
            *seen.requests.lock().unwrap(),
            vec![(http::Method::GET, "/hub".to_string())]
        );
    }

    #[tokio::test]
    async fn http3_preferred_falls_back_to_tcp_within_half_the_bound() {
        let (port, ca, seen) = tcp_server().await;
        let tls = trusting(&ca);
        let pool = Pool::default();
        let url = format!("https://127.0.0.1:{port}/hub/negotiate?negotiateVersion=1");
        let bound = Duration::from_secs(2);
        let direct = ProxyConfig::Disabled;
        let started = Instant::now();
        negotiate(
            &url,
            &tls,
            HttpVersion::Http3Preferred,
            &pool,
            &direct,
            bound,
        )
        .await
        .unwrap();
        let first = started.elapsed();
        assert!(
            first >= Duration::from_millis(900) && first < bound,
            "QUIC gets half the bound: {first:?}"
        );
        assert!(pool.uses_tcp("127.0.0.1", port));
        // The endpoint stays on TCP for the rest of the attempt.
        let started = Instant::now();
        negotiate(
            &url,
            &tls,
            HttpVersion::Http3Preferred,
            &pool,
            &direct,
            bound,
        )
        .await
        .unwrap();
        assert!(started.elapsed() < Duration::from_millis(900));
        assert_eq!(seen.requests.lock().unwrap().len(), 2);

        // Strict HTTP/3 never falls back.
        let error = negotiate(
            &url,
            &tls,
            HttpVersion::Http3,
            &Pool::default(),
            &direct,
            bound,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error.kind(), ErrorKind::Timeout | ErrorKind::Transport),
            "{error}"
        );
        assert_eq!(seen.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn http3_needs_a_secure_direct_endpoint() {
        let (port, ca, seen) = h3_server().await;
        let tls = trusting(&ca);
        let bound = Duration::from_secs(5);
        // Plaintext: strict HTTP/3 fails at once.
        let error = negotiate(
            &format!("http://127.0.0.1:{port}/hub/negotiate"),
            &tls,
            HttpVersion::Http3,
            &Pool::default(),
            &ProxyConfig::Disabled,
            bound,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        assert!(error.to_string().contains("plaintext"), "{error}");
        // Through a proxy (TCP only): strict HTTP/3 fails, the preference uses TCP.
        let proxy = ProxyConfig::Explicit(ProxyUrl::parse("http://127.0.0.1:9").unwrap());
        let url = format!("https://127.0.0.1:{port}/hub/negotiate");
        let error = negotiate(
            &url,
            &tls,
            HttpVersion::Http3,
            &Pool::default(),
            &proxy,
            bound,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        assert!(error.to_string().contains("proxy"), "{error}");
        let pool = Pool::default();
        let error = negotiate(
            &url,
            &tls,
            HttpVersion::Http3Preferred,
            &pool,
            &proxy,
            bound,
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::Proxy,
            "the preference went over TCP to the (closed) proxy: {error}"
        );
        assert!(pool.uses_tcp("127.0.0.1", port));
        assert!(seen.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn http3_certificates_must_verify() {
        let (port, _ca, seen) = h3_server().await;
        let (other_ca, _, _) = certificates();
        let error = negotiate(
            &format!("https://127.0.0.1:{port}/hub/negotiate"),
            &trusting(&other_ca),
            HttpVersion::Http3,
            &Pool::default(),
            &ProxyConfig::Disabled,
            Duration::from_secs(10),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Tls, "{error}");
        assert!(seen.requests.lock().unwrap().is_empty());
    }

    #[test]
    fn http3_skips_websockets_in_transport_selection() {
        let selection = || TransportSelection {
            candidates: vec![
                TransportKind::WebSockets,
                TransportKind::ServerSentEvents,
                TransportKind::LongPolling,
            ],
            skipped: Vec::new(),
        };
        let strict = selection().over_http3(HttpVersion::Http3);
        assert_eq!(
            strict.candidates,
            vec![TransportKind::ServerSentEvents, TransportKind::LongPolling]
        );
        assert_eq!(strict.skipped.len(), 1);
        assert_eq!(strict.skipped[0].transport(), TransportKind::WebSockets);
        assert!(strict.skipped[0].error().to_string().contains("RFC 9220"));
        // The preference keeps WebSockets (over TCP).
        let preferred = selection().over_http3(HttpVersion::Http3Preferred);
        assert_eq!(preferred.candidates.len(), 3);
        assert!(preferred.skipped.is_empty());
    }
}
