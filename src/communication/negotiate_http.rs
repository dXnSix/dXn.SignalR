//! Native negotiate requests: one HTTP/1.1 POST through hyper over the connection's own TCP or
//! rustls stream, so negotiate and the WebSocket share one trust configuration.

// Decoding and transport code must not panic on peer input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::sync::Arc;

use http::{header, Request, Uri};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

use super::{integrated_auth::SecurityProvider, request_hook::RequestHook, tls::TlsContext};
use crate::error::{Error, TimeoutOperation};

/// What a negotiate request is sent with: the connection's application headers, TLS, proxy,
/// cookies, integrated authentication and request hook.
pub(crate) struct Options<'a> {
    pub(crate) headers: &'a [(String, String)],
    pub(crate) tls: &'a TlsContext,
    pub(crate) proxy: &'a super::proxy::ProxyConfig,
    pub(crate) cookies: &'a crate::CookieJar,
    pub(crate) integrated: Option<&'a Arc<dyn SecurityProvider>>,
    pub(crate) hook: Option<&'a RequestHook>,
}

impl<'a> Options<'a> {
    /// The options of the connection `recipe` describes.
    pub(crate) fn of(recipe: &'a crate::client::ConnectionRecipe) -> Self {
        Self {
            headers: recipe.headers(),
            tls: recipe.tls(),
            proxy: recipe.proxy(),
            cookies: recipe.cookies(),
            integrated: recipe.integrated(),
            hook: recipe.request_hook(),
        }
    }
}

/// POSTs an empty body to `url` within `timeout` (the connection's connect timeout) and returns
/// the response text; non-2xx statuses are errors.
pub(crate) async fn post(
    url: &str,
    authorization: Option<String>,
    options: Options<'_>,
    timeout: std::time::Duration,
) -> Result<String, Error> {
    tokio::time::timeout(timeout, post_unbounded(url, authorization, options))
        .await
        .map_err(|_| Error::timed_out(TimeoutOperation::Negotiate, timeout))?
}

async fn post_unbounded(
    url: &str,
    authorization: Option<String>,
    options: Options<'_>,
) -> Result<String, Error> {
    let Options {
        headers,
        tls,
        proxy,
        cookies,
        integrated,
        hook,
    } = options;
    let uri: Uri = url.parse().map_err(|error| {
        Error::negotiate(None, format!("the negotiate URL is invalid: {error}"))
    })?;
    let secure = match uri.scheme_str() {
        Some("https") => true,
        Some("http") => false,
        other => {
            return Err(Error::negotiate(
                None,
                format!("the negotiate URL has the unsupported scheme {other:?}"),
            ))
        }
    };
    let authority = uri
        .authority()
        .ok_or_else(|| Error::negotiate(None, "the negotiate URL has no host"))?
        .clone();
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = authority
        .port_u16()
        .unwrap_or(if secure { 443 } else { 80 });
    let path = uri
        .path_and_query()
        .map(|path| path.as_str())
        .unwrap_or("/");

    // The official clients' headers, composed as the .NET client does (application headers
    // replace defaults; the credentials' Authorization wins).
    let composed = super::headers::compose(
        &[
            ("User-Agent", super::headers::user_agent()),
            ("Accept", "*/*".to_string()),
        ],
        headers,
        authorization,
    );
    // Every leg carries the jar's current cookies; a challenge leg of integrated authentication
    // carries its token instead of other credentials; the request hook sees the result.
    // The request hook sees the logical request once, as a .NET `HttpMessageHandlerFactory`
    // handler sees one `SendAsync`; cookies and integrated-authentication legs come after it.
    let composed = super::request_hook::apply(hook, None, "POST", url, composed);
    let build = |integrated: Option<String>| {
        let mut request = Request::post(path)
            .header(header::HOST, authority.as_str())
            .header(header::CONTENT_LENGTH, 0);
        let leg = super::headers::leg(&composed, cookies.cookie_header(url), integrated);
        for (name, value) in leg {
            request = request.header(name.as_str(), value.as_str());
        }
        request.body(Empty::<Bytes>::new()).map_err(|_| {
            Error::configuration(
                "authentication",
                "the credentials are not a valid Authorization header value",
            )
        })
    };
    // Built once before any I/O, so invalid headers fail first.
    build(None)?;

    // Through the proxy's CONNECT tunnel when one applies; TLS runs inside it.
    let open = || {
        http1::<Empty<Bytes>>(
            Endpoint {
                proxy,
                tls,
                secure,
                host,
                port,
            },
            integrated.is_some(),
            false,
            |error| {
                super::tls::classify(
                    error,
                    |message| Error::transport_failure(None, message),
                    "the negotiate HTTP handshake failed",
                )
            },
        )
    };
    let mut connection = open().await?;
    // Integrated-authentication challenges are answered on this connection (or a new one when
    // the server closes it).
    let response = super::integrated_auth::send(
        &mut connection,
        integrated,
        host,
        open,
        |headers| cookies.store_response(url, headers),
        build,
    )
    .await
    .map_err(|failure| match failure {
        super::integrated_auth::ExchangeFailure::Http(error) => super::tls::classify(
            error,
            |message| Error::transport_failure(None, message),
            "the negotiate request failed",
        ),
        super::integrated_auth::ExchangeFailure::Connect(error)
        | super::integrated_auth::ExchangeFailure::Authentication(error) => error,
    })?;
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| {
            super::tls::classify(
                error,
                |message| Error::transport_failure(None, message),
                "cannot read the negotiate response",
            )
        })?
        .to_bytes();
    if !status.is_success() {
        return Err(failure(status, &body));
    }
    String::from_utf8(body.to_vec()).map_err(|_| {
        Error::negotiate(
            Some(status.as_u16()),
            "the negotiate response is not UTF-8 text",
        )
    })
}

/// Where an HTTP/1.1 connection goes, and through what.
pub(crate) struct Endpoint<'a> {
    pub(crate) proxy: &'a super::proxy::ProxyConfig,
    pub(crate) tls: &'a TlsContext,
    pub(crate) secure: bool,
    pub(crate) host: &'a str,
    pub(crate) port: u16,
}

/// Opens an HTTP/1.1 connection through the proxy's CONNECT tunnel when one applies, with TLS
/// inside it for a secure endpoint, and runs it on its own task (with upgrades when `upgrades`).
/// With `bound`, the connection carries its RFC 5929 channel binding for integrated
/// authentication. `handshake_error` maps a failed HTTP handshake.
pub(crate) async fn http1<B>(
    endpoint: Endpoint<'_>,
    bound: bool,
    upgrades: bool,
    handshake_error: impl FnOnce(hyper::Error) -> Error,
) -> Result<super::integrated_auth::Connection<B>, Error>
where
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let tcp = super::proxy::connect(
        endpoint.proxy,
        endpoint.secure,
        endpoint.host,
        endpoint.port,
    )
    .await?;
    if endpoint.secure {
        let stream = tls_stream(endpoint.tls, endpoint.host, tcp).await?;
        let binding = if bound {
            channel_binding(&stream)
        } else {
            None
        };
        let sender = handshake(stream, upgrades, handshake_error).await?;
        Ok(super::integrated_auth::Connection { sender, binding })
    } else {
        let sender = handshake(tcp, upgrades, handshake_error).await?;
        Ok(super::integrated_auth::Connection {
            sender,
            binding: None,
        })
    }
}

async fn handshake<S, B>(
    stream: S,
    upgrades: bool,
    handshake_error: impl FnOnce(hyper::Error) -> Error,
) -> Result<hyper::client::conn::http1::SendRequest<B>, Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(handshake_error)?;
    if upgrades {
        tokio::spawn(async move {
            let _ = connection.with_upgrades().await;
        });
    } else {
        tokio::spawn(async move {
            let _ = connection.await;
        });
    }
    Ok(sender)
}

/// The RFC 5929 `tls-server-end-point` channel binding of a TLS connection.
#[cfg(rayx_tls)]
fn channel_binding(stream: &tokio_rustls::client::TlsStream<TcpStream>) -> Option<Vec<u8>> {
    let certificate = stream.get_ref().1.peer_certificates()?.first()?;
    Some(super::integrated_auth::tls_server_end_point(
        certificate.as_ref(),
    ))
}

#[cfg(not(rayx_tls))]
fn channel_binding(_stream: &TcpStream) -> Option<Vec<u8>> {
    None
}

/// Opens a TCP connection to the first reachable resolved address, with Nagle's algorithm off:
/// SignalR exchanges small frames (Pings, stateful Acks, invocations) whose latency must not wait
/// for delayed TCP acknowledgements.
pub(crate) async fn connect_tcp(host: &str, port: u16) -> Result<TcpStream, Error> {
    // `Uri::host` keeps the brackets of IPv6 literals; resolvers need them removed.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| {
            Error::transport_failure(None, format!("cannot resolve {host}:{port}: {error}"))
                .with_source(error)
        })?;
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                stream.set_nodelay(true).map_err(|error| {
                    Error::transport_failure(
                        None,
                        format!("cannot configure the connection to {host}:{port}: {error}"),
                    )
                    .with_source(error)
                })?;
                return Ok(stream);
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(match last_error {
        Some(error) => {
            Error::transport_failure(None, format!("cannot connect to {host}:{port}: {error}"))
                .with_source(error)
        }
        None => {
            Error::transport_failure(None, format!("cannot resolve {host}:{port}: no addresses"))
        }
    })
}

#[cfg(rayx_tls)]
pub(crate) async fn tls_stream(
    tls: &TlsContext,
    host: &str,
    tcp: TcpStream,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, Error> {
    let name = rustls_pki_types::ServerName::try_from(host.to_string()).map_err(|error| {
        Error::configuration("domain", format!("invalid TLS server name {host}: {error}"))
    })?;
    tokio_rustls::TlsConnector::from(tls.config()?)
        .connect(name, tcp)
        .await
        .map_err(|error| {
            Error::tls(format!("the TLS handshake with {host} failed: {error}")).with_source(error)
        })
}

#[cfg(not(rayx_tls))]
pub(crate) async fn tls_stream(
    _tls: &TlsContext,
    _host: &str,
    _tcp: TcpStream,
) -> Result<TcpStream, Error> {
    Err(super::tls::tls_disabled())
}

#[cfg(test)]
use super::negotiation::EXCERPT_BYTES;

/// A non-2xx negotiate response (see `negotiation::negotiate_failure`).
fn failure(status: http::StatusCode, body: &[u8]) -> Error {
    super::negotiation::negotiate_failure(
        status.as_u16(),
        status.canonical_reason().unwrap_or_default(),
        body,
    )
}

/// Describes a non-2xx negotiate response with a short, credential-redacted body excerpt.
#[cfg(test)]
fn failure_message(status: http::StatusCode, body: &[u8]) -> String {
    super::negotiation::negotiate_failure_message(
        status.as_u16(),
        status.canonical_reason().unwrap_or_default(),
        body,
    )
}

#[cfg(test)]
mod negotiate_http_errors {
    use super::{failure, failure_message as failure_text, EXCERPT_BYTES};
    use crate::ErrorKind;
    use http::StatusCode;

    #[test]
    fn failures_report_the_status_and_a_body_excerpt() {
        let message = failure_text(
            StatusCode::SERVICE_UNAVAILABLE,
            b"negotiation temporarily rejected",
        );
        assert_eq!(
            message,
            "the server returned 503 Service Unavailable: negotiation temporarily rejected"
        );
        assert_eq!(
            failure_text(StatusCode::UNAUTHORIZED, b""),
            "the server returned 401 Unauthorized"
        );
    }

    #[test]
    fn failures_are_authentication_errors_for_401_and_403_and_negotiate_errors_otherwise() {
        for (status, kind) in [
            (StatusCode::UNAUTHORIZED, ErrorKind::Authentication),
            (StatusCode::FORBIDDEN, ErrorKind::Authentication),
            (StatusCode::NOT_FOUND, ErrorKind::Negotiate),
            (StatusCode::SERVICE_UNAVAILABLE, ErrorKind::Negotiate),
        ] {
            let error = failure(status, b"");
            assert_eq!(error.kind(), kind, "{status}");
            assert_eq!(error.status(), Some(status.as_u16()));
        }
    }

    #[test]
    fn failure_excerpts_redact_credentials() {
        let message = failure_text(
            StatusCode::BAD_REQUEST,
            b"bad ws://host/hub?id=1&access_token=secret-token&x=1 Authorization: Basic c2VjcmV0 Bearer abc.def",
        );
        assert!(!message.contains("secret-token"), "{message}");
        assert!(!message.contains("c2VjcmV0"), "{message}");
        assert!(!message.contains("abc.def"), "{message}");
        assert!(message.contains("access_token=[REDACTED]"), "{message}");
    }

    #[test]
    fn redaction_never_splits_multi_byte_text() {
        for body in [
            "access_token=&üüüüüü".as_bytes(),
            "Bearer \u{fffd}ü access_token= ü Authorization:\nü".as_bytes(),
            "üüü access_token=ünicode-token&x=ü".as_bytes(),
        ] {
            let message = failure_text(StatusCode::BAD_REQUEST, body);
            assert!(!message.contains("ünicode-token"), "{message}");
        }
        // Cut inside a multi-byte character at the excerpt bound.
        let mut body = vec![b'a'; EXCERPT_BYTES - 1];
        body.extend("üaccess_token=secret".as_bytes());
        let message = failure_text(StatusCode::BAD_REQUEST, &body);
        assert!(!message.contains("secret"), "{message}");
    }

    #[test]
    fn authorization_redaction_stops_at_the_line_end() {
        let message = failure_text(
            StatusCode::BAD_REQUEST,
            b"Authorization: Basic c2VjcmV0\r\nnext line stays",
        );
        assert!(!message.contains("c2VjcmV0"), "{message}");
        assert!(message.contains("next line stays"), "{message}");
    }

    #[test]
    fn failure_excerpts_are_bounded() {
        let body = vec![b'x'; 10 * EXCERPT_BYTES];
        let message = failure_text(StatusCode::INTERNAL_SERVER_ERROR, &body);
        assert!(message.len() < EXCERPT_BYTES + 64, "{}", message.len());
    }
}

#[cfg(test)]
mod negotiate_tcp_connect {
    use super::connect_tcp;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn negotiate_tcp_connect_disables_nagle() {
        runtime().block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let stream = connect_tcp("127.0.0.1", port).await.unwrap();
            assert!(stream.nodelay().unwrap(), "TCP_NODELAY must be set");
        });
    }

    #[test]
    fn negotiate_tcp_connect_accepts_bracketed_ipv6_hosts() {
        runtime().block_on(async {
            // Hosts without IPv6 loopback cannot run this case.
            let Ok(listener) = tokio::net::TcpListener::bind("[::1]:0").await else {
                return;
            };
            let port = listener.local_addr().unwrap().port();
            let stream = connect_tcp("[::1]", port).await.unwrap();
            assert!(stream.nodelay().unwrap());
        });
    }

    #[test]
    fn negotiate_tcp_connect_reports_refused_and_unresolvable_hosts() {
        runtime().block_on(async {
            let port = {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                listener.local_addr().unwrap().port()
            };
            let refused = connect_tcp("127.0.0.1", port).await.unwrap_err();
            assert_eq!(refused.kind(), crate::ErrorKind::Transport);
            assert!(
                refused
                    .to_string()
                    .starts_with(&format!("cannot connect to 127.0.0.1:{port}")),
                "{refused}"
            );
            let unresolvable = connect_tcp("rayx-signalr.invalid", 80).await.unwrap_err();
            assert_eq!(unresolvable.kind(), crate::ErrorKind::Transport);
            assert!(
                unresolvable
                    .to_string()
                    .starts_with("cannot resolve rayx-signalr.invalid:80"),
                "{unresolvable}"
            );
        });
    }
}

#[cfg(test)]
mod connection_setup_bounds {
    use std::time::Duration;

    use super::post;
    use crate::communication::tls::TlsContext;

    #[tokio::test]
    async fn connection_setup_bounds_the_negotiate_request() {
        // The listener accepts TCP but never answers the negotiate request.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let silent = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let tls = TlsContext::for_connection(false, &Default::default()).unwrap();
        let started = std::time::Instant::now();
        let error = post(
            &format!("http://127.0.0.1:{port}/hub/negotiate?access_token=secret"),
            None,
            super::Options {
                headers: &[],
                tls: &tls,
                proxy: &crate::communication::proxy::ProxyConfig::Disabled,
                cookies: &crate::CookieJar::new(),
                integrated: None,
                hook: None,
            },
            Duration::from_millis(300),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::Timeout);
        assert_eq!(
            error.timeout_operation(),
            Some(crate::TimeoutOperation::Negotiate)
        );
        assert_eq!(error.timeout(), Some(Duration::from_millis(300)));
        assert!(
            !error.to_string().contains("secret"),
            "errors never repeat the URL: {error}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        silent.abort();
    }
}

#[cfg(test)]
mod transport_hooks_negotiate {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{post, Options};
    use crate::communication::{request_hook::RequestHook, tls::TlsContext};

    #[tokio::test]
    async fn transport_hooks_change_the_negotiate_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                let mut byte = [0u8; 1];
                stream.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            let body = "{\"negotiateVersion\":1}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(head).unwrap().to_ascii_lowercase()
        });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        let hook: RequestHook = Arc::new(move |request| {
            recorded.lock().unwrap().push((
                request.transport(),
                request.method().to_string(),
                request.url().to_string(),
                request.header("x-tenant").map(str::to_string),
            ));
            request.set_header("X-Hook", "negotiate").unwrap();
            request.remove_header("X-Requested-With");
        });
        let tls = TlsContext::for_connection(false, &Default::default()).unwrap();
        let url = format!("http://127.0.0.1:{port}/hub/negotiate?negotiateVersion=1");
        let body = post(
            &url,
            Some("Bearer token".to_string()),
            Options {
                headers: &[("X-Tenant".to_string(), "blue".to_string())],
                tls: &tls,
                proxy: &crate::communication::proxy::ProxyConfig::Disabled,
                cookies: &crate::CookieJar::new(),
                integrated: None,
                hook: Some(&hook),
            },
            std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(body, "{\"negotiateVersion\":1}");
        let head = server.await.unwrap();
        assert!(head.contains("\r\nx-hook: negotiate\r\n"), "{head}");
        assert!(!head.contains("x-requested-with"), "{head}");
        assert!(
            head.contains("\r\nauthorization: bearer token\r\n"),
            "{head}"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            vec![(None, "POST".to_string(), url, Some("blue".to_string()))],
            "the hook ran once, on the final headers"
        );
    }
}
