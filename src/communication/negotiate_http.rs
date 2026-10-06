//! Native negotiate requests: one HTTP/1.1 POST through hyper over the connection's own TCP or
//! rustls stream, so negotiate and the WebSocket share one trust configuration.

use http::{header, Request, Uri};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

use super::tls::TlsContext;

/// POSTs an empty body to `url` and returns the response text; non-2xx statuses are errors.
pub(crate) async fn post(
    url: &str,
    authorization: Option<String>,
    tls: &TlsContext,
) -> Result<String, String> {
    let uri: Uri = url
        .parse()
        .map_err(|error| format!("The negotiate URL is invalid: {error}"))?;
    let secure = match uri.scheme_str() {
        Some("https") => true,
        Some("http") => false,
        other => return Err(format!("Unsupported negotiate scheme {other:?}")),
    };
    let authority = uri
        .authority()
        .ok_or_else(|| "The negotiate URL has no host".to_string())?
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

    let mut request = Request::post(path)
        .header(header::HOST, authority.as_str())
        .header(header::CONTENT_LENGTH, 0);
    if let Some(value) = authorization {
        request = request.header(header::AUTHORIZATION, value);
    }
    let request = request
        .body(Empty::<Bytes>::new())
        .map_err(|_| "Cannot build the negotiate request".to_string())?;

    let tcp = connect_tcp(host, port).await?;
    if secure {
        send(tls_stream(tls, host, tcp).await?, request).await
    } else {
        send(tcp, request).await
    }
}

/// Opens a TCP connection to the first reachable resolved address, with Nagle's algorithm off:
/// SignalR exchanges small frames (Pings, stateful Acks, invocations) whose latency must not wait
/// for delayed TCP acknowledgements.
pub(crate) async fn connect_tcp(host: &str, port: u16) -> Result<TcpStream, String> {
    // `Uri::host` keeps the brackets of IPv6 literals; resolvers need them removed.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| format!("Cannot resolve {host}:{port}: {error}"))?;
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                stream.set_nodelay(true).map_err(|error| {
                    format!("Cannot configure the connection to {host}:{port}: {error}")
                })?;
                return Ok(stream);
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(match last_error {
        Some(error) => format!("Cannot connect to {host}:{port}: {error}"),
        None => format!("Cannot resolve {host}:{port}: no addresses"),
    })
}

#[cfg(rayx_tls)]
async fn tls_stream(
    tls: &TlsContext,
    host: &str,
    tcp: TcpStream,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, String> {
    let name = rustls_pki_types::ServerName::try_from(host.to_string())
        .map_err(|error| format!("Invalid TLS server name {host}: {error}"))?;
    tokio_rustls::TlsConnector::from(tls.config()?)
        .connect(name, tcp)
        .await
        .map_err(|error| format!("TLS handshake with {host} failed: {error}"))
}

#[cfg(not(rayx_tls))]
async fn tls_stream(_tls: &TlsContext, _host: &str, _tcp: TcpStream) -> Result<TcpStream, String> {
    Err(super::tls::TLS_DISABLED.to_string())
}

async fn send<S>(stream: S, request: Request<Empty<Bytes>>) -> Result<String, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| format!("The negotiate HTTP handshake failed: {error}"))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let response = sender
        .send_request(request)
        .await
        .map_err(|error| format!("The negotiate request failed: {error}"))?;
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("Cannot read the negotiate response: {error}"))?
        .to_bytes();
    if !status.is_success() {
        return Err(failure(status, &body));
    }
    String::from_utf8(body.to_vec())
        .map_err(|_| "The negotiate response is not UTF-8 text".to_string())
}

/// The longest body excerpt a negotiate failure reports.
const EXCERPT_BYTES: usize = 256;

/// Describes a non-2xx negotiate response with a short, credential-redacted body excerpt.
fn failure(status: http::StatusCode, body: &[u8]) -> String {
    let excerpt = String::from_utf8_lossy(&body[..body.len().min(EXCERPT_BYTES)]);
    let excerpt = redact(excerpt.trim());
    if excerpt.is_empty() {
        format!("the server returned {status}")
    } else {
        format!("the server returned {status}: {excerpt}")
    }
}

/// Replaces `access_token=`, `Bearer` and `Authorization` values with `[REDACTED]`.
fn redact(text: &str) -> String {
    // Authorization values run to the end of the line; token values end at a delimiter.
    let token_end = |character: char| {
        character.is_whitespace() || matches!(character, '&' | '"' | '\'' | ',' | ';')
    };
    let line_end = |character: char| matches!(character, '\r' | '\n' | '"' | '\'');
    let markers: [(&str, &dyn Fn(char) -> bool); 3] = [
        ("authorization:", &line_end),
        ("access_token=", &token_end),
        ("bearer ", &token_end),
    ];
    // ASCII lowercasing keeps byte offsets, and every index below is a marker end (ASCII) or a
    // `find` result, so all slices stay on character boundaries.
    let lower = text.to_ascii_lowercase();
    let mut redacted = String::with_capacity(text.len());
    let mut position = 0;
    while position < text.len() {
        let next = markers
            .iter()
            .filter_map(|(marker, ends)| {
                lower[position..]
                    .find(marker)
                    .map(|offset| (position + offset + marker.len(), ends))
            })
            .min_by_key(|(value_start, _)| *value_start);
        let Some((marker_end, ends)) = next else {
            redacted.push_str(&text[position..]);
            break;
        };
        let value_start = marker_end
            + (text[marker_end..].len() - text[marker_end..].trim_start_matches([' ', '\t']).len());
        let value_end = text[value_start..]
            .find(|character: char| ends(character))
            .map_or(text.len(), |offset| value_start + offset);
        redacted.push_str(&text[position..value_start]);
        if value_end > value_start {
            redacted.push_str("[REDACTED]");
        }
        position = value_end;
    }
    redacted
}

#[cfg(test)]
mod negotiate_http_errors {
    use super::{failure, EXCERPT_BYTES};
    use http::StatusCode;

    #[test]
    fn failures_report_the_status_and_a_body_excerpt() {
        let message = failure(
            StatusCode::SERVICE_UNAVAILABLE,
            b"negotiation temporarily rejected",
        );
        assert_eq!(
            message,
            "the server returned 503 Service Unavailable: negotiation temporarily rejected"
        );
        assert_eq!(
            failure(StatusCode::UNAUTHORIZED, b""),
            "the server returned 401 Unauthorized"
        );
    }

    #[test]
    fn failure_excerpts_redact_credentials() {
        let message = failure(
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
            let message = failure(StatusCode::BAD_REQUEST, body);
            assert!(!message.contains("ünicode-token"), "{message}");
        }
        // Cut inside a multi-byte character at the excerpt bound.
        let mut body = vec![b'a'; EXCERPT_BYTES - 1];
        body.extend("üaccess_token=secret".as_bytes());
        let message = failure(StatusCode::BAD_REQUEST, &body);
        assert!(!message.contains("secret"), "{message}");
    }

    #[test]
    fn authorization_redaction_stops_at_the_line_end() {
        let message = failure(
            StatusCode::BAD_REQUEST,
            b"Authorization: Basic c2VjcmV0\r\nnext line stays",
        );
        assert!(!message.contains("c2VjcmV0"), "{message}");
        assert!(message.contains("next line stays"), "{message}");
    }

    #[test]
    fn failure_excerpts_are_bounded() {
        let body = vec![b'x'; 10 * EXCERPT_BYTES];
        let message = failure(StatusCode::INTERNAL_SERVER_ERROR, &body);
        assert!(message.len() < EXCERPT_BYTES + 64, "{}", message.len());
    }
}
