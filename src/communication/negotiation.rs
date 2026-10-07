//! The negotiate state machine shared by native and browser clients: request → outcome →
//! (redirect →)* connection, plus the direct WebSocket path when negotiation is skipped.
//!
//! The HTTP request itself is injected, so the loop is unit-tested with canned responses and each
//! target supplies its own transport (hyper over the connection's TLS config natively, the
//! browser's fetch in WASM).

use std::future::Future;

use crate::client::{Authentication, ConnectionRecipe};
use crate::communication::transport::TransportKind;
use crate::error::{Error, TransportAttempt};
use crate::protocol::negotiate::NegotiateResponse;

use super::common::ConnectionData;

/// The most redirects one negotiation follows (as the official clients do).
pub(crate) const MAX_REDIRECTS: usize = 100;

/// One parsed negotiate response.
#[derive(Debug)]
pub(crate) enum NegotiateOutcome {
    /// A connection (version 0 `connectionId`, or version 1 with `connectionToken`).
    Connection(NegotiateResponse),
    /// A redirect to another service (for example Azure SignalR) with an optional access token.
    Redirect {
        url: String,
        access_token: Option<String>,
    },
    /// The server refused negotiation with a message.
    Error(String),
}

/// Classifies a negotiate response body; unrecognized bodies are errors naming the endpoint.
pub(crate) fn parse_outcome(text: &str, endpoint: &str) -> Result<NegotiateOutcome, Error> {
    let unrecognized = |detail: String| {
        Error::negotiate(
            None,
            format!("unrecognized negotiate response from {endpoint}: {detail}"),
        )
    };
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|error| unrecognized(error.to_string()))?;
    if let Some(error) = value.get("error").and_then(serde_json::Value::as_str) {
        return Ok(NegotiateOutcome::Error(error.to_string()));
    }
    if let Some(url) = value.get("url").and_then(serde_json::Value::as_str) {
        return Ok(NegotiateOutcome::Redirect {
            url: url.to_string(),
            access_token: value
                .get("accessToken")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        });
    }
    if value.get("connectionId").is_some() || value.get("connectionToken").is_some() {
        return NegotiateResponse::from_json(text)
            .map(NegotiateOutcome::Connection)
            .map_err(|error| unrecognized(error.to_string()));
    }
    Err(unrecognized(
        "no connectionId, url or error field".to_string(),
    ))
}

/// The longest body excerpt a negotiate failure reports.
pub(crate) const EXCERPT_BYTES: usize = 256;

/// A non-2xx negotiate response (`status` with its `reason` phrase): an authentication error for
/// 401 and 403, else a negotiate error, each with the status and a short, credential-redacted body
/// excerpt.
pub(crate) fn negotiate_failure(status: u16, reason: &str, body: &[u8]) -> Error {
    let message = negotiate_failure_message(status, reason, body);
    match status {
        401 | 403 => Error::authentication(Some(status), message),
        _ => Error::negotiate(Some(status), message),
    }
}

/// Describes a non-2xx negotiate response with a short, credential-redacted body excerpt.
pub(crate) fn negotiate_failure_message(status: u16, reason: &str, body: &[u8]) -> String {
    let excerpt = String::from_utf8_lossy(&body[..body.len().min(EXCERPT_BYTES)]);
    let excerpt = crate::error::redact(excerpt.trim());
    let status = if reason.is_empty() {
        status.to_string()
    } else {
        format!("{status} {reason}")
    };
    if excerpt.is_empty() {
        format!("the server returned {status}")
    } else {
        format!("the server returned {status}: {excerpt}")
    }
}

/// The negotiate URL for a hub base URL, keeping any query (Azure SignalR redirect URLs carry one).
pub(crate) fn negotiate_url(base: &str, stateful: bool) -> String {
    let (path, query) = split_query(base);
    let mut url = format!("{}negotiate", with_trailing_slash(path));
    let mut separator = '?';
    if let Some(query) = query {
        url.push('?');
        url.push_str(query);
        separator = '&';
    }
    if !url.contains("negotiateVersion=") {
        url.push(separator);
        url.push_str("negotiateVersion=1");
        separator = '&';
    }
    if stateful {
        url.push(separator);
        url.push_str("useStatefulReconnect=true");
    }
    url
}

/// Percent-encodes a query value like the official clients (`encodeURIComponent`), keeping only
/// RFC 3986 unreserved characters; tokens with `+`, `/`, `=` or spaces survive the query intact.
pub(crate) fn encode_query_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

/// The WebSocket URL for a hub base URL: `http`→`ws`, `https`→`wss`, plus percent-encoded query
/// parameters.
pub(crate) fn socket_url(base: &str, parameters: &[(&str, &str)]) -> String {
    let mut url = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base.to_string()
    };
    for (name, value) in parameters {
        url.push(if url.contains('?') { '&' } else { '?' });
        url.push_str(name);
        url.push('=');
        url.push_str(&encode_query_value(value));
    }
    url
}

fn split_query(url: &str) -> (&str, Option<&str>) {
    match url.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (url, None),
    }
}

fn with_trailing_slash(path: &str) -> String {
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

/// Negotiates (following redirects) or, with `skip_negotiation`, connects directly.
///
/// `request(url, authorization)` performs one negotiate POST and returns the body text.
pub(crate) async fn negotiate_with<F, Fut>(
    recipe: &ConnectionRecipe,
    mut request: F,
) -> Result<ConnectionData, Error>
where
    F: FnMut(String, Option<String>) -> Fut,
    Fut: Future<Output = Result<String, Error>>,
{
    if recipe.skips_negotiation() {
        return direct_connection(recipe);
    }

    let stateful = recipe.stateful_reconnect().is_some();
    let mut base = recipe.web_url();
    let mut authentication = recipe.authentication();
    let mut redirects = 0;
    loop {
        let url = negotiate_url(&base, stateful);
        let text = request(url, authentication.header_value())
            .await
            .map_err(|error| {
                error.context(format!("negotiation with {} failed", display(&base)))
            })?;
        match parse_outcome(&text, &display(&base))? {
            NegotiateOutcome::Connection(response) => {
                return connection_data(recipe, &base, &authentication, response);
            }
            NegotiateOutcome::Redirect { url, access_token } => {
                redirects += 1;
                if redirects > MAX_REDIRECTS {
                    return Err(Error::negotiate(
                        None,
                        format!(
                            "negotiation with {} failed: too many negotiate redirects",
                            recipe.display_endpoint()
                        ),
                    ));
                }
                base = url;
                if let Some(token) = access_token {
                    authentication = Authentication::Bearer { token };
                }
            }
            NegotiateOutcome::Error(message) => {
                return Err(Error::negotiate(
                    None,
                    format!(
                        "the server at {} rejected negotiation: {message}",
                        display(&base)
                    ),
                ));
            }
        }
    }
}

/// The endpoint without its query, which may carry tokens.
fn display(base: &str) -> String {
    split_query(base).0.to_string()
}

fn bearer_parameter(authentication: &Authentication) -> Option<&str> {
    match authentication {
        Authentication::Bearer { token } => Some(token.as_str()),
        _ => None,
    }
}

fn connection_data(
    recipe: &ConnectionRecipe,
    base: &str,
    authentication: &Authentication,
    response: NegotiateResponse,
) -> Result<ConnectionData, Error> {
    let protocol = recipe.protocol();
    let format = protocol.transfer_format();
    let supported = response.available_transports().iter().any(|transport| {
        transport.transport == TransportKind::WebSockets.name()
            && transport
                .transfer_formats
                .iter()
                .any(|candidate| candidate == format)
    });
    if !supported {
        let transport = TransportKind::WebSockets;
        return Err(Error::transport_unavailable(
            format!(
                "the server offers no transport this client supports for the {format} transfer \
                 format"
            ),
            vec![TransportAttempt::new(
                transport,
                Error::transport_failure(
                    Some(transport),
                    format!(
                        "the server does not offer {transport} with the {format} transfer format"
                    ),
                ),
            )],
        ));
    }
    let stateful = if response.use_stateful_reconnect() {
        recipe.stateful_reconnect()
    } else {
        None
    };
    let mut parameters = vec![("id", response.transport_id())];
    // Browsers cannot set WebSocket headers, so bearer tokens travel as `access_token`.
    if let Some(token) = bearer_parameter(authentication) {
        parameters.push(("access_token", token));
    }
    Ok(ConnectionData::new(
        socket_url(base, &parameters),
        response.connection_id().to_string(),
        protocol,
        recipe
            .clone()
            .with_transport_authentication(authentication.clone()),
        stateful,
    ))
}

/// Builds the WebSocket connection directly from the configured endpoint, without negotiating.
fn direct_connection(recipe: &ConnectionRecipe) -> Result<ConnectionData, Error> {
    if recipe.stateful_reconnect().is_some() {
        return Err(Error::configuration("skip_negotiation", SKIP_WITH_STATEFUL));
    }
    let authentication = recipe.authentication();
    let parameters: Vec<(&str, &str)> = bearer_parameter(&authentication)
        .map(|token| vec![("access_token", token)])
        .unwrap_or_default();
    Ok(ConnectionData::new(
        socket_url(&recipe.web_url(), &parameters),
        String::new(),
        recipe.protocol(),
        recipe.clone(),
        None,
    ))
}

/// Returned when `skip_negotiation` and stateful reconnect are combined.
pub(crate) const SKIP_WITH_STATEFUL: &str = "skip_negotiation() cannot be combined with \
with_stateful_reconnect(): stateful reconnect needs the connection token issued by negotiation";

#[cfg(test)]
mod negotiation_tests {
    use std::{cell::RefCell, rc::Rc};

    use super::{negotiate_url, negotiate_with, parse_outcome, socket_url, NegotiateOutcome};
    use crate::client::ConnectionConfiguration;
    use crate::StatefulReconnectConfig;

    type Requests = Rc<RefCell<Vec<(String, Option<String>)>>>;

    /// Runs `negotiate_with` against canned responses, recording each request.
    fn negotiate(
        configure: impl FnOnce(&mut ConnectionConfiguration),
        responses: Vec<&str>,
    ) -> (Result<super::ConnectionData, crate::Error>, Requests) {
        let mut configuration = ConnectionConfiguration::new("localhost".into(), "hub".into());
        configuration.unsecure();
        configuration.with_port(5000);
        configure(&mut configuration);
        let recipe = configuration.connection_recipe();
        let requests: Requests = Rc::default();
        let recorded = requests.clone();
        let responses = Rc::new(RefCell::new(
            responses
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>(),
        ));
        let result = futures::executor::block_on(negotiate_with(&recipe, move |url, auth| {
            recorded.borrow_mut().push((url, auth));
            let next = responses.borrow_mut().remove(0);
            async move { Ok(next) }
        }));
        (result, requests)
    }

    const V1: &str = r#"{"connectionId":"cid","connectionToken":"token","negotiateVersion":1,
        "availableTransports":[{"transport":"WebSockets","transferFormats":["Text","Binary"]}]}"#;

    #[test]
    fn negotiation_parses_connection_redirect_and_error_outcomes() {
        assert!(matches!(
            parse_outcome(V1, "http://host/hub"),
            Ok(NegotiateOutcome::Connection(_))
        ));
        let azure =
            r#"{"url":"https://demo.service.signalr.net/client/?hub=chat","accessToken":"jwt"}"#;
        match parse_outcome(azure, "http://host/hub").unwrap() {
            NegotiateOutcome::Redirect { url, access_token } => {
                assert_eq!(url, "https://demo.service.signalr.net/client/?hub=chat");
                assert_eq!(access_token.as_deref(), Some("jwt"));
            }
            other => panic!("expected a redirect, got {other:?}"),
        }
        match parse_outcome(r#"{"error":"Hub is disabled"}"#, "http://host/hub").unwrap() {
            NegotiateOutcome::Error(message) => assert_eq!(message, "Hub is disabled"),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    #[test]
    fn negotiation_rejects_unrecognized_bodies_naming_the_endpoint() {
        for body in ["<html>oops</html>", r#"{"unexpected":true}"#, ""] {
            let error = parse_outcome(body, "http://host/hub").unwrap_err();
            assert_eq!(error.kind(), crate::ErrorKind::Negotiate);
            assert!(error.to_string().contains("http://host/hub"), "{error}");
        }
    }

    #[test]
    fn negotiation_v0_uses_the_connection_id_and_v1_the_token() {
        let v0 = r#"{"connectionId":"cid","negotiateVersion":0,
            "availableTransports":[{"transport":"WebSockets","transferFormats":["Text"]}]}"#;
        let (result, _) = negotiate(|_| {}, vec![v0]);
        assert_eq!(
            result.unwrap().get_endpoint(),
            "ws://localhost:5000/hub?id=cid"
        );
        let (result, requests) = negotiate(|_| {}, vec![V1]);
        let data = result.unwrap();
        assert_eq!(data.get_endpoint(), "ws://localhost:5000/hub?id=token");
        assert_eq!(data.get_connection_id(), "cid");
        assert_eq!(
            requests.borrow()[0].0,
            "http://localhost:5000/hub/negotiate?negotiateVersion=1"
        );
    }

    #[test]
    fn negotiation_error_responses_fail_connect_with_the_server_text() {
        let (result, _) = negotiate(|_| {}, vec![r#"{"error":"Hub is disabled"}"#]);
        let error = result.err().unwrap();
        assert_eq!(error.kind(), crate::ErrorKind::Negotiate);
        assert!(error.to_string().contains("Hub is disabled"), "{error}");
    }

    #[test]
    fn negotiation_follows_redirects_with_the_redirect_token() {
        let redirect = r#"{"url":"https://demo.service.signalr.net/client/?hub=chat&asrs.op=%2Fclient","accessToken":"redirect-token"}"#;
        let (result, requests) = negotiate(
            |configuration| {
                configuration.authenticate_bearer("configured-token".to_string());
            },
            vec![redirect, V1],
        );
        let data = result.unwrap();
        let requests = requests.borrow();
        assert_eq!(requests[0].1.as_deref(), Some("Bearer configured-token"));
        assert_eq!(
            requests[1].0,
            "https://demo.service.signalr.net/client/negotiate?hub=chat&asrs.op=%2Fclient&negotiateVersion=1"
        );
        assert_eq!(requests[1].1.as_deref(), Some("Bearer redirect-token"));
        assert_eq!(
            data.get_endpoint(),
            "wss://demo.service.signalr.net/client/?hub=chat&asrs.op=%2Fclient&id=token&access_token=redirect-token"
        );
        assert_eq!(
            data.get_recipe().transport_authorization().as_deref(),
            Some("Bearer redirect-token")
        );
        assert_eq!(
            data.get_recipe().authentication().header_value().as_deref(),
            Some("Bearer configured-token"),
            "ordinary reconnect renegotiates from the configured endpoint and credentials"
        );
    }

    #[test]
    fn negotiation_redirects_keep_the_stateful_flag() {
        let redirect = r#"{"url":"http://other:6000/hub"}"#;
        let (result, requests) = negotiate(
            |configuration| {
                configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
            },
            vec![redirect, V1],
        );
        result.unwrap();
        let requests = requests.borrow();
        assert!(requests[0].0.ends_with("&useStatefulReconnect=true"));
        assert_eq!(
            requests[1].0,
            "http://other:6000/hub/negotiate?negotiateVersion=1&useStatefulReconnect=true"
        );
    }

    #[test]
    fn negotiation_stops_after_too_many_redirects() {
        let redirect = r#"{"url":"http://loop/hub"}"#;
        let (result, requests) = negotiate(|_| {}, vec![redirect; super::MAX_REDIRECTS + 1]);
        let error = result.err().unwrap();
        assert_eq!(error.kind(), crate::ErrorKind::Negotiate);
        assert!(
            error.to_string().contains("too many negotiate redirects"),
            "{error}"
        );
        assert_eq!(requests.borrow().len(), super::MAX_REDIRECTS + 1);
    }

    #[test]
    fn negotiation_urls_and_socket_urls_follow_the_official_clients() {
        assert_eq!(
            negotiate_url("https://host/hub", false),
            "https://host/hub/negotiate?negotiateVersion=1"
        );
        assert_eq!(
            negotiate_url("https://host/client/?hub=x", true),
            "https://host/client/negotiate?hub=x&negotiateVersion=1&useStatefulReconnect=true"
        );
        assert_eq!(
            socket_url("https://host/hub", &[("id", "t")]),
            "wss://host/hub?id=t"
        );
        assert_eq!(socket_url("http://host/hub", &[]), "ws://host/hub");
    }

    #[test]
    fn negotiation_percent_encodes_tokens_in_the_socket_url() {
        let redirect = r#"{"url":"https://demo/client/?hub=chat","accessToken":"a+b/c=d e\"<"}"#;
        let (result, _) = negotiate(|_| {}, vec![redirect, V1]);
        let endpoint = result.unwrap().get_endpoint();
        assert_eq!(
            endpoint,
            "wss://demo/client/?hub=chat&id=token&access_token=a%2Bb%2Fc%3Dd%20e%22%3C"
        );
        assert!(endpoint.parse::<http::Uri>().is_ok(), "{endpoint}");
    }

    #[test]
    fn negotiation_errors_never_echo_tokens_from_the_endpoint() {
        let redirect = r#"{"url":"https://demo.service.signalr.net/client/?hub=chat&access_token=leak","accessToken":"redirect-token"}"#;
        let (result, _) = negotiate(|_| {}, vec![redirect, r#"{"error":"denied"}"#]);
        let error = result.err().unwrap().to_string();
        assert!(!error.contains("leak"), "{error}");
        assert!(!error.contains("redirect-token"), "{error}");
    }
}

#[cfg(test)]
mod skip_negotiation {
    use super::{negotiate_with, SKIP_WITH_STATEFUL};
    use crate::client::ConnectionConfiguration;
    use crate::StatefulReconnectConfig;

    fn direct(
        configure: impl FnOnce(&mut ConnectionConfiguration),
    ) -> (Result<super::ConnectionData, crate::Error>, usize) {
        let mut configuration = ConnectionConfiguration::new("localhost".into(), "hub".into());
        configuration.with_port(5001);
        configuration.skip_negotiation();
        configure(&mut configuration);
        let recipe = configuration.connection_recipe();
        let mut requests = 0;
        let result = futures::executor::block_on(negotiate_with(&recipe, |_, _| {
            requests += 1;
            async {
                Err(crate::Error::negotiate(
                    None,
                    "negotiate must not be requested",
                ))
            }
        }));
        (result, requests)
    }

    #[test]
    fn skip_negotiation_connects_directly_without_a_request() {
        let (result, requests) = direct(|_| {});
        assert_eq!(requests, 0);
        assert_eq!(result.unwrap().get_endpoint(), "wss://localhost:5001/hub");
        let (result, _) = direct(|configuration| {
            configuration.unsecure();
        });
        assert_eq!(result.unwrap().get_endpoint(), "ws://localhost:5001/hub");
    }

    #[test]
    fn skip_negotiation_appends_the_bearer_token() {
        let (result, _) = direct(|configuration| {
            configuration.authenticate_bearer("direct-token".to_string());
        });
        assert_eq!(
            result.unwrap().get_endpoint(),
            "wss://localhost:5001/hub?access_token=direct-token"
        );
    }

    #[test]
    fn skip_negotiation_percent_encodes_the_bearer_token() {
        let (result, _) = direct(|configuration| {
            configuration.authenticate_bearer("a+b/c= d".to_string());
        });
        assert_eq!(
            result.unwrap().get_endpoint(),
            "wss://localhost:5001/hub?access_token=a%2Bb%2Fc%3D%20d"
        );
    }

    #[test]
    fn skip_negotiation_rejects_stateful_reconnect_before_io() {
        let (result, requests) = direct(|configuration| {
            configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        });
        assert_eq!(requests, 0);
        let error = result.err().unwrap();
        assert_eq!(error.setting(), Some("skip_negotiation"));
        assert!(error.to_string().contains(SKIP_WITH_STATEFUL), "{error}");
    }
}
