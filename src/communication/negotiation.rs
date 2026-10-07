//! The negotiate state machine shared by native and browser clients: request → outcome →
//! (redirect →)* connection, plus the direct WebSocket path when negotiation is skipped.
//!
//! The HTTP request itself is injected, so the loop is unit-tested with canned responses and each
//! target supplies its own transport (hyper over the connection's TLS config natively, the
//! browser's fetch in WASM).

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
    // A classic ASP.NET SignalR server answers negotiate with `ProtocolVersion`.
    if value.get("ProtocolVersion").is_some() {
        return Err(Error::negotiate(None, CLASSIC_SIGNALR));
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

/// The official clients' error for a classic ASP.NET SignalR server.
pub(crate) const CLASSIC_SIGNALR: &str = "Detected a connection attempt to an ASP.NET SignalR \
Server. This client only supports connecting to an ASP.NET Core SignalR Server. See \
https://aka.ms/signalr-core-differences for details.";

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
    let excerpt = String::from_utf8_lossy(body.get(..EXCERPT_BYTES).unwrap_or(body));
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
/// Negotiates through `request`. `bearer_in_query` puts a bearer token into the WebSocket URL as
/// `access_token` (browsers, which cannot set WebSocket headers); natively the token travels only
/// in the `Authorization` header, as the official .NET client sends it outside browsers.
pub(crate) async fn negotiate_with<F, Fut>(
    recipe: &ConnectionRecipe,
    bearer_in_query: bool,
    mut request: F,
) -> Result<ConnectionData, Error>
where
    F: FnMut(String, Option<String>) -> Fut,
    Fut: Future<Output = Result<String, Error>>,
{
    if recipe.skips_negotiation() {
        return direct_connection(recipe, bearer_in_query);
    }

    let stateful = recipe.stateful_reconnect().is_some();
    let mut base = recipe.web_url();
    let mut authentication = recipe.authentication();
    let mut redirected = false;
    let mut redirects = 0;
    loop {
        // An access-token factory is asked for a fresh token for every negotiate request,
        // redirect hops included, until a redirect issues the token.
        if !redirected {
            authentication = recipe.authentication().resolve().await?;
        }
        let url = negotiate_url(&base, stateful);
        let text = request(url, authentication.header_value())
            .await
            .map_err(|error| {
                // A timeout names only the bound: `negotiate timed out after <bound>`.
                if error.kind() == crate::ErrorKind::Timeout {
                    error
                } else {
                    error.context(format!("negotiation with {} failed", display(&base)))
                }
            })?;
        match parse_outcome(&text, &display(&base))? {
            NegotiateOutcome::Connection(response) => {
                return connection_data(
                    recipe,
                    &base,
                    &authentication,
                    redirected,
                    response,
                    bearer_in_query,
                );
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
                    redirected = true;
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

/// The transports to try for one negotiation, in the server's `availableTransports` order (as
/// the .NET client tries them), and the offered transports the client skips, each with why.
#[derive(Clone, Debug, Default)]
pub(crate) struct TransportSelection {
    pub(crate) candidates: Vec<TransportKind>,
    pub(crate) skipped: Vec<TransportAttempt>,
}

/// Selects the transports to try from the server's offers: those in `configured` that carry
/// `format` (Server-Sent Events: text only) and that this client can start; unknown transports
/// are ignored, as the .NET client ignores them.
pub(crate) fn select_transports(
    offered: &[crate::protocol::negotiate::TransportSpec],
    configured: crate::Transports,
    format: &str,
) -> TransportSelection {
    let mut selection = TransportSelection::default();
    for spec in offered {
        let Some(kind) = TransportKind::from_name(&spec.transport) else {
            continue;
        };
        if selection.candidates.contains(&kind)
            || selection
                .skipped
                .iter()
                .any(|attempt| attempt.transport() == kind)
        {
            continue;
        }
        let skip = if !configured.contains(kind) {
            Some("the transport is disabled by the client".to_string())
        } else if (kind == TransportKind::ServerSentEvents && format != "Text")
            || !spec
                .transfer_formats
                .iter()
                .any(|offered| offered == format)
        {
            Some(format!(
                "the transport does not support the {format} transfer format"
            ))
        } else if !kind.implemented() {
            Some("this client cannot start the transport yet".to_string())
        } else {
            None
        };
        match skip {
            Some(reason) => selection.skipped.push(TransportAttempt::new(
                kind,
                Error::transport_failure(Some(kind), reason),
            )),
            None => selection.candidates.push(kind),
        }
    }
    selection
}

/// The error of a connection whose transports could not be started: a single failed attempt
/// keeps its own error; several, or none at all, become `TransportUnavailable` listing every
/// skipped and failed transport.
pub(crate) fn transports_failed(
    skipped: Vec<TransportAttempt>,
    mut failed: Vec<TransportAttempt>,
) -> Error {
    if failed.len() == 1 {
        if let Some(attempt) = failed.pop() {
            return attempt.error().clone();
        }
    }
    let message = if failed.is_empty() {
        "the server offers no transport this client can use"
    } else {
        "no transport could be started"
    };
    let mut attempts = skipped;
    attempts.extend(failed);
    Error::transport_unavailable(message, attempts)
}

/// Negotiates and starts the first transport that starts, as the .NET client's
/// `SelectAndStartTransport`: the negotiated transports in the server's order; after a failed
/// start the next one is tried with a fresh negotiation (a new connection token), which requests
/// stateful reconnect only for WebSockets; a failed renegotiation counts as that transport's
/// failure. The first negotiation's errors are returned as they are; when no transport starts,
/// [`transports_failed`] reports every skipped and failed one.
pub(crate) async fn start_with_fallback<N, NF, S, SF, T>(
    recipe: &ConnectionRecipe,
    mut negotiate: N,
    mut start: S,
) -> Result<(ConnectionData, TransportKind, T), Error>
where
    N: FnMut(ConnectionRecipe) -> NF,
    NF: Future<Output = Result<ConnectionData, Error>>,
    S: FnMut(ConnectionData, TransportKind) -> SF,
    SF: Future<Output = Result<T, Error>>,
{
    let mut failed: Vec<TransportAttempt> = Vec::new();
    let mut configuration = negotiate(recipe.clone()).await?;
    let mut negotiated = true;
    loop {
        let Some(transport) = configuration.next_transport(&failed) else {
            return Err(transports_failed(configuration.skipped(), failed));
        };
        if !negotiated {
            match negotiate(recipe.for_transport(transport)).await {
                Ok(fresh) => {
                    configuration = fresh;
                    negotiated = true;
                }
                Err(error) => failed.push(TransportAttempt::new(transport, error)),
            }
            continue;
        }
        negotiated = false;
        match start(configuration.clone(), transport).await {
            Ok(started) => return Ok((configuration, transport, started)),
            Err(error) => {
                log::info!("The {transport} transport could not be started: {error}");
                failed.push(TransportAttempt::new(transport, error));
            }
        }
    }
}

fn connection_data(
    recipe: &ConnectionRecipe,
    base: &str,
    authentication: &Authentication,
    redirected: bool,
    response: NegotiateResponse,
    bearer_in_query: bool,
) -> Result<ConnectionData, Error> {
    let protocol = recipe.protocol();
    let format = protocol.transfer_format();
    let selection = select_transports(response.available_transports(), recipe.transports(), format);
    if selection.candidates.is_empty() {
        return Err(transports_failed(selection.skipped, Vec::new()));
    }
    // Stateful reconnect only over WebSockets, as the .NET client uses it (the server accepts the
    // request whatever transport follows).
    let stateful = if response.use_stateful_reconnect()
        && selection.candidates.first() == Some(&TransportKind::WebSockets)
    {
        recipe.stateful_reconnect()
    } else {
        None
    };
    let mut parameters = vec![("id", response.transport_id())];
    // Browsers cannot set WebSocket headers, so bearer tokens travel as `access_token`.
    if let Some(token) = bearer_parameter(authentication).filter(|_| bearer_in_query) {
        parameters.push(("access_token", token));
    }
    // An access-token factory is asked again when the WebSocket opens, unless a redirect issued
    // the transport's token.
    let factory = matches!(recipe.authentication(), Authentication::Factory(_));
    let transport_recipe = if factory && !redirected {
        // HTTP transports keep the token negotiate fetched; a WebSocket asks again.
        recipe
            .clone()
            .with_negotiated_authentication(authentication.clone())
    } else {
        recipe
            .clone()
            .with_transport_authentication(authentication.clone())
    };
    Ok(ConnectionData::new(
        socket_url(base, &parameters),
        response.connection_id().to_string(),
        transport_recipe,
        stateful,
        selection,
    ))
}

/// The URL of an HTTP transport (Long Polling, Server-Sent Events requests sent with `fetch`) for
/// a negotiated transport URL: `ws(s)` becomes `http(s)` and any `access_token` query parameter
/// is dropped, because these requests carry the credentials in the `Authorization` header.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn http_transport_url(url: &str) -> String {
    let url = if let Some(rest) = url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        url.to_string()
    };
    let (path, query) = split_query(&url);
    let parameters: Vec<&str> = query
        .map(|query| {
            query
                .split('&')
                .filter(|pair| !pair.is_empty() && !pair.starts_with("access_token="))
                .collect()
        })
        .unwrap_or_default();
    if parameters.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{}", parameters.join("&"))
    }
}

/// `url` with its `access_token` query set to `token` (percent-encoded), as a browser sends the
/// fresh token of an access-token factory with a WebSocket.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn with_access_token(url: &str, token: &str) -> String {
    let (path, query) = split_query(url);
    let mut parameters: Vec<&str> = query
        .map(|query| {
            query
                .split('&')
                .filter(|pair| !pair.is_empty() && !pair.starts_with("access_token="))
                .collect()
        })
        .unwrap_or_default();
    let token = format!("access_token={}", encode_query_value(token));
    parameters.push(&token);
    format!("{path}?{}", parameters.join("&"))
}

/// Builds the WebSocket connection directly from the configured endpoint, without negotiating.
fn direct_connection(
    recipe: &ConnectionRecipe,
    bearer_in_query: bool,
) -> Result<ConnectionData, Error> {
    if recipe.stateful_reconnect().is_some() {
        // As the .NET client: stateful reconnect needs the token negotiation issues.
        log::warn!("{SKIP_WITH_STATEFUL}");
    }
    let authentication = recipe.authentication();
    // Resolved when the WebSocket opens (`fresh_transport_token` in browsers).
    let parameters: Vec<(&str, &str)> = bearer_parameter(&authentication)
        .filter(|_| bearer_in_query)
        .map(|token| vec![("access_token", token)])
        .unwrap_or_default();
    Ok(ConnectionData::new(
        socket_url(&recipe.web_url(), &parameters),
        String::new(),
        recipe.clone(),
        None,
        TransportSelection {
            candidates: vec![TransportKind::WebSockets],
            skipped: Vec::new(),
        },
    ))
}

/// Returned when `skip_negotiation` and stateful reconnect are combined.
pub(crate) const SKIP_WITH_STATEFUL: &str = "skip_negotiation() disables \
with_stateful_reconnect(): stateful reconnect needs the connection token issued by negotiation";

#[cfg(test)]
mod negotiation_tests {
    use std::{cell::RefCell, rc::Rc};

    use super::{negotiate_url, negotiate_with, parse_outcome, socket_url, NegotiateOutcome};
    use crate::client::ConnectionConfiguration;
    use crate::StatefulReconnectConfig;

    type Requests = Rc<RefCell<Vec<(String, Option<String>)>>>;

    /// Runs `negotiate_with` against canned responses, recording each request, with the
    /// browser's token placement (an `access_token` query parameter).
    pub(super) fn negotiate(
        configure: impl FnOnce(&mut ConnectionConfiguration),
        responses: Vec<&str>,
    ) -> (Result<super::ConnectionData, crate::Error>, Requests) {
        negotiate_as(true, configure, responses)
    }

    pub(super) fn negotiate_as(
        bearer_in_query: bool,
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
        let result = futures::executor::block_on(negotiate_with(
            &recipe,
            bearer_in_query,
            move |url, auth| {
                recorded.borrow_mut().push((url, auth));
                let next = responses.borrow_mut().remove(0);
                async move { Ok(next) }
            },
        ));
        (result, requests)
    }

    pub(super) const V1: &str = r#"{"connectionId":"cid","connectionToken":"token","negotiateVersion":1,
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
    use super::negotiation_tests::{negotiate, negotiate_as, V1};
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
        let result = futures::executor::block_on(negotiate_with(&recipe, true, |_, _| {
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
    fn negotiate_parity_skip_negotiation_disables_stateful_reconnect_with_a_warning() {
        let ((result, requests), records) = crate::test_logging::capture(|| {
            direct(|configuration| {
                configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
            })
        });
        assert_eq!(requests, 0);
        let data = result.unwrap();
        assert_eq!(data.get_endpoint(), "wss://localhost:5001/hub");
        assert!(
            data.stateful_reconnect().is_none(),
            "the connection is ordinary"
        );
        assert!(
            records.iter().any(|record| record.level == log::Level::Warn
                && record.message.contains(SKIP_WITH_STATEFUL)),
            "a warning explains why"
        );
    }

    #[test]
    fn negotiate_parity_a_response_without_a_version_is_version_0() {
        let (result, _) = negotiate(
            |_| {},
            vec![
                r#"{"connectionId":"legacy-id","availableTransports":[{"transport":"WebSockets","transferFormats":["Text","Binary"]}]}"#,
            ],
        );
        assert_eq!(
            result.unwrap().get_endpoint(),
            "ws://localhost:5000/hub?id=legacy-id",
            "version 0 connects with the connection id"
        );
    }

    #[test]
    fn negotiate_parity_a_classic_signalr_server_gets_its_own_error() {
        let (result, _) = negotiate(
            |_| {},
            vec![
                r#"{"Url":"/signalr","ConnectionToken":"t","ConnectionId":"c","KeepAliveTimeout":20.0,"ProtocolVersion":"1.5"}"#,
            ],
        );
        let Err(error) = result else {
            panic!("a classic server must be rejected");
        };
        assert_eq!(error.kind(), crate::ErrorKind::Negotiate, "{error}");
        assert!(
            error.to_string().contains(super::CLASSIC_SIGNALR),
            "{error}"
        );
    }

    #[test]
    fn token_placement_native_websockets_carry_the_bearer_token_only_in_the_header() {
        let (result, requests) = negotiate_as(
            false,
            |configuration| {
                configuration.authenticate_bearer("native-token".to_string());
            },
            vec![V1],
        );
        let data = result.unwrap();
        assert_eq!(data.get_endpoint(), "ws://localhost:5000/hub?id=token");
        assert_eq!(
            data.get_recipe().transport_authorization().as_deref(),
            Some("Bearer native-token")
        );
        assert_eq!(
            requests.borrow()[0].1.as_deref(),
            Some("Bearer native-token")
        );

        // Browsers cannot set WebSocket headers: the token travels as `access_token`.
        let (browser, _) = negotiate(
            |configuration| {
                configuration.authenticate_bearer("native-token".to_string());
            },
            vec![V1],
        );
        assert!(browser
            .unwrap()
            .get_endpoint()
            .ends_with("&access_token=native-token"));
    }

    #[test]
    fn token_placement_a_redirect_token_travels_in_the_header_natively() {
        let redirect = r#"{"url":"https://demo.service.signalr.net/client/?hub=chat","accessToken":"redirect-token"}"#;
        let (result, _) = negotiate_as(false, |_| {}, vec![redirect, V1]);
        let data = result.unwrap();
        assert!(
            !data.get_endpoint().contains("access_token"),
            "{}",
            data.get_endpoint()
        );
        assert_eq!(
            data.get_recipe().transport_authorization().as_deref(),
            Some("Bearer redirect-token")
        );
    }

    #[test]
    fn token_placement_skip_negotiation_keeps_the_token_out_of_the_native_url() {
        let mut configuration = ConnectionConfiguration::new("localhost".into(), "hub".into());
        configuration.with_port(5001);
        configuration.skip_negotiation();
        configuration.authenticate_bearer("direct-token".to_string());
        let recipe = configuration.connection_recipe();
        let result = futures::executor::block_on(negotiate_with(&recipe, false, |_, _| {
            std::future::ready(Err(crate::Error::protocol(
                "negotiate must not be requested",
            )))
        }));
        assert_eq!(result.unwrap().get_endpoint(), "wss://localhost:5001/hub");
    }
}

/// The access-token factory in negotiation (R4.C1): a fresh token per negotiate, asked again for
/// the transport, and a redirect's token used instead.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod access_token_factory {
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };

    use super::{
        negotiation_tests::{negotiate_as, V1},
        with_access_token,
    };
    use crate::client::ConnectionConfiguration;

    /// A factory that issues `factory-1`, `factory-2`, ... and counts its calls.
    fn counting(configuration: &mut ConnectionConfiguration) -> Arc<AtomicU32> {
        let calls = Arc::new(AtomicU32::new(0));
        let counted = calls.clone();
        configuration.with_access_token_factory(move || {
            let call = counted.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Ok(format!("factory-{call}")) }
        });
        calls
    }

    #[test]
    fn access_token_factory_is_asked_for_negotiate_and_again_for_the_transport() {
        let mut calls = None;
        let (result, requests) = negotiate_as(false, |c| calls = Some(counting(c)), vec![V1]);
        let calls = calls.unwrap();
        let data = result.unwrap();
        assert_eq!(requests.borrow()[0].1.as_deref(), Some("Bearer factory-1"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let transport =
            futures::executor::block_on(data.get_recipe().current_transport_authorization())
                .unwrap();
        assert_eq!(
            transport.as_deref(),
            Some("Bearer factory-2"),
            "a fresh token"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn access_token_factory_a_redirect_token_wins() {
        let redirect = r#"{"url":"https://demo.service.signalr.net/client/?hub=chat","accessToken":"redirect-token"}"#;
        let mut calls = None;
        let (result, requests) =
            negotiate_as(false, |c| calls = Some(counting(c)), vec![redirect, V1]);
        let calls = calls.unwrap();
        let data = result.unwrap();
        let requests = requests.borrow();
        assert_eq!(requests[0].1.as_deref(), Some("Bearer factory-1"));
        assert_eq!(requests[1].1.as_deref(), Some("Bearer redirect-token"));
        let transport =
            futures::executor::block_on(data.get_recipe().current_transport_authorization())
                .unwrap();
        assert_eq!(transport.as_deref(), Some("Bearer redirect-token"));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the redirect's token is not replaced"
        );
    }

    #[test]
    fn access_token_factory_is_asked_again_on_a_redirect_hop_without_a_token() {
        let redirect = r#"{"url":"https://demo.service.signalr.net/client/?hub=chat"}"#;
        let mut calls = None;
        let (result, requests) =
            negotiate_as(false, |c| calls = Some(counting(c)), vec![redirect, V1]);
        result.unwrap();
        let requests = requests.borrow();
        assert_eq!(requests[0].1.as_deref(), Some("Bearer factory-1"));
        assert_eq!(requests[1].1.as_deref(), Some("Bearer factory-2"));
        assert_eq!(calls.unwrap().load(Ordering::SeqCst), 2);
    }

    #[test]
    fn access_token_factory_browser_resumes_keep_a_redirect_token() {
        // Browsers resume with the negotiated recipe: a redirect's token stays in the URL, a
        // factory token is replaced by a fresh one.
        let redirect = r#"{"url":"https://demo.service.signalr.net/client/?hub=chat","accessToken":"redirect-token"}"#;
        let (redirected, _) = negotiate_as(true, |c| drop(counting(c)), vec![redirect, V1]);
        let redirected = redirected.unwrap();
        assert!(redirected
            .get_endpoint()
            .contains("access_token=redirect-token"));
        assert_eq!(
            futures::executor::block_on(redirected.get_recipe().fresh_transport_token()).unwrap(),
            None
        );
        let mut calls = None;
        let (plain, _) = negotiate_as(true, |c| calls = Some(counting(c)), vec![V1]);
        assert_eq!(
            futures::executor::block_on(plain.unwrap().get_recipe().fresh_transport_token())
                .unwrap()
                .as_deref(),
            Some("factory-2")
        );
        assert_eq!(calls.unwrap().load(Ordering::SeqCst), 2);
    }

    #[test]
    fn access_token_factory_errors_fail_negotiate_redacted() {
        let (result, requests) = negotiate_as(
            false,
            |c| {
                c.with_access_token_factory(|| async {
                    Err::<String, _>("refresh failed for Bearer abc.def-secret".to_string())
                });
            },
            vec![V1],
        );
        let Err(error) = result else {
            panic!("the factory error fails negotiate")
        };
        assert_eq!(error.kind(), crate::ErrorKind::Authentication);
        let text = error.to_string();
        assert!(text.contains("access token factory failed"), "{text}");
        assert!(!text.contains("abc.def-secret"), "{text}");
        assert!(requests.borrow().is_empty(), "no request was sent");
    }

    #[test]
    fn access_token_factory_browser_urls_carry_the_fresh_token() {
        assert_eq!(
            with_access_token("wss://host/hub?id=abc&access_token=old", "new+token"),
            "wss://host/hub?id=abc&access_token=new%2Btoken"
        );
        assert_eq!(
            with_access_token("wss://host/hub?id=abc", "t"),
            "wss://host/hub?id=abc&access_token=t"
        );
    }
}

/// Transport selection (R5.C1): the server's order, the configured set, the transfer format the
/// hub protocol needs (Server-Sent Events: text only), and the errors when nothing can be used.
#[cfg(test)]
mod transport_selection {
    use super::{select_transports, transports_failed, TransportSelection};
    use crate::{
        protocol::negotiate::TransportSpec, Error, ErrorKind, TransportAttempt, TransportKind,
        Transports,
    };

    fn offer(transports: &[(&str, &[&str])]) -> Vec<TransportSpec> {
        transports
            .iter()
            .map(|(name, formats)| TransportSpec {
                transport: (*name).to_string(),
                transfer_formats: formats.iter().map(|format| (*format).to_string()).collect(),
            })
            .collect()
    }

    fn all_offered() -> Vec<TransportSpec> {
        offer(&[
            ("WebSockets", &["Text", "Binary"]),
            ("ServerSentEvents", &["Text"]),
            ("LongPolling", &["Text", "Binary"]),
        ])
    }

    fn skipped(selection: &TransportSelection) -> Vec<TransportKind> {
        selection
            .skipped
            .iter()
            .map(TransportAttempt::transport)
            .collect()
    }

    #[test]
    fn transport_selection_follows_the_server_order() {
        let offered = offer(&[("LongPolling", &["Text"]), ("WebSockets", &["Text"])]);
        let selection = select_transports(&offered, Transports::all(), "Text");
        assert_eq!(
            selection.candidates,
            [TransportKind::LongPolling, TransportKind::WebSockets]
        );
        assert!(selection.skipped.is_empty());
    }

    #[test]
    fn transport_selection_keeps_the_configured_set() {
        let selection = select_transports(&all_offered(), Transports::LONG_POLLING, "Text");
        assert_eq!(selection.candidates, [TransportKind::LongPolling]);
        assert_eq!(
            skipped(&selection),
            [TransportKind::WebSockets, TransportKind::ServerSentEvents]
        );
        assert!(selection.skipped[0]
            .error()
            .to_string()
            .contains("disabled by the client"));
    }

    #[test]
    fn transport_selection_never_pairs_server_sent_events_with_binary() {
        let lying = offer(&[("ServerSentEvents", &["Text", "Binary"])]);
        let selection = select_transports(&lying, Transports::all(), "Binary");
        assert!(selection.candidates.is_empty());
        assert!(selection.skipped[0]
            .error()
            .to_string()
            .contains("Binary transfer format"));
    }

    #[test]
    fn transport_selection_requires_the_transfer_format() {
        let offered = offer(&[("WebSockets", &["Text"])]);
        let selection = select_transports(&offered, Transports::all(), "Binary");
        assert!(selection.candidates.is_empty());
        let text = select_transports(&offered, Transports::all(), "Text");
        assert_eq!(text.candidates, [TransportKind::WebSockets]);
    }

    #[test]
    fn transport_selection_ignores_unknown_and_repeated_transports() {
        let offered = offer(&[
            ("WebTransport", &["Binary"]),
            ("WebSockets", &["Text"]),
            ("WebSockets", &["Text"]),
        ]);
        let selection = select_transports(&offered, Transports::all(), "Text");
        assert_eq!(selection.candidates, [TransportKind::WebSockets]);
        assert!(selection.skipped.is_empty());
    }

    #[test]
    fn transport_selection_covers_every_set_format_and_offer() {
        let sets = [
            Transports::WEB_SOCKETS,
            Transports::SERVER_SENT_EVENTS,
            Transports::LONG_POLLING,
            Transports::WEB_SOCKETS | Transports::LONG_POLLING,
            Transports::all(),
        ];
        let offers = [
            all_offered(),
            offer(&[("WebSockets", &["Text", "Binary"])]),
            offer(&[("LongPolling", &["Text", "Binary"])]),
            offer(&[("ServerSentEvents", &["Text"])]),
            Vec::new(),
        ];
        for set in sets {
            for offered in &offers {
                for format in ["Text", "Binary"] {
                    let selection = select_transports(offered, set, format);
                    for kind in &selection.candidates {
                        assert!(set.contains(*kind));
                        assert!(kind.implemented());
                        assert!(!(*kind == TransportKind::ServerSentEvents && format == "Binary"));
                    }
                    // Every offered, known transport is either a candidate or skipped, once.
                    assert_eq!(
                        selection.candidates.len() + selection.skipped.len(),
                        offered.len()
                    );
                }
            }
        }
    }

    /// Every configured set, every order of every subset of the server's offers (with the
    /// formats ASP.NET Core offers) and both transfer formats, against an expected outcome
    /// written out independently of `select_transports`.
    #[test]
    fn transport_selection_every_set_offer_order_and_format() {
        const KINDS: [TransportKind; 3] = [
            TransportKind::WebSockets,
            TransportKind::ServerSentEvents,
            TransportKind::LongPolling,
        ];
        fn formats(kind: TransportKind) -> &'static [&'static str] {
            if kind == TransportKind::ServerSentEvents {
                &["Text"]
            } else {
                &["Text", "Binary"]
            }
        }
        fn orders(remaining: &[TransportKind]) -> Vec<Vec<TransportKind>> {
            let mut all = vec![Vec::new()];
            for (index, kind) in remaining.iter().enumerate() {
                let mut rest = remaining.to_vec();
                rest.remove(index);
                for mut tail in orders(&rest) {
                    tail.insert(0, *kind);
                    all.push(tail);
                }
            }
            all
        }
        let sets: Vec<(Transports, Vec<TransportKind>)> = (1u8..8)
            .map(|bits| {
                let members: Vec<_> = KINDS
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| bits & (1 << index) != 0)
                    .map(|(_, kind)| *kind)
                    .collect();
                let set = members
                    .iter()
                    .map(|kind| Transports::from(*kind))
                    .reduce(|left, right| left | right)
                    .unwrap();
                (set, members)
            })
            .collect();
        let mut combinations = 0;
        for (set, members) in &sets {
            for order in orders(&KINDS) {
                for format in ["Text", "Binary"] {
                    let offered: Vec<_> = order
                        .iter()
                        .map(|kind| (kind.name(), formats(*kind)))
                        .collect();
                    let selection = select_transports(&offer(&offered), *set, format);
                    let mut expected_candidates = Vec::new();
                    let mut expected_skipped = Vec::new();
                    for kind in &order {
                        let reason = if !members.contains(kind) {
                            Some("disabled by the client")
                        } else if !formats(*kind).contains(&format) {
                            Some("transfer format")
                        } else if *kind == TransportKind::ServerSentEvents {
                            Some("cannot start")
                        } else {
                            None
                        };
                        match reason {
                            Some(reason) => expected_skipped.push((*kind, reason)),
                            None => expected_candidates.push(*kind),
                        }
                    }
                    assert_eq!(
                        selection.candidates, expected_candidates,
                        "{order:?} {format}"
                    );
                    assert_eq!(selection.skipped.len(), expected_skipped.len());
                    for (attempt, (kind, reason)) in selection.skipped.iter().zip(&expected_skipped)
                    {
                        assert_eq!(attempt.transport(), *kind, "{order:?} {format}");
                        assert!(
                            attempt.error().to_string().contains(reason),
                            "{kind}: {}",
                            attempt.error()
                        );
                    }
                    combinations += 1;
                }
            }
        }
        // 7 sets x 16 ordered offers x 2 formats.
        assert_eq!(combinations, 7 * 16 * 2);
    }

    #[test]
    fn transport_selection_skips_transports_this_client_cannot_start() {
        let selection = select_transports(&all_offered(), Transports::all(), "Text");
        assert_eq!(
            selection.candidates,
            [TransportKind::WebSockets, TransportKind::LongPolling]
        );
        assert_eq!(skipped(&selection), [TransportKind::ServerSentEvents]);
    }

    #[test]
    fn transport_selection_errors_list_every_attempt() {
        let failure =
            |kind| TransportAttempt::new(kind, Error::transport_failure(Some(kind), "refused"));
        let none = transports_failed(vec![failure(TransportKind::LongPolling)], Vec::new());
        assert_eq!(none.kind(), ErrorKind::TransportUnavailable);
        assert_eq!(none.attempts().len(), 1);

        // One failed start keeps its own error.
        let one = transports_failed(
            vec![failure(TransportKind::LongPolling)],
            vec![TransportAttempt::new(
                TransportKind::WebSockets,
                Error::timed_out(
                    crate::TimeoutOperation::Connect,
                    std::time::Duration::from_secs(1),
                ),
            )],
        );
        assert_eq!(one.kind(), ErrorKind::Timeout);

        let several = transports_failed(
            vec![failure(TransportKind::ServerSentEvents)],
            vec![
                failure(TransportKind::WebSockets),
                failure(TransportKind::LongPolling),
            ],
        );
        assert_eq!(several.kind(), ErrorKind::TransportUnavailable);
        assert_eq!(
            several
                .attempts()
                .iter()
                .map(TransportAttempt::transport)
                .collect::<Vec<_>>(),
            [
                TransportKind::ServerSentEvents,
                TransportKind::WebSockets,
                TransportKind::LongPolling
            ]
        );
    }

    #[test]
    fn transport_selection_skip_negotiation_needs_web_sockets_alone() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        let error = crate::SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.skip_negotiation();
        })
        .err()
        .expect("the default transports include more than WebSockets");
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.setting(), Some("skip_negotiation"));
        assert!(
            crate::SignalRClient::new("127.0.0.1", "hub", |configuration| {
                configuration.skip_negotiation();
                configuration.with_transports(Transports::WEB_SOCKETS);
            })
            .is_ok()
        );
    }

    #[test]
    fn transport_selection_stateful_reconnect_needs_web_sockets() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        let error = crate::SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.with_stateful_reconnect(crate::StatefulReconnectConfig::default());
            configuration.with_transports(Transports::LONG_POLLING);
        })
        .err()
        .expect("stateful reconnect without WebSockets is rejected");
        assert_eq!(error.setting(), Some("with_stateful_reconnect"));
    }

    /// Negotiation results offering `candidates`, numbered by negotiation.
    fn negotiated(
        recipe: &crate::client::ConnectionRecipe,
        candidates: &[TransportKind],
        number: usize,
    ) -> super::ConnectionData {
        super::ConnectionData::new(
            format!("ws://host/hub?id={number}"),
            format!("connection-{number}"),
            recipe.clone(),
            recipe.stateful_reconnect(),
            TransportSelection {
                candidates: candidates.to_vec(),
                skipped: Vec::new(),
            },
        )
    }

    fn stateful_recipe() -> crate::client::ConnectionRecipe {
        let mut configuration =
            crate::client::ConnectionConfiguration::new("host".into(), "hub".into());
        configuration.with_stateful_reconnect(crate::StatefulReconnectConfig::default());
        configuration.connection_recipe()
    }

    #[test]
    fn transport_selection_a_fallback_renegotiates_without_stateful_reconnect() {
        use std::cell::RefCell;
        let recipe = stateful_recipe();
        let requests = RefCell::new(Vec::new());
        let result = futures::executor::block_on(super::start_with_fallback(
            &recipe,
            |recipe: crate::client::ConnectionRecipe| {
                let number = {
                    let mut requests = requests.borrow_mut();
                    requests.push(recipe.stateful_reconnect().is_some());
                    requests.len()
                };
                let data = negotiated(
                    &recipe,
                    &[TransportKind::WebSockets, TransportKind::LongPolling],
                    number,
                );
                async move { Ok(data) }
            },
            |_, transport| async move {
                match transport {
                    TransportKind::WebSockets => Err(Error::transport_failure(
                        Some(transport),
                        "the upgrade was refused",
                    )),
                    other => Ok(other),
                }
            },
        ));
        let (data, transport, started) = result.unwrap();
        assert_eq!(
            (transport, started),
            (TransportKind::LongPolling, TransportKind::LongPolling)
        );
        assert_eq!(
            data.get_connection_id(),
            "connection-2",
            "a fresh connection token"
        );
        assert_eq!(
            *requests.borrow(),
            [true, false],
            "stateful reconnect is requested only for WebSockets"
        );
    }

    #[test]
    fn transport_selection_a_failed_renegotiation_counts_for_its_transport() {
        use std::cell::Cell;
        let recipe = stateful_recipe();
        let negotiations = Cell::new(0);
        let candidates = [
            TransportKind::WebSockets,
            TransportKind::LongPolling,
            TransportKind::ServerSentEvents,
        ];
        let result = futures::executor::block_on(super::start_with_fallback(
            &recipe,
            |recipe: crate::client::ConnectionRecipe| {
                negotiations.set(negotiations.get() + 1);
                let number = negotiations.get();
                // The renegotiation for Long Polling fails.
                let outcome = if number == 2 {
                    Err(Error::negotiate(Some(503), "unavailable"))
                } else {
                    Ok(negotiated(&recipe, &candidates, number))
                };
                async move { outcome }
            },
            |_, transport| async move {
                Err::<(), _>(Error::transport_failure(Some(transport), "refused"))
            },
        ));
        let Err(error) = result else {
            panic!("no transport started")
        };
        assert_eq!(error.kind(), ErrorKind::TransportUnavailable);
        let attempts: Vec<_> = error
            .attempts()
            .iter()
            .map(|attempt| (attempt.transport(), attempt.error().kind()))
            .collect();
        assert_eq!(
            attempts,
            [
                (TransportKind::WebSockets, ErrorKind::Transport),
                (TransportKind::LongPolling, ErrorKind::Negotiate),
                (TransportKind::ServerSentEvents, ErrorKind::Transport),
            ]
        );
        assert_eq!(negotiations.get(), 3);
    }

    #[test]
    fn transport_selection_stateful_reconnect_only_over_web_sockets() {
        let (result, _) = super::negotiation_tests::negotiate_as(
            false,
            |configuration| {
                configuration.with_stateful_reconnect(crate::StatefulReconnectConfig::default());
            },
            vec![
                r#"{"connectionId":"c","connectionToken":"t","negotiateVersion":1,"useStatefulReconnect":true,
                "availableTransports":[{"transport":"WebSockets","transferFormats":["Text","Binary"]}]}"#,
            ],
        );
        assert!(result.unwrap().stateful_reconnect().is_some());
    }
}

#[cfg(test)]
mod long_polling_urls {
    use super::http_transport_url;

    #[test]
    fn long_polling_browser_urls_use_http_and_carry_no_token() {
        assert_eq!(
            http_transport_url("wss://host/hub?id=abc&access_token=secret"),
            "https://host/hub?id=abc"
        );
        assert_eq!(
            http_transport_url("ws://host:5000/hub?access_token=secret&id=abc"),
            "http://host:5000/hub?id=abc"
        );
        assert_eq!(http_transport_url("https://host/hub"), "https://host/hub");
        assert_eq!(
            http_transport_url("ws://host/hub?access_token=secret"),
            "http://host/hub"
        );
    }
}
