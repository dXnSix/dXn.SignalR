//! The error type of every fallible public API: one [`Error`] with a matchable [`ErrorKind`].
//!
//! Errors are cheap to clone (one failure ends many pending calls) and carry structured fields
//! for the kinds that have them. Their `Display` text is stable, human-readable and redacted when
//! the error is built: it never contains bearer tokens, `access_token` values, `Authorization`
//! values or credentials embedded in URLs.

use std::{fmt, sync::Arc, time::Duration};

use crate::communication::transport::TransportKind;

/// What went wrong, for matching. New kinds may be added in minor releases, so a `match` needs a
/// wildcard arm.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// A setting cannot work: the configuration was rejected before any I/O, or a call exceeded a
    /// limit it sets (a message larger than the stateful replay buffer). [`Error::setting`] names
    /// the setting.
    Configuration,
    /// The negotiate request failed; [`Error::status`] is the HTTP status when the server answered.
    Negotiate,
    /// The server refused the credentials ([`Error::status`] is 401 or 403), or the credentials
    /// could not be obtained ([`std::error::Error::source`] is the cause).
    Authentication,
    /// An operation did not finish within its bound ([`Error::timeout_operation`],
    /// [`Error::timeout`]).
    Timeout,
    /// The TLS handshake or certificate verification failed.
    Tls,
    /// A proxy refused or failed the tunnel.
    Proxy,
    /// The network or the transport failed or dropped ([`Error::transport`] names the transport
    /// when one was involved).
    Transport,
    /// No transport could be started; [`Error::attempts`] lists every attempt with its error.
    TransportUnavailable,
    /// The SignalR handshake failed.
    Handshake,
    /// The server sent a malformed or unexpected message.
    Protocol,
    /// A payload could not be encoded, or decoded into the requested type.
    Serialization,
    /// The hub method failed; [`Error::server_message`] is the server's message.
    Hub,
    /// The connection is closed ([`Error::close_reason`]): locally, by the server, or because the
    /// reconnect attempts were exhausted.
    ConnectionClosed,
    /// The connection is paused (see `SignalRClient::pause`).
    Paused,
    /// The operation was canceled before it completed.
    Canceled,
    /// The operation is not valid in the connection's current state, as the official .NET
    /// client's `InvalidOperationException`: `start` on a client that is starting, connected,
    /// reconnecting or paused, or a call on a client that is not started.
    InvalidState,
}

/// The operation a [`ErrorKind::Timeout`] error bounded.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum TimeoutOperation {
    /// Opening the transport (TCP, TLS and the WebSocket upgrade).
    Connect,
    /// The negotiate request.
    Negotiate,
    /// The SignalR handshake.
    Handshake,
    /// No message arrived from the server within the server timeout.
    ServerTimeout,
}

impl fmt::Display for TimeoutOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            TimeoutOperation::Connect => "connect",
            TimeoutOperation::Negotiate => "negotiate",
            TimeoutOperation::Handshake => "handshake",
            TimeoutOperation::ServerTimeout => "server timeout",
        })
    }
}

/// Why a [`ErrorKind::ConnectionClosed`] connection closed.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum CloseReason {
    /// The application closed it (`disconnect`, or every client handle was dropped).
    Local,
    /// The server sent a Close message ([`Error::server_message`] is its optional error text and
    /// [`Error::server_allows_reconnect`] its reconnect permission).
    Server,
    /// The reconnect attempts were exhausted ([`std::error::Error::source`] is the last attempt's
    /// error, when one ran).
    ReconnectExhausted,
}

/// One transport a connection tried to start, and why it failed.
#[derive(Clone, Debug)]
pub struct TransportAttempt {
    transport: TransportKind,
    error: Error,
}

impl TransportAttempt {
    pub(crate) fn new(transport: TransportKind, error: Error) -> Self {
        Self { transport, error }
    }

    /// The transport that was tried.
    pub fn transport(&self) -> TransportKind {
        self.transport
    }

    /// Why it failed.
    pub fn error(&self) -> &Error {
        &self.error
    }
}

type Source = Arc<dyn std::error::Error + Send + Sync + 'static>;

/// The error of every fallible `rayx_signalr` API.
///
/// Match on [`kind`](Self::kind); the accessors return the structured fields of the kinds that
/// carry them. Cloning is cheap.
#[derive(Clone)]
pub struct Error {
    inner: Arc<Inner>,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct Inner {
    kind: ErrorKind,
    message: String,
    status: Option<u16>,
    setting: Option<&'static str>,
    timeout: Option<(TimeoutOperation, Duration)>,
    transport: Option<TransportKind>,
    allows_reconnect: Option<bool>,
    server_message: Option<String>,
    close_reason: Option<CloseReason>,
    attempts: Vec<TransportAttempt>,
    /// The underlying cause; `Error::source` returns it (so it can be downcast).
    cause: Option<Source>,
}

impl Inner {
    fn new(kind: ErrorKind, message: String) -> Self {
        Self {
            kind,
            message: redact(&message),
            status: None,
            setting: None,
            timeout: None,
            transport: None,
            allows_reconnect: None,
            server_message: None,
            close_reason: None,
            attempts: Vec::new(),
            cause: None,
        }
    }
}

impl Error {
    fn build(inner: Inner) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// The error's kind.
    pub fn kind(&self) -> ErrorKind {
        self.inner.kind
    }

    /// The HTTP status of a [`Negotiate`](ErrorKind::Negotiate),
    /// [`Authentication`](ErrorKind::Authentication) or [`Proxy`](ErrorKind::Proxy) failure, when
    /// the server or the proxy answered.
    pub fn status(&self) -> Option<u16> {
        self.inner.status
    }

    /// The configuration setting a [`Configuration`](ErrorKind::Configuration) error rejected.
    pub fn setting(&self) -> Option<&'static str> {
        self.inner.setting
    }

    /// The operation a [`Timeout`](ErrorKind::Timeout) error bounded.
    pub fn timeout_operation(&self) -> Option<TimeoutOperation> {
        self.inner.timeout.map(|(operation, _)| operation)
    }

    /// The bound a [`Timeout`](ErrorKind::Timeout) error exceeded.
    pub fn timeout(&self) -> Option<Duration> {
        self.inner.timeout.map(|(_, bound)| bound)
    }

    /// The transport a [`Transport`](ErrorKind::Transport) error concerns, when one was involved.
    pub fn transport(&self) -> Option<TransportKind> {
        self.inner.transport
    }

    /// Whether the server allowed reconnecting, when it decided: `Some(true)` for a dropped
    /// transport, the Close message's permission for a server close.
    pub fn server_allows_reconnect(&self) -> Option<bool> {
        self.inner.allows_reconnect
    }

    /// The server's message, verbatim (as `HubException.Message` in the official .NET client):
    /// the hub method's error for [`Hub`](ErrorKind::Hub), the Close message's error for a server
    /// close. Unlike `Display`, it is not redacted: it is whatever the server sent, so log it
    /// with the care the server's messages need.
    pub fn server_message(&self) -> Option<&str> {
        self.inner.server_message.as_deref()
    }

    /// Why a [`ConnectionClosed`](ErrorKind::ConnectionClosed) connection closed.
    pub fn close_reason(&self) -> Option<CloseReason> {
        self.inner.close_reason
    }

    /// Every transport a [`TransportUnavailable`](ErrorKind::TransportUnavailable) connection
    /// tried, with its error.
    pub fn attempts(&self) -> &[TransportAttempt] {
        &self.inner.attempts
    }

    /// Whether retrying the same operation later can succeed: timeouts, network and transport
    /// failures, negotiate answers 408, 429 and 5xx, and proxy failures without a status or with
    /// 408, 429 or 5xx (not refused credentials or a denied target).
    pub fn is_transient(&self) -> bool {
        match self.inner.kind {
            ErrorKind::Timeout | ErrorKind::Transport => true,
            ErrorKind::Negotiate => self.inner.status.is_some_and(|status| {
                status == 408 || status == 429 || (500..600).contains(&status)
            }),
            // An unreachable proxy or a gateway failure may pass; refused credentials or a denied
            // target do not.
            ErrorKind::Proxy => self.inner.status.is_none_or(|status| {
                status == 408 || status == 429 || (500..600).contains(&status)
            }),
            _ => false,
        }
    }

    // ── Constructors ────────────────────────────────────────────────────

    pub(crate) fn configuration(setting: &'static str, message: impl Into<String>) -> Self {
        let mut inner = Inner::new(
            ErrorKind::Configuration,
            format!("invalid configuration ({setting}): {}", message.into()),
        );
        inner.setting = Some(setting);
        Self::build(inner)
    }

    pub(crate) fn negotiate(status: Option<u16>, message: impl Into<String>) -> Self {
        let mut inner = Inner::new(ErrorKind::Negotiate, message.into());
        inner.status = status;
        Self::build(inner)
    }

    /// A proxy refused or failed the tunnel: `status` when it answered (native only; browsers
    /// use the browser's proxy).
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn proxy(status: Option<u16>, message: impl Into<String>) -> Self {
        let mut inner = Inner::new(ErrorKind::Proxy, message.into());
        inner.status = status;
        Self::build(inner)
    }

    pub(crate) fn authentication(status: Option<u16>, message: impl Into<String>) -> Self {
        let mut inner = Inner::new(ErrorKind::Authentication, message.into());
        inner.status = status;
        Self::build(inner)
    }

    pub(crate) fn timed_out(operation: TimeoutOperation, bound: Duration) -> Self {
        let mut inner = Inner::new(
            ErrorKind::Timeout,
            format!("{operation} timed out after {bound:?}"),
        );
        inner.timeout = Some((operation, bound));
        if operation == TimeoutOperation::ServerTimeout {
            inner.allows_reconnect = Some(true);
        }
        Self::build(inner)
    }

    #[cfg(any(not(target_arch = "wasm32"), test))]
    pub(crate) fn tls(message: impl Into<String>) -> Self {
        Self::build(Inner::new(ErrorKind::Tls, message.into()))
    }

    pub(crate) fn transport_failure(
        transport: Option<TransportKind>,
        message: impl Into<String>,
    ) -> Self {
        let mut inner = Inner::new(ErrorKind::Transport, message.into());
        inner.transport = transport;
        inner.allows_reconnect = Some(true);
        Self::build(inner)
    }

    pub(crate) fn transport_unavailable(
        message: impl Into<String>,
        attempts: Vec<TransportAttempt>,
    ) -> Self {
        let mut inner = Inner::new(ErrorKind::TransportUnavailable, message.into());
        inner.attempts = attempts;
        Self::build(inner)
    }

    pub(crate) fn handshake(message: impl Into<String>) -> Self {
        Self::build(Inner::new(ErrorKind::Handshake, message.into()))
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::build(Inner::new(ErrorKind::Protocol, message.into()))
    }

    pub(crate) fn serialization(message: impl Into<String>) -> Self {
        Self::build(Inner::new(ErrorKind::Serialization, message.into()))
    }

    pub(crate) fn hub(message: impl Into<String>) -> Self {
        // `server_message()` is the server's text verbatim, as `HubException.Message`; the
        // `Display` text is redacted.
        let message = message.into();
        let mut inner = Inner::new(
            ErrorKind::Hub,
            format!("the hub method failed: {}", redact(&message)),
        );
        inner.server_message = Some(message);
        Self::build(inner)
    }

    pub(crate) fn closed_locally() -> Self {
        let mut inner = Inner::new(
            ErrorKind::ConnectionClosed,
            "the connection was closed locally".to_string(),
        );
        inner.close_reason = Some(CloseReason::Local);
        Self::build(inner)
    }

    pub(crate) fn closed_by_server(message: Option<String>, allows_reconnect: bool) -> Self {
        // `server_message()` is the server's text verbatim; the `Display` text is redacted.
        let message = message.filter(|message| !message.is_empty());
        let text = match &message {
            Some(message) => format!("the server closed the connection: {}", redact(message)),
            None => "the server closed the connection".to_string(),
        };
        let mut inner = Inner::new(ErrorKind::ConnectionClosed, text);
        inner.close_reason = Some(CloseReason::Server);
        inner.server_message = message;
        inner.allows_reconnect = Some(allows_reconnect);
        Self::build(inner)
    }

    pub(crate) fn reconnect_exhausted(last_attempt: Option<Error>) -> Self {
        let mut inner = Inner::new(
            ErrorKind::ConnectionClosed,
            "the reconnect attempts were exhausted".to_string(),
        );
        inner.close_reason = Some(CloseReason::ReconnectExhausted);
        inner.cause = last_attempt.map(|error| Arc::new(error) as Source);
        Self::build(inner)
    }

    pub(crate) fn paused() -> Self {
        Self::build(Inner::new(
            ErrorKind::Paused,
            "the connection is paused".to_string(),
        ))
    }

    pub(crate) fn canceled(message: impl Into<String>) -> Self {
        Self::build(Inner::new(ErrorKind::Canceled, message.into()))
    }

    pub(crate) fn invalid_state(message: impl Into<String>) -> Self {
        Self::build(Inner::new(ErrorKind::InvalidState, message.into()))
    }

    /// A call or control on a client that was never started (or whose start failed).
    pub(crate) fn not_started() -> Self {
        Self::invalid_state("the connection is not started; call `start` first")
    }

    // ── Builders (used while the error is still uniquely owned) ─────────

    fn edit(mut self, change: impl FnOnce(&mut Inner)) -> Self {
        match Arc::get_mut(&mut self.inner) {
            Some(inner) => change(inner),
            None => {
                let mut inner = Inner {
                    kind: self.inner.kind,
                    message: self.inner.message.clone(),
                    status: self.inner.status,
                    setting: self.inner.setting,
                    timeout: self.inner.timeout,
                    transport: self.inner.transport,
                    allows_reconnect: self.inner.allows_reconnect,
                    server_message: self.inner.server_message.clone(),
                    close_reason: self.inner.close_reason,
                    attempts: self.inner.attempts.clone(),
                    cause: self.inner.cause.clone(),
                };
                change(&mut inner);
                self.inner = Arc::new(inner);
            }
        }
        self
    }

    /// Attaches the underlying cause, reported by `source()`.
    pub(crate) fn with_source(
        self,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        self.edit(|inner| inner.cause = Some(Arc::new(source)))
    }

    /// Prefixes the message with the context it occurred in.
    pub(crate) fn context(self, context: impl fmt::Display) -> Self {
        let context = redact(&context.to_string());
        self.edit(|inner| inner.message = format!("{context}: {}", inner.message))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_transport(self, transport: TransportKind) -> Self {
        self.edit(|inner| inner.transport = Some(transport))
    }

    /// Whether the transport this error ended may be replaced by a reconnect: everything except
    /// a server close or a protocol violation that forbade it.
    pub(crate) fn allows_reconnect(&self) -> bool {
        self.inner.allows_reconnect != Some(false)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, formatter)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = &self.inner;
        let mut debug = formatter.debug_struct("Error");
        debug.field("kind", &inner.kind);
        debug.field("message", &inner.message);
        if let Some(status) = inner.status {
            debug.field("status", &status);
        }
        if let Some(setting) = inner.setting {
            debug.field("setting", &setting);
        }
        if let Some(timeout) = inner.timeout {
            debug.field("timeout", &timeout);
        }
        if let Some(transport) = inner.transport {
            debug.field("transport", &transport);
        }
        if let Some(allows_reconnect) = inner.allows_reconnect {
            debug.field("server_allows_reconnect", &allows_reconnect);
        }
        if let Some(reason) = inner.close_reason {
            debug.field("close_reason", &reason);
        }
        if !inner.attempts.is_empty() {
            debug.field("attempts", &inner.attempts);
        }
        if let Some(cause) = &inner.cause {
            debug.field("source", &redact(&cause.to_string()));
        }
        debug.finish()
    }
}

/// The redacted [`Display`](fmt::Display) text, so code that propagated the former `String`
/// errors with `?` keeps compiling while it migrates to matching [`ErrorKind`]s.
impl From<Error> for String {
    fn from(error: Error) -> Self {
        error.to_string()
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner
            .cause
            .as_deref()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

/// Replaces `access_token=`, `Bearer` and `Authorization` values, and credentials in URLs
/// (`scheme://user:password@`), with `[REDACTED]`.
pub(crate) fn redact(text: &str) -> String {
    redact_url_credentials(&redact_markers(text))
}

fn redact_markers(text: &str) -> String {
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

/// Replaces the userinfo of every `scheme://userinfo@host` with `[REDACTED]`.
fn redact_url_credentials(text: &str) -> String {
    let mut redacted = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(scheme_end) = rest.find("://") {
        let authority_start = scheme_end + 3;
        redacted.push_str(&rest[..authority_start]);
        rest = &rest[authority_start..];
        let authority_end = rest
            .find(|character: char| {
                matches!(character, '/' | '?' | '#') || character.is_whitespace()
            })
            .unwrap_or(rest.len());
        if let Some(at) = rest[..authority_end].rfind('@') {
            redacted.push_str("[REDACTED]");
            rest = &rest[at..];
        }
    }
    redacted.push_str(rest);
    redacted
}

#[cfg(test)]
mod error_model {
    use std::{error::Error as _, time::Duration};

    use super::{redact, CloseReason, Error, ErrorKind, TimeoutOperation, TransportAttempt};
    use crate::communication::transport::TransportKind;

    const TOKEN: &str = "error-model-secret-token";

    #[test]
    fn error_model_is_clone_send_sync_and_a_std_error() {
        fn traits<T: Clone + Send + Sync + std::error::Error + 'static>() {}
        traits::<Error>();
        let error = Error::hub("boom");
        let clone = error.clone();
        assert_eq!(clone.kind(), ErrorKind::Hub);
        assert_eq!(clone.to_string(), error.to_string());
    }

    #[test]
    fn error_model_configuration_errors_name_the_setting() {
        let error = Error::configuration("with_keep_alive_interval", "must be greater than zero");
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.setting(), Some("with_keep_alive_interval"));
        assert_eq!(
            error.to_string(),
            "invalid configuration (with_keep_alive_interval): must be greater than zero"
        );
        assert!(!error.is_transient());
    }

    #[test]
    fn error_model_negotiate_and_authentication_report_the_status() {
        let missing = Error::negotiate(Some(404), "the server returned 404 Not Found");
        assert_eq!(missing.kind(), ErrorKind::Negotiate);
        assert_eq!(missing.status(), Some(404));
        let refused = Error::authentication(Some(401), "the server returned 401 Unauthorized");
        assert_eq!(refused.kind(), ErrorKind::Authentication);
        assert_eq!(refused.status(), Some(401));
        assert_eq!(Error::negotiate(None, "unrecognized").status(), None);
    }

    #[test]
    fn error_model_timeouts_name_the_operation_and_the_bound() {
        let error = Error::timed_out(TimeoutOperation::Negotiate, Duration::from_secs(2));
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.timeout_operation(), Some(TimeoutOperation::Negotiate));
        assert_eq!(error.timeout(), Some(Duration::from_secs(2)));
        assert_eq!(error.to_string(), "negotiate timed out after 2s");
        let silent = Error::timed_out(TimeoutOperation::ServerTimeout, Duration::from_secs(30));
        assert_eq!(silent.to_string(), "server timeout timed out after 30s");
        assert_eq!(silent.server_allows_reconnect(), Some(true));
    }

    #[test]
    fn error_model_transport_errors_name_the_transport_and_allow_reconnecting() {
        let error =
            Error::transport_failure(Some(TransportKind::WebSockets), "the WebSocket closed");
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert_eq!(error.transport(), Some(TransportKind::WebSockets));
        assert_eq!(error.server_allows_reconnect(), Some(true));
        assert!(error.allows_reconnect());
        assert_eq!(
            Error::transport_failure(None, "DNS failed").transport(),
            None
        );
    }

    #[test]
    fn error_model_transport_unavailable_lists_every_attempt() {
        let error = Error::transport_unavailable(
            "no transport could be started",
            vec![
                TransportAttempt::new(
                    TransportKind::WebSockets,
                    Error::transport_failure(Some(TransportKind::WebSockets), "upgrade refused"),
                ),
                TransportAttempt::new(
                    TransportKind::LongPolling,
                    Error::timed_out(TimeoutOperation::Connect, Duration::from_secs(15)),
                ),
            ],
        );
        assert_eq!(error.kind(), ErrorKind::TransportUnavailable);
        let attempts: Vec<_> = error
            .attempts()
            .iter()
            .map(|attempt| (attempt.transport(), attempt.error().kind()))
            .collect();
        assert_eq!(
            attempts,
            vec![
                (TransportKind::WebSockets, ErrorKind::Transport),
                (TransportKind::LongPolling, ErrorKind::Timeout),
            ]
        );
    }

    #[test]
    fn error_model_hub_errors_keep_the_server_message() {
        let error = Error::hub("detailed failure 42");
        assert_eq!(error.kind(), ErrorKind::Hub);
        assert_eq!(error.server_message(), Some("detailed failure 42"));
        assert_eq!(
            error.to_string(),
            "the hub method failed: detailed failure 42"
        );
    }

    #[test]
    fn error_model_closed_connections_carry_their_reason() {
        let local = Error::closed_locally();
        assert_eq!(local.kind(), ErrorKind::ConnectionClosed);
        assert_eq!(local.close_reason(), Some(CloseReason::Local));

        let server = Error::closed_by_server(Some("Server requested close".to_string()), false);
        assert_eq!(server.close_reason(), Some(CloseReason::Server));
        assert_eq!(server.server_message(), Some("Server requested close"));
        assert_eq!(server.server_allows_reconnect(), Some(false));
        assert!(!server.allows_reconnect());
        assert_eq!(
            server.to_string(),
            "the server closed the connection: Server requested close"
        );
        let quiet = Error::closed_by_server(None, true);
        assert_eq!(quiet.server_message(), None);
        assert!(quiet.allows_reconnect());

        let last = Error::transport_failure(Some(TransportKind::WebSockets), "connection refused");
        let exhausted = Error::reconnect_exhausted(Some(last));
        assert_eq!(
            exhausted.close_reason(),
            Some(CloseReason::ReconnectExhausted)
        );
        assert!(exhausted
            .source()
            .is_some_and(|source| source.to_string() == "connection refused"));
        assert_eq!(Error::paused().kind(), ErrorKind::Paused);
        assert_eq!(Error::canceled("superseded").kind(), ErrorKind::Canceled);
    }

    #[test]
    fn error_model_sources_chain_through_shared_clones() {
        let cause = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let error = Error::transport_failure(Some(TransportKind::WebSockets), "cannot connect")
            .with_source(cause);
        let clone = error.clone();
        let source = clone.source().expect("the cause is the source");
        assert_eq!(source.to_string(), "refused");
        assert!(source
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionRefused));
        assert!(Error::hub("no cause").source().is_none());
    }

    #[test]
    fn error_model_context_keeps_the_kind_and_fields() {
        let error = Error::negotiate(Some(503), "the server returned 503 Service Unavailable")
            .context("negotiation with http://host/hub failed");
        assert_eq!(error.kind(), ErrorKind::Negotiate);
        assert_eq!(error.status(), Some(503));
        assert_eq!(
            error.to_string(),
            "negotiation with http://host/hub failed: the server returned 503 Service Unavailable"
        );
        // A shared error is copied, never changed under its other holders.
        let shared = Error::paused();
        let holder = shared.clone();
        let changed = shared.context("while sending");
        assert_eq!(holder.to_string(), "the connection is paused");
        assert_eq!(
            changed.to_string(),
            "while sending: the connection is paused"
        );
    }

    #[test]
    fn error_model_is_transient_for_timeouts_transports_and_retryable_statuses() {
        let transient = [
            Error::timed_out(TimeoutOperation::Handshake, Duration::from_secs(15)),
            Error::transport_failure(None, "reset"),
            Error::negotiate(Some(500), "x"),
            Error::negotiate(Some(503), "x"),
            Error::negotiate(Some(408), "x"),
            Error::negotiate(Some(429), "x"),
        ];
        for error in transient {
            assert!(error.is_transient(), "{error:?}");
        }
        let permanent = [
            Error::negotiate(Some(404), "x"),
            Error::negotiate(None, "x"),
            Error::authentication(Some(401), "x"),
            Error::configuration("setting", "x"),
            Error::tls("x"),
            Error::handshake("x"),
            Error::protocol("x"),
            Error::serialization("x"),
            Error::hub("x"),
            Error::closed_locally(),
            Error::reconnect_exhausted(None),
            Error::paused(),
            Error::canceled("x"),
            Error::transport_unavailable("x", Vec::new()),
        ];
        for error in permanent {
            assert!(!error.is_transient(), "{error:?}");
        }
    }

    #[test]
    fn error_model_display_and_debug_are_redacted() {
        let messages = [
            format!("GET ws://host/hub?id=1&access_token={TOKEN}&x=1 failed"),
            format!("rejected Authorization: Bearer {TOKEN}"),
            format!("header bearer {TOKEN}"),
            format!("proxy http://user:{TOKEN}@proxy:8080/ refused"),
        ];
        for message in messages {
            for error in [
                Error::negotiate(Some(400), message.clone()),
                Error::hub(message.clone()),
                Error::closed_by_server(Some(message.clone()), false),
                Error::transport_failure(None, "x").context(message.clone()),
                Error::transport_failure(None, "x")
                    .with_source(std::io::Error::other(message.clone())),
            ] {
                assert!(!error.to_string().contains(TOKEN), "{error}");
                assert!(!format!("{error:?}").contains(TOKEN), "{error:?}");
            }
        }
        assert_eq!(
            redact("proxy http://user:pw@host:8080/x and https://plain.example/"),
            "proxy http://[REDACTED]@host:8080/x and https://plain.example/"
        );
    }

    #[test]
    fn server_text_hub_errors_keep_the_server_text_verbatim() {
        let text = format!("access_token={TOKEN} expired; Authorization: Bearer {TOKEN}");
        let error = Error::hub(text.clone());
        assert_eq!(error.server_message(), Some(text.as_str()));
        assert!(!error.to_string().contains(TOKEN), "{error}");
        assert!(!format!("{error:?}").contains(TOKEN), "{error:?}");
    }

    #[test]
    fn server_text_close_messages_keep_the_server_text_verbatim() {
        let text = format!("closing ws://host/hub?access_token={TOKEN}");
        let error = Error::closed_by_server(Some(text.clone()), true);
        assert_eq!(error.server_message(), Some(text.as_str()));
        assert!(!error.to_string().contains(TOKEN), "{error}");
        assert_eq!(
            Error::closed_by_server(Some(String::new()), false).server_message(),
            None
        );
    }

    #[test]
    fn error_model_redaction_never_splits_multi_byte_text() {
        for text in [
            "access_token=&üüüüüü",
            "Bearer \u{fffd}ü access_token= ü Authorization:\nü",
            "üüü access_token=ünicode-token&x=ü",
            "ü://ü:ü@ü/ü",
        ] {
            let redacted = redact(text);
            assert!(!redacted.contains("ünicode-token"), "{redacted}");
        }
    }
}
