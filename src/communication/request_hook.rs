//! Request hooks (`with_request_hook`), as the .NET client's `HttpMessageHandlerFactory` and
//! `WebSocketConfiguration`: the application sees, and may change, the headers of every HTTP
//! request the client sends natively (negotiate, the WebSocket upgrade, Long Polling and
//! Server-Sent Events requests) and of every browser `fetch` (negotiate, Long Polling, Server-Sent
//! Events sends). Browser WebSockets and `EventSource` streams carry no application headers, so
//! the hook does not see them.

use crate::{error::Error, TransportKind};

/// An HTTP request the client is about to send, as a request hook sees it
/// ([`ConnectionConfiguration::with_request_hook`](crate::ConnectionConfiguration::with_request_hook)).
///
/// The hook sees each logical request once, as a .NET `HttpMessageHandlerFactory` handler sees
/// one `SendAsync`: with the defaults, `with_header` values and the credentials' `Authorization`.
/// The cookie jar's cookies and integrated-authentication tokens are added after it, on every leg,
/// and a request retried on a fresh connection keeps its changes, as `SocketsHttpHandler` adds and
/// retries them inside the handler chain. `Debug` shows header names, never their values.
pub struct HttpRequest<'a> {
    transport: Option<TransportKind>,
    method: &'a str,
    url: &'a str,
    headers: Vec<(String, String)>,
}

impl HttpRequest<'_> {
    /// The transport the request belongs to, or `None` for a negotiate request.
    pub fn transport(&self) -> Option<TransportKind> {
        self.transport
    }

    /// The request method (`POST`, `GET`, `DELETE`).
    pub fn method(&self) -> &str {
        self.method
    }

    /// The request URL (`http(s)` or, for the WebSocket upgrade, `ws(s)`).
    pub fn url(&self) -> &str {
        self.url
    }

    /// The value of the header `name` (case-insensitive).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(existing, _)| existing.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Every header, in the order they are sent.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Sets the header `name`, replacing any header of that name.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Configuration`](crate::ErrorKind::Configuration) naming `with_request_hook`
    /// for an invalid name or value, or a header the transport owns (`Host`, `Content-Length`,
    /// `Connection`, `Upgrade`, `Sec-WebSocket-*` and the like); the request is then unchanged.
    /// The value is never repeated in the error.
    pub fn set_header(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), Error> {
        let (name, value) = (name.into(), value.into());
        super::headers::validate_for(HOOK, &name, &value)?;
        self.headers
            .retain(|(existing, _)| !existing.eq_ignore_ascii_case(&name));
        self.headers.push((name, value));
        Ok(())
    }

    /// Removes the header `name` (case-insensitive), if present.
    pub fn remove_header(&mut self, name: &str) {
        self.headers
            .retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
    }
}

/// Shows the transport, the method, the URL with its credentials redacted, and the header names:
/// header values (credentials, cookies) never appear.
impl std::fmt::Debug for HttpRequest<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpRequest")
            .field("transport", &self.transport)
            .field("method", &self.method)
            .field("url", &crate::error::redact(self.url))
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// The setting request-hook errors name.
const HOOK: &str = "with_request_hook";

/// The hook of `with_request_hook`.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type RequestHook = std::sync::Arc<dyn Fn(&mut HttpRequest<'_>) + Send + Sync>;
#[cfg(target_arch = "wasm32")]
pub(crate) type RequestHook = std::rc::Rc<dyn Fn(&mut HttpRequest<'_>)>;

/// The headers of a request after `hook` saw it (unchanged without a hook).
pub(crate) fn apply(
    hook: Option<&RequestHook>,
    transport: Option<TransportKind>,
    method: &str,
    url: &str,
    headers: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let Some(hook) = hook else {
        return headers;
    };
    let mut request = HttpRequest {
        transport,
        method,
        url,
        headers,
    };
    hook(&mut request);
    request.headers
}

#[cfg(test)]
mod transport_hooks_request {
    use super::{apply, RequestHook};
    use crate::{ErrorKind, TransportKind};

    fn headers() -> Vec<(String, String)> {
        vec![
            ("User-Agent".to_string(), "dXn SignalR/1.0".to_string()),
            ("Authorization".to_string(), "Bearer secret".to_string()),
        ]
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn hook(hook: impl Fn(&mut super::HttpRequest<'_>) + Send + Sync + 'static) -> RequestHook {
        std::sync::Arc::new(hook)
    }

    #[cfg(target_arch = "wasm32")]
    fn hook(hook: impl Fn(&mut super::HttpRequest<'_>) + 'static) -> RequestHook {
        std::rc::Rc::new(hook)
    }

    #[test]
    fn transport_hooks_see_the_request_and_change_its_headers() {
        let hook = hook(|request| {
            assert_eq!(request.transport(), Some(TransportKind::LongPolling));
            assert_eq!(request.method(), "GET");
            assert_eq!(request.url(), "https://example.com/hub?id=1");
            assert_eq!(request.header("authorization"), Some("Bearer secret"));
            assert_eq!(request.headers().count(), 2);
            request.set_header("X-Hook", "added").unwrap();
            request.set_header("user-agent", "custom").unwrap();
            request.remove_header("AUTHORIZATION");
        });
        let changed = apply(
            Some(&hook),
            Some(TransportKind::LongPolling),
            "GET",
            "https://example.com/hub?id=1",
            headers(),
        );
        assert_eq!(
            changed,
            vec![
                ("X-Hook".to_string(), "added".to_string()),
                ("user-agent".to_string(), "custom".to_string()),
            ]
        );
        assert_eq!(apply(None, None, "POST", "u", headers()), headers());
    }

    #[test]
    fn transport_hooks_debug_never_shows_header_values_or_url_credentials() {
        let hook = hook(|request| {
            let debug = format!("{request:?}");
            assert!(debug.contains("Authorization"), "{debug}");
            assert!(debug.contains("Cookie"), "{debug}");
            for secret in ["Bearer secret", "session=s3cret", "t0ken"] {
                assert!(!debug.contains(secret), "{debug}");
            }
        });
        let mut headers = headers();
        headers.push(("Cookie".to_string(), "session=s3cret".to_string()));
        apply(
            Some(&hook),
            Some(TransportKind::WebSockets),
            "GET",
            "wss://example.com/hub?id=1&access_token=t0ken",
            headers,
        );
    }

    #[test]
    fn transport_hooks_reject_invalid_or_transport_owned_headers() {
        let hook = hook(|request| {
            for (name, value) in [
                ("Host", "elsewhere"),
                ("Sec-WebSocket-Key", "x"),
                ("Bad Name", "x"),
                ("X-Secret", "line\r\nbreak-secret"),
            ] {
                let error = request.set_header(name, value).unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Configuration, "{error}");
                assert_eq!(error.setting(), Some("with_request_hook"), "{error}");
                assert!(!error.to_string().contains("secret"), "{error}");
            }
        });
        assert_eq!(apply(Some(&hook), None, "POST", "u", headers()), headers());
    }
}
