//! The HTTP headers every request carries, as the official clients send them, and validation of
//! application headers (`ConnectionConfiguration::with_header`).

use crate::error::Error;

/// `X-Requested-With`, which the official clients send so servers can tell SignalR requests
/// apart (ASP.NET Core answers them with 401 instead of a login redirect).
pub(crate) const X_REQUESTED_WITH: (&str, &str) = ("X-Requested-With", "XMLHttpRequest");

/// The header browsers carry the user agent in (they forbid setting `User-Agent`).
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) const BROWSER_USER_AGENT_HEADER: &str = "X-SignalR-User-Agent";

/// The user agent in the official clients' format,
/// `<product>/<major>.<minor> (<version>; <OS>; <runtime>; <runtime version>)`.
pub(crate) fn user_agent() -> String {
    let version = env!("CARGO_PKG_VERSION");
    let major_minor = version.split('.').take(2).collect::<Vec<_>>().join(".");
    format!(
        "dXn SignalR/{major_minor} ({version}; {}; {}; Unknown Runtime Version)",
        os_name(),
        runtime_name()
    )
}

fn os_name() -> &'static str {
    match std::env::consts::OS {
        "windows" => "Windows",
        "linux" => "Linux",
        "macos" => "macOS",
        "android" => "Android",
        "ios" => "iOS",
        // Browsers: the official JavaScript client reports no operating system either.
        _ => "Unknown OS",
    }
}

fn runtime_name() -> &'static str {
    if cfg!(target_arch = "wasm32") {
        "Browser"
    } else {
        "Rust"
    }
}

/// Headers the transport itself owns (message framing, the connection, the WebSocket upgrade),
/// which an application header must not set.
const RESERVED: [&str; 9] = [
    "host",
    "content-length",
    "connection",
    "upgrade",
    "transfer-encoding",
    "te",
    "trailer",
    "keep-alive",
    "proxy-connection",
];

/// The headers of one request, as the official .NET client's `HttpConnection` composes them: the
/// `defaults`, then the application's (`with_header`), each replacing a header of the same name
/// (an empty `User-Agent` removes it), then `X-Requested-With` always, then the credentials'
/// `Authorization` over any other.
pub(crate) fn compose(
    defaults: &[(&str, String)],
    application: &[(String, String)],
    authorization: Option<String>,
) -> Vec<(String, String)> {
    fn set(headers: &mut Vec<(String, String)>, name: &str, value: String) {
        headers.retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
        headers.push((name.to_string(), value));
    }
    let mut headers: Vec<(String, String)> = defaults
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect();
    for (name, value) in application {
        set(&mut headers, name, value.clone());
    }
    headers.retain(|(name, value)| !(name.eq_ignore_ascii_case("user-agent") && value.is_empty()));
    set(
        &mut headers,
        X_REQUESTED_WITH.0,
        X_REQUESTED_WITH.1.to_string(),
    );
    if let Some(authorization) = authorization {
        set(&mut headers, "Authorization", authorization);
    }
    headers
}

/// Adds the cookie jar's cookies to composed headers: after an application `Cookie` header, as
/// the .NET `HttpClientHandler` appends its container's cookies.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn with_cookies(
    mut headers: Vec<(String, String)>,
    cookies: Option<String>,
) -> Vec<(String, String)> {
    let Some(cookies) = cookies else {
        return headers;
    };
    match headers
        .iter_mut()
        .find(|(name, _)| name.eq_ignore_ascii_case("cookie"))
    {
        Some((_, value)) => {
            value.push_str("; ");
            value.push_str(&cookies);
        }
        None => headers.push(("Cookie".to_string(), cookies)),
    }
    headers
}

/// The headers of one request leg: `composed` with the jar's `cookies` after any application
/// `Cookie` header, and an integrated-authentication `integrated` token replacing other
/// credentials.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn leg(
    composed: &[(String, String)],
    cookies: Option<String>,
    integrated: Option<String>,
) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = composed
        .iter()
        .filter(|(name, _)| integrated.is_none() || !name.eq_ignore_ascii_case("authorization"))
        .cloned()
        .collect();
    headers = with_cookies(headers, cookies);
    if let Some(integrated) = integrated {
        headers.push(("Authorization".to_string(), integrated));
    }
    headers
}

/// Validates an application header before any I/O: a token name the transport does not own, and
/// a value of visible ASCII, spaces and tabs (no line breaks), as `http::HeaderValue` accepts.
pub(crate) fn validate(name: &str, value: &str) -> Result<(), Error> {
    validate_for("with_header", name, value)
}

/// [`validate`] for the setting `setting` names in its errors.
pub(crate) fn validate_for(setting: &'static str, name: &str, value: &str) -> Result<(), Error> {
    let lower = name.to_ascii_lowercase();
    if RESERVED.contains(&lower.as_str()) || lower.starts_with("sec-websocket-") {
        return Err(Error::configuration(
            setting,
            format!("the {name} header belongs to the transport and cannot be set"),
        ));
    }
    let token = |byte: u8| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte);
    if name.is_empty() || !name.bytes().all(token) {
        return Err(Error::configuration(
            setting,
            format!("{name:?} is not a valid HTTP header name"),
        ));
    }
    if !value
        .bytes()
        .all(|byte| byte == b'\t' || (b' '..=b'~').contains(&byte))
    {
        // The value may be a secret, so it is never repeated.
        return Err(Error::configuration(
            setting,
            format!("the value of the {name} header is not a valid HTTP header value"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod request_headers {
    use super::{user_agent, validate, X_REQUESTED_WITH};
    use crate::ErrorKind;

    #[test]
    fn request_headers_user_agent_follows_the_official_format() {
        let agent = user_agent();
        let version = env!("CARGO_PKG_VERSION");
        assert!(
            agent.starts_with(&format!(
                "dXn SignalR/{} ({version}; ",
                version.split('.').take(2).collect::<Vec<_>>().join(".")
            )),
            "{agent}"
        );
        assert!(
            agent.ends_with("; Rust; Unknown Runtime Version)"),
            "{agent}"
        );
        assert_eq!(X_REQUESTED_WITH, ("X-Requested-With", "XMLHttpRequest"));
    }

    #[test]
    fn request_headers_application_headers_replace_defaults_as_in_dotnet() {
        let defaults = [
            ("User-Agent", "default-agent".to_string()),
            ("Accept", "*/*".to_string()),
        ];
        let application = [
            ("user-agent".to_string(), "custom-agent".to_string()),
            ("X-Requested-With".to_string(), "spoofed".to_string()),
            ("authorization".to_string(), "Basic app".to_string()),
            ("X-Tenant".to_string(), "blue".to_string()),
        ];
        let headers = super::compose(&defaults, &application, Some("Bearer token".into()));
        let get = |name: &str| {
            headers
                .iter()
                .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
                .collect::<Vec<_>>()
        };
        assert_eq!(get("user-agent"), ["custom-agent"]);
        assert_eq!(
            get("x-requested-with"),
            ["XMLHttpRequest"],
            "always the official value"
        );
        assert_eq!(
            get("authorization"),
            ["Bearer token"],
            "the credentials win"
        );
        assert_eq!(get("accept"), ["*/*"]);
        assert_eq!(get("x-tenant"), ["blue"]);

        // An empty User-Agent removes it; without credentials the application's Authorization stays.
        let removed = super::compose(
            &defaults,
            &[
                ("User-Agent".to_string(), String::new()),
                ("Authorization".to_string(), "Basic app".to_string()),
            ],
            None,
        );
        assert!(!removed
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("user-agent")));
        assert!(removed.contains(&("Authorization".to_string(), "Basic app".to_string())));
    }

    #[test]
    fn request_headers_transport_headers_cannot_be_set() {
        for name in [
            "Host",
            "content-length",
            "Connection",
            "Upgrade",
            "Sec-WebSocket-Key",
        ] {
            let error = validate(name, "x").unwrap_err();
            assert_eq!(error.setting(), Some("with_header"), "{name}");
        }
        assert!(validate("Authorization", "Basic x").is_ok());
        assert!(validate("User-Agent", "").is_ok());
    }

    #[test]
    fn request_headers_validate_names_and_values_without_echoing_values() {
        assert!(validate("X-Tenant", "blue team\t7").is_ok());
        for name in ["", "Bad Name", "Bad:Name", "Ünïcode"] {
            let error = validate(name, "value").unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Configuration);
            assert_eq!(error.setting(), Some("with_header"));
        }
        let error = validate("X-Secret", "line\r\nInjected: secret-value").unwrap_err();
        assert_eq!(error.setting(), Some("with_header"));
        assert!(!error.to_string().contains("secret-value"), "{error}");
    }
}
