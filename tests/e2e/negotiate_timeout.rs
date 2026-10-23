//! Negotiate timeout: a service endpoint that accepts the negotiate request and never answers
//! (`/test-hang/negotiate`) fails `start` within the 2 s connect timeout plus 2 s with an
//! `ErrorKind::Timeout` naming the negotiate bound; the error contains neither the URL nor the
//! bearer token.

use std::time::{Duration, Instant};

use dxn_signalr::{ErrorKind, TimeoutOperation};

use crate::support::{self, Protocol};

const TOKEN: &str = "negotiate-timeout-secret-token";

async fn scenario(protocol: Protocol) {
    let client = support::unstarted("test-hang", protocol, |configuration| {
        configuration.authenticate_bearer(TOKEN.to_string());
        configuration.with_connect_timeout(Duration::from_secs(2));
    });
    let started = Instant::now();
    let error = client.start().await.unwrap_err();
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(4), "failed after {elapsed:?}");
    assert_eq!(error.kind(), ErrorKind::Timeout, "{error}");
    assert_eq!(error.timeout_operation(), Some(TimeoutOperation::Negotiate));
    assert_eq!(error.timeout(), Some(Duration::from_secs(2)));
    let text = format!("{error} {error:?}");
    let endpoint = support::required_variable("DXN_SIGNALR_E2E_SERVICE_URL");
    let authority = endpoint
        .split("://")
        .nth(1)
        .unwrap_or(&endpoint)
        .trim_end_matches('/');
    assert!(
        !text.contains(authority),
        "the error names the endpoint: {text}"
    );
    assert!(
        !text.contains("test-hang"),
        "the error names the hub: {text}"
    );
    assert!(!text.contains(TOKEN), "the error carries the token: {text}");
    client.disconnect();
}

crate::protocol_tests!("negotiate-timeout");
