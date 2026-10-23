//! Error kinds: every failure class reaches the caller as its `ErrorKind` with the structured
//! fields it carries: a hub method that throws (`Hub` with the server's message), connecting
//! without or with a rejected credential (`Authentication`, 401), an unknown hub path
//! (`Negotiate`, 404), a negotiate the service never answers under a 2 s connect timeout
//! (`Timeout` naming negotiate), a result that cannot be decoded into the requested type
//! (`Serialization`), invoking while paused (`Paused`), a server close with an error message
//! (`ConnectionClosed` carrying it), invoking after `disconnect` (`ConnectionClosed`, local) and
//! an untrusted certificate (`Tls`). No error repeats the test token.

use std::time::{Duration, Instant};

use dxn_signalr::{CloseReason, Error, ErrorKind, SignalRClient, TimeoutOperation};

use crate::support::{self, Protocol};

/// A bearer token the service rejects; no error may repeat it.
const TOKEN: &str = "error-kinds-secret-token";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const MARGIN: Duration = Duration::from_secs(2);

pub(crate) async fn scenario(protocol: Protocol) {
    let mut errors = vec![hub_failure(protocol).await];
    errors.extend(rejected_credentials(protocol).await);
    errors.push(unknown_hub(protocol).await);
    errors.push(negotiate_timeout(protocol).await);
    errors.push(undecodable_result(protocol).await);
    errors.push(invoke_while_paused(protocol).await);
    errors.push(server_close_with_message(protocol).await);
    errors.push(invoke_after_disconnect(protocol).await);
    errors.push(untrusted_certificate(protocol).await);
    for error in &errors {
        let text = format!("{error} {error:?}");
        assert!(!text.contains(TOKEN), "an error repeats the token: {text}");
    }
}

/// A hub method that throws: `Hub` with the server's message.
async fn hub_failure(protocol: Protocol) -> Error {
    let mut client = support::connect("test", protocol, |_| {}).await;
    let error = client
        .invoke_with_args::<i32, _>("Fail".to_string(), |arguments| {
            arguments.argument("error-kinds failure".to_string());
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Hub, "{error}");
    assert!(
        error
            .server_message()
            .is_some_and(|message| message.contains("error-kinds failure")),
        "{error}"
    );
    assert!(!error.is_transient(), "{error}");
    client.disconnect();
    error
}

/// Connecting without credentials and with a rejected token: `Authentication` with 401.
async fn rejected_credentials(protocol: Protocol) -> Vec<Error> {
    let mut errors = Vec::new();
    for token in [None, Some(TOKEN)] {
        let error = support::try_connect("test-auth", protocol, |configuration| {
            if let Some(token) = token {
                configuration.authenticate_bearer(token.to_string());
            }
        })
        .await
        .err()
        .expect("the service requires valid credentials");
        assert_eq!(
            (error.kind(), error.status()),
            (ErrorKind::Authentication, Some(401)),
            "{error}"
        );
        errors.push(error);
    }
    errors
}

/// An unknown hub path: `Negotiate` with 404.
async fn unknown_hub(protocol: Protocol) -> Error {
    let error = support::try_connect("no-such-hub", protocol, |configuration| {
        configuration.authenticate_bearer(TOKEN.to_string());
    })
    .await
    .err()
    .expect("connecting to an unknown hub must fail");
    assert_eq!(
        (error.kind(), error.status()),
        (ErrorKind::Negotiate, Some(404)),
        "{error}"
    );
    error
}

/// A negotiate the service never answers: `Timeout` naming negotiate and the 2 s connect
/// timeout, within the bound plus a margin.
async fn negotiate_timeout(protocol: Protocol) -> Error {
    let started = Instant::now();
    let error = support::try_connect("test-hang", protocol, |configuration| {
        configuration.with_connect_timeout(CONNECT_TIMEOUT);
        configuration.authenticate_bearer(TOKEN.to_string());
    })
    .await
    .err()
    .expect("a negotiate that never answers must fail");
    let elapsed = started.elapsed();
    assert_eq!(
        (error.kind(), error.timeout_operation(), error.timeout()),
        (
            ErrorKind::Timeout,
            Some(TimeoutOperation::Negotiate),
            Some(CONNECT_TIMEOUT)
        ),
        "{error}"
    );
    assert!(error.is_transient(), "{error}");
    assert!(
        elapsed >= CONNECT_TIMEOUT && elapsed <= CONNECT_TIMEOUT + MARGIN,
        "connect failed after {elapsed:?}"
    );
    error
}

/// A result that cannot be decoded into the requested type: `Serialization`; the connection
/// keeps working.
async fn undecodable_result(protocol: Protocol) -> Error {
    let mut client = support::connect("test", protocol, |_| {}).await;
    let error = client
        .invoke_with_args::<String, _>("Echo".to_string(), |arguments| {
            arguments.argument(7);
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization, "{error}");
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(8);
        })
        .await
        .unwrap();
    assert_eq!(echoed, 8);
    client.disconnect();
    error
}

/// Invoking while paused: `Paused`.
async fn invoke_while_paused(protocol: Protocol) -> Error {
    let mut client = support::connect("test", protocol, |_| {}).await;
    client.pause().await.unwrap();
    let error = support::connection_identity(&mut client).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Paused, "{error}");
    client.disconnect();
    error
}

/// A server close with an error message: pending work fails with `ConnectionClosed` carrying the
/// message and the server's refusal to reconnect.
async fn server_close_with_message(protocol: Protocol) -> Error {
    let mut client = support::connect("test", protocol, |_| {}).await;
    client
        .invoke_with_args::<(), _>("ScheduleServerClose".to_string(), |arguments| {
            arguments.argument(false).argument(500);
        })
        .await
        .unwrap();
    let error = client
        .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
            arguments.argument(10_000);
        })
        .await
        .unwrap_err();
    assert_eq!(
        (
            error.kind(),
            error.close_reason(),
            error.server_allows_reconnect()
        ),
        (
            ErrorKind::ConnectionClosed,
            Some(CloseReason::Server),
            Some(false)
        ),
        "{error}"
    );
    assert_eq!(
        error.server_message(),
        Some("Server requested close"),
        "{error}"
    );
    client.disconnect();
    error
}

/// Invoking after `disconnect`: `ConnectionClosed`, local, through every handle (`disconnect`
/// stops the connection for all of them, as `HubConnection.StopAsync` does).
async fn invoke_after_disconnect(protocol: Protocol) -> Error {
    let client = support::connect("test", protocol, |_| {}).await;
    let mut clone = client.clone();
    client.disconnect();
    let error = support::connection_identity(&mut clone).await.unwrap_err();
    assert_eq!(
        (error.kind(), error.close_reason()),
        (ErrorKind::ConnectionClosed, Some(CloseReason::Local)),
        "{error}"
    );
    error
}

/// An untrusted certificate: `Tls`. Browsers report it as a failed request, so only native
/// targets prove it. Inside the Android test app the platform trusts the run's CA, so the
/// certificate is the second CA's, as in the `tls` scenario.
async fn untrusted_certificate(protocol: Protocol) -> Error {
    let platform_trust = crate::tls::platform_trust();
    let endpoint = support::parse_endpoint(&support::required_variable(if platform_trust {
        "DXN_SIGNALR_E2E_UNTRUSTED_TLS_URL"
    } else {
        "DXN_SIGNALR_E2E_TLS_URL"
    }));
    let error = SignalRClient::connect_with(&endpoint.host, "test-tls", |configuration| {
        endpoint.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.authenticate_bearer(TOKEN.to_string());
        // As in the `tls` scenario: an `adb shell` binary has no JVM for the platform verifier.
        #[cfg(target_os = "android")]
        if !platform_trust {
            configuration.use_webpki_roots();
        }
    })
    .await
    .err()
    .expect("a client without the test CA must reject the server certificate");
    assert_eq!(error.kind(), ErrorKind::Tls, "{error}");
    error
}

crate::protocol_tests!("error-kinds");
