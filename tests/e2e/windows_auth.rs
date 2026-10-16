//! Integrated Windows authentication, as the .NET client's `UseDefaultCredentials`: the test
//! service's `/test-windows` hub is behind ASP.NET Core Negotiate authentication (Kerberos or NTLM
//! with the current Windows account). With `with_default_credentials` the client answers the
//! challenges of negotiate, the WebSocket upgrade and the HTTP transports, and the hub sees the
//! current account over each transport; without credentials, or with credentials the server
//! rejects, negotiate is refused with 401. Runs on Windows hosts (the catalog selects it only
//! there).

use rayx_signalr::{ErrorKind, SignalRClient, Transports};

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let account = std::env::var("USERNAME").unwrap_or_default();
    let mut transports = vec![Transports::WEB_SOCKETS, Transports::LONG_POLLING];
    if protocol == Protocol::Json {
        transports.push(Transports::SERVER_SENT_EVENTS);
    }
    for transport in transports {
        let mut client = support::try_connect("test-windows", protocol, |configuration| {
            configuration.with_default_credentials();
            configuration.with_transports(transport);
        })
        .await
        .unwrap_or_else(|error| {
            panic!("{transport:?}: integrated authentication was refused: {error}")
        });
        let identity = client
            .invoke::<String>("Identity".to_string())
            .await
            .unwrap();
        assert!(
            identity
                .to_ascii_lowercase()
                .ends_with(&format!("\\{}", account.to_ascii_lowercase())),
            "{transport:?}: the hub saw {identity:?}, not the current account {account:?}"
        );
        let echoed = client
            .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
                arguments.argument(11);
            })
            .await
            .unwrap();
        assert_eq!(echoed, 11, "{transport:?}");
        client.disconnect();
    }

    let error =
        SignalRClient::connect_with(&support::service().host, "test-windows", |configuration| {
            support::service().apply(configuration);
            support::apply_protocol(configuration, protocol);
        })
        .await
        .err()
        .expect("a client without credentials is refused");
    assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
    assert_eq!(error.status(), Some(401), "{error}");

    // Explicit credentials the server rejects (an account that does not exist, so no real
    // account's lockout counter moves) fail with 401 after the challenge is answered.
    let error =
        SignalRClient::connect_with(&support::service().host, "test-windows", |configuration| {
            support::service().apply(configuration);
            support::apply_protocol(configuration, protocol);
            configuration.with_credentials("rayx-signalr-no-such-user", "not the password", ".");
        })
        .await
        .err()
        .expect("rejected credentials are refused");
    assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
    assert_eq!(error.status(), Some(401), "{error}");
}

crate::protocol_tests!("windows-auth");
