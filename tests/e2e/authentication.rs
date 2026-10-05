//! Authentication: Basic and Bearer credentials reach the authorized hub, and the native client
//! authenticates the WebSocket upgrade itself with the `Authorization` header; missing or invalid
//! credentials fail `connect` with an authorization error that never echoes a credential.

use crate::support::{self, Protocol};

type Credentials<'a> = (Option<(&'a str, &'a str)>, Option<&'a str>);

fn configure(
    configuration: &mut rayx_signalr::ConnectionConfiguration,
    (basic, bearer): Credentials<'_>,
) {
    if let Some((user, password)) = basic {
        configuration.authenticate_basic(user.to_string(), Some(password.to_string()));
    }
    if let Some(token) = bearer {
        configuration.authenticate_bearer(token.to_string());
    }
}

/// Returns the hub-side identity and how the WebSocket request was authenticated.
async fn identity(protocol: Protocol, credentials: Credentials<'_>) -> (String, String) {
    let mut client = support::connect("test-auth", protocol, |configuration| {
        configure(configuration, credentials)
    })
    .await;
    let identity = client
        .invoke::<String>("AuthenticatedIdentity".to_string())
        .await
        .unwrap();
    let source = client
        .invoke::<String>("AuthenticationSource".to_string())
        .await
        .unwrap();
    client.disconnect();
    (identity, source)
}

async fn rejected(protocol: Protocol, credentials: Credentials<'_>) -> String {
    support::try_connect("test-auth", protocol, |configuration| {
        configure(configuration, credentials)
    })
    .await
    .err()
    .expect("connect must fail without valid credentials")
}

async fn scenario(protocol: Protocol) {
    assert_eq!(
        identity(protocol, (Some(("e2e-user", "e2e-password")), None)).await,
        ("Basic:e2e-user".to_string(), "header".to_string()),
        "native Basic authenticates the WebSocket upgrade with the Authorization header"
    );
    assert_eq!(
        identity(protocol, (None, Some("e2e-token"))).await,
        ("Bearer:e2e-bearer".to_string(), "header".to_string()),
        "native Bearer authenticates the WebSocket upgrade with the Authorization header"
    );

    for error in [
        rejected(protocol, (None, None)).await,
        rejected(protocol, (Some(("e2e-user", "wrong-password")), None)).await,
        rejected(protocol, (None, Some("wrong-token"))).await,
    ] {
        assert!(
            error.contains("401"),
            "expected an authorization error: {error}"
        );
        for secret in ["wrong-token", "e2e-token", "wrong-password", "e2e-password"] {
            assert!(!error.contains(secret), "error leaks a credential: {error}");
        }
    }
}

crate::protocol_tests!("authentication");
