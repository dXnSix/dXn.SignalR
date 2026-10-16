//! HTTPS proxies: the Aspire `test-proxy` also listens behind TLS with the run's test
//! certificate, requiring the run's credentials. As the .NET client (since .NET 8), an
//! `https://` proxy gets TLS from the client with the connection's own TLS configuration, the
//! `CONNECT` exchange (with the 407/Basic flow) runs inside it, and a secure endpoint's TLS runs
//! inside the tunnel: plaintext and TLS endpoints connect over every transport, each through a
//! TLS session and a tunnel the proxy counts. A proxy certificate the client does not trust,
//! and wrong credentials, fail with `ErrorKind::Proxy` without showing the credentials; and
//! `https_proxy` selects an HTTPS proxy as the system proxy.

use base64::{engine::general_purpose, Engine};
use rayx_signalr::{ErrorKind, SignalRClient, Transports};

use crate::{
    proxy::{clear_proxy_variables, credentials, proxy_endpoint, stats},
    support::{self, Protocol},
};

/// The test proxy's TLS listener as an `https://` proxy URL with `credentials`.
async fn https_proxy(credentials: &str) -> String {
    let port = stats().await.tls_port;
    assert_ne!(port, 0, "the test proxy has no TLS listener");
    // The listener is on the IPv4 loopback (the certificate names 127.0.0.1 too): name it so,
    // rather than make every connection try `localhost`'s IPv6 address first.
    let host = match proxy_endpoint().host.as_str() {
        "localhost" => "127.0.0.1".to_string(),
        host => host.to_string(),
    };
    format!("https://{credentials}@{host}:{port}")
}

async fn scenario(protocol: Protocol) {
    clear_proxy_variables();
    let plain = support::service();
    let secure = support::parse_endpoint(&support::required_variable("RAYX_SIGNALR_E2E_TLS_URL"));
    let ca = general_purpose::STANDARD
        .decode(support::required_variable(
            "RAYX_SIGNALR_E2E_TLS_CA_PEM_BASE64",
        ))
        .expect("the CA is base64");
    let proxy = https_proxy(&credentials()).await;

    // Plaintext over every transport the protocol can use, and TLS inside the tunnel.
    let mut cases = vec![
        (&plain, "test", Transports::WEB_SOCKETS),
        (&plain, "test", Transports::LONG_POLLING),
        (&secure, "test-tls", Transports::WEB_SOCKETS),
    ];
    if protocol == Protocol::Json {
        cases.push((&plain, "test", Transports::SERVER_SENT_EVENTS));
    }
    for (endpoint, hub, transport) in cases {
        let before = stats().await;
        let ca = ca.clone();
        let mut client = SignalRClient::connect_with(&endpoint.host, hub, |configuration| {
            endpoint.apply(configuration);
            support::apply_protocol(configuration, protocol);
            configuration.with_transports(transport);
            configuration.with_proxy(&proxy);
            // Trusted for the proxy's certificate and the TLS endpoint's alike.
            configuration.with_additional_root_certificates_pem(ca.clone());
        })
        .await
        .unwrap_or_else(|error| {
            panic!("{hub} over {transport:?} through the HTTPS proxy: {error}")
        });
        let echoed: i32 = client
            .invoke_with_args("Echo".to_string(), |arguments| {
                arguments.argument(7);
            })
            .await
            .unwrap();
        assert_eq!(echoed, 7);
        let peer: i32 = client.invoke("PeerPort".to_string()).await.unwrap();
        let after = stats().await;
        assert!(
            after.tls_sessions > before.tls_sessions,
            "{hub} over {transport:?}: no TLS session with the proxy"
        );
        assert!(
            after.tunnels > before.tunnels,
            "{hub} over {transport:?}: no tunnel"
        );
        assert!(
            after.ports.contains(&u16::try_from(peer).unwrap()),
            "{hub} over {transport:?}: the service's peer {peer} is not a proxy tunnel ({after:?})"
        );
        client.disconnect();
    }

    // The proxy's certificate must verify: without the test CA the client refuses the proxy.
    let error = SignalRClient::connect_with(&plain.host, "test", |configuration| {
        plain.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_proxy(&proxy);
        #[cfg(target_os = "android")]
        configuration.use_webpki_roots();
    })
    .await
    .err()
    .expect("an untrusted proxy certificate is refused");
    assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");
    let secret = credentials();
    let (_, password) = secret.split_once(':').unwrap();
    assert!(!error.to_string().contains(password), "{error}");

    // Wrong credentials: the proxy's 407 inside TLS.
    let wrong = https_proxy("rayx:wrong-secret").await;
    let ca_for_wrong = ca.clone();
    let error = SignalRClient::connect_with(&plain.host, "test", |configuration| {
        plain.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_proxy(&wrong);
        configuration.with_additional_root_certificates_pem(ca_for_wrong.clone());
    })
    .await
    .err()
    .expect("wrong proxy credentials are refused");
    assert_eq!(
        (error.kind(), error.status()),
        (ErrorKind::Proxy, Some(407)),
        "{error}"
    );
    assert!(!error.to_string().contains("wrong-secret"), "{error}");
    assert!(!format!("{error:?}").contains("wrong-secret"), "{error:?}");

    // The system proxy: http_proxy names the HTTPS proxy. The cell runs one test per process, so
    // the environment is this scenario's.
    std::env::set_var("http_proxy", &proxy);
    let before = stats().await.tls_sessions;
    let ca_for_system = ca.clone();
    let mut proxied = support::connect("test", protocol, move |configuration| {
        configuration.with_additional_root_certificates_pem(ca_for_system.clone());
    })
    .await;
    support::connection_identity(&mut proxied).await.unwrap();
    proxied.disconnect();
    assert!(
        stats().await.tls_sessions > before,
        "http_proxy names the HTTPS proxy"
    );
    clear_proxy_variables();
}

crate::protocol_tests!("proxy-https", std::time::Duration::from_secs(110));
