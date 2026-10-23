//! SOCKS proxies: through the Aspire `test-proxy`, which also speaks SOCKS5 with the run's
//! required credentials (RFC 1929), plaintext and TLS endpoints connect over every transport, each
//! through a SOCKS tunnel the proxy counts and whose port the service sees as its peer. As .NET's
//! `SocksHelper`, `socks5://` and `socks5h://` send a host name for the proxy to resolve and an IP
//! literal as an address. Wrong credentials fail with `ErrorKind::Proxy` (407) and never show them,
//! a target the proxy's ruleset denies fails with 403, and `all_proxy` selects a SOCKS proxy as the
//! system proxy.

use base64::{engine::general_purpose, Engine};
use dxn_signalr::{ErrorKind, SignalRClient, Transports};

use crate::{
    proxy::{clear_proxy_variables, credentials, proxy_endpoint, stats},
    support::{self, Protocol, ServiceEndpoint},
};

/// The SOCKS URL of the test proxy with `scheme` and `credentials` (`user:password`).
fn socks_url(scheme: &str, credentials: &str) -> String {
    let proxy = proxy_endpoint();
    let port = proxy.port.expect("the proxy URL has a port");
    format!("{scheme}://{credentials}@{}:{port}", proxy.host)
}

/// The SOCKS5 address type the client names `host` with: an IP literal as an address, anything
/// else as a domain name for the proxy to resolve.
fn address_type(host: &str) -> u8 {
    match host.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(_)) => 1,
        Ok(std::net::IpAddr::V6(_)) => 4,
        Err(_) => 3,
    }
}

async fn connect_through_socks(
    endpoint: &ServiceEndpoint,
    hub: &str,
    protocol: Protocol,
    transports: Transports,
    proxy: &str,
    ca: &[u8],
) -> SignalRClient {
    let ca = ca.to_vec();
    SignalRClient::connect_with(&endpoint.host, hub, |configuration| {
        endpoint.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_transports(transports);
        configuration.with_proxy(proxy);
        configuration.with_additional_root_certificates_pem(ca.clone());
    })
    .await
    .unwrap_or_else(|error| panic!("the SOCKS connection to {hub} failed: {error}"))
}

async fn scenario(protocol: Protocol) {
    clear_proxy_variables();
    let plain = support::service();
    let secure = support::parse_endpoint(&support::required_variable("DXN_SIGNALR_E2E_TLS_URL"));
    let ca = general_purpose::STANDARD
        .decode(support::required_variable(
            "DXN_SIGNALR_E2E_TLS_CA_PEM_BASE64",
        ))
        .expect("the CA is base64");
    let proxy = socks_url("socks5h", &credentials());

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
        let mut client =
            connect_through_socks(endpoint, hub, protocol, transport, &proxy, &ca).await;
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
            after.socks > before.socks,
            "{hub} over {transport:?}: no SOCKS request"
        );
        assert!(
            after.ports.contains(&u16::try_from(peer).unwrap()),
            "{hub} over {transport:?}: the service's peer {peer} is not a SOCKS tunnel ({after:?})"
        );
        let expected = address_type(&endpoint.host);
        assert!(
            after.socks_address_types[before.socks_address_types.len()..]
                .iter()
                .all(|kind| *kind == expected),
            "{hub} over {transport:?}: the destination {} went as address type {:?}, not {expected}",
            endpoint.host,
            after.socks_address_types
        );
        client.disconnect();
    }

    // socks5:// behaves as socks5h:// (.NET sends the name to the proxy for both).
    let before = stats().await;
    let socks5 = socks_url("socks5", &credentials());
    let mut client = connect_through_socks(
        &plain,
        "test",
        protocol,
        Transports::WEB_SOCKETS,
        &socks5,
        &ca,
    )
    .await;
    support::connection_identity(&mut client).await.unwrap();
    client.disconnect();
    let after = stats().await;
    assert!(after.socks > before.socks);
    assert_eq!(
        after.socks_address_types.last().copied(),
        Some(address_type(&plain.host))
    );

    // Wrong credentials: the proxy refuses the RFC 1929 sub-negotiation.
    let refused_before = stats().await.refused;
    let wrong = socks_url("socks5h", "dxn:wrong-secret");
    let error = SignalRClient::connect_with(&plain.host, "test", |configuration| {
        plain.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_proxy(&wrong);
    })
    .await
    .err()
    .expect("wrong SOCKS credentials are refused");
    assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");
    assert_eq!(error.status(), Some(407), "{error}");
    assert!(!error.is_transient());
    assert!(!error.to_string().contains("wrong-secret"), "{error}");
    assert!(!format!("{error:?}").contains("wrong-secret"), "{error:?}");
    assert!(stats().await.refused > refused_before);

    // A target the proxy's ruleset denies: reply 2.
    let error = SignalRClient::connect_with("denied.invalid", "test", |configuration| {
        configuration.unsecure();
        configuration.with_port(80);
        support::apply_protocol(configuration, protocol);
        configuration.with_proxy(&proxy);
    })
    .await
    .err()
    .expect("a denied target is refused");
    assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");
    assert_eq!(error.status(), Some(403), "{error}");
    assert!(error.to_string().contains("reply 0x02"), "{error}");

    // The system proxy: all_proxy names a SOCKS proxy, as .NET's `HttpEnvironmentProxy` allows.
    // The cell runs one test per process, so the environment is this scenario's.
    std::env::set_var("all_proxy", &proxy);
    let before = stats().await.socks;
    let mut proxied = support::connect("test", protocol, |_| {}).await;
    support::connection_identity(&mut proxied).await.unwrap();
    proxied.disconnect();
    assert!(
        stats().await.socks > before,
        "all_proxy tunnels through SOCKS"
    );
    clear_proxy_variables();
}

// Several tunnelled connections take a while on an emulator; the catalog bounds the cell at 120
// seconds.
crate::protocol_tests!("proxy-socks", std::time::Duration::from_secs(110));
