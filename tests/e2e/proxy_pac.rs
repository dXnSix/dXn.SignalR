//! Proxy auto-config: the Aspire `test-proxy` serves PAC scripts (`/proxy.pac?http=..&https=..`)
//! that route plain and secure endpoints over its credential-free listener by HTTP `CONNECT` or
//! SOCKS5, or directly. With `with_proxy_auto_config` every connection follows the script: each
//! tunnel is counted and its port is the service's peer, `DIRECT` opens no tunnel, an unreachable
//! first route falls through to the next, and the script is loaded once per `start`. A failing or
//! missing script fails with `ErrorKind::Proxy`.

use base64::{engine::general_purpose, Engine};
use rayx_signalr::{ErrorKind, SignalRClient};

use crate::{
    proxy::{clear_proxy_variables, proxy_endpoint, stats},
    support::{self, Protocol, ServiceEndpoint},
};

/// The test proxy's PAC script URL with `query`.
fn pac_url(path_and_query: &str) -> String {
    let proxy = proxy_endpoint();
    let port = proxy.port.expect("the proxy URL has a port");
    format!("http://{}:{port}{path_and_query}", proxy.host)
}

fn client(
    endpoint: &ServiceEndpoint,
    hub: &str,
    protocol: Protocol,
    pac: &str,
    ca: &[u8],
) -> SignalRClient {
    let (pac, ca) = (pac.to_string(), ca.to_vec());
    SignalRClient::new(&endpoint.host, hub, |configuration| {
        endpoint.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_proxy_auto_config(&pac);
        configuration.with_additional_root_certificates_pem(ca.clone());
    })
    .unwrap()
}

async fn echo(client: &mut SignalRClient) {
    let echoed: i32 = client
        .invoke_with_args("Echo".to_string(), |arguments| {
            arguments.argument(7);
        })
        .await
        .unwrap();
    assert_eq!(echoed, 7);
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
    let routes = pac_url("/proxy.pac?http=proxy&https=socks5");

    // A plain endpoint over HTTP CONNECT; the script is loaded once for the start's connections.
    let before = stats().await;
    let mut proxied = client(&plain, "test", protocol, &routes, &ca);
    proxied.start().await.unwrap();
    echo(&mut proxied).await;
    let peer: i32 = proxied.invoke("PeerPort".to_string()).await.unwrap();
    let after = stats().await;
    assert!(after.tunnels > before.tunnels, "no CONNECT tunnel opened");
    assert_eq!(after.socks, before.socks, "the plain route is HTTP CONNECT");
    assert!(
        after.ports.contains(&u16::try_from(peer).unwrap()),
        "the service's peer {peer} is not a proxy tunnel ({after:?})"
    );
    assert_eq!(
        after.pac_scripts,
        before.pac_scripts + 1,
        "one script load for negotiate and the transport"
    );

    // A new start loads the script again.
    proxied.stop().await;
    proxied.start().await.unwrap();
    echo(&mut proxied).await;
    assert_eq!(stats().await.pac_scripts, after.pac_scripts + 1);
    proxied.disconnect();

    // A secure endpoint over SOCKS5, with TLS inside the tunnel.
    let before = stats().await;
    let mut socks = client(&secure, "test-tls", protocol, &routes, &ca);
    socks.start().await.unwrap();
    echo(&mut socks).await;
    assert!(
        stats().await.socks > before.socks,
        "no SOCKS5 tunnel opened"
    );
    socks.disconnect();

    // DIRECT, and an unreachable proxy that falls through to DIRECT: no tunnel.
    for query in ["/proxy.pac?http=direct", "/proxy.pac?http=dead"] {
        let before = stats().await;
        let mut direct = client(&plain, "test", protocol, &pac_url(query), &ca);
        direct
            .start()
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        echo(&mut direct).await;
        let after = stats().await;
        assert_eq!(after.tunnels, before.tunnels, "{query}: no tunnel");
        assert_eq!(after.pac_scripts, before.pac_scripts + 1, "{query}");
        direct.disconnect();
    }

    // A failing script and a missing one are proxy errors.
    for query in ["/proxy.pac?http=throw", "/no-such-script.pac"] {
        let failing = client(&plain, "test", protocol, &pac_url(query), &ca);
        let error = failing
            .start()
            .await
            .err()
            .unwrap_or_else(|| panic!("{query}: the start succeeded"));
        assert_eq!(error.kind(), ErrorKind::Proxy, "{query}: {error}");
    }
}

crate::protocol_tests!("proxy-pac", std::time::Duration::from_secs(110));
