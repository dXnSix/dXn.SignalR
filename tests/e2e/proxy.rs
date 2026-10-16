//! Proxies: through the Aspire `test-proxy` (an HTTP CONNECT proxy that requires the run's
//! credentials), plaintext and TLS endpoints connect over all three transports, each through a
//! tunnel the proxy counts and whose port the service sees as its peer; TLS inside the tunnel
//! still verifies the server (without the test CA it fails with `Tls`); wrong credentials fail
//! with the proxy's 407 (`ErrorKind::Proxy`) and never show them; a denied target fails with 403;
//! and with the system proxy from `http_proxy`, a `no_proxy` match connects directly while no
//! match tunnels.

use base64::{engine::general_purpose, Engine};
use rayx_signalr::{ConnectionConfiguration, ErrorKind, SignalRClient, Transports};

/// Every proxy variable the client reads, in both cases.
const PROXY_VARIABLES: [&str; 9] = [
    "http_proxy",
    "HTTP_PROXY",
    "https_proxy",
    "HTTPS_PROXY",
    "all_proxy",
    "ALL_PROXY",
    "no_proxy",
    "NO_PROXY",
    "GATEWAY_INTERFACE",
];

/// Clears every proxy variable (the cell runs one test per process, so the environment is this
/// scenario's).
pub(crate) fn clear_proxy_variables() {
    for name in PROXY_VARIABLES {
        std::env::remove_var(name);
    }
}
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::support::{self, Protocol, ServiceEndpoint};

/// The proxy's counters (`GET /stats`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Stats {
    pub(crate) tunnels: u64,
    pub(crate) refused: u64,
    pub(crate) ports: Vec<u16>,
    /// SOCKS5 requests, and the address type each named its destination with.
    pub(crate) socks: u64,
    pub(crate) socks_address_types: Vec<u8>,
    /// PAC scripts served (read by `proxy-pac`, which iOS does not build).
    #[cfg_attr(target_os = "ios", allow(dead_code))]
    pub(crate) pac_scripts: u64,
    /// The TLS listener's port (`https://` proxies) and the TLS sessions clients opened with it.
    pub(crate) tls_port: u16,
    pub(crate) tls_sessions: u64,
}

pub(crate) fn proxy_endpoint() -> ServiceEndpoint {
    support::parse_endpoint(&support::required_variable("RAYX_SIGNALR_E2E_PROXY_URL"))
}

/// The proxy URL with `credentials` (`user:password`).
fn proxy_url(credentials: &str) -> String {
    let proxy = proxy_endpoint();
    let port = proxy.port.expect("the proxy URL has a port");
    format!("http://{credentials}@{}:{port}", proxy.host)
}

pub(crate) fn credentials() -> String {
    support::required_variable("RAYX_SIGNALR_E2E_PROXY_CREDENTIALS")
}

/// Reads the proxy's counters with a direct request.
pub(crate) async fn stats() -> Stats {
    let proxy = proxy_endpoint();
    let port = proxy.port.expect("the proxy URL has a port");
    let port = u16::try_from(port).expect("a TCP port");
    let mut stream = tokio::net::TcpStream::connect((proxy.host.as_str(), port))
        .await
        .unwrap();
    stream
        .write_all(b"GET /stats HTTP/1.1\r\nHost: test-proxy\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let (_, body) = response.split_once("\r\n\r\n").expect("an HTTP response");
    serde_json::from_str(body).unwrap()
}

/// Connects `hub` at `endpoint` over `transports` through the proxy with valid credentials.
async fn connect_through_proxy(
    endpoint: &ServiceEndpoint,
    hub: &str,
    protocol: Protocol,
    transports: Transports,
    configure: impl Fn(&mut ConnectionConfiguration),
) -> SignalRClient {
    let proxy = proxy_url(&credentials());
    SignalRClient::connect_with(&endpoint.host, hub, |configuration| {
        endpoint.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_transports(transports);
        configuration.with_proxy(&proxy);
        configure(configuration);
    })
    .await
    .unwrap_or_else(|error| panic!("the proxied connection to {hub} failed: {error}"))
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

    // Plaintext and TLS over every transport the protocol can use.
    let mut transports = vec![Transports::WEB_SOCKETS, Transports::LONG_POLLING];
    if protocol == Protocol::Json {
        transports.push(Transports::SERVER_SENT_EVENTS);
    }
    for transport in transports {
        for (endpoint, hub) in [(&plain, "test"), (&secure, "test-tls")] {
            let before = stats().await.tunnels;
            let ca = ca.clone();
            let mut client =
                connect_through_proxy(endpoint, hub, protocol, transport, move |configuration| {
                    configuration.with_additional_root_certificates_pem(ca.clone());
                })
                .await;
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
                after.tunnels > before,
                "{hub} over {transport:?}: no tunnel opened"
            );
            assert!(
                after.ports.contains(&u16::try_from(peer).unwrap()),
                "{hub} over {transport:?}: the service's peer {peer} is not a proxy tunnel ({after:?})"
            );
            client.disconnect();
        }
    }

    // TLS inside the tunnel still verifies the server: without the test CA it fails.
    let proxy = proxy_url(&credentials());
    let error = SignalRClient::connect_with(&secure.host, "test-tls", |configuration| {
        secure.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_proxy(&proxy);
        #[cfg(target_os = "android")]
        configuration.use_webpki_roots();
    })
    .await
    .err()
    .expect("a proxied TLS endpoint without the test CA is refused");
    assert_eq!(error.kind(), ErrorKind::Tls, "{error}");

    // Wrong credentials: the proxy's 407, without the credentials.
    let refused_before = stats().await.refused;
    let wrong = proxy_url("rayx:wrong-secret");
    let error = SignalRClient::connect_with(&plain.host, "test", |configuration| {
        plain.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_proxy(&wrong);
    })
    .await
    .err()
    .expect("wrong proxy credentials are refused");
    assert_eq!(error.kind(), ErrorKind::Proxy, "{error}");
    assert_eq!(error.status(), Some(407), "{error}");
    assert!(!error.is_transient());
    assert!(!error.to_string().contains("wrong-secret"), "{error}");
    assert!(!format!("{error:?}").contains("wrong-secret"), "{error:?}");
    assert!(stats().await.refused > refused_before);

    // A target on the proxy's deny list.
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

    // The system proxy: HTTP_PROXY tunnels unless NO_PROXY matches the host. The cell runs one
    // test per process, so the environment is this scenario's.
    std::env::set_var("http_proxy", &proxy);
    std::env::set_var("no_proxy", &plain.host);
    let before = stats().await.tunnels;
    let mut bypassed = support::connect("test", protocol, |_| {}).await;
    support::connection_identity(&mut bypassed).await.unwrap();
    bypassed.disconnect();
    assert_eq!(stats().await.tunnels, before, "NO_PROXY bypasses the proxy");

    std::env::remove_var("no_proxy");
    let mut proxied = support::connect("test", protocol, |_| {}).await;
    support::connection_identity(&mut proxied).await.unwrap();
    proxied.disconnect();
    assert!(stats().await.tunnels > before, "http_proxy tunnels");
    clear_proxy_variables();
}

// A dozen tunnelled connections take most of a minute on an emulator; the catalog bounds the
// cell at 120 seconds.
crate::protocol_tests!("proxy", std::time::Duration::from_secs(110));
