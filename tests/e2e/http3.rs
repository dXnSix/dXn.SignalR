//! Experimental HTTP/3 (feature `http3`) against the test service's HTTP/3 endpoint (Kestrel over
//! msquic): with `with_http_version(Http3)`, negotiate, Long Polling and Server-Sent Events run
//! over HTTP/3, and WebSockets are skipped (Kestrel does not bootstrap them over HTTP/3, RFC
//! 9220), so the default transport set starts the next transport and WebSockets alone fail with
//! `ErrorKind::TransportUnavailable`. `Http3Preferred` uses HTTP/3 where QUIC answers and falls
//! back to TCP (HTTP/2, as `Auto`) against the service's TCP-only HTTPS endpoint.

use std::time::Duration;

use base64::{engine::general_purpose, Engine};
use dxn_signalr::{ErrorKind, HttpVersion, SignalRClient, Transports};

use crate::support::{self, Protocol, ServiceEndpoint};

/// The value of `name` in the lines the test service describes a request with.
fn value<'a>(lines: &'a [String], name: &str) -> Option<&'a str> {
    let prefix = format!("{name}: ");
    lines.iter().find_map(|line| line.strip_prefix(&prefix))
}

async fn described(
    client: &mut SignalRClient,
    method: &str,
    hub_path: Option<&str>,
) -> Vec<String> {
    match hub_path {
        Some(path) => {
            let path = path.to_string();
            client
                .invoke_with_args::<Vec<String>, _>(method.to_string(), |arguments| {
                    arguments.argument(path.clone());
                })
                .await
        }
        None => client.invoke::<Vec<String>>(method.to_string()).await,
    }
    .unwrap()
}

async fn connect(
    endpoint: &ServiceEndpoint,
    protocol: Protocol,
    version: HttpVersion,
    transport: Transports,
    ca: &[u8],
) -> Result<SignalRClient, dxn_signalr::Error> {
    let ca = ca.to_vec();
    SignalRClient::connect_with(&endpoint.host, "test-tls", |configuration| {
        endpoint.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_http_version(version);
        configuration.with_transports(transport);
        configuration.with_additional_root_certificates_pem(ca.clone());
        // QUIC gets half of it under `Http3Preferred`.
        configuration.with_connect_timeout(Duration::from_secs(8));
    })
    .await
}

/// The HTTP versions of the last negotiate request and of the transport's request.
async fn protocols(client: &mut SignalRClient) -> (String, String) {
    let negotiate = described(client, "NegotiateRequest", Some("/test-tls")).await;
    let transport = described(client, "TransportRequest", None).await;
    (
        value(&negotiate, "protocol")
            .unwrap_or_default()
            .to_string(),
        value(&transport, "protocol")
            .unwrap_or_default()
            .to_string(),
    )
}

async fn scenario(protocol: Protocol) {
    let http3 = support::parse_endpoint(&support::required_variable("DXN_SIGNALR_E2E_HTTP3_URL"));
    let tcp_only = support::parse_endpoint(&support::required_variable("DXN_SIGNALR_E2E_TLS_URL"));
    let ca = general_purpose::STANDARD
        .decode(support::required_variable(
            "DXN_SIGNALR_E2E_TLS_CA_PEM_BASE64",
        ))
        .expect("the CA is base64");

    // Http3: negotiate and the HTTP transports over HTTP/3; with every transport allowed,
    // WebSockets are skipped and the next transport runs over HTTP/3.
    let mut transports = vec![Transports::LONG_POLLING, Transports::all()];
    if protocol == Protocol::Json {
        transports.push(Transports::SERVER_SENT_EVENTS);
    }
    for transport in transports {
        let mut client = connect(&http3, protocol, HttpVersion::Http3, transport, &ca)
            .await
            .unwrap_or_else(|error| panic!("{transport:?} over HTTP/3: {error}"));
        assert_eq!(
            protocols(&mut client).await,
            ("HTTP/3".to_string(), "HTTP/3".to_string()),
            "{transport:?}"
        );
        client.disconnect();
    }

    // WebSockets alone are unavailable over HTTP/3.
    let error = connect(
        &http3,
        protocol,
        HttpVersion::Http3,
        Transports::WEB_SOCKETS,
        &ca,
    )
    .await
    .err()
    .expect("WebSockets do not run over HTTP/3");
    assert_eq!(error.kind(), ErrorKind::TransportUnavailable, "{error}");
    assert!(error.to_string().contains("RFC 9220"), "{error}");

    // Http3Preferred: HTTP/3 where QUIC answers (negotiate and Long Polling) ...
    let mut client = connect(
        &http3,
        protocol,
        HttpVersion::Http3Preferred,
        Transports::LONG_POLLING,
        &ca,
    )
    .await
    .unwrap();
    assert_eq!(
        protocols(&mut client).await,
        ("HTTP/3".to_string(), "HTTP/3".to_string()),
        "preferred"
    );
    client.disconnect();

    // ... and TCP (HTTP/2, as Auto) where it does not, within half of the connect timeout.
    let started = std::time::Instant::now();
    let mut client = connect(
        &tcp_only,
        protocol,
        HttpVersion::Http3Preferred,
        Transports::LONG_POLLING,
        &ca,
    )
    .await
    .unwrap();
    assert!(started.elapsed() < Duration::from_secs(8));
    assert_eq!(
        protocols(&mut client).await,
        ("HTTP/2".to_string(), "HTTP/2".to_string()),
        "fallback"
    );
    client.disconnect();
}

crate::protocol_tests!("http3", std::time::Duration::from_secs(110));
