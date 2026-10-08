//! HTTP/2 as the .NET client uses it: against the test service's HTTPS endpoint (HTTP/1.1 and
//! HTTP/2), `with_http_version(Http2)` runs negotiate and every transport over HTTP/2, the
//! WebSocket through RFC 8441 extended CONNECT, all on one multiplexed connection; h2c (prior
//! knowledge) works against the HTTP/2-only plaintext endpoint; the default `Auto` uses HTTP/2
//! for negotiate and HTTP/1.1 for the WebSocket, and falls back to HTTP/1.1 against the
//! HTTP/1.1-only HTTPS endpoint, where `Http2` fails with `ErrorKind::Transport`. Each reconnect
//! attempt opens a new HTTP/2 connection, as a new .NET `HttpConnection` does.

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use base64::{engine::general_purpose, Engine};
use rayx_signalr::{CallbackHandler, ErrorKind, HttpVersion, SignalRClient, Transports};

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
    hub: &str,
    protocol: Protocol,
    version: HttpVersion,
    transport: Transports,
    ca: &[u8],
) -> Result<SignalRClient, rayx_signalr::Error> {
    let ca = ca.to_vec();
    SignalRClient::connect_with(&endpoint.host, hub, |configuration| {
        endpoint.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_http_version(version);
        configuration.with_transports(transport);
        configuration.with_additional_root_certificates_pem(ca.clone());
    })
    .await
}

/// The protocols of the last negotiate request of `hub_path` and of the transport's request, and
/// whether both came on the same connection.
async fn protocols(client: &mut SignalRClient, hub_path: &str) -> (String, String, bool) {
    let negotiate = described(client, "NegotiateRequest", Some(hub_path)).await;
    let transport = described(client, "TransportRequest", None).await;
    (
        value(&negotiate, "protocol")
            .unwrap_or_default()
            .to_string(),
        value(&transport, "protocol")
            .unwrap_or_default()
            .to_string(),
        value(&negotiate, "connection").is_some()
            && value(&negotiate, "connection") == value(&transport, "connection"),
    )
}

async fn scenario(protocol: Protocol) {
    let secure = support::parse_endpoint(&support::required_variable("RAYX_SIGNALR_E2E_TLS_URL"));
    let h2c = support::parse_endpoint(&support::required_variable("RAYX_SIGNALR_E2E_H2C_URL"));
    let http1_only = support::parse_endpoint(&support::required_variable(
        "RAYX_SIGNALR_E2E_HTTP1_TLS_URL",
    ));
    let ca = general_purpose::STANDARD
        .decode(support::required_variable(
            "RAYX_SIGNALR_E2E_TLS_CA_PEM_BASE64",
        ))
        .expect("the CA is base64");

    // Http2 over TLS: negotiate and every transport over HTTP/2, on one connection.
    let mut transports = vec![Transports::WEB_SOCKETS, Transports::LONG_POLLING];
    if protocol == Protocol::Json {
        transports.push(Transports::SERVER_SENT_EVENTS);
    }
    for transport in transports {
        let mut client = connect(
            &secure,
            "test-tls",
            protocol,
            HttpVersion::Http2,
            transport,
            &ca,
        )
        .await
        .unwrap_or_else(|error| panic!("{transport:?} over HTTP/2: {error}"));
        let (negotiate, used, shared) = protocols(&mut client, "/test-tls").await;
        assert_eq!(negotiate, "HTTP/2", "{transport:?}: negotiate");
        assert_eq!(used, "HTTP/2", "{transport:?}: the transport");
        assert!(
            shared,
            "{transport:?}: negotiate and the transport share one connection"
        );
        client.disconnect();
    }

    // A reconnect negotiates on a new HTTP/2 connection, though the server reset only the
    // WebSocket's stream and the old connection is still open.
    let mut client = SignalRClient::connect_with(&secure.host, "test-tls", |configuration| {
        secure.apply(configuration);
        support::apply_protocol(configuration, protocol);
        configuration.with_http_version(HttpVersion::Http2);
        configuration.with_transports(Transports::WEB_SOCKETS);
        configuration.with_additional_root_certificates_pem(ca.clone());
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await
    .unwrap();
    let before = described(&mut client, "NegotiateRequest", Some("/test-tls")).await;
    let reconnects = Arc::new(AtomicU32::new(0));
    let counted = reconnects.clone();
    let hook = client.on_reconnected(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    support::eventually("the reconnect", Duration::from_secs(20), || {
        let done = reconnects.load(Ordering::SeqCst) == 1;
        async move { done.then_some(()) }
    })
    .await;
    hook.unregister();
    let after = described(&mut client, "NegotiateRequest", Some("/test-tls")).await;
    assert_eq!(value(&after, "protocol"), Some("HTTP/2"));
    assert_ne!(
        value(&before, "connection"),
        value(&after, "connection"),
        "the reconnect opened a new HTTP/2 connection"
    );
    client.disconnect();

    // h2c with prior knowledge, the WebSocket through extended CONNECT.
    let mut client = connect(
        &h2c,
        "test",
        protocol,
        HttpVersion::Http2,
        Transports::WEB_SOCKETS,
        &ca,
    )
    .await
    .unwrap_or_else(|error| panic!("h2c: {error}"));
    let (negotiate, used, _) = protocols(&mut client, "/test").await;
    assert_eq!(
        (negotiate.as_str(), used.as_str()),
        ("HTTP/2", "HTTP/2"),
        "h2c"
    );
    client.disconnect();

    // Auto (the default): HTTP/2 for negotiate, HTTP/1.1 for the WebSocket.
    let mut client = connect(
        &secure,
        "test-tls",
        protocol,
        HttpVersion::Auto,
        Transports::WEB_SOCKETS,
        &ca,
    )
    .await
    .unwrap();
    let (negotiate, used, _) = protocols(&mut client, "/test-tls").await;
    assert_eq!(
        (negotiate.as_str(), used.as_str()),
        ("HTTP/2", "HTTP/1.1"),
        "Auto"
    );
    client.disconnect();

    // Auto falls back to HTTP/1.1 where the server offers only HTTP/1.1, over every transport.
    let mut client = connect(
        &http1_only,
        "test-tls",
        protocol,
        HttpVersion::Auto,
        Transports::LONG_POLLING,
        &ca,
    )
    .await
    .unwrap();
    let (negotiate, used, _) = protocols(&mut client, "/test-tls").await;
    assert_eq!(
        (negotiate.as_str(), used.as_str()),
        ("HTTP/1.1", "HTTP/1.1"),
        "fallback"
    );
    client.disconnect();

    // Http2 against the HTTP/1.1-only endpoint fails.
    let error = connect(
        &http1_only,
        "test-tls",
        protocol,
        HttpVersion::Http2,
        Transports::WEB_SOCKETS,
        &ca,
    )
    .await
    .err()
    .expect("HTTP/2 is unavailable there");
    assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
}

crate::protocol_tests!("http2", std::time::Duration::from_secs(110));
