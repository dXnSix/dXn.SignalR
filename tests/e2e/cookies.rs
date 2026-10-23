//! Cookies, as the .NET client's `HttpConnectionOptions.Cookies`: the test service's
//! `/test-affinity` hub sets an affinity cookie on negotiate and refuses every later request (the
//! WebSocket upgrade, Long Polling and Server-Sent Events requests) without it, as a load
//! balancer that routes by cookie does. Over each transport the client's jar sends it: the hub
//! sees the cookie the application's jar holds, and after a transport loss the reconnect
//! negotiates a fresh cookie and connects with it. No request arrives without one.

use std::time::Duration;

use dxn_signalr::{CookieJar, SignalRClient, Transports};

use crate::support::{self, Protocol};

async fn affinity_cookie(client: &mut SignalRClient) -> String {
    client
        .invoke::<String>("AffinityCookie".to_string())
        .await
        .unwrap()
}

async fn scenario(protocol: Protocol) {
    let endpoint = support::service();
    let hub_url = format!(
        "{}://{}{}/test-affinity",
        if endpoint.secure { "https" } else { "http" },
        endpoint.host,
        endpoint
            .port
            .map(|port| format!(":{port}"))
            .unwrap_or_default()
    );
    let mut transports = vec![Transports::WEB_SOCKETS, Transports::LONG_POLLING];
    if protocol == Protocol::Json {
        transports.push(Transports::SERVER_SENT_EVENTS);
    }
    for transport in transports {
        let jar = CookieJar::new();
        let mut client = support::connect("test-affinity", protocol, |configuration| {
            configuration.with_cookie_jar(jar.clone());
            configuration.with_transports(transport);
            configuration.with_reconnection_policy(support::fast_reconnect());
        })
        .await;
        let first = affinity_cookie(&mut client).await;
        assert!(
            !first.is_empty(),
            "{transport:?}: the transport carried no cookie"
        );
        assert_eq!(
            jar.cookie_header(&hub_url),
            Some(format!("dXnAffinity={first}")),
            "{transport:?}: the application's jar holds the cookie the hub saw"
        );

        // A reconnect negotiates afresh: the jar replaces the cookie and sends the new one.
        let identity = support::connection_identity(&mut client).await.unwrap();
        client
            .send("AbortPhysicalTransport".to_string())
            .await
            .unwrap();
        support::eventually(
            "a reconnect on a new connection",
            Duration::from_secs(20),
            || {
                let mut probe = client.clone();
                let identity = identity.clone();
                async move {
                    support::connection_identity(&mut probe)
                        .await
                        .ok()
                        .filter(|current| *current != identity)
                }
            },
        )
        .await;
        let second = affinity_cookie(&mut client).await;
        assert_ne!(
            second, first,
            "{transport:?}: the reconnect negotiated a fresh cookie"
        );
        assert_eq!(
            jar.cookie_header(&hub_url),
            Some(format!("dXnAffinity={second}"))
        );
        client.disconnect();
    }

    let mut probe = support::connect("test", protocol, |_| {}).await;
    let rejections = probe
        .invoke::<i32>("AffinityRejections".to_string())
        .await
        .unwrap();
    assert_eq!(
        rejections, 0,
        "no request arrived without the affinity cookie"
    );
    probe.disconnect();
}

crate::protocol_tests!("cookies");
