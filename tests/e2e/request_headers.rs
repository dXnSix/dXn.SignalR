//! Request headers and token placement as the official .NET client sends them: negotiate and the
//! WebSocket upgrade carry `User-Agent` in the official format, `X-Requested-With` and the
//! application's `with_header` headers (negotiate also `Accept: */*`), and the native WebSocket URL
//! carries no `access_token`: the bearer token travels only in `Authorization`. A request hook
//! (`with_request_hook`, as .NET's `HttpMessageHandlerFactory`) adds a header to both, naming the
//! request it saw.

use dxn_signalr::{SignalRClient, Transports};

use crate::support::{self, Protocol};

/// The value of `name` in the lines the test service describes a request with.
fn value<'a>(lines: &'a [String], name: &str) -> Option<&'a str> {
    let prefix = format!("{name}: ");
    lines.iter().find_map(|line| line.strip_prefix(&prefix))
}

async fn described(
    client: &mut SignalRClient,
    method: &str,
    argument: Option<&str>,
) -> Vec<String> {
    match argument {
        Some(argument) => {
            let argument = argument.to_string();
            client
                .invoke_with_args::<Vec<String>, _>(method.to_string(), |arguments| {
                    arguments.argument(argument.clone());
                })
                .await
        }
        None => client.invoke::<Vec<String>>(method.to_string()).await,
    }
    .unwrap()
}

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |configuration| {
        configuration.authenticate_bearer("e2e-token".to_string());
        configuration.with_header("X-Tenant", "blue");
        configuration.with_request_hook(|request| {
            let tag = match request.transport() {
                Some(transport) => format!("{transport:?} {}", request.method()),
                None => format!("negotiate {}", request.method()),
            };
            request.set_header("X-Hook", tag).unwrap();
        });
    })
    .await;

    let negotiate = described(&mut client, "NegotiateRequest", Some("/test")).await;
    assert!(
        value(&negotiate, "user-agent").is_some_and(|agent| agent.starts_with("dXn SignalR/")),
        "{negotiate:?}"
    );
    assert_eq!(
        value(&negotiate, "x-requested-with"),
        Some("XMLHttpRequest")
    );
    assert_eq!(value(&negotiate, "accept"), Some("*/*"));
    assert_eq!(value(&negotiate, "x-tenant"), Some("blue"));
    assert_eq!(value(&negotiate, "x-hook"), Some("negotiate POST"));
    assert_eq!(value(&negotiate, "authorization"), Some("present"));

    let transport = described(&mut client, "TransportRequest", None).await;
    assert!(
        value(&transport, "user-agent").is_some_and(|agent| agent.starts_with("dXn SignalR/")),
        "{transport:?}"
    );
    assert_eq!(
        value(&transport, "x-requested-with"),
        Some("XMLHttpRequest")
    );
    assert_eq!(value(&transport, "x-tenant"), Some("blue"));
    assert_eq!(value(&transport, "x-hook"), Some("WebSockets GET"));
    assert_eq!(value(&transport, "authorization"), Some("present"));
    assert_eq!(
        value(&transport, "access_token"),
        Some("absent"),
        "the native WebSocket URL carries no token"
    );
    client.disconnect();

    // Over Long Polling the hook sees the transport's requests too.
    let mut client = support::connect("test", protocol, |configuration| {
        configuration.with_transports(Transports::LONG_POLLING);
        configuration.with_request_hook(|request| {
            if let Some(transport) = request.transport() {
                request
                    .set_header("X-Hook", format!("{transport:?} {}", request.method()))
                    .unwrap();
            }
        });
    })
    .await;
    let transport = described(&mut client, "TransportRequest", None).await;
    assert_eq!(
        value(&transport, "x-hook"),
        Some("LongPolling GET"),
        "{transport:?}"
    );
    client.disconnect();
}

crate::protocol_tests!("request-headers");
