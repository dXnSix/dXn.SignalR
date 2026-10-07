//! Request headers and token placement as the official .NET client sends them: negotiate and the
//! WebSocket upgrade carry `User-Agent` in the official format, `X-Requested-With` and the
//! application's `with_header` headers (negotiate also `Accept: */*`), and the native WebSocket URL
//! carries no `access_token`: the bearer token travels only in `Authorization`.

use rayx_signalr::SignalRClient;

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
    })
    .await;

    let negotiate = described(&mut client, "NegotiateRequest", Some("/test")).await;
    assert!(
        value(&negotiate, "user-agent").is_some_and(|agent| agent.starts_with("RayX SignalR/")),
        "{negotiate:?}"
    );
    assert_eq!(
        value(&negotiate, "x-requested-with"),
        Some("XMLHttpRequest")
    );
    assert_eq!(value(&negotiate, "accept"), Some("*/*"));
    assert_eq!(value(&negotiate, "x-tenant"), Some("blue"));
    assert_eq!(value(&negotiate, "authorization"), Some("present"));

    let transport = described(&mut client, "TransportRequest", None).await;
    assert!(
        value(&transport, "user-agent").is_some_and(|agent| agent.starts_with("RayX SignalR/")),
        "{transport:?}"
    );
    assert_eq!(
        value(&transport, "x-requested-with"),
        Some("XMLHttpRequest")
    );
    assert_eq!(value(&transport, "x-tenant"), Some("blue"));
    assert_eq!(value(&transport, "authorization"), Some("present"));
    assert_eq!(
        value(&transport, "access_token"),
        Some("absent"),
        "the native WebSocket URL carries no token"
    );
    client.disconnect();
}

crate::protocol_tests!("request-headers");
