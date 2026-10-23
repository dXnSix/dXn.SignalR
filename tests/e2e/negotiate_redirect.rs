//! Negotiate redirect: the service's `/redirect` negotiate answers with the test hub URL and an
//! access token; the client renegotiates there, the hub sees the redirect token, and invocations
//! work.

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("redirect", protocol, |_| {}).await;
    let token = client
        .invoke::<String>("AccessTokenProbe".to_string())
        .await
        .unwrap();
    assert_eq!(
        token, "redirect-token",
        "the transport uses the redirect token"
    );
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(307);
        })
        .await
        .unwrap();
    assert_eq!(echoed, 307);
    client.disconnect();

    let mut probe = support::connect("test", protocol, |_| {}).await;
    let counts = (
        negotiate_count(&mut probe, "/redirect").await,
        negotiate_count(&mut probe, "/test").await,
    );
    // One redirect negotiate, then the redirected negotiate plus the probe's own.
    assert_eq!(counts, (1, 2));
    probe.disconnect();
}

async fn negotiate_count(probe: &mut dxn_signalr::SignalRClient, hub_path: &str) -> i32 {
    probe
        .invoke_with_args::<i32, _>("NegotiateCount".to_string(), |arguments| {
            arguments.argument(hub_path.to_string());
        })
        .await
        .unwrap()
}

crate::protocol_tests!("negotiate-redirect");
