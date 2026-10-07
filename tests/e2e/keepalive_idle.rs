//! Keep-alive: an idle client's Pings keep it connected to a hub that drops clients silent for
//! 3 s, so after 8 s of idleness the same connection serves an invocation. Long Polling keeps
//! itself alive (no Pings, as in the .NET client and ASP.NET Core), so over it the client stays
//! connected through 45 s of idleness.

use std::time::Duration;

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test-keepalive", protocol, |configuration| {
        configuration.with_keep_alive_interval(Duration::from_secs(1));
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let initial = support::connection_identity(&mut client).await.unwrap();

    let idle = if support::long_polling() {
        support::LONG_POLLING_IDLE
    } else {
        Duration::from_secs(8)
    };
    tokio::time::sleep(idle).await;

    let after_idle = support::connection_identity(&mut client).await.unwrap();
    assert_eq!(after_idle, initial, "the idle connection must stay alive");
    client.disconnect();
}

crate::protocol_tests!("keepalive-idle", support::LIVENESS_SCENARIO_TIMEOUT);
