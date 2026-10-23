//! Handshake timeout: an endpoint that accepts the WebSocket but never answers the SignalR
//! handshake makes `connect` fail with a handshake-timeout error within the 1 s timeout plus a
//! 2 s margin.

use std::time::{Duration, Instant};

use crate::support::{self, Protocol};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);
const MARGIN: Duration = Duration::from_secs(2);

async fn scenario(protocol: Protocol) {
    let started = Instant::now();
    let result = support::try_connect("silent-handshake", protocol, |configuration| {
        configuration.skip_negotiation();
        configuration.with_transports(dxn_signalr::Transports::WEB_SOCKETS);
        configuration.with_handshake_timeout(HANDSHAKE_TIMEOUT);
    })
    .await;
    let elapsed = started.elapsed();
    let error = result
        .err()
        .expect("connect must fail when the server never answers the handshake");
    assert_eq!(
        (error.kind(), error.timeout_operation(), error.timeout()),
        (
            dxn_signalr::ErrorKind::Timeout,
            Some(dxn_signalr::TimeoutOperation::Handshake),
            Some(HANDSHAKE_TIMEOUT)
        ),
        "unexpected error: {error}"
    );
    assert!(
        elapsed >= HANDSHAKE_TIMEOUT && elapsed <= HANDSHAKE_TIMEOUT + MARGIN,
        "connect failed after {elapsed:?}"
    );
}

crate::protocol_tests!("handshake-timeout");
