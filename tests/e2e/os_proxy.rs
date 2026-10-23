//! Operating-system proxy settings: the runner points the system proxy at the test proxy's
//! credential-free listener (the Android emulator's global HTTP proxy, or the Windows or macOS
//! user proxy on CI runners that opt in to system changes) and restores it afterwards. The client
//! reads it from the system (`with_os_proxy`, and by default, as the .NET client) for the host
//! `dxn-os-proxy.test`, which only the test proxy resolves (to the service): every connection
//! tunnels through it, while `without_proxy` cannot reach the host.

use dxn_signalr::{ConnectionConfiguration, ErrorKind, SignalRClient};

use crate::{
    proxy::{clear_proxy_variables, stats},
    support::{self, Protocol},
};

/// The host only the test proxy resolves, to the test service.
const ALIAS: &str = "dxn-os-proxy.test";

async fn connect(
    protocol: Protocol,
    configure: impl Fn(&mut ConnectionConfiguration),
) -> Result<SignalRClient, dxn_signalr::Error> {
    let service = support::service();
    SignalRClient::connect_with(ALIAS, "test", |configuration| {
        configuration.unsecure();
        if let Some(port) = service.port {
            configuration.with_port(port);
        }
        support::apply_protocol(configuration, protocol);
        configure(configuration);
    })
    .await
}

pub(crate) async fn scenario(protocol: Protocol) {
    // The system's settings alone: no proxy environment variable applies.
    clear_proxy_variables();
    let os_only: fn(&mut ConnectionConfiguration) = |configuration| {
        configuration.with_os_proxy();
    };
    let default: fn(&mut ConnectionConfiguration) = |_| {};
    for (name, configure) in [("with_os_proxy", os_only), ("the default", default)] {
        let before = stats().await;
        let mut client = connect(protocol, configure)
            .await
            .unwrap_or_else(|error| panic!("{name}: the system proxy was not used: {error}"));
        let echoed: i32 = client
            .invoke_with_args("Echo".to_string(), |arguments| {
                arguments.argument(7);
            })
            .await
            .unwrap();
        assert_eq!(echoed, 7);
        let peer: i32 = client.invoke("PeerPort".to_string()).await.unwrap();
        let after = stats().await;
        assert!(after.tunnels > before.tunnels, "{name}: no tunnel opened");
        assert!(
            after.ports.contains(&u16::try_from(peer).unwrap()),
            "{name}: the service's peer {peer} is not a proxy tunnel ({after:?})"
        );
        client.disconnect();
    }

    // Without a proxy the host does not resolve.
    let error = connect(protocol, |configuration| {
        configuration.without_proxy();
    })
    .await
    .err()
    .expect("the alias resolves only through the proxy");
    assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
}

crate::protocol_tests!("os-proxy");
