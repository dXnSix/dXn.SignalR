//! Shared helpers for the end-to-end scenarios.

use std::{future::Future, sync::Arc, time::Duration};

use rayx_signalr::{
    ConnectionConfiguration, ConstantDelayPolicy, ReconnectionConfig, SignalRClient,
};
use serde::{Deserialize, Serialize};

/// The test hub's simple entity (`TestEntity` in `RayX.SignalR.TestService`).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TestEntity {
    pub number: i32,
    pub text: String,
}

/// Upper bound for one scenario; the runner kills the process after 60 seconds.
pub const SCENARIO_TIMEOUT: Duration = Duration::from_secs(50);

/// Wire protocol selected by the scenario's test name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Json,
    #[cfg_attr(not(feature = "messagepack"), allow(dead_code))]
    MessagePack,
}

/// A parsed `http(s)://host[:port]` service endpoint.
#[derive(Clone, Debug)]
pub struct ServiceEndpoint {
    pub host: String,
    pub port: Option<i32>,
    pub secure: bool,
}

impl ServiceEndpoint {
    /// Applies host security and port to a connection configuration.
    pub fn apply(&self, configuration: &mut ConnectionConfiguration) {
        if self.secure {
            configuration.secure();
        } else {
            configuration.unsecure();
        }
        if let Some(port) = self.port {
            configuration.with_port(port);
        }
    }
}

/// Reads a required runner-provided variable.
pub fn required_variable(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "{name} is not set; run end-to-end scenarios through \
             `dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E`"
        )
    })
}

/// Parses an absolute HTTP(S) URL into the parts `connect_with` needs.
pub fn parse_endpoint(url: &str) -> ServiceEndpoint {
    let (secure, rest) = if let Some(rest) = url.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (false, rest)
    } else {
        panic!("service URL must be http(s): {url}");
    };
    let authority = rest.split('/').next().unwrap_or_default();
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (
            host,
            Some(
                port.parse()
                    .unwrap_or_else(|_| panic!("invalid port in {url}")),
            ),
        ),
        None => (authority, None),
    };
    ServiceEndpoint {
        host: host.to_string(),
        port,
        secure,
    }
}

/// The plaintext SignalR test service endpoint.
pub fn service() -> ServiceEndpoint {
    parse_endpoint(&required_variable("RAYX_SIGNALR_E2E_SERVICE_URL"))
}

/// Selects the protocol on a configuration.
pub fn apply_protocol(configuration: &mut ConnectionConfiguration, protocol: Protocol) {
    match protocol {
        Protocol::Json => {}
        #[cfg(feature = "messagepack")]
        Protocol::MessagePack => {
            configuration.with_messagepack_protocol();
        }
        #[cfg(not(feature = "messagepack"))]
        Protocol::MessagePack => {
            let _ = configuration;
            panic!("MessagePack scenarios require the messagepack feature")
        }
    }
}

/// A fast bounded reconnect policy for scenarios that drop the transport.
pub fn fast_reconnect() -> ReconnectionConfig {
    ReconnectionConfig {
        policy: Arc::new(ConstantDelayPolicy::new(
            Duration::from_millis(50),
            Some(100),
        )),
    }
}

/// Connects without unwrapping, for scenarios that expect `connect_with` to fail.
pub async fn try_connect<F>(
    hub: &str,
    protocol: Protocol,
    mut configure: F,
) -> Result<SignalRClient, String>
where
    F: FnMut(&mut ConnectionConfiguration),
{
    let endpoint = service();
    SignalRClient::connect_with(&endpoint.host, hub, |configuration| {
        endpoint.apply(configuration);
        apply_protocol(configuration, protocol);
        configure(configuration);
    })
    .await
}

/// Connects to `hub` on the test service with the selected protocol and extra configuration.
pub async fn connect<F>(hub: &str, protocol: Protocol, mut configure: F) -> SignalRClient
where
    F: FnMut(&mut ConnectionConfiguration),
{
    // Client logs reach the cell diagnostics when RUST_LOG is set.
    let _ = env_logger::builder().is_test(true).try_init();
    let endpoint = service();
    SignalRClient::connect_with(&endpoint.host, hub, |configuration| {
        endpoint.apply(configuration);
        apply_protocol(configuration, protocol);
        configure(configuration);
    })
    .await
    .unwrap_or_else(|error| panic!("connect to {hub} failed: {error}"))
}

/// Runs a scenario under the scenario bound. A watchdog on a plain OS thread reports a scenario
/// that outlives the bound, which can only happen when the runtime thread itself is blocked (the
/// bound is a Tokio timer), and ends the process so the cell fails with that diagnosis.
pub async fn bounded<F: Future<Output = ()>>(name: &str, scenario: F) {
    raise_scenario_thread_priority();
    let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watched = finished.clone();
    let label = name.to_string();
    std::thread::spawn(move || {
        std::thread::sleep(SCENARIO_TIMEOUT + Duration::from_secs(3));
        if !watched.load(std::sync::atomic::Ordering::Acquire) {
            eprintln!(
                "watchdog: {label} is still running {:?} after its start; the Tokio timer did not fire, so the runtime thread is blocked",
                SCENARIO_TIMEOUT + Duration::from_secs(3)
            );
            std::process::exit(102);
        }
    });
    let result = tokio::time::timeout(SCENARIO_TIMEOUT, scenario).await;
    finished.store(true, std::sync::atomic::Ordering::Release);
    result.unwrap_or_else(|_| panic!("{name} exceeded {SCENARIO_TIMEOUT:?}"));
}

/// On the iOS simulator the scenario binary is launched with `xcrun simctl spawn`, not as a
/// foreground app, and the system throttles and coalesces its wakeups; the scenario thread (which
/// runs the whole current-thread runtime) asks for the user-interactive class a foreground app's
/// networking would have.
#[cfg(target_os = "ios")]
fn raise_scenario_thread_priority() {
    /// `QOS_CLASS_USER_INTERACTIVE` from `<sys/qos.h>`.
    const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    // SAFETY: the call only changes the calling thread's scheduling class.
    let status = unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) };
    if status != 0 {
        eprintln!("cannot raise the scenario thread's QoS class: error {status}");
    }
}

#[cfg(not(target_os = "ios"))]
fn raise_scenario_thread_priority() {}

/// Polls `probe` until it returns `Some`, failing with `what` after `limit`.
pub async fn eventually<T, F, Fut>(what: &str, limit: Duration, mut probe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    tokio::time::timeout(limit, async {
        loop {
            if let Some(value) = probe().await {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out after {limit:?} waiting for {what}"))
}

/// Declares the `json` and `messagepack` tests of a scenario module.
#[macro_export]
macro_rules! protocol_tests {
    ($name:literal) => {
        #[tokio::test]
        async fn json() {
            $crate::support::bounded(
                concat!($name, " json"),
                scenario($crate::support::Protocol::Json),
            )
            .await;
        }

        #[cfg(feature = "messagepack")]
        #[tokio::test]
        async fn messagepack() {
            $crate::support::bounded(
                concat!($name, " messagepack"),
                scenario($crate::support::Protocol::MessagePack),
            )
            .await;
        }
    };
}

/// Returns the hub-side connection id of the current logical connection.
pub async fn connection_identity(client: &mut SignalRClient) -> Result<String, String> {
    client
        .invoke::<String>("ConnectionIdentity".to_string())
        .await
}
