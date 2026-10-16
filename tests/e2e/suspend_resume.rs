//! Suspension: a stateful and an ordinary client (both with a reconnection policy) each leave an
//! invocation pending, and the scenario asks the runner to suspend this process (Windows
//! `NtSuspendProcess`, Unix and the iOS simulator `SIGSTOP`, Android `kill -STOP`) twice:
//!
//! - for 20 s, longer than the keep-alive interval but shorter than the service's 30-second
//!   client timeout: both clients keep their connections and their invocations complete;
//! - for 40 s, longer than the client timeout, after which ASP.NET Core has ended both hub
//!   connections (no client can resume them): within 5 s of resuming, both clients have detected
//!   the loss, their pending invocations have failed with a connection error, and they have
//!   reconnected on new connections.

use std::{
    io::Write,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use rayx_signalr::{
    CallbackHandler, ConnectionStatus, ErrorKind, SignalRClient, StatefulReconnectConfig,
};

use crate::support::{self, Protocol};

/// How soon after resuming the clients must have recovered from the long suspension.
const RECOVERY: Duration = Duration::from_secs(5);

/// Asks the runner to suspend this process for `seconds` and returns when it resumed: a plain
/// thread sleeping in 50 ms steps sees the pause.
async fn suspend(seconds: u64) -> Instant {
    let gap = Duration::from_secs(seconds) * 3 / 4;
    let done = Arc::new(AtomicBool::new(false));
    let resumed = Arc::new(Mutex::new(None));
    // Taken before the readiness line: the thread may start only after the suspension.
    let mut last = Instant::now();
    {
        let (done, resumed) = (done.clone(), resumed.clone());
        std::thread::spawn(move || {
            while !done.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(50));
                let now = Instant::now();
                if now - last >= gap {
                    *resumed.lock().unwrap() = Some(now);
                    return;
                }
                last = now;
            }
        });
    }
    let ready = Instant::now();
    {
        let mut stdout = std::io::stdout().lock();
        writeln!(
            stdout,
            "RAYX_SIGNALR_SUSPEND_READY pid={} seconds={seconds}",
            std::process::id()
        )
        .unwrap();
        stdout.flush().unwrap();
    }
    let resumed_at = support::eventually(
        "the runner's suspension and the resume",
        Duration::from_secs(seconds + 20),
        || {
            let resumed = resumed.clone();
            async move { *resumed.lock().unwrap() }
        },
    )
    .await;
    done.store(true, Ordering::Release);
    eprintln!(
        "suspended for {seconds} s: resumed {:.1} s after the readiness line",
        (resumed_at - ready).as_secs_f64()
    );
    resumed_at
}

/// Invokes `DelayedConnectionIdentity(delay)` in the background and records when it ended.
fn pending_identity(
    client: &SignalRClient,
    delay: i32,
) -> tokio::task::JoinHandle<(Result<String, rayx_signalr::Error>, Instant)> {
    let mut client = client.clone();
    tokio::spawn(async move {
        let result = client
            .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
                arguments.argument(delay);
            })
            .await;
        (result, Instant::now())
    })
}

/// Records when `client` raised each lifecycle event.
struct Events {
    events: Arc<Mutex<Vec<(&'static str, Instant)>>>,
    hooks: Vec<Box<dyn FnOnce() + Send>>,
}

impl Events {
    fn record(client: &SignalRClient) -> Self {
        let events = Arc::new(Mutex::new(Vec::new()));
        let hook = |name: &'static str| {
            let events = events.clone();
            move || events.lock().unwrap().push((name, Instant::now()))
        };
        let (reconnecting, reconnected, closed) =
            (hook("reconnecting"), hook("reconnected"), hook("closed"));
        let reconnecting = client.on_reconnecting(move |_| reconnecting());
        let reconnected = client.on_reconnected(move |_| reconnected());
        let closed = client.on_closed(move |_| closed());
        Self {
            events,
            hooks: vec![
                Box::new(move || reconnecting.unregister()),
                Box::new(move || reconnected.unregister()),
                Box::new(move || closed.unregister()),
            ],
        }
    }

    fn names(&self) -> Vec<&'static str> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _)| *name)
            .collect()
    }

    fn reconnected(&self) -> Option<Instant> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .find(|(name, _)| *name == "reconnected")
            .map(|(_, at)| *at)
    }

    fn unregister(self) {
        for unregister in self.hooks {
            unregister();
        }
    }
}

async fn scenario(protocol: Protocol) {
    let mut stateful = support::connect("test-stateful", protocol, |configuration| {
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let mut ordinary = support::connect("test", protocol, |configuration| {
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let clients = [("stateful", &mut stateful), ("ordinary", &mut ordinary)];
    let mut identities = Vec::new();
    let mut events = Vec::new();
    for (name, client) in clients {
        identities.push(support::connection_identity(client).await.unwrap());
        events.push((name, Events::record(client)));
    }

    // A suspension shorter than the client timeout: both connections survive. The invocations
    // complete on the server during the suspension and are the clients' last frames before it.
    let pending = [
        pending_identity(&stateful, 10_000),
        pending_identity(&ordinary, 10_000),
    ];
    tokio::time::sleep(Duration::from_secs(1)).await;
    suspend(20).await;
    for ((name, _), (pending, identity)) in events.iter().zip(pending.into_iter().zip(&identities))
    {
        let (result, _) = tokio::time::timeout(RECOVERY, pending)
            .await
            .unwrap_or_else(|_| panic!("the {name} invocation completes after the resume"))
            .unwrap();
        assert_eq!(&result.unwrap(), identity, "{name}");
    }
    // Past the moment the server would have timed the clients out without their Pings.
    tokio::time::sleep(Duration::from_secs(12)).await;
    for ((name, recorded), (client, identity)) in events
        .iter()
        .zip([&mut stateful, &mut ordinary].into_iter().zip(&identities))
    {
        assert_eq!(client.status(), ConnectionStatus::Connected, "{name}");
        assert_eq!(
            &support::connection_identity(client).await.unwrap(),
            identity,
            "the {name} client kept its connection"
        );
        assert!(
            recorded.names().is_empty(),
            "{name}: {:?}",
            recorded.names()
        );
    }

    // A suspension longer than the client timeout: the server ends both hub connections.
    let pending = [
        pending_identity(&stateful, 120_000),
        pending_identity(&ordinary, 120_000),
    ];
    tokio::time::sleep(Duration::from_secs(1)).await;
    let resumed_at = suspend(40).await;
    for ((name, _), pending) in events.iter().zip(pending) {
        let (result, failed_at) = tokio::time::timeout(RECOVERY * 3, pending)
            .await
            .unwrap_or_else(|_| panic!("the {name} invocation fails after the resume"))
            .unwrap();
        let error = result.expect_err("the connection was lost");
        // The closed socket, a server Close, or the server timeout found by suspension detection.
        assert!(
            matches!(
                error.kind(),
                ErrorKind::Transport | ErrorKind::ConnectionClosed
            ) || error.timeout_operation() == Some(rayx_signalr::TimeoutOperation::ServerTimeout),
            "{name}: {error}"
        );
        assert!(
            failed_at.saturating_duration_since(resumed_at) <= RECOVERY,
            "the {name} invocation failed {:?} after resuming",
            failed_at.saturating_duration_since(resumed_at)
        );
    }
    for ((name, recorded), (client, identity)) in events
        .iter()
        .zip([&mut stateful, &mut ordinary].into_iter().zip(&identities))
    {
        let reconnected_at = support::eventually(
            "the reconnect after the long suspension",
            RECOVERY * 3,
            || async { recorded.reconnected() },
        )
        .await;
        assert!(
            reconnected_at.saturating_duration_since(resumed_at) <= RECOVERY,
            "the {name} client reconnected {:?} after resuming",
            reconnected_at.saturating_duration_since(resumed_at)
        );
        assert_ne!(
            &support::connection_identity(client).await.unwrap(),
            identity,
            "the {name} client reconnected on a new connection"
        );
    }

    for (_, recorded) in events {
        recorded.unregister();
    }
    stateful.disconnect();
    ordinary.disconnect();
}

crate::protocol_tests!("suspend-resume", Duration::from_secs(110));
