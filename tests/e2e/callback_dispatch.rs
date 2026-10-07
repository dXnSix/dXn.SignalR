//! Callback dispatch: server-to-client calls run on the client's dispatch task, one at a time and
//! in arrival order, as in the official .NET client, so a slow callback never delays an invoke
//! completion.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures::channel::oneshot;
use rayx_signalr::CallbackHandler;

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let (release, released) = oneshot::channel::<()>();
    let released = Arc::new(Mutex::new(Some(released)));
    let recorded = events.clone();
    let registration = client.register_async("dispatchProbe".to_string(), move |context| {
        let events = recorded.clone();
        // The first call waits until the scenario releases it.
        let wait = released.lock().unwrap().take();
        async move {
            let value = context.argument::<i32>(0).unwrap();
            events.lock().unwrap().push(format!("start {value}"));
            if let Some(wait) = wait {
                let _ = wait.await;
            }
            events.lock().unwrap().push(format!("end {value}"));
        }
    });

    client
        .send_with_args("TriggerOrderedCallbacks".to_string(), |arguments| {
            arguments.argument("dispatchProbe".to_string());
            arguments.argument(3);
        })
        .await
        .unwrap();
    support::eventually("the first callback", Duration::from_secs(5), || {
        let events = events.clone();
        async move { (!events.lock().unwrap().is_empty()).then_some(()) }
    })
    .await;

    // The first callback still runs; an invoke completes regardless.
    let started = Instant::now();
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(7);
        })
        .await
        .unwrap();
    assert_eq!(echoed, 7);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the invoke waited {:?} behind a callback",
        started.elapsed()
    );
    // Callbacks run one at a time: the next two calls wait for the first.
    assert_eq!(*events.lock().unwrap(), ["start 1"]);

    let _ = release.send(());
    let all = support::eventually("every callback", Duration::from_secs(5), || {
        let events = events.clone();
        async move {
            let events = events.lock().unwrap();
            (events.len() == 6).then(|| events.clone())
        }
    })
    .await;
    assert_eq!(
        all,
        ["start 1", "end 1", "start 2", "end 2", "start 3", "end 3"]
    );
    registration.unregister();
}

crate::protocol_tests!("callback-dispatch");
