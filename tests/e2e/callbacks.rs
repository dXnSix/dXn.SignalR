//! Callbacks: typed single- and multi-argument callbacks, unregistration, and client results.

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use rayx_signalr::{CallbackHandler, InvocationContext, SignalRClient};

use crate::support::{self, Protocol, TestEntity};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;

    let single = Arc::new(Mutex::new(Vec::<TestEntity>::new()));
    let captured = single.clone();
    let single_registration = client.register("e2eEntity".to_string(), move |context| {
        captured
            .lock()
            .unwrap()
            .push(context.argument::<TestEntity>(0).unwrap());
    });
    client
        .send_with_args("TriggerEntityCallback".to_string(), |arguments| {
            arguments.argument("e2eEntity".to_string());
        })
        .await
        .unwrap();
    support::eventually(
        "the single-argument callback",
        Duration::from_secs(5),
        || {
            let single = single.clone();
            async move { (!single.lock().unwrap().is_empty()).then_some(()) }
        },
    )
    .await;
    assert_eq!(
        single.lock().unwrap()[0],
        TestEntity {
            number: 1,
            text: "callback".to_string()
        }
    );

    let multi = Arc::new(Mutex::new(None::<(String, i32, TestEntity)>));
    let captured = multi.clone();
    let multi_registration = client.register("e2eMulti".to_string(), move |context| {
        *captured.lock().unwrap() = Some((
            context.argument::<String>(0).unwrap(),
            context.argument::<i32>(1).unwrap(),
            context.argument::<TestEntity>(2).unwrap(),
        ));
    });
    client
        .send_with_args("TriggerMultiArgumentCallback".to_string(), |arguments| {
            arguments.argument("e2eMulti".to_string());
        })
        .await
        .unwrap();
    let received = support::eventually(
        "the multi-argument callback",
        Duration::from_secs(5),
        || {
            let multi = multi.clone();
            async move { multi.lock().unwrap().clone() }
        },
    )
    .await;
    assert_eq!(
        received,
        (
            "alpha".to_string(),
            42,
            TestEntity {
                number: 7,
                text: "multi".to_string()
            }
        )
    );

    // Unregistered callbacks stop receiving; a later barrier callback proves delivery ordering.
    let temporary = Arc::new(AtomicU32::new(0));
    let counted = temporary.clone();
    let temporary_registration = client.register("e2eTemporary".to_string(), move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    let barrier = Arc::new(AtomicU32::new(0));
    let counted = barrier.clone();
    let barrier_registration = client.register("e2eBarrier".to_string(), move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    trigger(&mut client, "e2eTemporary").await;
    trigger(&mut client, "e2eBarrier").await;
    wait_for(&barrier, 1).await;
    assert_eq!(temporary.load(Ordering::SeqCst), 1);
    temporary_registration.unregister();
    trigger(&mut client, "e2eTemporary").await;
    trigger(&mut client, "e2eBarrier").await;
    wait_for(&barrier, 2).await;
    assert_eq!(
        temporary.load(Ordering::SeqCst),
        1,
        "an unregistered callback must not be invoked"
    );

    // Client results: the hub's InvokeAsync receives the value completed by the callback.
    let result_registration = client.register("e2eClientResult".to_string(), |mut context| {
        let value = context.argument::<i32>(0).unwrap();
        InvocationContext::spawn(async move {
            context.complete(value * 2).await.unwrap();
        });
    });
    client
        .invoke_with_args::<(), _>("RequestClientResult".to_string(), |arguments| {
            arguments
                .argument("e2eClientResult".to_string())
                .argument(21);
        })
        .await
        .unwrap();
    let result = support::eventually(
        "the client result at the hub",
        Duration::from_secs(10),
        || {
            let mut probe = client.clone();
            async move {
                probe
                    .invoke::<Option<i32>>("ClientResult".to_string())
                    .await
                    .unwrap()
            }
        },
    )
    .await;
    assert_eq!(result, 42);

    single_registration.unregister();
    multi_registration.unregister();
    barrier_registration.unregister();
    result_registration.unregister();
    client.disconnect();
}

async fn trigger(client: &mut SignalRClient, callback: &str) {
    client
        .send_with_args("TriggerCallback".to_string(), |arguments| {
            arguments.argument(callback.to_string());
        })
        .await
        .unwrap();
}

async fn wait_for(counter: &Arc<AtomicU32>, expected: u32) {
    support::eventually("the barrier callback", Duration::from_secs(5), || {
        let counter = counter.clone();
        async move { (counter.load(Ordering::SeqCst) >= expected).then_some(()) }
    })
    .await;
}

crate::protocol_tests!("callbacks");
