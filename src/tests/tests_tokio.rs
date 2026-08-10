use futures::StreamExt;
use log::info;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{spawn, time::Instant};

use crate::{
    communication::CommunicationClient, execution::CallbackHandler, tests::TestEntity,
    ConstantDelayPolicy, DisconnectionHandler, ReconnectionConfig, ReconnectionHandler,
    SignalRClient, StatefulReconnectConfig,
};

struct MyDisconnectionHandler {}

impl DisconnectionHandler for MyDisconnectionHandler {
    fn on_disconnected(&self, reconnection: ReconnectionHandler) {
        tokio::spawn(async move {
            if let Err(e) = reconnection.reconnect_with_policy().await {
                eprintln!("Failed to reconnect: {}", e);
            }
        });
    }
}

#[test_log::test(tokio::test)]
async fn test_service() {
    let mut client = SignalRClient::connect_with("localhost", "test", |c| {
        c.with_port(5220);
        c.unsecure();
        c.with_disconnection_handler(MyDisconnectionHandler {});
    })
    .await
    .unwrap();

    let re = client
        .invoke::<TestEntity>("SingleEntity".to_string())
        .await;

    assert!(re.is_ok());

    let entity = re.unwrap();
    assert_eq!(entity.text, "test".to_string());

    info!("Entity {}, {}", entity.text, entity.number);

    let mut he = client
        .enumerate::<TestEntity>("HundredEntities".to_string())
        .await;

    while let Some(item) = he.next().await {
        info!("Entity {}, {}", item.text, item.number);
    }

    info!("Finished fetching entities, calling pushes");

    let push1 = client
        .invoke_with_args::<bool, _>("PushEntity".to_string(), |c| {
            c.argument(TestEntity {
                text: "push1".to_string(),
                number: 100,
            });
        })
        .await;

    assert!(push1.unwrap());

    let mut secondclient = client.clone();

    let push2 = secondclient
        .invoke_with_args::<TestEntity, _>("PushTwoEntities".to_string(), |c| {
            c.argument(TestEntity {
                text: "entity1".to_string(),
                number: 200,
            })
            .argument(TestEntity {
                text: "entity2".to_string(),
                number: 300,
            });
        })
        .await;

    assert!(push2.is_ok());
    let entity = push2.unwrap();
    assert_eq!(entity.number, 500);
    info!("Merged Entity {}, {}", entity.text, entity.number);

    drop(secondclient);

    let c1 = client.register("callback1".to_string(), |ctx| {
        if let Ok(entity) = ctx.argument::<TestEntity>(0) {
            info!(
                "Callback results entity: {}, {}",
                entity.text, entity.number
            );
        }
    });

    let c2 = client.register("callback2".to_string(), |mut ctx| {
        if let Ok(entity) = ctx.argument::<TestEntity>(0) {
            info!(
                "Callback2 results entity: {}, {}",
                entity.text, entity.number
            );

            let e2 = entity.clone();
            spawn(async move {
                info!("Completing callback2");
                let _ = ctx.complete(e2).await;
            });
        }
    });

    info!("Calling callback1");

    _ = client
        .send_with_args("TriggerEntityCallback".to_string(), |c| {
            c.argument("callback1".to_string());
        })
        .await;

    info!("Calling callback2");

    let succ = client
        .invoke_with_args::<bool, _>("TriggerEntityResponse".to_string(), |c| {
            c.argument("callback2".to_string());
        })
        .await;

    assert!(succ.unwrap());

    let now = Instant::now();
    {
        let mut me = client
            .enumerate::<TestEntity>("MillionEntities".to_string())
            .await;
        while (me.next().await).is_some() {}
    }

    let elapsed = now.elapsed();
    info!("1 million entities fetched in: {:.2?}", elapsed);

    c1.unregister();
    c2.unregister();

    client.disconnect();
}

#[test_log::test(tokio::test)]
async fn interop_ordinary_reconnect_json() {
    tokio::time::timeout(Duration::from_secs(15), run_ordinary_reconnect(false))
        .await
        .expect("native JSON ordinary reconnect scenario timed out");
}

pub(crate) async fn run_ordinary_reconnect(messagepack: bool) {
    let mut client = SignalRClient::connect_with("127.0.0.1", "test", |config| {
        config.with_port(5220);
        config.unsecure();
        config.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(
                Duration::from_millis(25),
                Some(40),
            )),
        });
        #[cfg(feature = "messagepack")]
        if messagepack {
            config.with_messagepack_protocol();
        }
        #[cfg(not(feature = "messagepack"))]
        let _ = messagepack;
    })
    .await
    .unwrap();

    let initial_connection_id = client
        .invoke::<String>("ConnectionIdentity".to_string())
        .await
        .unwrap();

    let callback_count = Arc::new(Mutex::new(0_u32));
    let callback_capture = callback_count.clone();
    let registration = client.register("nativeOrdinaryCallback".to_string(), move |_| {
        *callback_capture.lock().expect("callback count poisoned") += 1;
    });

    CommunicationClient::force_test_transport_drop()
        .await
        .unwrap();

    let reconnected_connection_id = client
        .invoke::<String>("ConnectionIdentity".to_string())
        .await
        .unwrap();
    assert_ne!(reconnected_connection_id, initial_connection_id);

    client
        .send_with_args("TriggerCallback".to_string(), |arguments| {
            arguments.argument("nativeOrdinaryCallback".to_string());
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if *callback_count.lock().expect("callback count poisoned") == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for callback after ordinary reconnect");

    registration.unregister();
    client.disconnect();
}

#[test_log::test(tokio::test)]
async fn interop_stateful_reconnect_json() {
    tokio::time::timeout(Duration::from_secs(15), run_stateful_reconnect(false))
        .await
        .expect("native JSON stateful scenario timed out");
}

pub(crate) async fn run_stateful_reconnect(messagepack: bool) {
    let mut client = SignalRClient::connect_with("127.0.0.1", "test-stateful", |config| {
        config.with_port(5220);
        config.unsecure();
        config.with_stateful_reconnect(StatefulReconnectConfig::default());
        config.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(
                Duration::from_millis(25),
                Some(40),
            )),
        });
        #[cfg(feature = "messagepack")]
        if messagepack {
            config.with_messagepack_protocol();
        }
        #[cfg(not(feature = "messagepack"))]
        let _ = messagepack;
    })
    .await
    .unwrap();

    let connection_id = client
        .invoke::<String>("ConnectionIdentity".to_string())
        .await
        .unwrap();

    let callback_values = Arc::new(Mutex::new(Vec::<i32>::new()));
    let callback_capture = callback_values.clone();
    let registration = client.register("nativeStatefulCallback".to_string(), move |context| {
        let value = context.argument::<i32>(0).unwrap();
        callback_capture
            .lock()
            .expect("callback values poisoned")
            .push(value);
    });

    let mut stream = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(30);
            arguments.argument(5);
        })
        .await;
    let mut stream_values = Vec::new();
    while let Some(value) = stream.next().await {
        stream_values.push(value);
        if matches!(value, 4 | 14) {
            CommunicationClient::force_test_transport_drop()
                .await
                .unwrap();
        }
    }
    assert_eq!(stream_values, (1..=30).collect::<Vec<_>>());

    CommunicationClient::pause_next_test_trackable();
    let mut invocation_client = client.clone();
    let pending_invocation = spawn(async move {
        invocation_client
            .invoke::<String>("ConnectionIdentity".to_string())
            .await
    });
    CommunicationClient::wait_for_test_trackable_pause()
        .await
        .unwrap();
    CommunicationClient::force_test_transport_drop()
        .await
        .unwrap();
    assert_eq!(pending_invocation.await.unwrap().unwrap(), connection_id);

    let resumed_connection_id = client
        .invoke::<String>("ConnectionIdentity".to_string())
        .await
        .unwrap();
    assert_eq!(resumed_connection_id, connection_id);

    client
        .send_with_args("TriggerOrderedCallbacks".to_string(), |arguments| {
            arguments.argument("nativeStatefulCallback".to_string());
            arguments.argument(12);
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if callback_values
                .lock()
                .expect("callback values poisoned")
                .len()
                == 12
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for stateful callback dispatch");
    assert_eq!(
        *callback_values.lock().expect("callback values poisoned"),
        (1..=12).collect::<Vec<_>>()
    );

    registration.unregister();
    client.disconnect();
}
