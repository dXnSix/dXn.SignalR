//! Invoke: no, one and several arguments, Serde-renamed DTOs, void methods, hub exceptions and
//! unknown methods; the connection keeps serving invocations afterwards.

use serde::{Deserialize, Serialize};

use crate::support::{self, Protocol, TestEntity};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct ComplexEntity {
    #[serde(rename = "entity_id")]
    id: i32,
    #[serde(rename = "display_name")]
    name: String,
    tags: Vec<String>,
    nested: Option<TestEntity>,
    #[serde(rename = "optional_note")]
    note: Option<String>,
}

pub(crate) async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;

    let entity = client
        .invoke::<TestEntity>("SingleEntity".to_string())
        .await
        .unwrap();
    assert_eq!(
        entity,
        TestEntity {
            number: 1,
            text: "test".to_string()
        }
    );

    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(7);
        })
        .await
        .unwrap();
    assert_eq!(echoed, 7);

    let merged = client
        .invoke_with_args::<TestEntity, _>("PushTwoEntities".to_string(), |arguments| {
            arguments
                .argument(TestEntity {
                    number: 200,
                    text: "entity1".to_string(),
                })
                .argument(TestEntity {
                    number: 300,
                    text: "entity2".to_string(),
                });
        })
        .await
        .unwrap();
    assert_eq!(merged.number, 500);
    assert_eq!(merged.text, "entity1_entity2");

    let complex = ComplexEntity {
        id: 41,
        name: "rayx".to_string(),
        tags: vec!["a".to_string(), "b".to_string()],
        nested: Some(TestEntity {
            number: 3,
            text: "inner".to_string(),
        }),
        note: None,
    };
    let described = client
        .invoke_with_args::<String, _>("DescribeComplex".to_string(), |arguments| {
            arguments.argument(complex.clone());
        })
        .await
        .unwrap();
    assert_eq!(
        described, "41|rayx|a,b|inner:3|null",
        "the hub must see the Serde wire names"
    );
    let returned = client
        .invoke_with_args::<ComplexEntity, _>("EchoComplex".to_string(), |arguments| {
            arguments.argument(complex.clone());
        })
        .await
        .unwrap();
    assert_eq!(
        returned,
        ComplexEntity {
            id: 42,
            name: "RAYX".to_string(),
            tags: vec!["a".to_string(), "b".to_string(), "server".to_string()],
            nested: Some(TestEntity {
                number: 6,
                text: "inner".to_string(),
            }),
            note: None,
        }
    );

    client.invoke::<()>("VoidCall".to_string()).await.unwrap();
    let recorded = client
        .invoke::<Vec<String>>("RecordedSends".to_string())
        .await
        .unwrap();
    assert_eq!(recorded, vec!["void-call".to_string()]);

    let failure = client
        .invoke_with_args::<i32, _>("Fail".to_string(), |arguments| {
            arguments.argument("detailed failure 42".to_string());
        })
        .await
        .unwrap_err();
    assert_eq!(failure.kind(), rayx_signalr::ErrorKind::Hub, "{failure}");
    assert!(
        failure
            .server_message()
            .is_some_and(|message| message.contains("detailed failure 42")),
        "{failure}"
    );

    let unknown = client
        .invoke::<i32>("NoSuchHubMethod".to_string())
        .await
        .unwrap_err();
    assert_eq!(unknown.kind(), rayx_signalr::ErrorKind::Hub, "{unknown}");

    let still_serving = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(9);
        })
        .await
        .unwrap();
    assert_eq!(still_serving, 9);
    client.disconnect();
}

crate::protocol_tests!("invoke");
