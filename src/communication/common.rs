// Decoding and transport code must not panic on peer input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use crate::client::ConnectionRecipe;
use crate::error::Error;
use crate::execution::{Storage, UpdatableActionStorage};
use crate::protocol::codec::{InboundMessage, OutboundMessage};
use crate::protocol::hub_protocol::HubProtocolKind;

/// Hands one decoded message to the action storage; returns the error a server Close ends the
/// connection with (its optional error text and its reconnect permission).
pub(crate) fn dispatch_decoded<S: Storage>(
    storage: &mut S,
    message: InboundMessage<'_>,
) -> Option<Error> {
    match message {
        InboundMessage::Close {
            error,
            allow_reconnect,
        } => Some(Error::closed_by_server(
            error.map(std::borrow::Cow::into_owned),
            allow_reconnect,
        )),
        message => {
            storage.dispatch(message);
            None
        }
    }
}

/// Logs a record that could not be decoded; decoder messages can quote payload fragments, so
/// they are logged only at `trace`.
pub(crate) fn log_undecodable(error: &Error) {
    log::error!("A message could not be decoded ({:?})", error.kind());
    log::trace!("Undecodable message: {error}");
}

#[derive(Clone)]
pub struct ConnectionData {
    endpoint: String,
    connection_id: String,
    protocol_kind: HubProtocolKind,
    recipe: ConnectionRecipe,
    stateful_reconnect: Option<crate::StatefulReconnectConfig>,
}

impl ConnectionData {
    pub(crate) fn new(
        endpoint: String,
        connection_id: String,
        protocol_kind: HubProtocolKind,
        recipe: ConnectionRecipe,
        stateful_reconnect: Option<crate::StatefulReconnectConfig>,
    ) -> Self {
        Self {
            endpoint,
            connection_id,
            protocol_kind,
            recipe,
            stateful_reconnect,
        }
    }

    pub fn get_endpoint(&self) -> String {
        self.endpoint.clone()
    }

    #[allow(dead_code)]
    pub fn get_connection_id(&self) -> String {
        self.connection_id.clone()
    }

    pub fn get_protocol_kind(&self) -> HubProtocolKind {
        self.protocol_kind
    }

    pub(crate) fn get_recipe(&self) -> ConnectionRecipe {
        self.recipe.clone()
    }

    pub(crate) fn stateful_reconnect(&self) -> Option<crate::StatefulReconnectConfig> {
        self.stateful_reconnect
    }
}

pub trait Communication: Clone {
    async fn connect(configuration: &ConnectionData) -> Result<Self, Error>;
    /// Sends one encoded hub message (tracked for replay with stateful reconnect).
    async fn send_message(&mut self, message: OutboundMessage) -> Result<(), Error>;
    fn get_storage(&self) -> Result<UpdatableActionStorage, Error>;
    fn get_protocol_kind(&self) -> HubProtocolKind;
    /// Closes the connection locally for every handle, like the official clients' `stop`.
    async fn disconnect(&self);
    /// Releases this handle: closes the connection only when no other handle remains.
    async fn release(&mut self);
}

pub struct HttpClient {}

impl HttpClient {
    /// Negotiates (following redirects) or connects directly when negotiation is skipped.
    pub(crate) async fn negotiate(recipe: &ConnectionRecipe) -> Result<ConnectionData, Error> {
        super::negotiation::negotiate_with(recipe, |url, authorization| {
            Self::post_text(url, authorization, recipe)
        })
        .await
    }

    /// Native negotiate: hyper over the connection's TCP or rustls stream, within the connect
    /// timeout.
    #[cfg(not(target_arch = "wasm32"))]
    async fn post_text(
        endpoint: String,
        authorization: Option<String>,
        recipe: &ConnectionRecipe,
    ) -> Result<String, Error> {
        let timeout = recipe.liveness().connect_timeout;
        super::negotiate_http::post(&endpoint, authorization, recipe.tls(), timeout).await
    }

    /// Browser negotiate: `fetch` on the global scope, so it also works in Web Workers. The
    /// request is aborted when the connect timeout elapses or the caller stops waiting.
    #[cfg(target_arch = "wasm32")]
    async fn post_text(
        endpoint: String,
        authorization: Option<String>,
        recipe: &ConnectionRecipe,
    ) -> Result<String, Error> {
        use futures::future::{select, Either};

        /// Aborts the fetch when dropped; aborting a finished request does nothing.
        struct AbortOnDrop(web_sys::AbortController);

        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }

        let timeout = recipe.liveness().connect_timeout;
        let controller = web_sys::AbortController::new().map_err(|_| {
            Error::transport_failure(None, "the browser cannot create an AbortController")
        })?;
        let request = Self::fetch_text(endpoint, authorization, controller.signal());
        let _abort = AbortOnDrop(controller);
        let timer = gloo_timers::future::sleep(timeout);
        match select(std::pin::pin!(request), std::pin::pin!(timer)).await {
            Either::Left((result, _)) => result,
            Either::Right(((), _)) => Err(Error::timed_out(
                crate::error::TimeoutOperation::Negotiate,
                timeout,
            )),
        }
    }

    #[cfg(target_arch = "wasm32")]
    async fn fetch_text(
        endpoint: String,
        authorization: Option<String>,
        signal: web_sys::AbortSignal,
    ) -> Result<String, Error> {
        let mut request = gloo_net::http::Request::post(&endpoint).abort_signal(Some(&signal));
        if let Some(value) = authorization {
            request = request.header("Authorization", &value);
        }
        let response = request.send().await.map_err(|error| {
            Error::transport_failure(None, format!("the negotiate request failed: {error}"))
        })?;
        if !response.ok() {
            let body = response.binary().await.unwrap_or_default();
            return Err(super::negotiation::negotiate_failure(
                response.status(),
                &response.status_text(),
                &body,
            ));
        }
        response.text().await.map_err(|error| {
            Error::negotiate(
                Some(response.status()),
                format!("the negotiate response has no text body: {error}"),
            )
        })
    }
}

/// Payloads, arguments and results never reach a log record above `trace`.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod log_policy {
    use futures::{FutureExt, StreamExt};

    use super::{dispatch_decoded, log_undecodable};
    use crate::{
        client::SignalRClient,
        execution::{Callback, Storage, UpdatableActionStorage},
        protocol::codec::{self, InboundMessage},
        test_logging::{capture, CapturedRecord},
        Error, ErrorKind,
    };

    const MARKER: &str = "payload-marker-5c1e";

    /// Handles one decoded record as the receive loops do without stateful reconnect.
    fn receive(storage: &mut UpdatableActionStorage, message: Result<InboundMessage<'_>, Error>) {
        match message {
            Ok(message) => {
                dispatch_decoded(storage, message);
            }
            Err(error) => log_undecodable(&error),
        }
    }

    /// Registers work that receives the marker as a result, a mismatched result, a stream item,
    /// a mismatched stream item and a callback argument; returns the checks to run afterwards.
    fn pending_work(storage: &mut UpdatableActionStorage) -> impl FnOnce() {
        let result = storage.add_invocation::<String>("Echo_1".to_string());
        let mismatch = storage.add_invocation::<i32>("Echo_2".to_string());
        let mut items = storage.add_stream::<String>("Items_3".to_string(), Box::new(|| {}));
        let _mismatched_items = storage.add_stream::<i32>("Items_4".to_string(), Box::new(|| {}));
        let argument = std::sync::Arc::new(std::sync::Mutex::new(None));
        let seen = argument.clone();
        storage.add_callback(
            "Notify".to_string(),
            Callback::sync(move |context| {
                let _ = context.argument::<i32>(0);
                *seen.lock().unwrap() = context.argument::<String>(0).ok();
            }),
            SignalRClient::detached(storage.clone()),
        );
        move || {
            assert_eq!(result.now_or_never().unwrap().unwrap(), MARKER);
            let error = mismatch.now_or_never().unwrap().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Serialization, "{error}");
            assert_eq!(
                items.next().now_or_never().flatten().as_deref(),
                Some(MARKER)
            );
            assert_eq!(argument.lock().unwrap().as_deref(), Some(MARKER));
        }
    }

    fn assert_no_marker_above_trace(records: &[CapturedRecord]) {
        for record in records {
            assert!(
                record.level == log::Level::Trace || !record.message.contains(MARKER),
                "a {} record contains a payload: {record:?}",
                record.level
            );
        }
    }

    #[test]
    fn log_policy_json_payloads_stay_at_trace() {
        let mut storage = UpdatableActionStorage::new();
        let verify = pending_work(&mut storage);
        let frame = [
            format!(r#"{{"type":2,"invocationId":"Items_3","item":"{MARKER}"}}"#),
            format!(r#"{{"type":2,"invocationId":"Items_4","item":"{MARKER}"}}"#),
            format!(r#"{{"type":1,"target":"Notify","arguments":["{MARKER}"]}}"#),
            format!(r#"{{"type":3,"invocationId":"Echo_1","result":"{MARKER}"}}"#),
            format!(r#"{{"type":3,"invocationId":"Echo_2","result":"{MARKER}"}}"#),
            format!(r#"{{"type":"{MARKER}"}}"#),
        ]
        .map(|record| record + "\u{1e}")
        .concat();

        let ((), records) = capture(|| {
            for record in codec::json_records(&frame) {
                receive(&mut storage, codec::decode_json(record));
            }
            storage.callbacks().run_queued();
        });

        verify();
        assert_no_marker_above_trace(&records);
        // The serde texts that quote the payload are kept at trace.
        assert!(
            records.iter().any(|record| record.message.contains(MARKER)),
            "{records:#?}"
        );
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn log_policy_messagepack_payloads_stay_at_trace() {
        use rmpv::Value;

        fn record(items: Vec<Value>) -> Vec<u8> {
            let mut bytes = Vec::new();
            rmpv::encode::write_value(&mut bytes, &Value::Array(items)).unwrap();
            crate::protocol::msgpack::frame_message(&bytes)
        }

        let mut storage = UpdatableActionStorage::new();
        let verify = pending_work(&mut storage);
        let headers = || Value::Map(Vec::new());
        let frame = [
            record(vec![2.into(), headers(), "Items_3".into(), MARKER.into()]),
            record(vec![2.into(), headers(), "Items_4".into(), MARKER.into()]),
            record(vec![
                1.into(),
                headers(),
                Value::Nil,
                "Notify".into(),
                Value::Array(vec![MARKER.into()]),
            ]),
            record(vec![
                3.into(),
                headers(),
                "Echo_1".into(),
                3.into(),
                MARKER.into(),
            ]),
            record(vec![
                3.into(),
                headers(),
                "Echo_2".into(),
                3.into(),
                MARKER.into(),
            ]),
        ]
        .concat();

        let ((), records) = capture(|| {
            for record in codec::messagepack_records(&frame) {
                receive(&mut storage, record.and_then(codec::decode_messagepack));
            }
            storage.callbacks().run_queued();
        });

        verify();
        assert_no_marker_above_trace(&records);
    }
}
