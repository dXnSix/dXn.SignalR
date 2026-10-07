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

/// What stateful sequencing decided for one inbound message.
pub(crate) struct Sequenced {
    /// Whether the message is new and must be dispatched (a replayed duplicate is not).
    pub(crate) process: bool,
    /// The Ack that is due. It is sent after the message is dispatched: the message already
    /// counts as received, so it must reach the application even when the Ack cannot be sent
    /// (the dying transport then ends, still resumable).
    pub(crate) acknowledgement: Option<u64>,
}

impl Sequenced {
    /// A stateful control message (Ack, Sequence): consumed by the replay buffer.
    pub(crate) const CONSUMED: Self = Self {
        process: false,
        acknowledgement: None,
    };
}

/// Whether a transport that ended for `reason` ends the stateful logical connection: as in the
/// .NET client, a protocol violation (a malformed, oversized, unexpected or out-of-sequence
/// message, or a frame of the wrong kind) is never resumed, since the server would replay the
/// offending message; the reconnect negotiates afresh.
pub(crate) fn ends_stateful_connection(reason: &Error) -> bool {
    reason.kind() == crate::ErrorKind::Protocol
}

/// Whether a stateful connection whose transport ended for `reason` resumes transparently: as
/// the .NET client's WebSocket transport, once and at once after an ungraceful loss, without the
/// reconnection policy. A server Close, a server timeout (the .NET `HubConnection` ends its
/// connection) and a protocol violation end the logical connection instead.
pub(crate) fn resumes_transparently(reason: &Error) -> bool {
    !ends_stateful_connection(reason)
        && reason.kind() != crate::ErrorKind::ConnectionClosed
        && reason.timeout_operation() != Some(crate::TimeoutOperation::ServerTimeout)
}

/// Hands one decoded message to the action storage; returns the error that ends the transport: a
/// server Close (its optional error text and its reconnect permission), or a message type a
/// server never sends to a client (StreamInvocation, CancelInvocation), which the .NET client
/// rejects as unexpected. Unknown message types are ignored.
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
        InboundMessage::Ignored(
            message_type @ (crate::protocol::negotiate::MessageType::StreamInvocation
            | crate::protocol::negotiate::MessageType::CancelInvocation),
        ) => Some(Error::protocol(format!(
            "the server sent an unexpected {message_type:?} message"
        ))),
        message => {
            storage.dispatch(message);
            None
        }
    }
}

/// The error a malformed message ends the transport with, as in the .NET client (the reconnection
/// policy then runs). Decoder messages can quote payload fragments, so they are logged only at
/// `trace` and never become part of the error.
pub(crate) fn protocol_violation(error: &Error) -> Error {
    log::error!(
        "The server sent a message that could not be decoded ({:?}); the transport ends",
        error.kind()
    );
    log::trace!("Undecodable message: {error}");
    Error::protocol("the server sent a message that could not be decoded")
}

#[derive(Clone)]
pub struct ConnectionData {
    endpoint: String,
    connection_id: String,
    /// The recipe with any transport credentials a redirect issued (browsers carry them in the
    /// endpoint's query instead).
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    recipe: ConnectionRecipe,
    stateful_reconnect: Option<crate::StatefulReconnectConfig>,
    /// The transports to try, in the server's order, and the offered ones skipped (with why).
    selection: super::negotiation::TransportSelection,
}

impl ConnectionData {
    pub(crate) fn new(
        endpoint: String,
        connection_id: String,
        recipe: ConnectionRecipe,
        stateful_reconnect: Option<crate::StatefulReconnectConfig>,
        selection: super::negotiation::TransportSelection,
    ) -> Self {
        Self {
            endpoint,
            connection_id,
            recipe,
            stateful_reconnect,
            selection,
        }
    }

    /// The first transport to try that is not among `failed`.
    pub(crate) fn next_transport(
        &self,
        failed: &[crate::TransportAttempt],
    ) -> Option<super::transport::TransportKind> {
        self.selection
            .candidates
            .iter()
            .copied()
            .find(|kind| !failed.iter().any(|attempt| attempt.transport() == *kind))
    }

    /// The offered transports the client skipped, with why.
    pub(crate) fn skipped(&self) -> Vec<crate::TransportAttempt> {
        self.selection.skipped.clone()
    }

    pub fn get_endpoint(&self) -> String {
        self.endpoint.clone()
    }

    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub fn get_connection_id(&self) -> String {
        self.connection_id.clone()
    }

    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn get_recipe(&self) -> ConnectionRecipe {
        self.recipe.clone()
    }

    pub(crate) fn stateful_reconnect(&self) -> Option<crate::StatefulReconnectConfig> {
        self.stateful_reconnect
    }
}

pub trait Communication: Clone {
    /// Starts (or restarts) the client with a fresh negotiation, as `HubConnection.StartAsync`.
    async fn start(&self) -> Result<(), Error>;
    /// Sends one encoded hub message (tracked for replay with stateful reconnect).
    async fn send_message(&mut self, message: OutboundMessage) -> Result<(), Error>;
    fn get_storage(&self) -> Result<UpdatableActionStorage, Error>;
    fn get_protocol_kind(&self) -> HubProtocolKind;
    /// Closes the connection locally for every handle, like the official clients' `stop`; the
    /// transport closes in the background.
    async fn disconnect(&self);
    /// As `disconnect`, and waits until the transport closed or the close timeout elapsed.
    async fn stop(&self);
    /// Releases this handle: closes the connection only when no other handle remains.
    async fn release(&mut self);
}

pub struct HttpClient {}

impl HttpClient {
    /// Negotiates (following redirects) or connects directly when negotiation is skipped.
    pub(crate) async fn negotiate(recipe: &ConnectionRecipe) -> Result<ConnectionData, Error> {
        super::negotiation::negotiate_with(
            recipe,
            cfg!(target_arch = "wasm32"),
            |url, authorization| Self::post_text(url, authorization, recipe),
        )
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
        super::negotiate_http::post(
            &endpoint,
            authorization,
            recipe.headers(),
            recipe.tls(),
            timeout,
        )
        .await
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
        let request = Self::fetch_text(
            endpoint,
            authorization,
            recipe.headers(),
            controller.signal(),
        );
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
        headers: &[(String, String)],
        signal: web_sys::AbortSignal,
    ) -> Result<String, Error> {
        use super::headers::{compose, user_agent, BROWSER_USER_AGENT_HEADER};
        // Browsers forbid `User-Agent`, so the official format travels in its own header.
        let mut request = gloo_net::http::Request::post(&endpoint).abort_signal(Some(&signal));
        for (name, value) in compose(
            &[(BROWSER_USER_AGENT_HEADER, user_agent())],
            headers,
            authorization,
        ) {
            request = request.header(&name, &value);
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

    use super::{dispatch_decoded, protocol_violation};
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
            Err(error) => {
                protocol_violation(&error);
            }
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
            SignalRClient::detached(storage.clone()).downgrade(),
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
