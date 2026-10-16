//! Client-to-server streams, as the official .NET client's upload streams: a stream argument is
//! not one of the invocation's arguments; it gets a stream id from the connection's id counter
//! (listed in the invocation's `streamIds`), and once the invocation was sent a pump forwards its
//! items as StreamItem messages and ends it with one Completion: without an error when the stream
//! ended, `Stream errored by client: '<error>'` when an item could not be encoded, and
//! `Stream canceled by client.` when the caller canceled the call. Pumps send only on the logical
//! connection the invocation was accepted on, as the .NET client sends on the connection state
//! it captured: while that connection is paused or resuming (stateful reconnect) a pump waits for
//! its next transport, and when it ends (a local close, an ordinary drop or pause, or a new
//! logical connection) the pumps are aborted, their streams dropped and nothing more sent.

use futures::{
    channel::oneshot,
    future::{select, AbortHandle, Abortable, Either},
    Stream, StreamExt,
};
use log::debug;
use serde::Serialize;

use super::callback::{CallbackRegistry, Replies};
use crate::{
    error::Error,
    protocol::{codec::OutboundMessage, hub_protocol::HubProtocolKind},
};

/// The text of the Completion that ends a stream the caller canceled, as the .NET client sends.
pub(crate) const STREAM_CANCELED: &str = "Stream canceled by client.";

/// The text of the Completion that ends a stream that failed, as the .NET client sends.
pub(crate) fn stream_errored(error: &Error) -> String {
    format!("Stream errored by client: '{error}'")
}

/// A stream's StreamItem messages, encoded as its items arrive.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type UploadItems = futures::stream::BoxStream<'static, Result<OutboundMessage, Error>>;
#[cfg(target_arch = "wasm32")]
pub(crate) type UploadItems =
    futures::stream::LocalBoxStream<'static, Result<OutboundMessage, Error>>;

#[cfg(not(target_arch = "wasm32"))]
type Start = Box<dyn FnOnce(String) -> UploadItems + Send>;
#[cfg(target_arch = "wasm32")]
type Start = Box<dyn FnOnce(String) -> UploadItems>;

/// One stream argument of a call, waiting for its stream id.
pub(crate) struct Upload {
    protocol: HubProtocolKind,
    start: Start,
}

impl Upload {
    pub(crate) fn new<S, T>(stream: S, protocol: HubProtocolKind) -> Self
    where
        S: Stream<Item = T> + crate::platform::MaybeSend + 'static,
        T: Serialize + crate::platform::MaybeSend + 'static,
    {
        Self {
            protocol,
            start: Box::new(move |stream_id: String| {
                let items = stream
                    .map(move |item| OutboundMessage::stream_item(protocol, &stream_id, &item));
                #[cfg(not(target_arch = "wasm32"))]
                return items.boxed();
                #[cfg(target_arch = "wasm32")]
                return items.boxed_local();
            }),
        }
    }
}

/// The stream arguments of one call with their stream ids, started once the call was sent.
pub(crate) struct Uploads(Vec<(String, Upload)>);

impl Uploads {
    /// Gives every stream the next id of the connection's counter, in argument order.
    pub(crate) fn assign(uploads: Vec<Upload>, mut next_id: impl FnMut() -> String) -> Self {
        Self(
            uploads
                .into_iter()
                .map(|upload| (next_id(), upload))
                .collect(),
        )
    }

    /// The stream ids, for the invocation's `streamIds`.
    pub(crate) fn ids(&self) -> Vec<String> {
        self.0.iter().map(|(id, _)| id.clone()).collect()
    }

    /// Starts a pump per stream, sending through `sink` on the logical connection
    /// `logical_connection` the call was accepted on; returns what cancels them.
    pub(crate) fn start(
        self,
        registry: &CallbackRegistry,
        sink: Replies,
        logical_connection: u64,
        #[cfg(not(target_arch = "wasm32"))] runtime: Option<tokio::runtime::Handle>,
    ) -> UploadCancellation {
        let mut cancellation = UploadCancellation::default();
        for (stream_id, upload) in self.0 {
            let (cancel, canceled) = oneshot::channel();
            let (finished, done) = oneshot::channel::<()>();
            cancellation.cancels.push(cancel);
            cancellation.done.push(done);
            let (abort, registration) = AbortHandle::new_pair();
            let tracked = registry.track_upload(abort, logical_connection);
            let items = (upload.start)(stream_id.clone());
            let pumping = pump(
                stream_id,
                upload.protocol,
                items,
                Sender {
                    sink: sink.clone(),
                    registry: registry.clone(),
                    logical_connection,
                },
                canceled,
            );
            let registry = registry.clone();
            let task = async move {
                let _ = Abortable::new(pumping, registration).await;
                registry.finish_upload(tracked);
                let _ = finished.send(());
            };
            #[cfg(not(target_arch = "wasm32"))]
            match runtime
                .clone()
                .or_else(|| tokio::runtime::Handle::try_current().ok())
            {
                Some(runtime) => {
                    runtime.spawn(task);
                }
                None => debug!("No runtime to pump a client-to-server stream on"),
            }
            #[cfg(target_arch = "wasm32")]
            wasm_bindgen_futures::spawn_local(task);
        }
        cancellation
    }
}

/// Cancels the streams of one call: each ends with `Stream canceled by client.`.
#[derive(Default)]
pub(crate) struct UploadCancellation {
    cancels: Vec<oneshot::Sender<()>>,
    /// Resolve when each pump ended.
    done: Vec<oneshot::Receiver<()>>,
}

impl UploadCancellation {
    pub(crate) fn cancel(self) {
        for cancel in self.cancels {
            let _ = cancel.send(());
        }
    }

    /// Cancels the streams and waits until every pump ended, so their Completions went out
    /// before whatever the caller sends next (a CancelInvocation, which would otherwise end the
    /// streams on the server without telling it they were canceled).
    pub(crate) async fn cancel_and_wait(self) {
        for cancel in self.cancels {
            let _ = cancel.send(());
        }
        for done in self.done {
            let _ = done.await;
        }
    }
}

/// Cancels the streams of a call when dropped before [`disarm`](Self::disarm), as dropping an
/// `invoke` future cancels its upload streams like the .NET `InvokeAsync` cancellation token.
pub(crate) struct CancelOnDrop(Option<UploadCancellation>);

impl CancelOnDrop {
    pub(crate) fn new(cancellation: UploadCancellation) -> Self {
        Self(Some(cancellation))
    }

    pub(crate) fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancellation) = self.0.take() {
            cancellation.cancel();
        }
    }
}

/// Sends a pump's messages on one logical connection.
pub(crate) struct Sender {
    pub(crate) sink: Replies,
    pub(crate) registry: CallbackRegistry,
    pub(crate) logical_connection: u64,
}

impl Sender {
    /// Sends `message`; a send that fails while the logical connection is still current (it is
    /// paused or resuming) waits for its next transport and tries again. False once the logical
    /// connection ended.
    async fn send(&self, message: OutboundMessage) -> bool {
        loop {
            let seen = self.registry.transports_installed();
            match self
                .sink
                .send(message.clone(), self.logical_connection)
                .await
            {
                Ok(()) => return true,
                Err(error) if !self.registry.is_current(self.logical_connection) => {
                    debug!("A client-to-server stream stopped: {error}");
                    return false;
                }
                Err(error) => {
                    debug!("A client-to-server stream waits for its connection: {error}");
                    if self.registry.next_transport(seen).await.is_err() {
                        return false;
                    }
                }
            }
        }
    }
}

/// Forwards one stream's items in order, then its Completion. Once the logical connection ended
/// nothing more is sent.
pub(crate) async fn pump(
    stream_id: String,
    protocol: HubProtocolKind,
    mut items: UploadItems,
    sender: Sender,
    canceled: oneshot::Receiver<()>,
) {
    // A cancellation that can no longer come (its sender dropped) never ends the stream.
    let canceled = async move {
        if canceled.await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    let mut canceled = std::pin::pin!(canceled);
    let error = loop {
        match select(items.next(), canceled.as_mut()).await {
            Either::Left((Some(Ok(message)), _)) => {
                if !sender.send(message).await {
                    return;
                }
            }
            Either::Left((Some(Err(error)), _)) => break Some(stream_errored(&error)),
            Either::Left((None, _)) => break None,
            Either::Right(((), _)) => break Some(STREAM_CANCELED.to_string()),
        }
    };
    // The caller's stream is dropped before the Completion goes out.
    drop(items);
    match OutboundMessage::stream_completion(protocol, stream_id.clone(), error) {
        Ok(completion) => {
            sender.send(completion).await;
        }
        Err(error) => debug!("Cannot encode the Completion of stream {stream_id}: {error}"),
    }
}

#[cfg(test)]
mod client_streams {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };

    use futures::{channel::oneshot, stream, FutureExt, StreamExt};
    use serde::Serialize;

    use super::{pump, Sender, Upload, Uploads, STREAM_CANCELED};
    use crate::{
        error::Error,
        execution::{
            callback::{CallbackRegistry, Replies},
            ArgumentConfiguration, ReplyFuture, ReplySink, Storage, UpdatableActionStorage,
        },
        protocol::{
            codec::{OutboundMessage, SerializedFrame},
            hub_protocol::HubProtocolKind,
            invoke::Invocation,
            negotiate::MessageType,
        },
    };

    /// Records what a pump sends; refuses sends while `refusing` is set.
    struct Recorder {
        sent: Mutex<Vec<(MessageType, SerializedFrame)>>,
        refusing: AtomicBool,
    }

    impl Recorder {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                sent: Mutex::new(Vec::new()),
                refusing: AtomicBool::new(false),
            })
        }

        fn texts(&self) -> Vec<String> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|(_, frame)| String::from_utf8(frame.bytes().to_vec()).unwrap())
                .collect()
        }
    }

    impl ReplySink for Recorder {
        fn protocol(&self) -> HubProtocolKind {
            HubProtocolKind::Json
        }

        fn send(&self, message: OutboundMessage, _logical_connection: u64) -> ReplyFuture {
            let result = if self.refusing.load(Ordering::SeqCst) {
                Err(Error::paused())
            } else {
                self.sent
                    .lock()
                    .unwrap()
                    .push((message.message_type, message.frame));
                Ok(())
            };
            Box::pin(async move { result })
        }

        fn send_detached(&self, message: OutboundMessage, logical_connection: u64) {
            let _ = futures::executor::block_on(self.send(message, logical_connection));
        }
    }

    fn sender(recorder: &Arc<Recorder>, registry: &CallbackRegistry) -> Sender {
        Sender {
            sink: recorder.clone() as Replies,
            registry: registry.clone(),
            logical_connection: 0,
        }
    }

    fn items(values: Vec<i32>) -> super::UploadItems {
        stream::iter(values)
            .map(|value| OutboundMessage::stream_item(HubProtocolKind::Json, "1", &value))
            .boxed()
    }

    fn item(value: i32) -> String {
        format!("{{\"type\":2,\"invocationId\":\"1\",\"item\":{value}}}\u{1e}")
    }

    const END: &str = "{\"type\":3,\"invocationId\":\"1\"}\u{1e}";

    /// Signals when dropped: the caller's stream was dropped.
    struct Signal(futures::channel::mpsc::UnboundedSender<()>);

    impl Drop for Signal {
        fn drop(&mut self) {
            let _ = self.0.unbounded_send(());
        }
    }

    /// An endless stream that signals on `dropped` when it is dropped.
    fn endless_signaling() -> (
        impl futures::Stream<Item = i32> + Send + 'static,
        futures::channel::mpsc::UnboundedReceiver<()>,
    ) {
        let (dropped, signal) = futures::channel::mpsc::unbounded();
        let guard = Signal(dropped);
        let endless = stream::pending::<i32>().map(move |value| {
            let _ = &guard;
            value
        });
        (endless, signal)
    }

    /// A value whose serialization always fails.
    struct Unencodable;

    impl Serialize for Unencodable {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("not encodable"))
        }
    }

    #[test]
    fn client_streams_json_frames_match_the_dotnet_client() {
        let item = OutboundMessage::stream_item(HubProtocolKind::Json, "1", &5).unwrap();
        assert_eq!(item.message_type, MessageType::StreamItem);
        assert_eq!(
            item.frame,
            SerializedFrame::text("{\"type\":2,\"invocationId\":\"1\",\"item\":5}\u{1e}")
        );
        let end =
            OutboundMessage::stream_completion(HubProtocolKind::Json, "1".into(), None).unwrap();
        assert_eq!(end.frame, SerializedFrame::text(END));
        let canceled = OutboundMessage::stream_completion(
            HubProtocolKind::Json,
            "1".into(),
            Some(STREAM_CANCELED.to_string()),
        )
        .unwrap();
        assert_eq!(
            canceled.frame,
            SerializedFrame::text(
                "{\"type\":3,\"invocationId\":\"1\",\"error\":\"Stream canceled by client.\"}\u{1e}"
            )
        );
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn client_streams_messagepack_frames_match_the_dotnet_client() {
        let item = OutboundMessage::stream_item(HubProtocolKind::MessagePack, "1", &5).unwrap();
        // [2, {}, "1", 5] behind its varint length.
        assert_eq!(
            item.frame.bytes().as_ref(),
            [0x06, 0x94, 0x02, 0x80, 0xa1, b'1', 0x05]
        );
        let end =
            OutboundMessage::stream_completion(HubProtocolKind::MessagePack, "1".into(), None)
                .unwrap();
        // [3, {}, "1", 2]: a void result.
        assert_eq!(
            end.frame.bytes().as_ref(),
            [0x06, 0x94, 0x03, 0x80, 0xa1, b'1', 0x02]
        );
        let failed = OutboundMessage::stream_completion(
            HubProtocolKind::MessagePack,
            "1".into(),
            Some("x".into()),
        )
        .unwrap();
        // [3, {}, "1", 1, "x"]: an error result.
        assert_eq!(
            failed.frame.bytes().as_ref(),
            [0x08, 0x95, 0x03, 0x80, 0xa1, b'1', 0x01, 0xa1, b'x']
        );
    }

    #[test]
    fn client_streams_invocations_list_stream_ids_instead_of_arguments() {
        let mut configuration =
            ArgumentConfiguration::new(Invocation::create_single("Sum", HubProtocolKind::Json));
        configuration
            .argument(1)
            .stream(stream::iter([1, 2]))
            .argument(2);
        let (mut invocation, uploads) = configuration.build_invocation().unwrap();
        // The client's own counter: the stream takes its id before the invocation does.
        let mut storage = UpdatableActionStorage::new();
        let uploads = Uploads::assign(uploads, || storage.create_key());
        invocation.with_streams(uploads.ids());
        invocation.with_invocation_id(storage.create_key());
        let message = OutboundMessage::invocation(&invocation).unwrap();
        assert_eq!(
            message.frame,
            SerializedFrame::text(
                "{\"type\":1,\"invocationId\":\"2\",\"target\":\"Sum\",\"arguments\":[1,2],\"streamIds\":[\"1\"]}\u{1e}"
            )
        );
    }

    #[test]
    fn client_streams_items_go_out_in_order_then_one_completion() {
        let recorder = Recorder::new();
        let registry = CallbackRegistry::new();
        let (_cancel, canceled) = oneshot::channel();
        futures::executor::block_on(pump(
            "1".into(),
            HubProtocolKind::Json,
            items(vec![1, 2, 3]),
            sender(&recorder, &registry),
            canceled,
        ));
        assert_eq!(
            recorder.texts(),
            [item(1), item(2), item(3), END.to_string()]
        );
    }

    #[test]
    fn client_streams_an_unencodable_item_ends_the_stream_with_the_dotnet_text() {
        let recorder = Recorder::new();
        let registry = CallbackRegistry::new();
        let (_cancel, canceled) = oneshot::channel();
        let failing = stream::iter([Unencodable])
            .map(|item| OutboundMessage::stream_item(HubProtocolKind::Json, "1", &item))
            .boxed();
        futures::executor::block_on(pump(
            "1".into(),
            HubProtocolKind::Json,
            failing,
            sender(&recorder, &registry),
            canceled,
        ));
        let texts = recorder.texts();
        assert_eq!(texts.len(), 1);
        assert!(
            texts[0].starts_with(
                "{\"type\":3,\"invocationId\":\"1\",\"error\":\"Stream errored by client: '"
            ) && texts[0].contains("not encodable"),
            "{}",
            texts[0]
        );
    }

    #[test]
    fn client_streams_canceling_ends_the_stream_with_stream_canceled() {
        let recorder = Recorder::new();
        let registry = CallbackRegistry::new();
        let (cancel, canceled) = oneshot::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        struct Flag(Arc<AtomicBool>);
        impl Drop for Flag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let flag = Flag(dropped.clone());
        let endless = stream::iter([1])
            .chain(stream::pending())
            .map(move |value: i32| {
                let _ = &flag;
                OutboundMessage::stream_item(HubProtocolKind::Json, "1", &value)
            })
            .boxed();
        let pumping = pump(
            "1".into(),
            HubProtocolKind::Json,
            endless,
            sender(&recorder, &registry),
            canceled,
        );
        futures::executor::block_on(async move {
            let mut pumping = std::pin::pin!(pumping.fuse());
            // The first item goes out; then the stream waits.
            assert!(pumping.as_mut().now_or_never().is_none());
            let _ = cancel.send(());
            pumping.await;
        });
        assert!(
            dropped.load(Ordering::SeqCst),
            "the caller's stream is dropped"
        );
        assert_eq!(
            recorder.texts(),
            [
                item(1),
                format!(
                    "{{\"type\":3,\"invocationId\":\"1\",\"error\":\"{STREAM_CANCELED}\"}}\u{1e}"
                ),
            ]
        );
    }

    #[test]
    fn client_streams_a_paused_connection_holds_the_stream_until_its_next_transport() {
        // A stateful connection paused or resuming: its logical connection is still current.
        let recorder = Recorder::new();
        let registry = CallbackRegistry::new();
        let (_cancel, canceled) = oneshot::channel();
        recorder.refusing.store(true, Ordering::SeqCst);
        let pumping = pump(
            "1".into(),
            HubProtocolKind::Json,
            items(vec![1, 2]),
            sender(&recorder, &registry),
            canceled,
        );
        futures::executor::block_on(async {
            let mut pumping = std::pin::pin!(pumping.fuse());
            assert!(pumping.as_mut().now_or_never().is_none(), "the pump waits");
            assert!(recorder.texts().is_empty());
            // The connection resumes.
            recorder.refusing.store(false, Ordering::SeqCst);
            registry.transport_installed();
            pumping.await;
        });
        assert_eq!(recorder.texts(), [item(1), item(2), END.to_string()]);
    }

    #[test]
    fn client_streams_nothing_is_sent_once_the_logical_connection_ended() {
        let recorder = Recorder::new();
        let registry = CallbackRegistry::new();
        let (_cancel, canceled) = oneshot::channel();
        // The logical connection the call went out on ended; the sink refuses.
        registry.new_logical_connection();
        recorder.refusing.store(true, Ordering::SeqCst);
        futures::executor::block_on(pump(
            "1".into(),
            HubProtocolKind::Json,
            items(vec![1, 2, 3]),
            sender(&recorder, &registry),
            canceled,
        ));
        assert!(recorder.texts().is_empty(), "no item and no Completion");
    }

    #[tokio::test]
    async fn client_streams_close_and_a_new_logical_connection_abort_the_pumps() {
        for close in [true, false] {
            let registry = CallbackRegistry::new();
            let recorder = Recorder::new();
            let (endless, mut dropped) = endless_signaling();
            let uploads =
                Uploads::assign(vec![Upload::new(endless, HubProtocolKind::Json)], || {
                    "1".to_string()
                });
            let _cancellation = uploads.start(
                &registry,
                recorder.clone() as Replies,
                0,
                Some(tokio::runtime::Handle::current()),
            );
            tokio::task::yield_now().await;
            if close {
                registry.close();
            } else {
                registry.new_logical_connection();
            }
            tokio::time::timeout(std::time::Duration::from_secs(5), dropped.next())
                .await
                .expect("the pump is aborted and drops the stream");
            assert!(recorder.texts().is_empty(), "nothing is sent");
        }
    }

    #[tokio::test]
    async fn client_streams_failed_pending_work_aborts_the_pumps() {
        // An ordinary drop or pause ends the logical connection and fails pending work.
        let mut storage = UpdatableActionStorage::new();
        let recorder = Recorder::new();
        let (endless, mut dropped) = endless_signaling();
        let uploads = Uploads::assign(vec![Upload::new(endless, HubProtocolKind::Json)], || {
            "1".to_string()
        });
        let _cancellation = uploads.start(
            storage.callbacks(),
            recorder.clone() as Replies,
            0,
            Some(tokio::runtime::Handle::current()),
        );
        tokio::task::yield_now().await;
        storage.fail_pending(&Error::closed_locally());
        tokio::time::timeout(std::time::Duration::from_secs(5), dropped.next())
            .await
            .expect("the pump is aborted and drops the stream");
    }

    #[tokio::test]
    async fn client_streams_of_an_ended_connection_never_start() {
        // The call was accepted on logical connection 0, but the client closed or moved on
        // before its pumps started.
        for close in [true, false] {
            let registry = CallbackRegistry::new();
            if close {
                registry.close();
            } else {
                registry.new_logical_connection();
            }
            let recorder = Recorder::new();
            let (endless, mut dropped) = endless_signaling();
            let uploads =
                Uploads::assign(vec![Upload::new(endless, HubProtocolKind::Json)], || {
                    "1".to_string()
                });
            let _cancellation = uploads.start(
                &registry,
                recorder.clone() as Replies,
                0,
                Some(tokio::runtime::Handle::current()),
            );
            tokio::time::timeout(std::time::Duration::from_secs(5), dropped.next())
                .await
                .expect("the pump is aborted at once");
            assert!(recorder.texts().is_empty());
        }
    }
}
