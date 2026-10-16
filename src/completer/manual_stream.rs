use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::{Context, Poll, Waker},
};

use futures::{stream::FusedStream, Stream};

use crate::error::Error;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type StreamDropHandler = Box<dyn FnOnce() + Send + 'static>;
#[cfg(target_arch = "wasm32")]
pub(crate) type StreamDropHandler = Box<dyn FnOnce() + 'static>;

struct ManualStreamState<T> {
    queue: Arc<Mutex<VecDeque<T>>>,
    /// Set once, by the first failure that ends the stream.
    error: Arc<OnceLock<Error>>,
    waker: Arc<Mutex<Option<Waker>>>,
    closed: Arc<AtomicBool>,
    drop_handler: Arc<Mutex<Option<StreamDropHandler>>>,
}

impl<T> Clone for ManualStreamState<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            error: self.error.clone(),
            waker: self.waker.clone(),
            closed: self.closed.clone(),
            drop_handler: self.drop_handler.clone(),
        }
    }
}

impl<T> ManualStreamState<T> {
    // The drop handler is `Send` natively; the browser build shares this code.
    #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
    pub fn new(drop_handler: Option<StreamDropHandler>) -> Self {
        ManualStreamState {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            error: Arc::new(OnceLock::new()),
            waker: Arc::new(Mutex::new(None)),
            closed: Arc::new(AtomicBool::new(false)),
            drop_handler: Arc::new(Mutex::new(drop_handler)),
        }
    }

    fn push(&self, item: T) {
        let mut queue = self.queue.lock().unwrap();
        // Checked under the queue lock, so nothing lands behind an end the consumer has seen.
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        queue.push_back(item);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            // debug!("Waking stream...");
            waker.wake();
        }
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.drop_handler.lock().unwrap().take();
        // Take the waker under the queue lock: `poll_next` checks `closed` and stores its waker
        // under the same lock, so the end wakes a pending consumer instead of slipping past it.
        let _queue = self.queue.lock().unwrap();
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

pub struct ManualStream<T> {
    state: ManualStreamState<T>,
    terminated: bool,
}

impl<T> ManualStream<T> {
    pub fn create() -> (Self, ManualStreamCompleter<T>) {
        Self::create_internal(None)
    }

    pub(crate) fn create_with_drop_handler(
        drop_handler: StreamDropHandler,
    ) -> (Self, ManualStreamCompleter<T>) {
        Self::create_internal(Some(drop_handler))
    }

    fn create_internal(
        drop_handler: Option<StreamDropHandler>,
    ) -> (Self, ManualStreamCompleter<T>) {
        let state = ManualStreamState::new(drop_handler);

        (
            ManualStream {
                state: state.clone(),
                terminated: false,
            },
            ManualStreamCompleter { state },
        )
    }
}

impl<T> ManualStream<T> {
    /// Returns the error that ended the stream: [`ErrorKind::Hub`](crate::ErrorKind::Hub) when the
    /// server completed it with an error, the connection's error when the connection failed it
    /// (for example [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed) or
    /// [`ErrorKind::Paused`](crate::ErrorKind::Paused)), or the send error when its invocation
    /// could not be sent. `None` while the stream is open or after a normal completion.
    pub fn error(&self) -> Option<&Error> {
        self.state.error.get()
    }
}

impl<T> Drop for ManualStream<T> {
    fn drop(&mut self) {
        let handler = { self.state.drop_handler.lock().unwrap().take() };
        if let Some(handler) = handler {
            handler();
        }
    }
}

pub struct ManualStreamCompleter<T> {
    state: ManualStreamState<T>,
}

impl<T> ManualStreamCompleter<T> {
    pub fn push(&self, item: T) {
        self.state.push(item);
    }

    pub fn close(&self) {
        self.state.close();
    }

    /// Ends the stream with an error that [`ManualStream::error`] reports.
    pub fn fail(&self, error: Error) {
        if !self.state.closed.load(Ordering::Acquire) {
            let _ = self.state.error.set(error);
        }
        self.state.close();
    }
}

/// Items queued before the end are yielded first; after that the stream returns `None` on every
/// poll.
impl<T> Stream for ManualStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }
        let mut queue = self.state.queue.lock().unwrap();
        if let Some(item) = queue.pop_front() {
            Poll::Ready(Some(item))
        } else if self.state.closed.load(Ordering::Acquire) {
            drop(queue);
            self.terminated = true;
            Poll::Ready(None)
        } else {
            *self.state.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T> FusedStream for ManualStream<T> {
    /// `true` once the stream has returned `None`.
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

#[cfg(test)]
mod tests {
    use super::ManualStream;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn dropping_an_open_stream_runs_its_handler_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let captured = drops.clone();
        let (stream, completer) =
            ManualStream::<i32>::create_with_drop_handler(Box::new(move || {
                captured.fetch_add(1, Ordering::SeqCst);
            }));

        drop(stream);
        drop(completer);

        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn natural_completion_disarms_the_drop_handler() {
        let drops = Arc::new(AtomicUsize::new(0));
        let captured = drops.clone();
        let (stream, completer) =
            ManualStream::<i32>::create_with_drop_handler(Box::new(move || {
                captured.fetch_add(1, Ordering::SeqCst);
            }));

        completer.close();
        completer.close();
        drop(stream);

        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod fused_end {
    use super::ManualStream;
    use futures::{stream::FusedStream, FutureExt, StreamExt};
    use std::{sync::mpsc, thread, time::Duration};

    #[test]
    fn a_closed_stream_returns_none_on_every_later_poll() {
        let (mut stream, completer) = ManualStream::<i32>::create();

        completer.close();

        for _ in 0..3 {
            assert_eq!(stream.next().now_or_never(), Some(None));
            assert!(stream.is_terminated());
        }
        assert!(stream.error().is_none());
    }

    #[test]
    fn a_failed_stream_returns_none_on_every_later_poll_and_keeps_its_error() {
        let (mut stream, completer) = ManualStream::<i32>::create();

        completer.fail(crate::Error::hub("boom"));

        for _ in 0..3 {
            assert_eq!(stream.next().now_or_never(), Some(None));
            assert!(stream.is_terminated());
            assert_eq!(
                stream.error().and_then(crate::Error::server_message),
                Some("boom")
            );
        }
    }

    #[test]
    fn items_queued_before_the_end_are_yielded_first() {
        let (mut stream, completer) = ManualStream::<i32>::create();

        completer.push(1);
        completer.push(2);
        completer.fail(crate::Error::hub("boom"));

        assert_eq!(stream.next().now_or_never(), Some(Some(1)));
        assert_eq!(stream.next().now_or_never(), Some(Some(2)));
        assert!(!stream.is_terminated());
        assert_eq!(stream.next().now_or_never(), Some(None));
        assert_eq!(stream.next().now_or_never(), Some(None));
        assert_eq!(
            stream.error().and_then(crate::Error::server_message),
            Some("boom")
        );
    }

    #[test]
    fn items_pushed_after_the_end_are_dropped() {
        let (mut stream, completer) = ManualStream::<i32>::create();

        completer.push(1);
        completer.close();
        completer.push(2);

        assert_eq!(stream.next().now_or_never(), Some(Some(1)));
        assert_eq!(stream.next().now_or_never(), Some(None));
        completer.push(3);
        assert_eq!(stream.next().now_or_never(), Some(None));
    }

    #[test]
    fn an_open_empty_stream_is_pending_and_not_terminated() {
        let (mut stream, _completer) = ManualStream::<i32>::create();

        assert_eq!(stream.next().now_or_never(), None);
        assert!(!stream.is_terminated());
    }

    #[test]
    fn closing_wakes_a_pending_consumer_that_then_stays_ended() {
        let (mut stream, completer) = ManualStream::<i32>::create();
        let (sender, results) = mpsc::channel();

        let consumer = thread::spawn(move || {
            futures::executor::block_on(async {
                for _ in 0..3 {
                    sender.send(stream.next().await).unwrap();
                }
            });
        });
        thread::sleep(Duration::from_millis(50));
        completer.close();

        for _ in 0..3 {
            let next = results.recv_timeout(Duration::from_secs(5));
            assert_eq!(next, Ok(None));
        }
        consumer.join().unwrap();
    }
}
