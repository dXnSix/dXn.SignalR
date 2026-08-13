use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

use futures::Stream;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type StreamDropHandler = Box<dyn FnOnce() + Send + 'static>;
#[cfg(target_arch = "wasm32")]
pub(crate) type StreamDropHandler = Box<dyn FnOnce() + 'static>;

struct ManualStreamState<T> {
    queue: Arc<Mutex<VecDeque<Option<T>>>>,
    waker: Arc<Mutex<Option<Waker>>>,
    closed: Arc<AtomicBool>,
    drop_handler: Arc<Mutex<Option<StreamDropHandler>>>,
}

impl<T> Clone for ManualStreamState<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            waker: self.waker.clone(),
            closed: self.closed.clone(),
            drop_handler: self.drop_handler.clone(),
        }
    }
}

impl<T> ManualStreamState<T> {
    pub fn new(drop_handler: Option<StreamDropHandler>) -> Self {
        ManualStreamState {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            waker: Arc::new(Mutex::new(None)),
            closed: Arc::new(AtomicBool::new(false)),
            drop_handler: Arc::new(Mutex::new(drop_handler)),
        }
    }

    fn push(&self, item: T) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(Some(item));
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
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(None);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

pub struct ManualStream<T> {
    state: ManualStreamState<T>,
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
            },
            ManualStreamCompleter { state },
        )
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
}

impl<T> Stream for ManualStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // debug!("Polling stream...");
        let mut queue = self.state.queue.lock().unwrap();
        if let Some(item) = queue.pop_front() {
            // debug!("Item popped...");
            match item {
                Some(value) => {
                    // debug!("Poll Ready with value");
                    Poll::Ready(Some(value))
                }
                None => {
                    // debug!("Poll Ready without value");
                    Poll::Ready(None)
                }
            }
        } else {
            // debug!("Waker is peding..");
            let mut waker = self.state.waker.lock().unwrap();
            *waker = Some(cx.waker().clone());
            Poll::Pending
        }
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
