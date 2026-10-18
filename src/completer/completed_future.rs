use std::marker::Unpin;
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// A future that is immediately ready with a value.
///
/// `CompletedFuture` is a utility struct that implements the `Future` trait and returns
/// an immediately ready value when polled. This is useful in scenarios where an async
/// implementation cannot await any call but still needs to return a future, serving as
/// an async-to-sync boundary utility.
///
/// # Examples
///
/// ```
/// use rayx_signalr::CompletedFuture;
///
/// # futures::executor::block_on(async {
/// let future = CompletedFuture::new(42);
/// let result = future.await;
/// assert_eq!(result, 42);
/// # });
/// ```
pub struct CompletedFuture<T: Unpin> {
    data: RefCell<Option<T>>,
}

impl<T: Unpin> CompletedFuture<T> {
    /// Creates a new `CompletedFuture` with the given value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value that the future will immediately return when polled.
    ///
    /// # Examples
    ///
    /// ```
    /// let future = rayx_signalr::CompletedFuture::new(42);
    /// ```
    #[allow(dead_code)]
    pub fn new(data: T) -> Self {
        Self {
            data: RefCell::new(Some(data)),
        }
    }
}

impl<T: Unpin> Future for CompletedFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _: &mut Context) -> Poll<Self::Output> {
        let data = self.data.borrow_mut().take().unwrap();

        Poll::Ready(data)
    }
}
