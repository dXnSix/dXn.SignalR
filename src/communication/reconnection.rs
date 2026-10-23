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

use std::time::Duration;

use crate::error::Error;

/// The lifecycle of one logical connection, shared by both targets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleState {
    /// Built by `SignalRClient::new` and never started.
    NotStarted,
    /// `start` is negotiating and connecting.
    Connecting,
    Disconnected,
    Connected,
    Reconnecting,
    /// Paused by the application: no transport, no keep-alive and no automatic reconnect until
    /// `resume`.
    Paused,
    LocalClosed,
}

/// The status of a client's connection, as reported by `SignalRClient::status`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ConnectionStatus {
    /// `start` is negotiating and connecting (the official .NET client's `Connecting`).
    Connecting,
    /// A transport is open and the handshake completed.
    Connected,
    /// A reconnect (automatic, manual or after `resume`) is running.
    Reconnecting,
    /// Paused by `pause`; `resume` reconnects.
    Paused,
    /// Not started yet, or the transport is gone and nothing is reconnecting (a failed start, a
    /// manual-mode drop, exhausted attempts or a server close); `start`, `resume` or a manual
    /// reconnect may connect again.
    Disconnected,
    /// Closed locally by `disconnect`; `start` connects again.
    Closed,
}

/// What `pause` did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PauseOutcome {
    Paused,
    AlreadyPaused,
}

/// What `resume` did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResumeOutcome {
    /// A reconnect began with this generation.
    Started(u64),
    /// The client is connecting, connected or already reconnecting: nothing to do.
    AlreadyActive,
}

/// The lifecycle state machine. Every start and every reconnect owns a generation; pausing,
/// closing and a terminal server close move to a new generation, so a superseded attempt can
/// never complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Lifecycle {
    state: LifecycleState,
    generation: u64,
}

impl Lifecycle {
    /// A client that was never started.
    pub(crate) const fn new() -> Self {
        Self {
            state: LifecycleState::NotStarted,
            generation: 0,
        }
    }

    #[cfg(test)]
    pub(crate) const fn connected() -> Self {
        Self {
            state: LifecycleState::Connected,
            generation: 0,
        }
    }

    pub(crate) fn state(&self) -> LifecycleState {
        self.state
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn status(&self) -> ConnectionStatus {
        match self.state {
            LifecycleState::Connecting => ConnectionStatus::Connecting,
            LifecycleState::Connected => ConnectionStatus::Connected,
            LifecycleState::Reconnecting => ConnectionStatus::Reconnecting,
            LifecycleState::Paused => ConnectionStatus::Paused,
            LifecycleState::NotStarted | LifecycleState::Disconnected => {
                ConnectionStatus::Disconnected
            }
            LifecycleState::LocalClosed => ConnectionStatus::Closed,
        }
    }

    /// Whether `generation` is the start or reconnect that currently runs.
    pub(crate) fn owns(&self, generation: u64) -> bool {
        matches!(
            self.state,
            LifecycleState::Connecting | LifecycleState::Reconnecting
        ) && self.generation == generation
    }

    /// Why a start or reconnect of an older generation stopped.
    pub(crate) fn superseded(&self) -> Error {
        match self.state {
            LifecycleState::LocalClosed => Error::closed_locally(),
            LifecycleState::Paused => Error::paused(),
            _ => Error::canceled("the connection attempt was superseded"),
        }
    }

    /// Starts the client, as `HubConnection.StartAsync`: from a client that was never started,
    /// is disconnected or was closed locally. A client that is starting, connected, reconnecting
    /// or paused is left unchanged.
    pub(crate) fn start(&mut self) -> Result<u64, Error> {
        match self.state {
            LifecycleState::NotStarted
            | LifecycleState::Disconnected
            | LifecycleState::LocalClosed => Ok(self.advance(LifecycleState::Connecting)),
            LifecycleState::Connecting => Err(Error::invalid_state(
                "the connection cannot be started while it is starting",
            )),
            LifecycleState::Connected => Err(Error::invalid_state(
                "the connection cannot be started while it is connected",
            )),
            LifecycleState::Reconnecting => Err(Error::invalid_state(
                "the connection cannot be started while it is reconnecting",
            )),
            LifecycleState::Paused => Err(Error::invalid_state(
                "the connection cannot be started while it is paused; resume it",
            )),
        }
    }

    /// Starts a reconnect (automatic or manual) from a connected or disconnected client.
    pub(crate) fn begin(&mut self) -> Result<u64, Error> {
        match self.state {
            LifecycleState::LocalClosed => Err(Error::closed_locally()),
            LifecycleState::Paused => Err(Error::paused()),
            LifecycleState::NotStarted => Err(Error::not_started()),
            LifecycleState::Connecting => Err(Error::canceled("the connection is starting")),
            LifecycleState::Reconnecting => {
                Err(Error::canceled("a reconnect attempt is already running"))
            }
            LifecycleState::Connected | LifecycleState::Disconnected => {
                Ok(self.advance(LifecycleState::Reconnecting))
            }
        }
    }

    fn advance(&mut self, state: LifecycleState) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.state = state;
        self.generation
    }

    /// Completes the start or reconnect of `generation`; false when it was superseded.
    pub(crate) fn complete(&mut self, generation: u64) -> bool {
        if !self.owns(generation) {
            return false;
        }
        self.state = LifecycleState::Connected;
        true
    }

    /// Ends the start or reconnect of `generation` without a connection; false when it was
    /// superseded.
    pub(crate) fn fail(&mut self, generation: u64) -> bool {
        if !self.owns(generation) {
            return false;
        }
        self.state = LifecycleState::Disconnected;
        true
    }

    /// Records a lost transport that nothing reconnects automatically (manual mode).
    pub(crate) fn transport_lost(&mut self) {
        if self.state == LifecycleState::Connected {
            self.state = LifecycleState::Disconnected;
        }
    }

    /// Records a terminal server close; a later start, manual reconnect or resume may still
    /// begin a new generation. A paused or locally closed client keeps its state.
    pub(crate) fn remote_close(&mut self) {
        if !matches!(
            self.state,
            LifecycleState::LocalClosed | LifecycleState::Paused | LifecycleState::NotStarted
        ) {
            self.advance(LifecycleState::Disconnected);
        }
    }

    pub(crate) fn local_close(&mut self) {
        self.advance(LifecycleState::LocalClosed);
    }

    /// Pauses a connected, starting, reconnecting or disconnected client, superseding any
    /// running start or reconnect. Pausing a paused client is a no-op; a closed or unstarted
    /// client rejects it.
    pub(crate) fn pause(&mut self) -> Result<PauseOutcome, Error> {
        match self.state {
            LifecycleState::LocalClosed => Err(Error::closed_locally()),
            LifecycleState::NotStarted => Err(Error::not_started()),
            LifecycleState::Paused => Ok(PauseOutcome::AlreadyPaused),
            LifecycleState::Connected
            | LifecycleState::Connecting
            | LifecycleState::Reconnecting
            | LifecycleState::Disconnected => {
                self.advance(LifecycleState::Paused);
                Ok(PauseOutcome::Paused)
            }
        }
    }

    /// Resumes a paused or disconnected client by beginning a reconnect. Resuming a starting,
    /// connected or reconnecting client is a no-op; a closed or unstarted client rejects it.
    pub(crate) fn resume(&mut self) -> Result<ResumeOutcome, Error> {
        match self.state {
            LifecycleState::LocalClosed => Err(Error::closed_locally()),
            LifecycleState::NotStarted => Err(Error::not_started()),
            LifecycleState::Connecting
            | LifecycleState::Connected
            | LifecycleState::Reconnecting => Ok(ResumeOutcome::AlreadyActive),
            LifecycleState::Paused | LifecycleState::Disconnected => Ok(ResumeOutcome::Started(
                self.advance(LifecycleState::Reconnecting),
            )),
        }
    }
}

/// The starts and reconnect runs in flight, which a local close cancels at once (as
/// `HubConnection.StopAsync` cancels `StartAsync` and the reconnect loop) instead of letting
/// them run to their next check.
#[derive(Clone, Debug, Default)]
pub(crate) struct Runs {
    /// Each run in flight under its own key, so one ending never forgets another's handle.
    handles: std::sync::Arc<std::sync::Mutex<Vec<(u64, futures::future::AbortHandle)>>>,
    next: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Runs {
    /// Runs `future` until it ends or `cancel_all` (a local close or a pause) ends it with
    /// `superseded()`.
    pub(crate) async fn supervise<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, Error>>,
        superseded: impl FnOnce() -> Error,
    ) -> Result<T, Error> {
        use crate::platform::LockExt;
        let key = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (handle, registration) = futures::future::AbortHandle::new_pair();
        self.handles.locked().push((key, handle));
        let result = futures::future::Abortable::new(future, registration).await;
        self.handles.locked().retain(|(running, _)| *running != key);
        result.unwrap_or_else(|_| Err(superseded()))
    }

    /// Cancels every run in flight.
    pub(crate) fn cancel_all(&self) {
        use crate::platform::LockExt;
        for (_, handle) in self.handles.locked().drain(..) {
            handle.abort();
        }
    }
}

/// A start or reconnect attempt in flight. Calls made meanwhile wait for its outcome, as the
/// official .NET client's connection lock makes them wait, then run on the new connection or
/// fail with the attempt's error.
#[derive(Clone)]
pub(crate) struct AttemptOutcome {
    id: u64,
    outcome: futures::future::Shared<futures::channel::oneshot::Receiver<Result<(), Error>>>,
}

/// Reports the outcome of the attempt an [`AttemptOutcome`] waits for.
pub(crate) struct AttemptReport(futures::channel::oneshot::Sender<Result<(), Error>>);

impl AttemptOutcome {
    pub(crate) fn new() -> (AttemptReport, Self) {
        use futures::FutureExt;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let (report, outcome) = futures::channel::oneshot::channel();
        let attempt = Self {
            id: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            outcome: outcome.shared(),
        };
        (AttemptReport(report), attempt)
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Waits for the attempt: `Ok` when it connected, or when it was canceled (the caller reads
    /// the client's state again); otherwise the attempt's error.
    pub(crate) async fn wait(&self) -> Result<(), Error> {
        self.outcome.clone().await.unwrap_or(Ok(()))
    }
}

impl std::fmt::Debug for AttemptOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AttemptOutcome")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl AttemptReport {
    pub(crate) fn report(self, result: &Result<(), Error>) {
        let _ = self.0.send(result.clone());
    }
}

/// How the first attempt of a reconnect run is scheduled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FirstAttempt {
    /// After the policy's first delay (automatic reconnect).
    AfterPolicyDelay,
    /// Immediately, if the policy allows a first attempt (manual `reconnect_with_policy`).
    WithoutDelay,
    /// Immediately, whatever the policy says (`resume`); later attempts follow the policy.
    Always,
}

impl FirstAttempt {
    /// The delay before the attempt `context` describes, or `None` when the run is over.
    /// `initial` is the first delay when the caller already asked the policy for it (an
    /// automatic run decides with it whether to start at all).
    pub(crate) fn delay(
        self,
        policy: &dyn ReconnectionPolicy,
        context: &RetryContext,
        initial: Option<Duration>,
    ) -> Option<Duration> {
        let first = context.previous_retry_count == 0;
        if first && self == FirstAttempt::Always {
            return Some(Duration::ZERO);
        }
        if let (true, Some(initial)) = (first, initial) {
            return Some(initial);
        }
        let delay = ask_policy(policy, context)?;
        if first && self == FirstAttempt::WithoutDelay {
            return Some(Duration::ZERO);
        }
        Some(delay)
    }
}

/// Asks the policy for the next delay; a policy that panics ends the run, as the .NET client
/// treats a throwing `IRetryPolicy` (natively; a browser panic aborts the module).
pub(crate) fn ask_policy(
    policy: &dyn ReconnectionPolicy,
    context: &RetryContext,
) -> Option<Duration> {
    match crate::execution::guarded("the reconnection policy", || {
        policy.next_retry_delay_with(context)
    }) {
        Ok(delay) => delay,
        Err(_) => {
            log::error!("The reconnection policy panicked; reconnecting stops");
            None
        }
    }
}

/// Serializes start and reconnect ownership across a native client's clones and tasks.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub(crate) struct ReconnectCoordinator {
    state: std::sync::Arc<std::sync::Mutex<Coordinated>>,
    runs: Runs,
}

/// The lifecycle and the connection id of the current connection, changed together.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
struct Coordinated {
    lifecycle: Lifecycle,
    connection_id: Option<String>,
}

#[cfg(not(target_arch = "wasm32"))]
impl ReconnectCoordinator {
    /// The coordinator of a client that was never started.
    pub(crate) fn new() -> Self {
        Self::with_lifecycle(Lifecycle::new())
    }

    #[cfg(test)]
    pub(crate) fn connected() -> Self {
        Self::with_lifecycle(Lifecycle::connected())
    }

    fn with_lifecycle(lifecycle: Lifecycle) -> Self {
        Self {
            state: std::sync::Arc::new(std::sync::Mutex::new(Coordinated {
                lifecycle,
                connection_id: None,
            })),
            runs: Runs::default(),
        }
    }

    fn with<R>(&self, change: impl FnOnce(&mut Lifecycle) -> R) -> R {
        use crate::platform::LockExt;
        change(&mut self.state.locked().lifecycle)
    }

    /// Completes the start or reconnect of `generation`: a fresh connection records its
    /// negotiated id (`Some`), a stateful resume keeps the current one (`None`). False when the
    /// generation was superseded.
    pub(crate) fn complete_connection(
        &self,
        generation: u64,
        connection_id: Option<Option<String>>,
    ) -> bool {
        use crate::platform::LockExt;
        let mut state = self.state.locked();
        if !state.lifecycle.complete(generation) {
            return false;
        }
        if let Some(connection_id) = connection_id {
            state.connection_id = connection_id;
        }
        true
    }

    /// The negotiated id of the current connection, while connected.
    pub(crate) fn connection_id(&self) -> Option<String> {
        use crate::platform::LockExt;
        let state = self.state.locked();
        (state.lifecycle.state() == LifecycleState::Connected)
            .then(|| state.connection_id.clone())
            .flatten()
    }

    pub(crate) fn start(&self) -> Result<u64, Error> {
        self.with(Lifecycle::start)
    }

    pub(crate) fn begin(&self) -> Result<u64, Error> {
        self.with(Lifecycle::begin)
    }

    /// Runs a start, reconnect or resume until it ends or a local close or a pause cancels it.
    pub(crate) async fn supervise<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        self.runs.supervise(future, || self.superseded()).await
    }

    #[cfg(test)]
    pub(crate) fn complete(&self, generation: u64) -> bool {
        self.with(|lifecycle| lifecycle.complete(generation))
    }

    pub(crate) fn fail(&self, generation: u64) -> bool {
        self.with(|lifecycle| lifecycle.fail(generation))
    }

    pub(crate) fn owns(&self, generation: u64) -> bool {
        self.with(|lifecycle| lifecycle.owns(generation))
    }

    pub(crate) fn superseded(&self) -> Error {
        self.with(|lifecycle| lifecycle.superseded())
    }

    pub(crate) fn transport_lost(&self) {
        self.with(Lifecycle::transport_lost)
    }

    /// Records a terminal server close; a later manual reconnect may still begin a new generation.
    pub(crate) fn remote_close(&self) {
        self.with(Lifecycle::remote_close)
    }

    /// Closes locally and cancels the start or reconnect run in flight.
    pub(crate) fn local_close(&self) {
        self.with(Lifecycle::local_close);
        self.runs.cancel_all();
    }

    /// Pauses and cancels the start, reconnect or resume in flight, which the pause supersedes.
    pub(crate) fn pause(&self) -> Result<PauseOutcome, Error> {
        let outcome = self.with(Lifecycle::pause)?;
        self.runs.cancel_all();
        Ok(outcome)
    }

    pub(crate) fn resume(&self) -> Result<ResumeOutcome, Error> {
        self.with(Lifecycle::resume)
    }

    pub(crate) fn lifecycle(&self) -> LifecycleState {
        self.with(|lifecycle| lifecycle.state())
    }

    pub(crate) fn generation(&self) -> u64 {
        self.with(|lifecycle| lifecycle.generation())
    }

    pub(crate) fn status(&self) -> ConnectionStatus {
        self.with(|lifecycle| lifecycle.status())
    }
}

/// What a reconnection policy decides the next delay from, as the official .NET client's
/// `RetryContext`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RetryContext {
    /// How many attempts of this reconnect run failed so far (0 before the first attempt).
    pub previous_retry_count: u32,
    /// How long the run has been reconnecting.
    pub elapsed: Duration,
    /// Why the run needs another attempt: the error that ended the transport before the first
    /// attempt, then the last attempt's error.
    pub retry_reason: crate::Error,
}

impl RetryContext {
    /// A context, for example to test a policy:
    ///
    /// ```
    /// use std::time::Duration;
    /// use dxn_signalr::{DelayListPolicy, ReconnectionPolicy, RetryContext};
    ///
    /// fn second_delay(reason: dxn_signalr::Error) {
    ///     let policy = DelayListPolicy::new([Duration::ZERO, Duration::from_secs(2)]);
    ///     let context = RetryContext::new(1, Duration::from_secs(1), reason);
    ///     assert_eq!(
    ///         policy.next_retry_delay_with(&context),
    ///         Some(Duration::from_secs(2))
    ///     );
    /// }
    /// ```
    pub fn new(previous_retry_count: u32, elapsed: Duration, retry_reason: crate::Error) -> Self {
        Self {
            previous_retry_count,
            elapsed,
            retry_reason,
        }
    }
}

/// Decides whether and when the client reconnects, as the official .NET client's `IRetryPolicy`.
///
/// Implement [`next_retry_delay_with`](Self::next_retry_delay_with) to see the
/// [`RetryContext`] (including why the client is reconnecting), or the older
/// [`next_retry_delay`](Self::next_retry_delay). A policy that implements neither never
/// reconnects. A policy that panics stops the reconnect run, which then ends as if the policy had
/// returned `None`.
pub trait ReconnectionPolicy: crate::platform::MaybeSendSync {
    /// Returns the delay before the next reconnection attempt, or `None` to stop reconnecting.
    /// `retry_count` is the number of failed attempts so far and `elapsed_milliseconds` how long
    /// the run has been reconnecting.
    fn next_retry_delay(&self, retry_count: u32, elapsed_milliseconds: u64) -> Option<Duration> {
        let _ = (retry_count, elapsed_milliseconds);
        None
    }

    /// Returns the delay before the next reconnection attempt, or `None` to stop reconnecting,
    /// as `IRetryPolicy.NextRetryDelay(RetryContext)`. Defaults to
    /// [`next_retry_delay`](Self::next_retry_delay).
    fn next_retry_delay_with(&self, context: &RetryContext) -> Option<Duration> {
        let elapsed = u64::try_from(context.elapsed.as_millis()).unwrap_or(u64::MAX);
        self.next_retry_delay(context.previous_retry_count, elapsed)
    }
}

/// A reconnection policy that never retries.
pub struct NoReconnectPolicy;

impl ReconnectionPolicy for NoReconnectPolicy {
    fn next_retry_delay(&self, _retry_count: u32, _elapsed_milliseconds: u64) -> Option<Duration> {
        None
    }
}

/// A reconnection policy that retries with a constant delay.
pub struct ConstantDelayPolicy {
    delay: Duration,
    max_attempts: Option<u32>,
}

impl ConstantDelayPolicy {
    /// Retries after `delay` each time, at most `max_attempts` times (`None`: without limit).
    pub fn new(delay: Duration, max_attempts: Option<u32>) -> Self {
        Self {
            delay,
            max_attempts,
        }
    }
}

impl ReconnectionPolicy for ConstantDelayPolicy {
    fn next_retry_delay(&self, retry_count: u32, _elapsed_milliseconds: u64) -> Option<Duration> {
        if let Some(max) = self.max_attempts {
            if retry_count >= max {
                return None;
            }
        }
        Some(self.delay)
    }
}

/// A reconnection policy that retries with a linear backoff.
pub struct LinearBackoffPolicy {
    initial_delay: Duration,
    increment: Duration,
    max_delay: Option<Duration>,
    max_attempts: Option<u32>,
}

impl LinearBackoffPolicy {
    /// Waits `initial_delay`, then `increment` longer on each further attempt, capped at
    /// `max_delay`, for at most `max_attempts` attempts (`None`: no cap or no limit).
    pub fn new(
        initial_delay: Duration,
        increment: Duration,
        max_delay: Option<Duration>,
        max_attempts: Option<u32>,
    ) -> Self {
        Self {
            initial_delay,
            increment,
            max_delay,
            max_attempts,
        }
    }
}

impl ReconnectionPolicy for LinearBackoffPolicy {
    fn next_retry_delay(&self, retry_count: u32, _elapsed_milliseconds: u64) -> Option<Duration> {
        if let Some(max) = self.max_attempts {
            if retry_count >= max {
                return None;
            }
        }

        let delay = self.initial_delay + self.increment * retry_count;

        if let Some(max_delay) = self.max_delay {
            if delay > max_delay {
                return Some(max_delay);
            }
        }

        Some(delay)
    }
}

/// A reconnection policy that retries with an exponential backoff.
pub struct ExponentialBackoffPolicy {
    initial_delay: Duration,
    factor: f64,
    max_delay: Option<Duration>,
    max_attempts: Option<u32>,
}

impl ExponentialBackoffPolicy {
    /// Waits `initial_delay`, then multiplies the delay by `factor` on each further attempt,
    /// capped at `max_delay`, for at most `max_attempts` attempts (`None`: no cap or no limit).
    pub fn new(
        initial_delay: Duration,
        factor: f64,
        max_delay: Option<Duration>,
        max_attempts: Option<u32>,
    ) -> Self {
        Self {
            initial_delay,
            factor,
            max_delay,
            max_attempts,
        }
    }
}

impl ReconnectionPolicy for ExponentialBackoffPolicy {
    fn next_retry_delay(&self, retry_count: u32, _elapsed_milliseconds: u64) -> Option<Duration> {
        if let Some(max) = self.max_attempts {
            if retry_count >= max {
                return None;
            }
        }

        let delay_secs = self.initial_delay.as_secs_f64() * self.factor.powi(retry_count as i32);
        let delay = Duration::from_secs_f64(delay_secs);

        if let Some(max_delay) = self.max_delay {
            if delay > max_delay {
                return Some(max_delay);
            }
        }

        Some(delay)
    }
}

/// Configuration for reconnection.
#[derive(Clone)]
pub struct ReconnectionConfig {
    /// The policy that decides whether and when to retry; the default is [`NoReconnectPolicy`].
    pub policy: std::sync::Arc<dyn ReconnectionPolicy>,
}

impl Default for ReconnectionConfig {
    fn default() -> Self {
        Self {
            policy: std::sync::Arc::new(NoReconnectPolicy),
        }
    }
}

/// Retries after each delay of a list, then stops, as the official .NET client's
/// `WithAutomaticReconnect(TimeSpan[])`: the first attempt waits `delays[0]`, the second
/// `delays[1]`, and so on.
#[derive(Clone, Debug, Default)]
pub struct DelayListPolicy {
    delays: Vec<Duration>,
}

impl DelayListPolicy {
    /// Retries once after each of `delays`, in order.
    pub fn new(delays: impl IntoIterator<Item = Duration>) -> Self {
        Self {
            delays: delays.into_iter().collect(),
        }
    }
}

impl ReconnectionPolicy for DelayListPolicy {
    fn next_retry_delay(&self, retry_count: u32, _elapsed_milliseconds: u64) -> Option<Duration> {
        self.delays.get(retry_count as usize).copied()
    }
}

/// The official clients' default reconnect delays: 0, 2, 10 and 30 seconds, then no further
/// attempts (as `withAutomaticReconnect()` in the JavaScript and Swift clients). Opt in with
/// `ReconnectionConfig { policy: Arc::new(DefaultRetryPolicy) }`; `ReconnectionConfig::default()`
/// stays `NoReconnectPolicy`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultRetryPolicy;

impl DefaultRetryPolicy {
    /// The delays in seconds before attempts 0 to 3.
    pub const DELAYS_SECONDS: [u64; 4] = [0, 2, 10, 30];
}

impl ReconnectionPolicy for DefaultRetryPolicy {
    fn next_retry_delay(&self, retry_count: u32, _elapsed_milliseconds: u64) -> Option<Duration> {
        Self::DELAYS_SECONDS
            .get(retry_count as usize)
            .map(|seconds| Duration::from_secs(*seconds))
    }
}

#[cfg(test)]
mod lifecycle_controls {
    use std::time::Duration;

    use super::{
        ConnectionStatus, DefaultRetryPolicy, FirstAttempt, Lifecycle, LifecycleState,
        NoReconnectPolicy, PauseOutcome, ReconnectionConfig, ReconnectionPolicy, ResumeOutcome,
    };
    use crate::{CloseReason, ErrorKind};

    fn retry(previous_retry_count: u32) -> super::RetryContext {
        super::RetryContext::new(
            previous_retry_count,
            Duration::ZERO,
            crate::Error::canceled("test"),
        )
    }

    fn reconnecting() -> (Lifecycle, u64) {
        let mut lifecycle = Lifecycle::connected();
        let generation = lifecycle.begin().unwrap();
        (lifecycle, generation)
    }

    #[test]
    fn lifecycle_controls_status_maps_every_state() {
        let mut lifecycle = Lifecycle::connected();
        assert_eq!(lifecycle.status(), ConnectionStatus::Connected);
        let generation = lifecycle.begin().unwrap();
        assert_eq!(lifecycle.status(), ConnectionStatus::Reconnecting);
        lifecycle.fail(generation);
        assert_eq!(lifecycle.status(), ConnectionStatus::Disconnected);
        lifecycle.pause().unwrap();
        assert_eq!(lifecycle.status(), ConnectionStatus::Paused);
        lifecycle.local_close();
        assert_eq!(lifecycle.status(), ConnectionStatus::Closed);
    }

    #[test]
    fn lifecycle_controls_pause_is_accepted_from_every_open_state() {
        let mut connected = Lifecycle::connected();
        assert_eq!(connected.pause().ok(), Some(PauseOutcome::Paused));
        assert_eq!(connected.state(), LifecycleState::Paused);

        let (mut reconnecting, _) = reconnecting();
        assert_eq!(reconnecting.pause().ok(), Some(PauseOutcome::Paused));

        let mut disconnected = Lifecycle::connected();
        disconnected.transport_lost();
        assert_eq!(disconnected.status(), ConnectionStatus::Disconnected);
        assert_eq!(disconnected.pause().ok(), Some(PauseOutcome::Paused));
    }

    #[test]
    fn lifecycle_controls_pause_invalidates_the_running_reconnect() {
        let (mut lifecycle, generation) = reconnecting();
        lifecycle.pause().unwrap();
        assert!(!lifecycle.owns(generation));
        assert!(
            !lifecycle.complete(generation),
            "a stale completion never leaves Paused"
        );
        lifecycle.fail(generation);
        assert_eq!(lifecycle.state(), LifecycleState::Paused);
        assert_eq!(lifecycle.superseded().kind(), ErrorKind::Paused);
    }

    #[test]
    fn lifecycle_controls_paused_clients_refuse_reconnects_and_server_closes() {
        let mut lifecycle = Lifecycle::connected();
        lifecycle.pause().unwrap();
        assert_eq!(lifecycle.begin().unwrap_err().kind(), ErrorKind::Paused);
        lifecycle.remote_close();
        lifecycle.transport_lost();
        assert_eq!(lifecycle.state(), LifecycleState::Paused);
    }

    #[test]
    fn lifecycle_controls_resume_starts_a_new_generation_from_paused_or_disconnected() {
        let mut paused = Lifecycle::connected();
        paused.pause().unwrap();
        let ResumeOutcome::Started(generation) = paused.resume().unwrap() else {
            panic!("resume from Paused must start a reconnect");
        };
        assert!(paused.owns(generation));
        assert!(paused.complete(generation));
        assert_eq!(paused.status(), ConnectionStatus::Connected);

        let (mut disconnected, generation) = reconnecting();
        disconnected.fail(generation);
        assert!(matches!(
            disconnected.resume(),
            Ok(ResumeOutcome::Started(next)) if next != generation
        ));
    }

    #[test]
    fn lifecycle_controls_repeated_pause_and_resume_are_no_ops() {
        let mut lifecycle = Lifecycle::connected();
        assert_eq!(lifecycle.resume().ok(), Some(ResumeOutcome::AlreadyActive));
        assert_eq!(lifecycle.state(), LifecycleState::Connected);
        let (mut running, generation) = reconnecting();
        assert_eq!(running.resume().ok(), Some(ResumeOutcome::AlreadyActive));
        assert!(running.owns(generation));

        lifecycle.pause().unwrap();
        let paused = lifecycle;
        assert_eq!(lifecycle.pause().ok(), Some(PauseOutcome::AlreadyPaused));
        assert_eq!(lifecycle, paused, "a repeated pause changes nothing");
    }

    #[test]
    fn lifecycle_controls_closed_is_terminal() {
        let (mut lifecycle, generation) = reconnecting();
        lifecycle.local_close();
        assert!(!lifecycle.complete(generation));
        for error in [
            lifecycle.pause().unwrap_err(),
            lifecycle.resume().unwrap_err(),
            lifecycle.begin().unwrap_err(),
        ] {
            assert_eq!(error.close_reason(), Some(CloseReason::Local));
        }
        lifecycle.remote_close();
        assert_eq!(lifecycle.status(), ConnectionStatus::Closed);
    }

    #[test]
    fn lifecycle_controls_default_retry_policy_matches_the_official_clients() {
        let policy = DefaultRetryPolicy;
        let delays: Vec<_> = (0..5)
            .map(|retry| policy.next_retry_delay(retry, 0))
            .collect();
        assert_eq!(
            delays,
            vec![
                Some(Duration::ZERO),
                Some(Duration::from_secs(2)),
                Some(Duration::from_secs(10)),
                Some(Duration::from_secs(30)),
                None,
            ]
        );
        let default = ReconnectionConfig::default();
        assert_eq!(default.policy.next_retry_delay(0, 0), None);
    }

    #[test]
    fn lifecycle_controls_an_ended_stateful_connection_renews_for_a_fresh_negotiation() {
        use crate::communication::stateful::{
            SerializedFrame, StatefulMessageBuffer, TrackResult, STALE_TRANSPORT,
        };
        use crate::protocol::negotiate::MessageType;

        let mut buffer = StatefulMessageBuffer::new(1_024).unwrap();
        let old_generation = buffer.transport_generation();
        let frame = || SerializedFrame::text("{}");
        buffer.track(MessageType::Invocation, frame()).unwrap();
        assert!(!buffer.is_disposed());
        buffer.dispose();
        assert!(
            buffer.is_disposed(),
            "a disposed buffer can only be renewed"
        );

        buffer.renew();
        assert!(!buffer.is_disposed());
        assert_eq!(buffer.buffered_bytes(), 0);
        assert_eq!(
            buffer.track(MessageType::Invocation, frame()).ok(),
            Some(TrackResult::Buffered(1)),
            "a renewed buffer numbers frames from the start"
        );
        assert_eq!(
            buffer
                .track_for(old_generation, MessageType::Invocation, frame())
                .unwrap_err()
                .to_string(),
            STALE_TRANSPORT,
            "writers of the ended connection can never track frames"
        );
    }

    #[test]
    fn lifecycle_controls_resume_attempts_immediately_then_follows_the_policy() {
        let policy = DefaultRetryPolicy;
        assert_eq!(
            FirstAttempt::Always.delay(&NoReconnectPolicy, &retry(0), None),
            Some(Duration::ZERO)
        );
        assert_eq!(
            FirstAttempt::Always.delay(&NoReconnectPolicy, &retry(1), None),
            None
        );
        assert_eq!(
            FirstAttempt::Always.delay(&policy, &retry(1), None),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            FirstAttempt::WithoutDelay.delay(&NoReconnectPolicy, &retry(0), None),
            None
        );
        let slow = super::ConstantDelayPolicy::new(Duration::from_secs(5), Some(2));
        assert_eq!(
            FirstAttempt::WithoutDelay.delay(&slow, &retry(0), None),
            Some(Duration::ZERO)
        );
        assert_eq!(
            FirstAttempt::AfterPolicyDelay.delay(&slow, &retry(0), None),
            Some(Duration::from_secs(5))
        );
    }
}

/// Starting and restarting, as `HubConnection.StartAsync` (R16.C2, R16.C3).
#[cfg(test)]
mod start_lifecycle {
    use super::{ConnectionStatus, Lifecycle, LifecycleState};
    use crate::ErrorKind;

    #[test]
    fn client_start_lifecycle_moves_from_not_started_through_connecting() {
        let mut lifecycle = Lifecycle::new();
        assert_eq!(lifecycle.state(), LifecycleState::NotStarted);
        assert_eq!(lifecycle.status(), ConnectionStatus::Disconnected);
        for refused in [lifecycle.begin(), lifecycle.pause().map(|_| 0)] {
            assert_eq!(refused.unwrap_err().kind(), ErrorKind::InvalidState);
        }
        assert_eq!(
            lifecycle.resume().unwrap_err().kind(),
            ErrorKind::InvalidState
        );
        assert_eq!(lifecycle.state(), LifecycleState::NotStarted);

        let generation = lifecycle.start().unwrap();
        assert_eq!(lifecycle.status(), ConnectionStatus::Connecting);
        assert!(lifecycle.owns(generation));
        assert!(lifecycle.complete(generation));
        assert_eq!(lifecycle.status(), ConnectionStatus::Connected);
    }

    #[test]
    fn client_start_lifecycle_a_failed_start_disconnects() {
        let mut lifecycle = Lifecycle::new();
        let generation = lifecycle.start().unwrap();
        assert!(lifecycle.fail(generation));
        assert_eq!(lifecycle.status(), ConnectionStatus::Disconnected);
        assert!(lifecycle.start().is_ok(), "a failed start may be retried");
    }

    #[test]
    fn client_start_lifecycle_pause_and_close_supersede_a_start() {
        let mut paused = Lifecycle::new();
        let generation = paused.start().unwrap();
        paused.pause().unwrap();
        assert!(!paused.complete(generation));
        assert_eq!(paused.superseded().kind(), ErrorKind::Paused);

        let mut closed = Lifecycle::new();
        let generation = closed.start().unwrap();
        closed.local_close();
        assert!(!closed.complete(generation));
        assert_eq!(
            closed.superseded().close_reason(),
            Some(crate::CloseReason::Local)
        );
    }

    #[test]
    fn client_restart_lifecycle_starts_again_after_a_local_or_remote_close() {
        let mut lifecycle = Lifecycle::new();
        let first = lifecycle.start().unwrap();
        lifecycle.complete(first);
        lifecycle.local_close();
        assert_eq!(lifecycle.status(), ConnectionStatus::Closed);
        let second = lifecycle.start().unwrap();
        assert_ne!(second, first, "a restart is a new generation");
        assert!(lifecycle.complete(second));

        lifecycle.remote_close();
        assert_eq!(lifecycle.status(), ConnectionStatus::Disconnected);
        let third = lifecycle.start().unwrap();
        assert!(lifecycle.complete(third));
        assert_eq!(lifecycle.status(), ConnectionStatus::Connected);
    }

    #[test]
    fn client_restart_lifecycle_rejects_start_while_active() {
        let mut connecting = Lifecycle::new();
        connecting.start().unwrap();
        let mut connected = Lifecycle::connected();
        let mut reconnecting = Lifecycle::connected();
        reconnecting.begin().unwrap();
        let mut paused = Lifecycle::connected();
        paused.pause().unwrap();
        for lifecycle in [
            &mut connecting,
            &mut connected,
            &mut reconnecting,
            &mut paused,
        ] {
            let before = *lifecycle;
            assert_eq!(
                lifecycle.start().unwrap_err().kind(),
                ErrorKind::InvalidState
            );
            assert_eq!(*lifecycle, before, "a refused start changes nothing");
        }
        assert_eq!(
            connecting.begin().unwrap_err().kind(),
            ErrorKind::Canceled,
            "no manual reconnect while starting"
        );
    }
}

/// Reconnection policies see a `RetryContext`, as `IRetryPolicy.NextRetryDelay` (R16.C25).
#[cfg(test)]
mod retry_context {
    use std::time::Duration;

    use super::{ask_policy, DelayListPolicy, FirstAttempt, ReconnectionPolicy, RetryContext};

    fn context(previous_retry_count: u32, elapsed: Duration) -> RetryContext {
        RetryContext::new(
            previous_retry_count,
            elapsed,
            crate::Error::canceled("dropped"),
        )
    }

    #[test]
    fn retry_context_policies_with_the_old_signature_keep_working() {
        struct Counting;
        impl ReconnectionPolicy for Counting {
            fn next_retry_delay(&self, retry_count: u32, elapsed: u64) -> Option<Duration> {
                (retry_count < 2).then(|| Duration::from_millis(elapsed + u64::from(retry_count)))
            }
        }
        let context = context(1, Duration::from_millis(40));
        assert_eq!(
            Counting.next_retry_delay_with(&context),
            Some(Duration::from_millis(41))
        );
        assert_eq!(
            Counting.next_retry_delay_with(&super::RetryContext::new(
                2,
                Duration::ZERO,
                crate::Error::canceled("x")
            )),
            None
        );
    }

    #[test]
    fn retry_context_policies_see_the_reason_and_unimplemented_ones_never_retry() {
        struct ByReason;
        impl ReconnectionPolicy for ByReason {
            fn next_retry_delay_with(&self, context: &RetryContext) -> Option<Duration> {
                (context.retry_reason.kind() == crate::ErrorKind::Canceled)
                    .then_some(context.elapsed)
            }
        }
        struct Silent;
        impl ReconnectionPolicy for Silent {}
        let context = context(0, Duration::from_secs(3));
        assert_eq!(
            ByReason.next_retry_delay_with(&context),
            Some(Duration::from_secs(3))
        );
        assert_eq!(Silent.next_retry_delay_with(&context), None);
    }

    #[test]
    fn retry_context_a_panicking_policy_stops_reconnecting() {
        struct Panicking;
        impl ReconnectionPolicy for Panicking {
            fn next_retry_delay_with(&self, _context: &RetryContext) -> Option<Duration> {
                panic!("policy failure")
            }
        }
        assert_eq!(ask_policy(&Panicking, &context(0, Duration::ZERO)), None);
        assert_eq!(
            FirstAttempt::AfterPolicyDelay.delay(&Panicking, &context(1, Duration::ZERO), None),
            None
        );
    }

    #[test]
    fn retry_context_delay_list_policy_matches_with_automatic_reconnect() {
        let policy = DelayListPolicy::new([Duration::ZERO, Duration::from_millis(500)]);
        let delays: Vec<_> = (0..3)
            .map(|retry| policy.next_retry_delay_with(&context(retry, Duration::ZERO)))
            .collect();
        assert_eq!(
            delays,
            [Some(Duration::ZERO), Some(Duration::from_millis(500)), None]
        );
        assert_eq!(
            DelayListPolicy::default().next_retry_delay_with(&context(0, Duration::ZERO)),
            None
        );
    }
}

/// The negotiated connection id, as `HubConnection.ConnectionId` (R16.C5).
#[cfg(all(test, not(target_arch = "wasm32")))]
mod connection_id {
    use super::ReconnectCoordinator;

    #[test]
    fn connection_id_follows_fresh_connections_and_survives_a_stateful_resume() {
        let coordinator = ReconnectCoordinator::new();
        assert_eq!(coordinator.connection_id(), None, "not started");
        let start = coordinator.start().unwrap();
        assert_eq!(coordinator.connection_id(), None, "connecting");
        assert!(coordinator.complete_connection(start, Some(Some("first".into()))));
        assert_eq!(coordinator.connection_id().as_deref(), Some("first"));

        let resume = coordinator.begin().unwrap();
        assert_eq!(coordinator.connection_id(), None, "reconnecting");
        assert!(coordinator.complete_connection(resume, None));
        assert_eq!(
            coordinator.connection_id().as_deref(),
            Some("first"),
            "a stateful resume keeps the id"
        );

        let fresh = coordinator.begin().unwrap();
        assert!(coordinator.complete_connection(fresh, Some(Some("second".into()))));
        assert_eq!(coordinator.connection_id().as_deref(), Some("second"));

        coordinator.local_close();
        assert_eq!(coordinator.connection_id(), None, "stopped");
    }

    #[test]
    fn connection_id_is_none_without_negotiation_and_after_a_superseded_attempt() {
        let coordinator = ReconnectCoordinator::new();
        let start = coordinator.start().unwrap();
        assert!(coordinator.complete_connection(start, Some(None)));
        assert_eq!(
            coordinator.connection_id(),
            None,
            "skip_negotiation has no id"
        );

        let stale = coordinator.begin().unwrap();
        coordinator.pause().unwrap();
        assert!(!coordinator.complete_connection(stale, Some(Some("stale".into()))));
        assert_eq!(coordinator.connection_id(), None);
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod reconnect_lifecycle {
    use super::{LifecycleState, ReconnectCoordinator};

    #[test]
    fn reconnect_lifecycle_is_single_flight() {
        let coordinator = ReconnectCoordinator::connected();
        let generation = coordinator.begin().unwrap();
        assert_eq!(coordinator.lifecycle(), LifecycleState::Reconnecting);
        assert_eq!(
            coordinator.begin().unwrap_err().kind(),
            crate::ErrorKind::Canceled
        );
        assert!(coordinator.complete(generation));
        assert_eq!(coordinator.lifecycle(), LifecycleState::Connected);
    }

    #[test]
    fn reconnect_lifecycle_rejects_stale_completion_and_local_close() {
        let coordinator = ReconnectCoordinator::connected();
        let stale_generation = coordinator.begin().unwrap();
        coordinator.local_close();
        assert!(!coordinator.complete(stale_generation));
        assert_eq!(coordinator.lifecycle(), LifecycleState::LocalClosed);
        assert_eq!(
            coordinator.begin().unwrap_err().close_reason(),
            Some(crate::CloseReason::Local)
        );
    }

    #[test]
    fn reconnect_lifecycle_can_retry_after_failure() {
        let coordinator = ReconnectCoordinator::connected();
        let generation = coordinator.begin().unwrap();
        coordinator.fail(generation);
        assert_eq!(coordinator.lifecycle(), LifecycleState::Disconnected);
        assert!(coordinator.begin().is_ok());
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod functional_baseline_remote_close {
    use super::{LifecycleState, ReconnectCoordinator};

    #[test]
    fn remote_close_disconnects_and_invalidates_in_flight_reconnects() {
        let coordinator = ReconnectCoordinator::connected();
        let generation = coordinator.begin().unwrap();
        coordinator.remote_close();
        assert_eq!(coordinator.lifecycle(), LifecycleState::Disconnected);
        assert!(!coordinator.complete(generation));
        assert!(
            coordinator.begin().is_ok(),
            "a manual reconnect may still begin"
        );
    }

    #[test]
    fn remote_close_never_reopens_a_local_close() {
        let coordinator = ReconnectCoordinator::connected();
        coordinator.local_close();
        coordinator.remote_close();
        assert_eq!(coordinator.lifecycle(), LifecycleState::LocalClosed);
    }
}
