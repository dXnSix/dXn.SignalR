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
    /// A transport is open and the handshake completed.
    Connected,
    /// A reconnect (automatic, manual or after `resume`) is running.
    Reconnecting,
    /// Paused by `pause`; `resume` reconnects.
    Paused,
    /// The transport is gone and nothing is reconnecting (a manual-mode drop, exhausted
    /// attempts or a server close); `resume` or a manual reconnect may still connect again.
    Disconnected,
    /// Closed locally by `disconnect`; terminal.
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
    /// The client is connected or already reconnecting: nothing to do.
    AlreadyActive,
}

/// The lifecycle state machine. Every reconnect owns a generation; pausing, closing and a
/// terminal server close move to a new generation, so a superseded attempt can never complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Lifecycle {
    state: LifecycleState,
    generation: u64,
}

impl Lifecycle {
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
            LifecycleState::Connected => ConnectionStatus::Connected,
            LifecycleState::Reconnecting => ConnectionStatus::Reconnecting,
            LifecycleState::Paused => ConnectionStatus::Paused,
            LifecycleState::Disconnected => ConnectionStatus::Disconnected,
            LifecycleState::LocalClosed => ConnectionStatus::Closed,
        }
    }

    /// Whether `generation` is the reconnect that currently runs.
    pub(crate) fn owns(&self, generation: u64) -> bool {
        self.state == LifecycleState::Reconnecting && self.generation == generation
    }

    /// Why a reconnect of an older generation stopped.
    pub(crate) fn superseded(&self) -> Error {
        match self.state {
            LifecycleState::LocalClosed => Error::closed_locally(),
            LifecycleState::Paused => Error::paused(),
            _ => Error::canceled("the reconnect attempt was superseded"),
        }
    }

    /// Starts a reconnect (automatic or manual) from a connected or disconnected client.
    pub(crate) fn begin(&mut self) -> Result<u64, Error> {
        match self.state {
            LifecycleState::LocalClosed => Err(Error::closed_locally()),
            LifecycleState::Paused => Err(Error::paused()),
            LifecycleState::Reconnecting => {
                Err(Error::canceled("a reconnect attempt is already running"))
            }
            LifecycleState::Connected | LifecycleState::Disconnected => Ok(self.start()),
        }
    }

    fn start(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.state = LifecycleState::Reconnecting;
        self.generation
    }

    /// Completes the reconnect of `generation`; false when it was superseded.
    pub(crate) fn complete(&mut self, generation: u64) -> bool {
        if !self.owns(generation) {
            return false;
        }
        self.state = LifecycleState::Connected;
        true
    }

    /// Ends the reconnect of `generation` without a connection; false when it was superseded.
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

    /// Records a terminal server close; a later manual reconnect or resume may still begin a new
    /// generation. A paused or locally closed client keeps its state.
    pub(crate) fn remote_close(&mut self) {
        if !matches!(
            self.state,
            LifecycleState::LocalClosed | LifecycleState::Paused
        ) {
            self.generation = self.generation.wrapping_add(1);
            self.state = LifecycleState::Disconnected;
        }
    }

    pub(crate) fn local_close(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.state = LifecycleState::LocalClosed;
    }

    /// Pauses a connected, reconnecting or disconnected client, superseding any running
    /// reconnect. Pausing a paused client is a no-op; a closed client rejects it.
    pub(crate) fn pause(&mut self) -> Result<PauseOutcome, Error> {
        match self.state {
            LifecycleState::LocalClosed => Err(Error::closed_locally()),
            LifecycleState::Paused => Ok(PauseOutcome::AlreadyPaused),
            LifecycleState::Connected
            | LifecycleState::Reconnecting
            | LifecycleState::Disconnected => {
                self.generation = self.generation.wrapping_add(1);
                self.state = LifecycleState::Paused;
                Ok(PauseOutcome::Paused)
            }
        }
    }

    /// Resumes a paused or disconnected client by beginning a reconnect. Resuming a connected or
    /// reconnecting client is a no-op; a closed client rejects it.
    pub(crate) fn resume(&mut self) -> Result<ResumeOutcome, Error> {
        match self.state {
            LifecycleState::LocalClosed => Err(Error::closed_locally()),
            LifecycleState::Connected | LifecycleState::Reconnecting => {
                Ok(ResumeOutcome::AlreadyActive)
            }
            LifecycleState::Paused | LifecycleState::Disconnected => {
                Ok(ResumeOutcome::Started(self.start()))
            }
        }
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
    /// The delay before attempt `retry_count` (0-based), or `None` when the run is over.
    pub(crate) fn delay(
        self,
        policy: &dyn ReconnectionPolicy,
        retry_count: u32,
        elapsed_milliseconds: u64,
    ) -> Option<Duration> {
        if retry_count == 0 && self == FirstAttempt::Always {
            return Some(Duration::ZERO);
        }
        let delay = policy.next_retry_delay(retry_count, elapsed_milliseconds)?;
        if retry_count == 0 && self == FirstAttempt::WithoutDelay {
            return Some(Duration::ZERO);
        }
        Some(delay)
    }
}

/// Serializes reconnect ownership across a native client's clones and tasks.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
pub(crate) struct ReconnectCoordinator {
    state: std::sync::Arc<std::sync::Mutex<Lifecycle>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl ReconnectCoordinator {
    pub(crate) fn connected() -> Self {
        Self {
            state: std::sync::Arc::new(std::sync::Mutex::new(Lifecycle::connected())),
        }
    }

    fn with<R>(&self, change: impl FnOnce(&mut Lifecycle) -> R) -> R {
        use crate::platform::LockExt;
        change(&mut self.state.locked())
    }

    pub(crate) fn begin(&self) -> Result<u64, Error> {
        self.with(Lifecycle::begin)
    }

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

    pub(crate) fn local_close(&self) {
        self.with(Lifecycle::local_close)
    }

    pub(crate) fn pause(&self) -> Result<PauseOutcome, Error> {
        self.with(Lifecycle::pause)
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

/// Defines the behavior for reconnection attempts.
pub trait ReconnectionPolicy: crate::platform::MaybeSendSync {
    /// Returns the delay before the next reconnection attempt.
    /// Returns `None` if no more attempts should be made.
    fn next_retry_delay(&self, retry_count: u32, elapsed_milliseconds: u64) -> Option<Duration>;
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
    pub policy: std::sync::Arc<dyn ReconnectionPolicy>,
}

impl Default for ReconnectionConfig {
    fn default() -> Self {
        Self {
            policy: std::sync::Arc::new(NoReconnectPolicy),
        }
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
            FirstAttempt::Always.delay(&NoReconnectPolicy, 0, 0),
            Some(Duration::ZERO)
        );
        assert_eq!(FirstAttempt::Always.delay(&NoReconnectPolicy, 1, 0), None);
        assert_eq!(
            FirstAttempt::Always.delay(&policy, 1, 0),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            FirstAttempt::WithoutDelay.delay(&NoReconnectPolicy, 0, 0),
            None
        );
        let slow = super::ConstantDelayPolicy::new(Duration::from_secs(5), Some(2));
        assert_eq!(
            FirstAttempt::WithoutDelay.delay(&slow, 0, 0),
            Some(Duration::ZERO)
        );
        assert_eq!(
            FirstAttempt::AfterPolicyDelay.delay(&slow, 0, 0),
            Some(Duration::from_secs(5))
        );
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
