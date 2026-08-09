use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleState {
    Disconnected,
    Connected,
    Reconnecting,
    LocalClosed,
}

#[derive(Clone, Copy, Debug)]
struct CoordinatorState {
    lifecycle: LifecycleState,
    generation: u64,
}

/// Serializes reconnect ownership and rejects late results from superseded attempts.
#[derive(Clone, Debug)]
pub(crate) struct ReconnectCoordinator {
    state: std::sync::Arc<std::sync::Mutex<CoordinatorState>>,
}

impl ReconnectCoordinator {
    pub(crate) fn connected() -> Self {
        Self {
            state: std::sync::Arc::new(std::sync::Mutex::new(CoordinatorState {
                lifecycle: LifecycleState::Connected,
                generation: 0,
            })),
        }
    }

    pub(crate) fn begin(&self) -> Result<u64, &'static str> {
        let mut state = self.state.lock().expect("reconnect coordinator poisoned");
        match state.lifecycle {
            LifecycleState::LocalClosed => Err("Client was locally disconnected"),
            LifecycleState::Reconnecting => Err("A reconnect attempt is already running"),
            LifecycleState::Connected | LifecycleState::Disconnected => {
                state.generation = state.generation.wrapping_add(1);
                state.lifecycle = LifecycleState::Reconnecting;
                Ok(state.generation)
            }
        }
    }

    pub(crate) fn complete(&self, generation: u64) -> bool {
        let mut state = self.state.lock().expect("reconnect coordinator poisoned");
        if state.lifecycle != LifecycleState::Reconnecting || state.generation != generation {
            return false;
        }
        state.lifecycle = LifecycleState::Connected;
        true
    }

    pub(crate) fn fail(&self, generation: u64) {
        let mut state = self.state.lock().expect("reconnect coordinator poisoned");
        if state.lifecycle == LifecycleState::Reconnecting && state.generation == generation {
            state.lifecycle = LifecycleState::Disconnected;
        }
    }

    pub(crate) fn local_close(&self) {
        let mut state = self.state.lock().expect("reconnect coordinator poisoned");
        state.generation = state.generation.wrapping_add(1);
        state.lifecycle = LifecycleState::LocalClosed;
    }

    pub(crate) fn lifecycle(&self) -> LifecycleState {
        self.state
            .lock()
            .expect("reconnect coordinator poisoned")
            .lifecycle
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

#[cfg(test)]
mod reconnect_lifecycle {
    use super::{LifecycleState, ReconnectCoordinator};

    #[test]
    fn reconnect_lifecycle_is_single_flight() {
        let coordinator = ReconnectCoordinator::connected();
        let generation = coordinator.begin().unwrap();
        assert_eq!(coordinator.lifecycle(), LifecycleState::Reconnecting);
        assert_eq!(
            coordinator.begin(),
            Err("A reconnect attempt is already running")
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
        assert_eq!(coordinator.begin(), Err("Client was locally disconnected"));
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
