//! Connection liveness shared by both targets: keep-alive Pings, server-timeout detection,
//! suspension detection and the handshake response check. The scheduler works on caller-supplied
//! millisecond timestamps from a [`LivenessClock`], so each target drives it with its own clock
//! sources and tests drive it deterministically.

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

use crate::{
    error::Error,
    protocol::{hub_protocol::HubProtocolKind, negotiate::HandshakeResponse},
};

use super::stateful::SerializedFrame;

/// Keep-alive, server-timeout, handshake-timeout and connection-setup durations (the official
/// clients' defaults).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LivenessConfig {
    pub(crate) keep_alive_interval: Duration,
    pub(crate) server_timeout: Duration,
    pub(crate) handshake_timeout: Duration,
    /// Bounds each negotiate request and opening the transport.
    pub(crate) connect_timeout: Duration,
    /// Bounds how long `stop` waits for the transport to close.
    pub(crate) close_timeout: Duration,
    /// How long the server may hold a Long Polling poll (100 s, the JavaScript client's poll
    /// timeout); each poll is bounded by it plus the connect timeout.
    pub(crate) long_polling_timeout: Duration,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            keep_alive_interval: Duration::from_secs(15),
            server_timeout: Duration::from_secs(30),
            handshake_timeout: Duration::from_secs(15),
            // The official .NET client's negotiate bound (its HttpClient timeout).
            connect_timeout: Duration::from_secs(120),
            // The official .NET client's `HttpConnectionOptions.CloseTimeout`.
            close_timeout: Duration::from_secs(5),
            // The JavaScript client's Long Polling poll timeout.
            long_polling_timeout: Duration::from_secs(100),
        }
    }
}

/// The official .NET client checks its liveness timers once per second, so a zero keep-alive
/// interval or server timeout acts as one second there; it does here too.
const ZERO_TIMER: Duration = Duration::from_secs(1);

fn timer(value: Duration) -> Duration {
    if value.is_zero() {
        ZERO_TIMER
    } else {
        value
    }
}

impl LivenessConfig {
    /// Every duration is accepted, as the official .NET client accepts them; values that cannot
    /// work are reported here and logged as warnings when the client is built.
    pub(crate) fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        for (name, value) in [
            ("keep-alive interval", self.keep_alive_interval),
            ("server timeout", self.server_timeout),
        ] {
            if value.is_zero() {
                warnings.push(format!(
                    "the {name} is zero; it acts as {ZERO_TIMER:?}, the official .NET client's timer tick"
                ));
            }
        }
        for (name, value) in [
            ("handshake timeout", self.handshake_timeout),
            ("connect timeout", self.connect_timeout),
        ] {
            if value.is_zero() {
                warnings.push(format!(
                    "the {name} is zero, so every connection attempt times out at once"
                ));
            }
        }
        let keep_alive = timer(self.keep_alive_interval);
        let server_timeout = timer(self.server_timeout);
        if server_timeout <= keep_alive {
            warnings.push(format!(
                "the server timeout ({server_timeout:?}) is not greater than the keep-alive \
                 interval ({keep_alive:?}), so the connection is considered lost whenever the \
                 server is idle; the server's keep-alive interval should be well below the \
                 client's server timeout (the defaults are 15 s and 30 s)"
            ));
        }
        warnings
    }

    /// Logs the values that cannot work.
    pub(crate) fn warn(&self) {
        for warning in self.warnings() {
            log::warn!("{warning}");
        }
    }
}

/// The clocks a [`LivenessClock`] is built from, in milliseconds.
pub(crate) trait ClockSource {
    /// A monotonic reading. It advances while the process is stopped or the page is frozen, but
    /// may stop while the whole device sleeps (Linux and Android `CLOCK_MONOTONIC`).
    fn monotonic_ms(&self) -> u64;
    /// The wall clock, which includes device sleep but can be adjusted; `None` when unavailable.
    fn wall_ms(&self) -> Option<u64>;
}

/// A wall clock that moved this much further than the monotonic clock between two readings
/// counts as time the monotonic clock missed (device sleep); smaller differences are clock
/// noise and slewing.
const WALL_CLOCK_JUMP_MS: u64 = 1_000;

/// The liveness clock: milliseconds since the clock was created that never go backwards and
/// include time the device slept, as the official .NET client's liveness checks on
/// `DateTime.UtcNow` do. It follows the monotonic clock, and when the wall clock moved more than
/// a second further between two readings, it adds the difference: the monotonic clock stopped
/// while the device slept. A wall clock that moved backwards adds nothing.
#[derive(Debug)]
pub(crate) struct LivenessClock<S> {
    source: S,
    started: u64,
    monotonic: u64,
    wall: Option<u64>,
    /// Sleep the monotonic clock missed.
    slept: u64,
}

impl<S: ClockSource> LivenessClock<S> {
    pub(crate) fn new(source: S) -> Self {
        let monotonic = source.monotonic_ms();
        let wall = source.wall_ms();
        Self {
            source,
            started: monotonic,
            monotonic,
            wall,
            slept: 0,
        }
    }

    /// The current reading.
    pub(crate) fn now(&mut self) -> u64 {
        let monotonic = self.source.monotonic_ms().max(self.monotonic);
        let wall = self.source.wall_ms();
        let monotonic_elapsed = monotonic - self.monotonic;
        if let (Some(wall), Some(previous)) = (wall, self.wall) {
            let wall_elapsed = wall.saturating_sub(previous);
            if wall_elapsed > monotonic_elapsed.saturating_add(WALL_CLOCK_JUMP_MS) {
                self.slept = self.slept.saturating_add(wall_elapsed - monotonic_elapsed);
            }
        }
        self.monotonic = monotonic;
        if wall.is_some() {
            self.wall = wall;
        }
        (monotonic - self.started).saturating_add(self.slept)
    }
}

/// How often the official .NET client checks its liveness timers; the receive loops wake at
/// least this often, so a wake after a suspension is found late at once.
pub(crate) const LIVENESS_TICK: Duration = Duration::from_secs(1);

/// What a liveness wake found (`LivenessTracker::check_wake`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WakeCheck {
    /// The wake came on time, or late by less than a keep-alive interval (scheduling jitter).
    OnTime,
    /// The wake came more than a keep-alive interval late, so the app was suspended, but a frame
    /// arrived within the server timeout: send a Ping at once.
    SuspendedPing,
    /// The wake came more than a keep-alive interval late and nothing arrived for the server
    /// timeout: treat the transport as dropped now.
    SuspendedDropped {
        /// How long the wake came late.
        late_ms: u64,
    },
}

/// What a connection should do now (the browser's single liveness task polls for it; native
/// tasks wait on the individual deadlines).
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LivenessAction {
    /// Nothing is due before `next_deadline` (milliseconds on the caller's clock).
    Idle { next_deadline: u64 },
    /// No frame was sent for a keep-alive interval: send a Ping (then call `on_sent`).
    SendPing,
    /// No frame was received for the server timeout: the transport is dead.
    TimedOut,
}

/// Tracks the last sent and received frame times of one physical connection.
#[derive(Clone, Debug)]
pub(crate) struct LivenessTracker {
    keep_alive_ms: u64,
    server_timeout_ms: u64,
    last_sent: u64,
    last_received: u64,
}

impl LivenessTracker {
    pub(crate) fn new(config: &LivenessConfig, now: u64) -> Self {
        Self {
            keep_alive_ms: timer(config.keep_alive_interval).as_millis() as u64,
            server_timeout_ms: timer(config.server_timeout).as_millis() as u64,
            last_sent: now,
            last_received: now,
        }
    }

    pub(crate) fn on_sent(&mut self, now: u64) {
        self.last_sent = self.last_sent.max(now);
    }

    pub(crate) fn on_received(&mut self, now: u64) {
        self.last_received = self.last_received.max(now);
    }

    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn poll(&self, now: u64) -> LivenessAction {
        let timeout_at = self.last_received.saturating_add(self.server_timeout_ms);
        if now >= timeout_at {
            return LivenessAction::TimedOut;
        }
        let ping_at = self.last_sent.saturating_add(self.keep_alive_ms);
        if now >= ping_at {
            return LivenessAction::SendPing;
        }
        LivenessAction::Idle {
            next_deadline: ping_at.min(timeout_at),
        }
    }

    /// When a Ping is due without other outbound frames.
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn ping_deadline(&self) -> u64 {
        self.last_sent.saturating_add(self.keep_alive_ms)
    }

    /// When the server timeout expires without inbound frames.
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn timeout_deadline(&self) -> u64 {
        self.last_received.saturating_add(self.server_timeout_ms)
    }

    /// Checks a wake scheduled for `scheduled` that ran at `now` (both on the liveness clock):
    /// one more than a keep-alive interval late means the app was suspended (a stopped process,
    /// a frozen page, a sleeping device), so the connection pings at once or, when nothing
    /// arrived for the server timeout, treats the transport as dropped without waiting a further
    /// server timeout.
    pub(crate) fn check_wake(&self, scheduled: u64, now: u64) -> WakeCheck {
        let late_ms = now.saturating_sub(scheduled);
        if late_ms <= self.keep_alive_ms {
            return WakeCheck::OnTime;
        }
        if now >= self.timeout_deadline() {
            WakeCheck::SuspendedDropped { late_ms }
        } else {
            WakeCheck::SuspendedPing
        }
    }
}

/// The error of a suspension found at a wake (`WakeCheck::SuspendedDropped`): the server timeout,
/// as the official .NET client's `OnServerTimeout` reports it when its timer finds the deadline
/// passed, so the reconnection policy runs at once (a stateful connection is not resumed: the
/// server ends it after the same silence). Logs how long the app was suspended.
pub(crate) fn suspended_timeout(late_ms: u64, server_timeout: Duration) -> Error {
    log::warn!(
        "The app was suspended for about {} s and nothing arrived from the server for the server \
         timeout; the connection is considered lost",
        late_ms / 1_000
    );
    Error::timed_out(crate::TimeoutOperation::ServerTimeout, server_timeout)
}

/// The protocol-specific Ping frame: JSON `{"type":6}` record or MessagePack framed `[6]`.
pub(crate) fn ping_frame(protocol: HubProtocolKind) -> SerializedFrame {
    match protocol {
        HubProtocolKind::Json => SerializedFrame::text("{\"type\":6}\u{1e}"),
        #[cfg(feature = "messagepack")]
        HubProtocolKind::MessagePack => SerializedFrame::binary(
            crate::protocol::msgpack::frame_message(&crate::protocol::msgpack::encode_ping()),
        ),
    }
}

/// The SignalR Close message a client sends before it closes the transport, as the official
/// clients' `stop` does: `{"type":7}` or `[7, nil, false]`.
pub(crate) fn close_frame(protocol: HubProtocolKind) -> SerializedFrame {
    match protocol {
        HubProtocolKind::Json => SerializedFrame::text("{\"type\":7}\u{1e}"),
        #[cfg(feature = "messagepack")]
        HubProtocolKind::MessagePack => SerializedFrame::binary(
            crate::protocol::msgpack::frame_message(&crate::protocol::msgpack::encode_close()),
        ),
    }
}

/// Validates the server's handshake response, which is always a JSON record even when the hub
/// protocol is MessagePack (the frame is then binary). An `error` field, a hub message, an empty
/// or a malformed response fails the handshake. Returns the bytes after the response: hub
/// messages the server sent in the same frame, which the receiver must still process.
pub(crate) fn parse_handshake_response(frame: &[u8]) -> Result<&[u8], Error> {
    const SEPARATOR: u8 = 0x1e;
    let invalid =
        || Error::handshake("the SignalR handshake failed: the server sent an invalid response");
    let end = frame
        .iter()
        .position(|byte| *byte == SEPARATOR)
        .ok_or_else(invalid)?;
    let (record, rest) = frame.split_at(end);
    if record.iter().all(u8::is_ascii_whitespace) {
        return Err(Error::handshake(
            "the SignalR handshake failed: the server sent an empty response",
        ));
    }
    let response = serde_json::from_slice::<HandshakeResponse>(record).map_err(|_| invalid())?;
    if let Some(error) = response.error() {
        return Err(Error::handshake(format!(
            "the SignalR handshake failed: {error}"
        )));
    }
    if response.is_hub_message() {
        return Err(Error::handshake(
            "the SignalR handshake failed: expected a handshake response from the server",
        ));
    }
    // `rest` starts with the separator.
    Ok(rest.get(1..).unwrap_or_default())
}

#[cfg(test)]
mod liveness_tests {
    use std::time::Duration;

    use super::{
        parse_handshake_response, ping_frame, ClockSource, LivenessAction, LivenessClock,
        LivenessConfig, LivenessTracker, WakeCheck,
    };
    use crate::communication::stateful::{is_trackable, SerializedFrame};
    use crate::protocol::{hub_protocol::HubProtocolKind, negotiate::MessageType};

    fn config(keep_alive: u64, server_timeout: u64) -> LivenessConfig {
        LivenessConfig {
            keep_alive_interval: Duration::from_millis(keep_alive),
            server_timeout: Duration::from_millis(server_timeout),
            handshake_timeout: Duration::from_millis(1_000),
            connect_timeout: Duration::from_millis(1_000),
            close_timeout: Duration::from_millis(1_000),
            long_polling_timeout: Duration::from_millis(1_000),
        }
    }

    #[test]
    fn liveness_defaults_match_the_official_clients() {
        let defaults = LivenessConfig::default();
        assert_eq!(defaults.keep_alive_interval, Duration::from_secs(15));
        assert_eq!(defaults.server_timeout, Duration::from_secs(30));
        assert_eq!(defaults.handshake_timeout, Duration::from_secs(15));
        assert_eq!(defaults.connect_timeout, Duration::from_secs(120));
        assert_eq!(defaults.close_timeout, Duration::from_secs(5));
        assert_eq!(defaults.long_polling_timeout, Duration::from_secs(100));
    }

    #[test]
    fn liveness_parity_accepts_every_duration_and_warns_about_values_that_cannot_work() {
        assert!(LivenessConfig::default().warnings().is_empty());
        assert!(config(1_000, 2_000).warnings().is_empty());
        let zero_keep_alive = config(0, 30_000).warnings();
        assert_eq!(zero_keep_alive.len(), 1, "{zero_keep_alive:?}");
        assert!(zero_keep_alive[0].contains("keep-alive interval is zero"));
        for short in [
            config(15_000, 15_000),
            config(15_000, 10_000),
            config(15_000, 0),
        ] {
            assert!(
                short
                    .warnings()
                    .iter()
                    .any(|warning| warning.contains("not greater than the keep-alive")),
                "{:?}",
                short.warnings()
            );
        }
        let mut zero_setup = config(1_000, 2_000);
        zero_setup.handshake_timeout = Duration::ZERO;
        zero_setup.connect_timeout = Duration::ZERO;
        assert_eq!(zero_setup.warnings().len(), 2);
    }

    #[test]
    fn liveness_parity_zero_timers_act_as_the_dotnet_timer_tick() {
        let tracker = LivenessTracker::new(&config(0, 0), 0);
        assert!(matches!(tracker.poll(500), LivenessAction::Idle { .. }));
        assert_eq!(tracker.poll(1_000), LivenessAction::TimedOut);
    }

    #[test]
    fn liveness_requests_a_ping_after_an_idle_keep_alive_interval() {
        let mut tracker = LivenessTracker::new(&config(1_000, 3_000), 0);
        assert_eq!(
            tracker.poll(500),
            LivenessAction::Idle {
                next_deadline: 1_000
            }
        );
        assert_eq!(tracker.poll(1_000), LivenessAction::SendPing);
        tracker.on_sent(1_000);
        assert_eq!(
            tracker.poll(1_200),
            LivenessAction::Idle {
                next_deadline: 2_000
            }
        );
    }

    #[test]
    fn liveness_outbound_frames_postpone_the_ping() {
        let mut tracker = LivenessTracker::new(&config(1_000, 3_000), 0);
        tracker.on_sent(900);
        assert_eq!(
            tracker.poll(1_500),
            LivenessAction::Idle {
                next_deadline: 1_900
            }
        );
    }

    #[test]
    fn liveness_times_out_without_inbound_frames_and_inbound_frames_reset_it() {
        let mut tracker = LivenessTracker::new(&config(1_000, 3_000), 0);
        tracker.on_sent(2_500);
        assert_eq!(tracker.poll(3_000), LivenessAction::TimedOut);
        tracker.on_received(2_900);
        assert_ne!(tracker.poll(3_000), LivenessAction::TimedOut);
        assert_eq!(tracker.timeout_deadline(), 5_900);
        assert_eq!(tracker.poll(5_900), LivenessAction::TimedOut);
    }

    /// A response record: the text followed by the record separator.
    fn record(text: &str) -> Vec<u8> {
        let mut bytes = text.as_bytes().to_vec();
        bytes.push(0x1e);
        bytes
    }

    #[test]
    fn liveness_handshake_responses_are_validated() {
        assert_eq!(parse_handshake_response(&record("{}")).ok(), Some(&b""[..]));
        let rejected = parse_handshake_response(&record(
            r#"{"error":"Requested protocol 'x' is not available."}"#,
        ))
        .unwrap_err();
        assert_eq!(rejected.kind(), crate::ErrorKind::Handshake);
        assert!(rejected.to_string().contains("not available"), "{rejected}");
        for invalid in [
            record("not json"),
            record(""),
            Vec::new(),
            b"{}".to_vec(),
            record(r#"{"type":6}"#),
            record("[1,2]"),
        ] {
            assert!(parse_handshake_response(&invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn liveness_handshake_keeps_messages_sent_in_the_same_frame() {
        let mut text = record("{}");
        text.extend(record(r#"{"type":6}"#));
        assert_eq!(
            parse_handshake_response(&text).ok(),
            Some(&record(r#"{"type":6}"#)[..])
        );
        let mut binary = record("{}");
        binary.extend([0x02, 0x91, 0x06]);
        assert_eq!(
            parse_handshake_response(&binary).ok(),
            Some(&[0x02, 0x91, 0x06][..])
        );
    }

    #[test]
    fn liveness_json_ping_is_a_type_6_record() {
        assert_eq!(
            ping_frame(HubProtocolKind::Json),
            SerializedFrame::text("{\"type\":6}\u{1e}")
        );
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn liveness_messagepack_ping_is_a_framed_type_6_array() {
        assert_eq!(
            ping_frame(HubProtocolKind::MessagePack),
            SerializedFrame::binary(vec![0x02, 0x91, 0x06])
        );
    }

    #[test]
    fn liveness_options_reach_the_connection_recipe() {
        let mut configuration =
            crate::client::ConnectionConfiguration::new("localhost".into(), "hub".into());
        configuration.with_keep_alive_interval(Duration::from_secs(1));
        configuration.with_server_timeout(Duration::from_secs(2));
        configuration.with_handshake_timeout(Duration::from_secs(3));
        configuration.with_connect_timeout(Duration::from_secs(4));
        let liveness = configuration.connection_recipe().liveness();
        assert_eq!(liveness.keep_alive_interval, Duration::from_secs(1));
        assert_eq!(liveness.server_timeout, Duration::from_secs(2));
        assert_eq!(liveness.handshake_timeout, Duration::from_secs(3));
        assert_eq!(liveness.connect_timeout, Duration::from_secs(4));
    }

    #[test]
    fn liveness_pings_never_enter_the_stateful_replay_buffer() {
        assert!(!is_trackable(MessageType::Ping));
    }

    /// An injected clock source: the test sets both readings.
    #[derive(Clone, Default)]
    struct TestClock(std::rc::Rc<std::cell::Cell<(u64, Option<u64>)>>);

    impl TestClock {
        fn set(&self, monotonic: u64, wall: Option<u64>) {
            self.0.set((monotonic, wall));
        }
    }

    impl ClockSource for TestClock {
        fn monotonic_ms(&self) -> u64 {
            self.0.get().0
        }

        fn wall_ms(&self) -> Option<u64> {
            self.0.get().1
        }
    }

    fn clock(monotonic: u64, wall: u64) -> (TestClock, LivenessClock<TestClock>) {
        let source = TestClock::default();
        source.set(monotonic, Some(wall));
        let clock = LivenessClock::new(source.clone());
        (source, clock)
    }

    #[test]
    fn suspension_clock_follows_the_monotonic_clock_while_the_device_is_awake() {
        let (source, mut clock) = clock(500, 1_000_000);
        assert_eq!(clock.now(), 0);
        // Wall-clock noise and slewing below a second are ignored, so nothing accumulates.
        for step in 1..=1_000u64 {
            source.set(500 + step * 10, Some(1_000_000 + step * 10 + step % 3));
            assert_eq!(clock.now(), step * 10);
        }
    }

    #[test]
    fn suspension_clock_counts_device_sleep_the_monotonic_clock_missed() {
        let (source, mut clock) = clock(0, 50_000);
        source.set(1_000, Some(51_000));
        assert_eq!(clock.now(), 1_000);
        // The device slept for ten minutes: the monotonic clock stopped, the wall clock did not.
        source.set(1_200, Some(651_200));
        assert_eq!(clock.now(), 601_200);
        source.set(2_200, Some(652_200));
        assert_eq!(clock.now(), 602_200);
    }

    #[test]
    fn suspension_clock_ignores_a_wall_clock_that_moved_backwards_or_is_missing() {
        let (source, mut clock) = clock(0, 50_000);
        source.set(1_000, Some(10_000));
        assert_eq!(clock.now(), 1_000);
        // Measured from the adjusted wall clock on.
        source.set(2_000, Some(11_000));
        assert_eq!(clock.now(), 2_000);
        source.set(3_000, None);
        assert_eq!(clock.now(), 3_000);
        source.set(4_000, Some(12_000));
        assert_eq!(clock.now(), 4_000);
        // A monotonic reading that went backwards never moves the clock back.
        source.set(3_500, Some(12_500));
        assert_eq!(clock.now(), 4_000);
    }

    #[test]
    fn suspension_wakes_late_by_less_than_a_keep_alive_interval_are_jitter() {
        let config = LivenessConfig::default();
        let tracker = LivenessTracker::new(&config, 0);
        assert_eq!(tracker.check_wake(1_000, 1_000), WakeCheck::OnTime);
        assert_eq!(tracker.check_wake(1_000, 16_000), WakeCheck::OnTime);
        assert_eq!(tracker.check_wake(5_000, 1_000), WakeCheck::OnTime);
        // At the default intervals, a connection ticking every second with up to five seconds
        // of scheduling delay never reports one.
        let mut now = 0;
        for tick in 0..600u64 {
            let scheduled = now + 1_000;
            now = scheduled + (tick * 787) % 5_000;
            assert_eq!(tracker.check_wake(scheduled, now), WakeCheck::OnTime);
        }
    }

    #[test]
    fn suspension_pings_at_once_when_a_frame_arrived_within_the_server_timeout() {
        let mut tracker = LivenessTracker::new(&LivenessConfig::default(), 0);
        tracker.on_received(10_000);
        // Scheduled for 11 s, ran at 30 s: 19 s late, 20 s of silence.
        assert_eq!(tracker.check_wake(11_000, 30_000), WakeCheck::SuspendedPing);
    }

    #[test]
    fn suspension_drops_the_transport_at_once_after_the_server_timeout() {
        let mut tracker = LivenessTracker::new(&LivenessConfig::default(), 0);
        tracker.on_received(1_000);
        // Suspended right after a tick for 40 s: the server timeout (30 s) has passed.
        assert_eq!(
            tracker.check_wake(2_000, 42_000),
            WakeCheck::SuspendedDropped { late_ms: 40_000 }
        );
        // Measured on the liveness clock, so device sleep the monotonic clock missed counts.
        let (source, mut clock) = clock(0, 100_000);
        let tracker = LivenessTracker::new(&LivenessConfig::default(), clock.now());
        let scheduled = clock.now() + 1_000;
        source.set(900, Some(3_700_900));
        assert!(matches!(
            tracker.check_wake(scheduled, clock.now()),
            WakeCheck::SuspendedDropped { .. }
        ));
    }

    #[test]
    fn suspension_drops_the_transport_with_the_server_timeout_as_dotnet() {
        let error = super::suspended_timeout(40_000, std::time::Duration::from_secs(30));
        assert_eq!(error.kind(), crate::ErrorKind::Timeout);
        assert_eq!(
            error.timeout_operation(),
            Some(crate::TimeoutOperation::ServerTimeout)
        );
        assert!(error.allows_reconnect(), "the reconnection policy runs");
        assert!(
            !crate::communication::common::resumes_transparently(&error),
            "as .NET, a server timeout is not resumed"
        );
    }

    #[test]
    fn suspension_is_measured_from_when_the_wake_was_scheduled() {
        // A deadline already due when the wait began (a deferred stateful Ack) is not lateness:
        // the loop measures from the later of the deadline and the time it started waiting.
        let mut tracker = LivenessTracker::new(&LivenessConfig::default(), 0);
        tracker.on_received(40_000);
        let ack_due = 1_000;
        let waited_from = 41_000;
        assert_eq!(
            tracker.check_wake(ack_due.max(waited_from), 41_001),
            WakeCheck::OnTime
        );
        assert_eq!(
            tracker.check_wake(ack_due, 41_001),
            WakeCheck::SuspendedPing
        );
    }
}
