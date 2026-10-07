//! Connection liveness shared by both targets: keep-alive Pings, server-timeout detection and
//! the handshake response check. The scheduler works on caller-supplied millisecond timestamps,
//! so each target drives it with its own clock and tests drive it deterministically.

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
        parse_handshake_response, ping_frame, LivenessAction, LivenessConfig, LivenessTracker,
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
}
