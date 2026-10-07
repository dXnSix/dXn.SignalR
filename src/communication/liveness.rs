//! Connection liveness shared by both targets: keep-alive Pings, server-timeout detection and
//! the handshake response check. The scheduler works on caller-supplied millisecond timestamps,
//! so each target drives it with its own clock and tests drive it deterministically.

use std::time::Duration;

use crate::{
    error::Error,
    protocol::{
        hub_protocol::HubProtocolKind,
        messages::MessageParser,
        negotiate::{HandshakeResponse, Ping},
    },
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
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            keep_alive_interval: Duration::from_secs(15),
            server_timeout: Duration::from_secs(30),
            handshake_timeout: Duration::from_secs(15),
            // The official .NET client's negotiate bound (its HttpClient timeout).
            connect_timeout: Duration::from_secs(120),
        }
    }
}

impl LivenessConfig {
    /// Rejects zero durations and a server timeout that does not exceed the keep-alive interval.
    pub(crate) fn validate(&self) -> Result<(), Error> {
        for (setting, name, value) in [
            (
                "with_keep_alive_interval",
                "keep-alive interval",
                self.keep_alive_interval,
            ),
            ("with_server_timeout", "server timeout", self.server_timeout),
            (
                "with_handshake_timeout",
                "handshake timeout",
                self.handshake_timeout,
            ),
            (
                "with_connect_timeout",
                "connect timeout",
                self.connect_timeout,
            ),
        ] {
            if value.is_zero() {
                return Err(Error::configuration(
                    setting,
                    format!("the {name} must be greater than zero"),
                ));
            }
        }
        if self.server_timeout <= self.keep_alive_interval {
            return Err(Error::configuration(
                "with_server_timeout",
                format!(
                    "the server timeout ({:?}) must be greater than the keep-alive interval ({:?})",
                    self.server_timeout, self.keep_alive_interval
                ),
            ));
        }
        Ok(())
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
            keep_alive_ms: config.keep_alive_interval.as_millis() as u64,
            server_timeout_ms: config.server_timeout.as_millis() as u64,
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
        HubProtocolKind::Json => SerializedFrame::Text(
            MessageParser::to_json(&Ping::new()).expect("a Ping always serializes"),
        ),
        #[cfg(feature = "messagepack")]
        HubProtocolKind::MessagePack => SerializedFrame::Binary(
            crate::protocol::msgpack::frame_message(&crate::protocol::msgpack::encode_ping()),
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
    let record = &frame[..end];
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
    Ok(&frame[end + 1..])
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
        }
    }

    #[test]
    fn liveness_defaults_match_the_official_clients() {
        let defaults = LivenessConfig::default();
        assert_eq!(defaults.keep_alive_interval, Duration::from_secs(15));
        assert_eq!(defaults.server_timeout, Duration::from_secs(30));
        assert_eq!(defaults.handshake_timeout, Duration::from_secs(15));
        assert_eq!(defaults.connect_timeout, Duration::from_secs(120));
        assert!(defaults.validate().is_ok());
    }

    #[test]
    fn liveness_rejects_zero_durations_and_a_short_server_timeout() {
        assert!(config(0, 30_000).validate().is_err());
        assert!(config(15_000, 0).validate().is_err());
        assert!(config(15_000, 15_000).validate().is_err());
        assert!(config(15_000, 10_000).validate().is_err());
        let mut zero_handshake = config(1_000, 2_000);
        zero_handshake.handshake_timeout = Duration::ZERO;
        assert!(zero_handshake.validate().is_err());
        let mut zero_connect = config(1_000, 2_000);
        zero_connect.connect_timeout = Duration::ZERO;
        let error = zero_connect.validate().unwrap_err();
        assert_eq!(error.setting(), Some("with_connect_timeout"));
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
            SerializedFrame::Text("{\"type\":6}\u{1e}".to_string())
        );
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn liveness_messagepack_ping_is_a_framed_type_6_array() {
        assert_eq!(
            ping_frame(HubProtocolKind::MessagePack),
            SerializedFrame::Binary(vec![0x02, 0x91, 0x06])
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

    /// The host does not resolve, so only validation before any I/O can produce this error.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn liveness_invalid_options_fail_connect_before_io() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = runtime
            .block_on(crate::SignalRClient::connect_with(
                "liveness.invalid",
                "hub",
                |configuration| {
                    configuration.with_keep_alive_interval(Duration::from_secs(30));
                },
            ))
            .err()
            .expect("an invalid liveness configuration must fail connect");
        assert_eq!(error.kind(), crate::ErrorKind::Configuration);
        assert_eq!(error.setting(), Some("with_server_timeout"));
    }

    #[test]
    fn liveness_pings_never_enter_the_stateful_replay_buffer() {
        assert!(!is_trackable(MessageType::Ping));
    }
}
