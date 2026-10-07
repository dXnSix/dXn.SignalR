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

use std::collections::VecDeque;

use log::{debug, trace};

use crate::{error::Error, protocol::negotiate::MessageType};

/// The log target of the stateful sequencing records: tracked and received sequence ids with their
/// decisions, Acks, Sequence messages and resend ranges at `debug`, each tagged with the physical
/// connection's generation (`StatefulMessageBuffer::transport_generation`). They never contain
/// payloads.
pub(crate) const TRACE_TARGET: &str = "rayx_signalr::stateful";

pub(crate) use crate::protocol::codec::SerializedFrame;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BufferedFrame {
    pub sequence_id: u64,
    pub payload: SerializedFrame,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TrackResult {
    Buffered(u64),
    Bypass,
    Backpressured,
}

/// Protocol-independent model for SignalR's bounded serialized-frame replay state.
#[derive(Debug)]
pub(crate) struct StatefulMessageBuffer {
    capacity: usize,
    buffered_bytes: usize,
    frames: VecDeque<BufferedFrame>,
    next_outgoing: u64,
    last_acknowledged: u64,
    next_incoming: u64,
    highest_incoming: u64,
    last_acknowledgement_sent: u64,
    awaiting_sequence: bool,
    resending: bool,
    disposed: bool,
    /// Incremented when the physical transport is lost or the buffer is disposed; a writer of an
    /// older transport can no longer track frames.
    transport_generation: u64,
}

/// Returned when a writer of a lost transport tries to track a frame.
#[cfg(any(not(target_arch = "wasm32"), test))]
pub(crate) const STALE_TRANSPORT: &str = "the connection was lost before the message was sent";

/// The configuration setting that sizes the replay buffer.
const BUFFER_SETTING: &str = "StatefulReconnectConfig::buffer_size_bytes";

fn overflow(counter: &str) -> Error {
    Error::protocol(format!(
        "the {counter} stateful sequence counter overflowed"
    ))
}

impl StatefulMessageBuffer {
    pub(crate) fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::configuration(
                BUFFER_SETTING,
                "the stateful reconnect buffer size must be greater than zero",
            ));
        }
        Ok(Self::empty(capacity))
    }

    fn empty(capacity: usize) -> Self {
        Self {
            capacity,
            buffered_bytes: 0,
            frames: VecDeque::new(),
            next_outgoing: 1,
            last_acknowledged: 0,
            next_incoming: 1,
            highest_incoming: 0,
            last_acknowledgement_sent: 0,
            awaiting_sequence: false,
            resending: false,
            disposed: false,
            transport_generation: 0,
        }
    }

    /// The generation of the current physical transport.
    #[cfg(any(not(target_arch = "wasm32"), test))]
    pub(crate) fn transport_generation(&self) -> u64 {
        self.transport_generation
    }

    /// Tracks a frame for the transport of `generation`; frames of a lost transport are rejected
    /// so they never take a sequence number the resumed connection did not send.
    #[cfg(any(not(target_arch = "wasm32"), test))]
    pub(crate) fn track_for(
        &mut self,
        generation: u64,
        message_type: MessageType,
        payload: SerializedFrame,
    ) -> Result<TrackResult, Error> {
        if generation != self.transport_generation {
            return Err(Error::transport_failure(
                Some(crate::communication::transport::TransportKind::WebSockets),
                STALE_TRANSPORT,
            ));
        }
        self.track(message_type, payload)
    }

    pub(crate) fn track(
        &mut self,
        message_type: MessageType,
        payload: SerializedFrame,
    ) -> Result<TrackResult, Error> {
        self.ensure_active()?;
        if !is_trackable(message_type) {
            return Ok(TrackResult::Bypass);
        }
        let bytes = payload.len();
        if bytes > self.capacity {
            return Err(Error::configuration(
                BUFFER_SETTING,
                format!(
                    "a {bytes}-byte message exceeds the {}-byte stateful reconnect buffer",
                    self.capacity
                ),
            ));
        }
        if self.resending || self.buffered_bytes + bytes > self.capacity {
            trace!(
                target: TRACE_TARGET,
                "generation={} backpressure bytes={bytes} buffered={} capacity={} resending={}",
                self.transport_generation,
                self.buffered_bytes,
                self.capacity,
                self.resending
            );
            return Ok(TrackResult::Backpressured);
        }
        let sequence_id = self.next_outgoing;
        self.next_outgoing = self
            .next_outgoing
            .checked_add(1)
            .ok_or_else(|| overflow("outgoing"))?;
        self.buffered_bytes += bytes;
        self.frames.push_back(BufferedFrame {
            sequence_id,
            payload,
        });
        debug!(
            target: TRACE_TARGET,
            "generation={} track sequence={sequence_id} type={message_type:?} bytes={bytes}",
            self.transport_generation
        );
        Ok(TrackResult::Buffered(sequence_id))
    }

    /// Applies a received Ack or Sequence: `sequence_id` is its decoded id, or the decode error of
    /// a malformed control message. Every rejection, including an id of 0, is recorded.
    pub(crate) fn receive_control(
        &mut self,
        message_type: MessageType,
        sequence_id: Result<u64, Error>,
    ) -> Result<(), Error> {
        let name = match message_type {
            MessageType::Ack => "ack",
            MessageType::Sequence => "sequence",
            _ => return Ok(()),
        };
        let sequence_id = match sequence_id {
            Ok(sequence_id) => sequence_id,
            Err(error) => {
                debug!(
                    target: TRACE_TARGET,
                    "generation={} {name}-rejected reason=malformed",
                    self.transport_generation
                );
                return Err(error);
            }
        };
        if message_type == MessageType::Ack {
            self.acknowledge(sequence_id).map(|_| ())
        } else {
            self.reset_incoming_sequence(sequence_id)
        }
    }

    pub(crate) fn acknowledge(&mut self, sequence_id: u64) -> Result<usize, Error> {
        self.ensure_active()?;
        if let Err(error) = crate::protocol::stateful::validate_sequence_id(sequence_id) {
            return Err(self.rejected("ack", sequence_id, "zero", error));
        }
        if sequence_id >= self.next_outgoing {
            let error = Error::protocol(format!(
                "Ack {sequence_id} is beyond the highest sent sequence {}",
                self.next_outgoing - 1
            ));
            return Err(self.rejected("ack", sequence_id, "beyond-highest-sent", error));
        }
        if sequence_id <= self.last_acknowledged {
            debug!(
                target: TRACE_TARGET,
                "generation={} ack-received sequence={sequence_id} already-acknowledged={}",
                self.transport_generation,
                self.last_acknowledged
            );
            return Ok(0);
        }
        self.last_acknowledged = sequence_id;
        let mut released = 0;
        let mut released_frames = 0;
        while self
            .frames
            .front()
            .is_some_and(|frame| frame.sequence_id <= sequence_id)
        {
            if let Some(frame) = self.frames.pop_front() {
                let bytes = frame.payload.len();
                self.buffered_bytes -= bytes;
                released += bytes;
                released_frames += 1;
            }
        }
        debug!(
            target: TRACE_TARGET,
            "generation={} ack-received sequence={sequence_id} released={released_frames} buffered={}",
            self.transport_generation,
            self.frames.len()
        );
        Ok(released)
    }

    pub(crate) fn disconnected(&mut self) -> Result<(), Error> {
        self.ensure_active()?;
        debug!(
            target: TRACE_TARGET,
            "generation={} transport-lost unacknowledged={} highest-received={}",
            self.transport_generation,
            self.frames.len(),
            self.highest_incoming
        );
        self.transport_generation = self.transport_generation.wrapping_add(1);
        self.awaiting_sequence = true;
        // The final Ack may have been lost with the transport. Allow the
        // resumed connection to acknowledge the cumulative high-water mark.
        self.last_acknowledgement_sent = 0;
        Ok(())
    }

    pub(crate) fn reset_incoming_sequence(&mut self, sequence_id: u64) -> Result<(), Error> {
        self.ensure_active()?;
        if let Err(error) = crate::protocol::stateful::validate_sequence_id(sequence_id) {
            return Err(self.rejected("sequence", sequence_id, "zero", error));
        }
        if !self.awaiting_sequence {
            let error = Error::protocol("a Sequence message arrived while the transport is active");
            return Err(self.rejected("sequence", sequence_id, "transport-active", error));
        }
        let maximum = self
            .highest_incoming
            .checked_add(1)
            .ok_or_else(|| overflow("incoming"))?;
        if sequence_id > maximum {
            let error = Error::protocol(format!(
                "Sequence {sequence_id} leaves a gap after {maximum}"
            ));
            return Err(self.rejected("sequence", sequence_id, "gap", error));
        }
        debug!(
            target: TRACE_TARGET,
            "generation={} sequence-received sequence={sequence_id} highest-received={}",
            self.transport_generation,
            self.highest_incoming
        );
        self.next_incoming = sequence_id;
        self.awaiting_sequence = false;
        Ok(())
    }

    pub(crate) fn should_process(&mut self, message_type: MessageType) -> Result<bool, Error> {
        self.ensure_active()?;
        if !is_trackable(message_type) {
            return Ok(true);
        }
        if self.awaiting_sequence {
            debug!(
                target: TRACE_TARGET,
                "generation={} receive-rejected type={message_type:?} reason=before-sequence",
                self.transport_generation
            );
            return Err(Error::protocol(
                "a message arrived before the Sequence message after a resume",
            ));
        }
        let sequence_id = self.next_incoming;
        self.next_incoming = self
            .next_incoming
            .checked_add(1)
            .ok_or_else(|| overflow("incoming"))?;
        if sequence_id <= self.highest_incoming {
            debug!(
                target: TRACE_TARGET,
                "generation={} receive sequence={sequence_id} type={message_type:?} duplicate \
                 highest-received={}",
                self.transport_generation,
                self.highest_incoming
            );
            return Ok(false);
        }
        self.highest_incoming = sequence_id;
        debug!(
            target: TRACE_TARGET,
            "generation={} receive sequence={sequence_id} type={message_type:?} accepted",
            self.transport_generation
        );
        Ok(true)
    }

    pub(crate) fn take_acknowledgement(&mut self) -> Result<Option<u64>, Error> {
        self.ensure_active()?;
        if self.highest_incoming == 0 || self.highest_incoming <= self.last_acknowledgement_sent {
            return Ok(None);
        }
        self.last_acknowledgement_sent = self.highest_incoming;
        debug!(
            target: TRACE_TARGET,
            "generation={} ack-sent sequence={}",
            self.transport_generation,
            self.highest_incoming
        );
        Ok(Some(self.highest_incoming))
    }

    pub(crate) fn begin_resend(&mut self) -> Result<(u64, Vec<BufferedFrame>), Error> {
        self.ensure_active()?;
        self.resending = true;
        let sequence_id = self
            .frames
            .front()
            .map_or(self.next_outgoing, |frame| frame.sequence_id);
        debug!(
            target: TRACE_TARGET,
            "generation={} sequence-sent sequence={sequence_id}",
            self.transport_generation
        );
        match (self.frames.front(), self.frames.back()) {
            (Some(first), Some(last)) => debug!(
                target: TRACE_TARGET,
                "generation={} resend first={} last={} frames={} bytes={}",
                self.transport_generation,
                first.sequence_id,
                last.sequence_id,
                self.frames.len(),
                self.buffered_bytes
            ),
            _ => debug!(
                target: TRACE_TARGET,
                "generation={} resend frames=0",
                self.transport_generation
            ),
        }
        Ok((sequence_id, self.frames.iter().cloned().collect()))
    }

    /// Ends the resend that `begin_resend` started; `completed` is false when sending the
    /// Sequence message or a buffered frame failed (the frames stay buffered for the next resume).
    pub(crate) fn finish_resend(&mut self, completed: bool) -> Result<(), Error> {
        self.ensure_active()?;
        self.resending = false;
        if completed {
            debug!(
                target: TRACE_TARGET,
                "generation={} resend-finished",
                self.transport_generation
            );
        } else {
            debug!(
                target: TRACE_TARGET,
                "generation={} resend-failed",
                self.transport_generation
            );
        }
        Ok(())
    }

    /// Records a rejected Ack or Sequence (`message` names it) and returns its error.
    fn rejected(&self, message: &str, sequence_id: u64, reason: &str, error: Error) -> Error {
        debug!(
            target: TRACE_TARGET,
            "generation={} {message}-rejected sequence={sequence_id} reason={reason}",
            self.transport_generation
        );
        error
    }

    /// Whether the logical connection ended; only a fresh negotiation (`renew`) can follow.
    pub(crate) fn is_disposed(&self) -> bool {
        self.disposed
    }

    /// Starts over for a freshly negotiated logical connection: empty, active, and on a new
    /// transport generation so writers of the old connection can never track frames.
    pub(crate) fn renew(&mut self) {
        let transport_generation = self.transport_generation.wrapping_add(1);
        *self = Self::empty(self.capacity);
        self.transport_generation = transport_generation;
        debug!(
            target: TRACE_TARGET,
            "generation={transport_generation} renewed for a fresh logical connection"
        );
    }

    pub(crate) fn dispose(&mut self) {
        debug!(
            target: TRACE_TARGET,
            "generation={} disposed unacknowledged={}",
            self.transport_generation,
            self.frames.len()
        );
        self.transport_generation = self.transport_generation.wrapping_add(1);
        self.disposed = true;
        self.resending = false;
        self.awaiting_sequence = false;
        self.last_acknowledgement_sent = 0;
        self.frames.clear();
        self.buffered_bytes = 0;
    }

    #[cfg(test)]
    pub(crate) fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    fn ensure_active(&self) -> Result<(), Error> {
        if self.disposed {
            Err(Error::canceled("the stateful connection has ended"))
        } else {
            Ok(())
        }
    }
}

pub(crate) fn is_trackable(message_type: MessageType) -> bool {
    matches!(
        message_type,
        MessageType::Invocation
            | MessageType::StreamItem
            | MessageType::Completion
            | MessageType::StreamInvocation
            | MessageType::CancelInvocation
    )
}

#[cfg(test)]
mod stateful_buffer_tests {
    use super::{SerializedFrame, StatefulMessageBuffer, TrackResult};
    use crate::protocol::negotiate::MessageType;

    fn text(value: &str) -> SerializedFrame {
        SerializedFrame::text(value)
    }

    #[test]
    fn stateful_buffer_is_bounded_and_ack_releases_capacity() {
        let mut buffer = StatefulMessageBuffer::new(6).unwrap();
        assert_eq!(
            buffer.track(MessageType::Invocation, text("abc")).unwrap(),
            TrackResult::Buffered(1)
        );
        assert_eq!(
            buffer.track(MessageType::Completion, text("def")).unwrap(),
            TrackResult::Buffered(2)
        );
        assert_eq!(
            buffer.track(MessageType::Invocation, text("x")).unwrap(),
            TrackResult::Backpressured
        );
        assert_eq!(buffer.acknowledge(1).unwrap(), 3);
        assert_eq!(buffer.buffered_bytes(), 3);
        assert_eq!(
            buffer.track(MessageType::Invocation, text("x")).unwrap(),
            TrackResult::Buffered(3)
        );
    }

    #[test]
    fn stateful_buffer_bypasses_control_and_rejects_oversized_frames() {
        let mut buffer = StatefulMessageBuffer::new(3).unwrap();
        assert_eq!(
            buffer.track(MessageType::Ack, text("control")).unwrap(),
            TrackResult::Bypass
        );
        assert!(buffer.track(MessageType::Invocation, text("four")).is_err());
    }

    #[test]
    fn stateful_resume_replays_in_order_and_blocks_new_sends() {
        let mut buffer = StatefulMessageBuffer::new(10).unwrap();
        buffer.track(MessageType::Invocation, text("a")).unwrap();
        buffer.track(MessageType::Invocation, text("b")).unwrap();
        let (sequence, frames) = buffer.begin_resend().unwrap();
        assert_eq!(sequence, 1);
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.sequence_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            buffer.track(MessageType::Invocation, text("c")).unwrap(),
            TrackResult::Backpressured
        );
        buffer.finish_resend(true).unwrap();
    }

    #[test]
    fn stateful_incoming_sequence_suppresses_duplicates_and_rejects_gaps() {
        let mut buffer = StatefulMessageBuffer::new(10).unwrap();
        assert!(buffer.should_process(MessageType::Invocation).unwrap());
        assert!(buffer.should_process(MessageType::Invocation).unwrap());
        buffer.disconnected().unwrap();
        assert!(buffer.reset_incoming_sequence(4).is_err());
        buffer.reset_incoming_sequence(2).unwrap();
        assert!(!buffer.should_process(MessageType::Invocation).unwrap());
        assert!(buffer.should_process(MessageType::Invocation).unwrap());
    }

    #[test]
    fn stateful_invalid_ack_and_terminal_cleanup_fail_closed() {
        let mut buffer = StatefulMessageBuffer::new(10).unwrap();
        buffer.track(MessageType::Invocation, text("a")).unwrap();
        assert!(buffer.acknowledge(2).is_err());
        buffer.dispose();
        assert_eq!(buffer.buffered_bytes(), 0);
        assert!(buffer.track(MessageType::Invocation, text("b")).is_err());
    }

    #[test]
    fn stateful_resend_snapshot_survives_concurrent_ack() {
        let mut buffer = StatefulMessageBuffer::new(10).unwrap();
        buffer.track(MessageType::Invocation, text("a")).unwrap();
        buffer.track(MessageType::Invocation, text("b")).unwrap();
        let (_, snapshot) = buffer.begin_resend().unwrap();
        assert_eq!(buffer.acknowledge(2).unwrap(), 2);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(buffer.buffered_bytes(), 0);
    }

    #[test]
    fn stateful_sequence_counters_fail_on_overflow() {
        let mut buffer = StatefulMessageBuffer::new(10).unwrap();
        buffer.next_outgoing = u64::MAX;
        assert!(buffer.track(MessageType::Invocation, text("a")).is_err());

        let mut buffer = StatefulMessageBuffer::new(10).unwrap();
        buffer.next_incoming = u64::MAX;
        assert!(buffer.should_process(MessageType::Invocation).is_err());
    }

    #[test]
    fn production_stateful_protocol_repairs_emit_cumulative_ack() {
        let mut buffer = StatefulMessageBuffer::new(10).unwrap();
        assert_eq!(buffer.take_acknowledgement().unwrap(), None);

        assert!(buffer.should_process(MessageType::Invocation).unwrap());
        assert_eq!(buffer.take_acknowledgement().unwrap(), Some(1));
        assert_eq!(buffer.take_acknowledgement().unwrap(), None);

        assert!(buffer.should_process(MessageType::StreamItem).unwrap());
        assert_eq!(buffer.take_acknowledgement().unwrap(), Some(2));

        buffer.disconnected().unwrap();
        assert_eq!(buffer.take_acknowledgement().unwrap(), Some(2));
    }

    #[test]
    fn stateful_configuration_rejects_zero_and_defaults_to_100000() {
        assert_eq!(
            crate::StatefulReconnectConfig::default().buffer_size_bytes,
            100_000
        );
        assert!(crate::StatefulReconnectConfig::new(0).is_err());
    }
}

#[cfg(test)]
mod stateful_transport_generation {
    use super::{SerializedFrame, StatefulMessageBuffer, TrackResult, STALE_TRANSPORT};
    use crate::protocol::negotiate::MessageType;

    fn frame() -> SerializedFrame {
        SerializedFrame::text("{\"type\":1}\u{1e}")
    }

    #[test]
    fn writers_of_a_lost_transport_cannot_track_frames() {
        let mut buffer = StatefulMessageBuffer::new(1_000).unwrap();
        let first = buffer.transport_generation();
        assert!(matches!(
            buffer.track_for(first, MessageType::Invocation, frame()),
            Ok(TrackResult::Buffered(1))
        ));
        buffer.disconnected().unwrap();
        let before = buffer.buffered_bytes();
        let stale = buffer
            .track_for(first, MessageType::Invocation, frame())
            .unwrap_err();
        assert_eq!(stale.kind(), crate::ErrorKind::Transport);
        assert_eq!(stale.to_string(), STALE_TRANSPORT);
        assert_eq!(
            buffer.buffered_bytes(),
            before,
            "a stale frame is never buffered"
        );
    }

    #[test]
    fn the_resumed_transport_tracks_the_next_sequence_while_the_old_one_stays_rejected() {
        let mut buffer = StatefulMessageBuffer::new(1_000).unwrap();
        let first = buffer.transport_generation();
        buffer
            .track_for(first, MessageType::Invocation, frame())
            .unwrap();
        buffer.disconnected().unwrap();
        let (_, replay) = buffer.begin_resend().unwrap();
        assert_eq!(replay.len(), 1);
        buffer.finish_resend(true).unwrap();
        let resumed = buffer.transport_generation();
        assert_ne!(resumed, first);
        assert!(matches!(
            buffer.track_for(resumed, MessageType::Invocation, frame()),
            Ok(TrackResult::Buffered(2))
        ));
        assert!(buffer
            .track_for(first, MessageType::Invocation, frame())
            .is_err());
    }

    #[test]
    fn disposing_invalidates_every_writer() {
        let mut buffer = StatefulMessageBuffer::new(1_000).unwrap();
        let generation = buffer.transport_generation();
        buffer.dispose();
        assert!(buffer
            .track_for(generation, MessageType::Invocation, frame())
            .is_err());
    }
}

#[cfg(test)]
mod sequence_trace {
    use super::{SerializedFrame, StatefulMessageBuffer, TrackResult, TRACE_TARGET};
    use crate::protocol::negotiate::MessageType;
    use crate::test_logging::{capture, CapturedRecord};

    /// Payload text that must never reach a sequencing record.
    const MARKER: &str = "sequence-trace-payload-marker";

    fn frame() -> SerializedFrame {
        SerializedFrame::text(format!("{{\"type\":1,\"target\":\"{MARKER}\"}}\u{1e}"))
    }

    /// The sequencing messages, after checking every record's level, target and payload freedom.
    fn sequencing(records: &[CapturedRecord]) -> Vec<String> {
        let records: Vec<_> = records
            .iter()
            .filter(|record| record.target == TRACE_TARGET)
            .collect();
        for record in &records {
            assert_eq!(record.level, log::Level::Debug, "{}", record.message);
            assert!(!record.message.contains(MARKER), "{}", record.message);
        }
        records
            .iter()
            .map(|record| record.message.clone())
            .collect()
    }

    fn assert_contains(records: &[String], expected: &str) {
        assert!(
            records.iter().any(|record| record == expected),
            "missing `{expected}` in {records:#?}"
        );
    }

    #[test]
    fn sequence_trace_records_tracked_frames_and_received_acks() {
        let (_, records) = capture(|| {
            let mut buffer = StatefulMessageBuffer::new(10_000).unwrap();
            buffer.track(MessageType::Invocation, frame()).unwrap();
            buffer.track(MessageType::Completion, frame()).unwrap();
            assert_eq!(
                buffer.track(MessageType::Ping, frame()).unwrap(),
                TrackResult::Bypass
            );
            buffer.acknowledge(1).unwrap();
            buffer.acknowledge(1).unwrap();
        });
        let records = sequencing(&records);
        let bytes = frame().len();
        assert_eq!(
            records,
            vec![
                format!("generation=0 track sequence=1 type=Invocation bytes={bytes}"),
                format!("generation=0 track sequence=2 type=Completion bytes={bytes}"),
                "generation=0 ack-received sequence=1 released=1 buffered=1".to_string(),
                "generation=0 ack-received sequence=1 already-acknowledged=1".to_string(),
            ]
        );
    }

    #[test]
    fn sequence_trace_records_received_ids_with_their_decisions_and_the_acks_sent() {
        let (_, records) = capture(|| {
            let mut buffer = StatefulMessageBuffer::new(10_000).unwrap();
            assert!(buffer.should_process(MessageType::Invocation).unwrap());
            assert!(buffer.should_process(MessageType::StreamItem).unwrap());
            assert!(buffer.should_process(MessageType::Ping).unwrap());
            assert_eq!(buffer.take_acknowledgement().unwrap(), Some(2));
            buffer.disconnected().unwrap();
            buffer.reset_incoming_sequence(2).unwrap();
            assert!(!buffer.should_process(MessageType::StreamItem).unwrap());
            assert!(buffer.should_process(MessageType::Completion).unwrap());
            assert_eq!(buffer.take_acknowledgement().unwrap(), Some(3));
        });
        let records = sequencing(&records);
        assert_eq!(
            records,
            vec![
                "generation=0 receive sequence=1 type=Invocation accepted",
                "generation=0 receive sequence=2 type=StreamItem accepted",
                "generation=0 ack-sent sequence=2",
                "generation=0 transport-lost unacknowledged=0 highest-received=2",
                "generation=1 sequence-received sequence=2 highest-received=2",
                "generation=1 receive sequence=2 type=StreamItem duplicate highest-received=2",
                "generation=1 receive sequence=3 type=Completion accepted",
                "generation=1 ack-sent sequence=3",
            ]
        );
    }

    #[test]
    fn sequence_trace_records_a_scripted_resume() {
        let (_, records) = capture(|| {
            let mut buffer = StatefulMessageBuffer::new(10_000).unwrap();
            for _ in 0..3 {
                buffer.track(MessageType::Invocation, frame()).unwrap();
            }
            assert!(buffer.should_process(MessageType::Completion).unwrap());
            buffer.take_acknowledgement().unwrap();
            buffer.acknowledge(1).unwrap();
            buffer.disconnected().unwrap();

            // The resumed transport: Sequence and the unacknowledged frames go out first.
            let (sequence, frames) = buffer.begin_resend().unwrap();
            assert_eq!(sequence, 2);
            assert_eq!(frames.len(), 2);
            buffer.finish_resend(true).unwrap();
            buffer.reset_incoming_sequence(1).unwrap();
            assert!(!buffer.should_process(MessageType::Completion).unwrap());
            buffer.acknowledge(3).unwrap();

            // An empty resend still announces where the next sequence starts.
            buffer.disconnected().unwrap();
            assert_eq!(buffer.begin_resend().unwrap().0, 4);
            buffer.finish_resend(true).unwrap();
            buffer.dispose();
            buffer.renew();
        });
        let records = sequencing(&records);
        let bytes = frame().len();
        for expected in [
            format!("generation=0 track sequence=3 type=Invocation bytes={bytes}"),
            "generation=0 receive sequence=1 type=Completion accepted".to_string(),
            "generation=0 ack-sent sequence=1".to_string(),
            "generation=0 ack-received sequence=1 released=1 buffered=2".to_string(),
            "generation=0 transport-lost unacknowledged=2 highest-received=1".to_string(),
            "generation=1 sequence-sent sequence=2".to_string(),
            format!(
                "generation=1 resend first=2 last=3 frames=2 bytes={}",
                2 * bytes
            ),
            "generation=1 resend-finished".to_string(),
            "generation=1 sequence-received sequence=1 highest-received=1".to_string(),
            "generation=1 receive sequence=1 type=Completion duplicate highest-received=1"
                .to_string(),
            "generation=1 ack-received sequence=3 released=2 buffered=0".to_string(),
            "generation=1 transport-lost unacknowledged=0 highest-received=1".to_string(),
            "generation=2 sequence-sent sequence=4".to_string(),
            "generation=2 resend frames=0".to_string(),
            "generation=2 disposed unacknowledged=0".to_string(),
            "generation=4 renewed for a fresh logical connection".to_string(),
        ] {
            assert_contains(&records, &expected);
        }
    }

    #[test]
    fn sequence_trace_records_rejected_acks_and_sequences() {
        let (_, records) = capture(|| {
            let mut buffer = StatefulMessageBuffer::new(10_000).unwrap();
            buffer.track(MessageType::Invocation, frame()).unwrap();
            assert!(buffer.acknowledge(0).is_err());
            assert!(buffer.acknowledge(2).is_err());
            assert!(buffer.reset_incoming_sequence(1).is_err());
            buffer.disconnected().unwrap();
            assert!(buffer.should_process(MessageType::Invocation).is_err());
            assert!(buffer.reset_incoming_sequence(0).is_err());
            assert!(buffer.reset_incoming_sequence(5).is_err());
        });
        let records = sequencing(&records);
        for expected in [
            "generation=0 ack-rejected sequence=0 reason=zero",
            "generation=0 ack-rejected sequence=2 reason=beyond-highest-sent",
            "generation=0 sequence-rejected sequence=1 reason=transport-active",
            "generation=1 receive-rejected type=Invocation reason=before-sequence",
            "generation=1 sequence-rejected sequence=0 reason=zero",
            "generation=1 sequence-rejected sequence=5 reason=gap",
        ] {
            assert_contains(&records, expected);
        }
    }

    #[test]
    fn sequence_trace_records_rejected_control_messages_from_the_wire() {
        use crate::protocol::stateful::decode_json_control;

        let (_, records) = capture(|| {
            let mut buffer = StatefulMessageBuffer::new(10_000).unwrap();
            let mut receive = |message_type, decoded| {
                assert!(buffer.receive_control(message_type, decoded).is_err());
            };
            receive(
                MessageType::Ack,
                decode_json_control(r#"{"type":8,"sequenceId":0}"#, MessageType::Ack),
            );
            receive(
                MessageType::Ack,
                decode_json_control(r#"{"type":8}"#, MessageType::Ack),
            );
            receive(
                MessageType::Sequence,
                decode_json_control("not json", MessageType::Sequence),
            );
            receive(
                MessageType::Sequence,
                decode_json_control(r#"{"type":8,"sequenceId":1}"#, MessageType::Sequence),
            );
            #[cfg(feature = "messagepack")]
            {
                use crate::protocol::stateful::decode_messagepack_control;
                receive(
                    MessageType::Ack,
                    decode_messagepack_control(&[0x92, 0x08, 0x00], MessageType::Ack),
                );
                receive(
                    MessageType::Sequence,
                    decode_messagepack_control(&[0x91, 0x09], MessageType::Sequence),
                );
            }
        });
        let records = sequencing(&records);
        let mut expected = vec![
            "generation=0 ack-rejected sequence=0 reason=zero",
            "generation=0 ack-rejected reason=malformed",
            "generation=0 sequence-rejected reason=malformed",
            "generation=0 sequence-rejected reason=malformed",
        ];
        if cfg!(feature = "messagepack") {
            expected.extend([
                "generation=0 ack-rejected sequence=0 reason=zero",
                "generation=0 sequence-rejected reason=malformed",
            ]);
        }
        assert_eq!(records, expected);
    }

    #[test]
    fn sequence_trace_accepts_valid_control_messages_from_the_wire() {
        use crate::protocol::stateful::decode_json_control;

        let (_, records) = capture(|| {
            let mut buffer = StatefulMessageBuffer::new(10_000).unwrap();
            buffer.track(MessageType::Invocation, frame()).unwrap();
            buffer
                .receive_control(
                    MessageType::Ack,
                    decode_json_control(r#"{"type":8,"sequenceId":1}"#, MessageType::Ack),
                )
                .unwrap();
            buffer.disconnected().unwrap();
            buffer
                .receive_control(
                    MessageType::Sequence,
                    decode_json_control(r#"{"type":9,"sequenceId":1}"#, MessageType::Sequence),
                )
                .unwrap();
        });
        let records = sequencing(&records);
        assert_contains(
            &records,
            "generation=0 ack-received sequence=1 released=1 buffered=0",
        );
        assert_contains(
            &records,
            "generation=1 sequence-received sequence=1 highest-received=0",
        );
    }

    #[test]
    fn sequence_trace_records_a_failed_resend() {
        let (_, records) = capture(|| {
            let mut buffer = StatefulMessageBuffer::new(10_000).unwrap();
            buffer.track(MessageType::Invocation, frame()).unwrap();
            buffer.disconnected().unwrap();
            buffer.begin_resend().unwrap();
            // Sending the Sequence or a buffered frame failed: the frame stays buffered.
            buffer.finish_resend(false).unwrap();
            buffer.disconnected().unwrap();
            let (sequence, frames) = buffer.begin_resend().unwrap();
            assert_eq!((sequence, frames.len()), (1, 1));
            buffer.finish_resend(true).unwrap();
        });
        let records = sequencing(&records);
        let bytes = frame().len();
        assert_eq!(
            records,
            vec![
                format!("generation=0 track sequence=1 type=Invocation bytes={bytes}"),
                "generation=0 transport-lost unacknowledged=1 highest-received=0".to_string(),
                "generation=1 sequence-sent sequence=1".to_string(),
                format!("generation=1 resend first=1 last=1 frames=1 bytes={bytes}"),
                "generation=1 resend-failed".to_string(),
                "generation=1 transport-lost unacknowledged=1 highest-received=0".to_string(),
                "generation=2 sequence-sent sequence=1".to_string(),
                format!("generation=2 resend first=1 last=1 frames=1 bytes={bytes}"),
                "generation=2 resend-finished".to_string(),
            ]
        );
    }

    #[test]
    fn sequence_trace_is_silent_above_debug() {
        let (_, records) = capture(|| {
            let mut buffer = StatefulMessageBuffer::new(10_000).unwrap();
            buffer.track(MessageType::Invocation, frame()).unwrap();
            buffer.should_process(MessageType::Invocation).unwrap();
            buffer.take_acknowledgement().unwrap();
            buffer.disconnected().unwrap();
            buffer.begin_resend().unwrap();
        });
        assert!(
            records
                .iter()
                .filter(|record| record.target == TRACE_TARGET)
                .all(|record| record.level >= log::Level::Debug),
            "{records:#?}"
        );
    }
}
