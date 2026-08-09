use std::collections::VecDeque;

use crate::protocol::negotiate::MessageType;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SerializedFrame {
    Text(String),
    Binary(Vec<u8>),
}

impl SerializedFrame {
    fn len(&self) -> usize {
        match self {
            Self::Text(value) => value.len(),
            Self::Binary(value) => value.len(),
        }
    }
}

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
    awaiting_sequence: bool,
    resending: bool,
    disposed: bool,
}

impl StatefulMessageBuffer {
    pub(crate) fn new(capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("Stateful reconnect buffer size must be greater than zero".to_string());
        }
        Ok(Self {
            capacity,
            buffered_bytes: 0,
            frames: VecDeque::new(),
            next_outgoing: 1,
            last_acknowledged: 0,
            next_incoming: 1,
            highest_incoming: 0,
            awaiting_sequence: false,
            resending: false,
            disposed: false,
        })
    }

    pub(crate) fn track(
        &mut self,
        message_type: MessageType,
        payload: SerializedFrame,
    ) -> Result<TrackResult, String> {
        self.ensure_active()?;
        if !is_trackable(message_type) {
            return Ok(TrackResult::Bypass);
        }
        let bytes = payload.len();
        if bytes > self.capacity {
            return Err(format!(
                "Serialized frame size {bytes} exceeds stateful reconnect capacity {}",
                self.capacity
            ));
        }
        if self.resending || self.buffered_bytes + bytes > self.capacity {
            return Ok(TrackResult::Backpressured);
        }
        let sequence_id = self.next_outgoing;
        self.next_outgoing = self
            .next_outgoing
            .checked_add(1)
            .ok_or_else(|| "Outgoing stateful sequence counter overflow".to_string())?;
        self.buffered_bytes += bytes;
        self.frames.push_back(BufferedFrame {
            sequence_id,
            payload,
        });
        Ok(TrackResult::Buffered(sequence_id))
    }

    pub(crate) fn acknowledge(&mut self, sequence_id: u64) -> Result<usize, String> {
        self.ensure_active()?;
        crate::protocol::stateful::validate_sequence_id(sequence_id)?;
        if sequence_id >= self.next_outgoing {
            return Err(format!(
                "Ack {sequence_id} is beyond the highest sent sequence {}",
                self.next_outgoing - 1
            ));
        }
        if sequence_id <= self.last_acknowledged {
            return Ok(0);
        }
        self.last_acknowledged = sequence_id;
        let mut released = 0;
        while self
            .frames
            .front()
            .is_some_and(|frame| frame.sequence_id <= sequence_id)
        {
            if let Some(frame) = self.frames.pop_front() {
                let bytes = frame.payload.len();
                self.buffered_bytes -= bytes;
                released += bytes;
            }
        }
        Ok(released)
    }

    pub(crate) fn disconnected(&mut self) -> Result<(), String> {
        self.ensure_active()?;
        self.awaiting_sequence = true;
        Ok(())
    }

    pub(crate) fn reset_incoming_sequence(&mut self, sequence_id: u64) -> Result<(), String> {
        self.ensure_active()?;
        crate::protocol::stateful::validate_sequence_id(sequence_id)?;
        if !self.awaiting_sequence {
            return Err("Unexpected Sequence message while transport is active".to_string());
        }
        let maximum = self
            .highest_incoming
            .checked_add(1)
            .ok_or_else(|| "Incoming stateful sequence counter overflow".to_string())?;
        if sequence_id > maximum {
            return Err(format!(
                "Sequence {sequence_id} creates a future gap beyond {maximum}"
            ));
        }
        self.next_incoming = sequence_id;
        self.awaiting_sequence = false;
        Ok(())
    }

    pub(crate) fn should_process(&mut self, message_type: MessageType) -> Result<bool, String> {
        self.ensure_active()?;
        if !is_trackable(message_type) {
            return Ok(true);
        }
        if self.awaiting_sequence {
            return Err("Trackable message arrived before Sequence after resume".to_string());
        }
        let sequence_id = self.next_incoming;
        self.next_incoming = self
            .next_incoming
            .checked_add(1)
            .ok_or_else(|| "Incoming stateful sequence counter overflow".to_string())?;
        if sequence_id <= self.highest_incoming {
            return Ok(false);
        }
        self.highest_incoming = sequence_id;
        Ok(true)
    }

    pub(crate) fn begin_resend(&mut self) -> Result<(u64, Vec<BufferedFrame>), String> {
        self.ensure_active()?;
        self.resending = true;
        let sequence_id = self
            .frames
            .front()
            .map_or(self.next_outgoing, |frame| frame.sequence_id);
        Ok((sequence_id, self.frames.iter().cloned().collect()))
    }

    pub(crate) fn finish_resend(&mut self) -> Result<(), String> {
        self.ensure_active()?;
        self.resending = false;
        Ok(())
    }

    pub(crate) fn dispose(&mut self) {
        self.disposed = true;
        self.resending = false;
        self.awaiting_sequence = false;
        self.frames.clear();
        self.buffered_bytes = 0;
    }

    pub(crate) fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    fn ensure_active(&self) -> Result<(), String> {
        if self.disposed {
            Err("Stateful reconnect buffer was disposed".to_string())
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
        SerializedFrame::Text(value.to_string())
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
        buffer.finish_resend().unwrap();
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
    fn stateful_configuration_rejects_zero_and_defaults_to_100000() {
        assert_eq!(
            crate::StatefulReconnectConfig::default().buffer_size_bytes,
            100_000
        );
        assert!(crate::StatefulReconnectConfig::new(0).is_err());
    }
}
