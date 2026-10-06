use serde::{Deserialize, Serialize};

use super::negotiate::MessageType;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Ack {
    r#type: MessageType,
    sequence_id: u64,
}

impl Ack {
    pub(crate) fn new(sequence_id: u64) -> Self {
        Self {
            r#type: MessageType::Ack,
            sequence_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Sequence {
    r#type: MessageType,
    sequence_id: u64,
}

impl Sequence {
    pub(crate) fn new(sequence_id: u64) -> Self {
        Self {
            r#type: MessageType::Sequence,
            sequence_id,
        }
    }
}

/// Decodes a JSON Ack or Sequence record of the `expected` type into its sequence id. The id is
/// not validated here: `StatefulMessageBuffer::receive_control` rejects (and records) an id of 0.
pub(crate) fn decode_json_control(text: &str, expected: MessageType) -> Result<u64, String> {
    // Ack and Sequence share their JSON shape.
    let control = crate::protocol::messages::MessageParser::parse_message::<Ack>(text)?;
    if control.r#type != expected {
        return Err(format!("Expected {expected:?} control message"));
    }
    Ok(control.sequence_id)
}

pub(crate) fn validate_sequence_id(sequence_id: u64) -> Result<u64, String> {
    if sequence_id == 0 {
        Err("SignalR stateful sequence IDs start at 1".to_string())
    } else {
        Ok(sequence_id)
    }
}

#[cfg(feature = "messagepack")]
pub(crate) fn encode_messagepack_control(
    message_type: MessageType,
    sequence_id: u64,
) -> Result<Vec<u8>, String> {
    validate_sequence_id(sequence_id)?;
    if !matches!(message_type, MessageType::Ack | MessageType::Sequence) {
        return Err("Only Ack and Sequence are stateful control messages".to_string());
    }
    let mut payload = Vec::new();
    rmp::encode::write_array_len(&mut payload, 2).map_err(|error| error.to_string())?;
    rmp::encode::write_uint(&mut payload, message_type as u8 as u64)
        .map_err(|error| error.to_string())?;
    rmp::encode::write_uint(&mut payload, sequence_id).map_err(|error| error.to_string())?;
    Ok(crate::protocol::msgpack::frame_message(&payload))
}

/// Decodes a MessagePack Ack or Sequence (`[type, sequenceId]`) of the `expected` type into its
/// sequence id. The id is not validated here: `StatefulMessageBuffer::receive_control` rejects
/// (and records) an id of 0.
#[cfg(feature = "messagepack")]
pub(crate) fn decode_messagepack_control(
    payload: &[u8],
    expected: MessageType,
) -> Result<u64, String> {
    let items = crate::protocol::msgpack::parse_msgpack_message(payload)?;
    if items.len() != 2 {
        return Err("Ack and Sequence MessagePack arrays must contain two items".to_string());
    }
    let message_type = items[0]
        .as_u64()
        .ok_or_else(|| "Stateful control message type is not an integer".to_string())?;
    if message_type != expected as u8 as u64 {
        return Err(format!("Expected {:?} control message", expected));
    }
    items[1]
        .as_u64()
        .ok_or_else(|| "Stateful sequence ID is not an unsigned integer".to_string())
}

#[cfg(test)]
mod stateful_golden_frames {
    use super::{Ack, Sequence};
    use crate::protocol::messages::MessageParser;

    #[test]
    fn stateful_golden_frames_json_ack_and_sequence() {
        assert_eq!(
            MessageParser::to_json(&Ack::new(42)).unwrap(),
            "{\"type\":8,\"sequenceId\":42}\u{001e}"
        );
        assert_eq!(
            MessageParser::to_json(&Sequence::new(43)).unwrap(),
            "{\"type\":9,\"sequenceId\":43}\u{001e}"
        );
    }

    #[test]
    fn stateful_rejects_zero_sequence_id() {
        assert!(super::validate_sequence_id(0).is_err());
        assert_eq!(super::validate_sequence_id(1), Ok(1));
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn stateful_golden_frames_messagepack_ack_and_sequence() {
        use super::{decode_messagepack_control, encode_messagepack_control};
        use crate::protocol::negotiate::MessageType;

        let ack = encode_messagepack_control(MessageType::Ack, 42).unwrap();
        let sequence = encode_messagepack_control(MessageType::Sequence, 43).unwrap();
        assert_eq!(ack, vec![3, 0x92, 8, 42]);
        assert_eq!(sequence, vec![3, 0x92, 9, 43]);
        assert_eq!(
            decode_messagepack_control(&ack[1..], MessageType::Ack).unwrap(),
            42
        );
        assert_eq!(
            decode_messagepack_control(&sequence[1..], MessageType::Sequence).unwrap(),
            43
        );
        let wide = encode_messagepack_control(MessageType::Ack, 300).unwrap();
        assert_eq!(
            decode_messagepack_control(&wide[1..], MessageType::Ack).unwrap(),
            300
        );
    }
}
