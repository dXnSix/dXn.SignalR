//! Inbound hub-message framing across WebSocket messages, as the official .NET client's
//! pipe-based parser frames them: a JSON record (ended by the record separator) or a MessagePack
//! message (prefixed with its length) may span several WebSocket messages, so the incomplete rest
//! of each message is kept for the next one. There is no size cap unless the application sets one
//! (`with_maximum_message_size`).

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

use crate::error::Error;

const RECORD_SEPARATOR: char = '\u{1e}';

fn too_large(limit: usize) -> Error {
    Error::protocol(format!(
        "the server sent a message larger than the {limit}-byte maximum message size"
    ))
}

fn check(limit: Option<usize>, length: usize) -> Result<(), Error> {
    match limit {
        Some(limit) if length > limit => Err(too_large(limit)),
        _ => Ok(()),
    }
}

/// Frames JSON records across text WebSocket messages.
#[derive(Debug, Default)]
pub(crate) struct TextRecords {
    pending: String,
    limit: Option<usize>,
}

impl TextRecords {
    pub(crate) fn new(limit: Option<usize>) -> Self {
        Self {
            pending: String::new(),
            limit,
        }
    }

    /// Calls `each` with every complete record of `text`, preceded by what earlier messages left,
    /// in order, until `each` returns `Some` (which ends the transport); keeps the incomplete
    /// rest. A record larger than the maximum message size is a protocol violation.
    pub(crate) fn feed<R>(
        &mut self,
        text: &str,
        mut each: impl FnMut(&str) -> Option<R>,
    ) -> Result<Option<R>, Error> {
        if self.pending.is_empty() {
            return self.frame(text, &mut each);
        }
        let mut buffer = std::mem::take(&mut self.pending);
        buffer.push_str(text);
        self.frame(&buffer, &mut each)
    }

    fn frame<R>(
        &mut self,
        text: &str,
        each: &mut impl FnMut(&str) -> Option<R>,
    ) -> Result<Option<R>, Error> {
        let (complete, rest) = match text.rfind(RECORD_SEPARATOR) {
            Some(end) => text.split_at(end + RECORD_SEPARATOR.len_utf8()),
            None => ("", text),
        };
        for record in complete
            .split(RECORD_SEPARATOR)
            .filter(|record| !record.is_empty())
        {
            check(self.limit, record.len())?;
            if let Some(ended) = each(record) {
                return Ok(Some(ended));
            }
        }
        check(self.limit, rest.len())?;
        self.pending = rest.to_string();
        Ok(None)
    }
}

/// Frames length-prefixed MessagePack messages across binary WebSocket messages.
#[cfg(feature = "messagepack")]
#[derive(Debug, Default)]
pub(crate) struct BinaryRecords {
    pending: Vec<u8>,
    limit: Option<usize>,
}

/// The parse of a length prefix (a little-endian base-128 varint of at most 5 bytes, below 2 GiB
/// as the .NET `BinaryMessageParser` requires).
#[cfg(feature = "messagepack")]
enum Prefix {
    Complete { length: usize, size: usize },
    Incomplete,
}

#[cfg(feature = "messagepack")]
fn prefix(bytes: &[u8]) -> Result<Prefix, Error> {
    let mut length = 0usize;
    for (index, byte) in bytes.iter().take(5).enumerate() {
        if index == 4 && *byte > 7 {
            return Err(Error::protocol(
                "the MessagePack message length prefix exceeds 2 GiB",
            ));
        }
        length |= usize::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            return Ok(Prefix::Complete {
                length,
                size: index + 1,
            });
        }
    }
    if bytes.len() >= 5 {
        return Err(Error::protocol(
            "the MessagePack message length prefix is longer than 5 bytes",
        ));
    }
    Ok(Prefix::Incomplete)
}

#[cfg(feature = "messagepack")]
impl BinaryRecords {
    pub(crate) fn new(limit: Option<usize>) -> Self {
        Self {
            pending: Vec::new(),
            limit,
        }
    }

    /// As [`TextRecords::feed`], for length-prefixed MessagePack messages.
    pub(crate) fn feed<R>(
        &mut self,
        bytes: &[u8],
        mut each: impl FnMut(&[u8]) -> Option<R>,
    ) -> Result<Option<R>, Error> {
        if self.pending.is_empty() {
            return self.frame(bytes, &mut each);
        }
        let mut buffer = std::mem::take(&mut self.pending);
        buffer.extend_from_slice(bytes);
        self.frame(&buffer, &mut each)
    }

    fn frame<R>(
        &mut self,
        bytes: &[u8],
        each: &mut impl FnMut(&[u8]) -> Option<R>,
    ) -> Result<Option<R>, Error> {
        let mut rest = bytes;
        while !rest.is_empty() {
            // An incomplete prefix is at most 4 bytes; a complete one bounds the message before
            // its body arrives.
            let Prefix::Complete { length, size } = prefix(rest)? else {
                break;
            };
            check(self.limit, length)?;
            let Some(record) = size.checked_add(length).and_then(|end| rest.get(size..end)) else {
                break;
            };
            rest = rest.get(size + length..).unwrap_or_default();
            if let Some(ended) = each(record) {
                return Ok(Some(ended));
            }
        }
        self.pending = rest.to_vec();
        Ok(None)
    }
}

#[cfg(test)]
mod framing_carryover {
    use super::TextRecords;
    use crate::ErrorKind;

    fn collect(records: &mut TextRecords, text: &str) -> Vec<String> {
        let mut seen = Vec::new();
        records
            .feed(text, |record| {
                seen.push(record.to_string());
                None::<()>
            })
            .unwrap();
        seen
    }

    #[test]
    fn framing_carryover_a_json_record_spans_websocket_messages() {
        let mut records = TextRecords::new(None);
        assert_eq!(
            collect(&mut records, "{\"type\":6}\u{1e}{\"type\":1,"),
            ["{\"type\":6}"]
        );
        assert!(collect(&mut records, "\"target\":").is_empty());
        assert_eq!(
            collect(&mut records, "\"x\"}\u{1e}{\"type\":6}\u{1e}"),
            ["{\"type\":1,\"target\":\"x\"}", "{\"type\":6}"]
        );
        assert!(records.pending.is_empty());
    }

    #[test]
    fn framing_carryover_the_maximum_message_size_bounds_records_and_their_rest() {
        let mut records = TextRecords::new(Some(8));
        assert_eq!(collect(&mut records, "{\"a\":1}\u{1e}"), ["{\"a\":1}"]);
        let error = records
            .feed("{\"b\":123456}\u{1e}", |_| None::<()>)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        let mut partial = TextRecords::new(Some(8));
        assert!(partial.feed("{\"c\":1234567", |_| None::<()>).is_err());
        // Unbounded by default.
        let mut unbounded = TextRecords::new(None);
        let large = format!("{{\"d\":\"{}\"}}", "x".repeat(1 << 20));
        assert_eq!(collect(&mut unbounded, &format!("{large}\u{1e}")).len(), 1);
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn framing_carryover_a_messagepack_message_and_its_prefix_span_websocket_messages() {
        use super::BinaryRecords;

        let mut records = BinaryRecords::new(None);
        let mut seen: Vec<Vec<u8>> = Vec::new();
        let mut feed = |records: &mut BinaryRecords, bytes: &[u8]| {
            records
                .feed(bytes, |record| {
                    seen.push(record.to_vec());
                    None::<()>
                })
                .unwrap();
        };
        // A 200-byte message has a two-byte prefix (0xc8 0x01); split inside the prefix, then
        // inside the body.
        let body = vec![0x07u8; 200];
        let mut message = vec![0xc8, 0x01];
        message.extend_from_slice(&body);
        message.extend_from_slice(&[0x02, 0x91, 0x06]);
        feed(&mut records, &message[..1]);
        feed(&mut records, &message[1..50]);
        feed(&mut records, &message[50..]);
        assert_eq!(seen, [body, vec![0x91, 0x06]]);
        assert!(records.pending.is_empty());
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn framing_carryover_oversized_prefixes_and_messages_are_protocol_violations() {
        use super::BinaryRecords;

        for prefix in [
            &[0x80, 0x80, 0x80, 0x80, 0x80][..],
            &[0xff, 0xff, 0xff, 0xff, 0x08][..],
        ] {
            let error = BinaryRecords::new(None)
                .feed(prefix, |_| None::<()>)
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Protocol, "{prefix:?}");
        }
        // The largest length .NET accepts is still framed (and waits for its body).
        assert!(BinaryRecords::new(None)
            .feed(&[0xff, 0xff, 0xff, 0xff, 0x07], |_| None::<()>)
            .is_ok());
        // The maximum message size is checked against the prefix, before the body arrives.
        let error = BinaryRecords::new(Some(8))
            .feed(&[0x09, 0x00], |_| None::<()>)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
    }
}

/// Property tests: framing never panics on peer input, and any split of the inbound stream into
/// WebSocket messages yields the same records.
#[cfg(test)]
mod framing_carryover_properties {
    use proptest::prelude::*;

    use super::TextRecords;
    use crate::ErrorKind;

    /// Feeds `chunks` in order and collects every record, or the error that ended the stream.
    fn text_records(limit: Option<usize>, chunks: &[&str]) -> Result<Vec<String>, ErrorKind> {
        let mut records = TextRecords::new(limit);
        let mut seen = Vec::new();
        for chunk in chunks {
            records
                .feed(chunk, |record| {
                    seen.push(record.to_string());
                    None::<()>
                })
                .map_err(|error| error.kind())?;
        }
        Ok(seen)
    }

    /// `text` split at the given char positions.
    fn split_text(text: &str, cuts: &[prop::sample::Index]) -> Vec<String> {
        let boundaries: Vec<usize> = text.char_indices().map(|(index, _)| index).collect();
        let mut positions: Vec<usize> = cuts
            .iter()
            .filter(|_| !boundaries.is_empty())
            .map(|cut| boundaries[cut.index(boundaries.len())])
            .collect();
        positions.sort_unstable();
        positions.dedup();
        let mut chunks = Vec::new();
        let mut start = 0;
        for position in positions {
            chunks.push(text[start..position].to_string());
            start = position;
        }
        chunks.push(text[start..].to_string());
        chunks
    }

    proptest! {
        #[test]
        fn framing_carryover_json_records_survive_any_split(
            records in prop::collection::vec("[^\u{1e}]{0,24}", 0..8),
            cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..8),
        ) {
            let stream: String = records.iter().map(|record| format!("{record}\u{1e}")).collect();
            let expected: Vec<String> =
                records.into_iter().filter(|record| !record.is_empty()).collect();
            let chunks = split_text(&stream, &cuts);
            let chunks: Vec<&str> = chunks.iter().map(String::as_str).collect();
            prop_assert_eq!(text_records(None, &chunks), Ok(expected));
        }

        #[test]
        fn framing_carryover_arbitrary_text_never_panics(
            chunks in prop::collection::vec(".{0,32}", 0..6),
            limit in prop::option::of(0usize..48),
        ) {
            let chunks: Vec<&str> = chunks.iter().map(String::as_str).collect();
            if let Err(kind) = text_records(limit, &chunks) {
                prop_assert_eq!(kind, ErrorKind::Protocol);
                prop_assert!(limit.is_some(), "only a size limit fails text framing");
            }
        }

        #[test]
        fn framing_carryover_json_records_above_the_maximum_size_fail(
            record in "[^\u{1e}]{9,32}",
        ) {
            let stream = format!("{record}\u{1e}");
            prop_assert_eq!(text_records(Some(8), &[&stream]), Err(ErrorKind::Protocol));
        }
    }

    #[cfg(feature = "messagepack")]
    mod binary {
        use proptest::prelude::*;

        use super::super::BinaryRecords;
        use crate::{protocol::msgpack::frame_message, ErrorKind};

        fn binary_records(
            limit: Option<usize>,
            chunks: &[&[u8]],
        ) -> Result<Vec<Vec<u8>>, ErrorKind> {
            let mut records = BinaryRecords::new(limit);
            let mut seen = Vec::new();
            for chunk in chunks {
                records
                    .feed(chunk, |record| {
                        seen.push(record.to_vec());
                        None::<()>
                    })
                    .map_err(|error| error.kind())?;
            }
            Ok(seen)
        }

        fn split<'a>(bytes: &'a [u8], cuts: &[prop::sample::Index]) -> Vec<&'a [u8]> {
            let mut positions: Vec<usize> =
                cuts.iter().map(|cut| cut.index(bytes.len() + 1)).collect();
            positions.sort_unstable();
            positions.dedup();
            let mut chunks = Vec::new();
            let mut start = 0;
            for position in positions {
                chunks.push(&bytes[start..position]);
                start = position;
            }
            chunks.push(&bytes[start..]);
            chunks
        }

        proptest! {
            #[test]
            fn framing_carryover_messagepack_messages_survive_any_split(
                payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..300), 0..6),
                cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..8),
            ) {
                let stream: Vec<u8> =
                    payloads.iter().flat_map(|payload| frame_message(payload)).collect();
                prop_assert_eq!(binary_records(None, &split(&stream, &cuts)), Ok(payloads));
            }

            #[test]
            fn framing_carryover_arbitrary_bytes_never_panic(
                bytes in prop::collection::vec(any::<u8>(), 0..256),
                cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..6),
                limit in prop::option::of(0usize..64),
            ) {
                if let Err(kind) = binary_records(limit, &split(&bytes, &cuts)) {
                    prop_assert_eq!(kind, ErrorKind::Protocol);
                }
            }

            #[test]
            fn framing_carryover_messagepack_messages_within_the_maximum_size_survive_any_split(
                payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=200), 0..4),
                cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..8),
            ) {
                let stream: Vec<u8> =
                    payloads.iter().flat_map(|payload| frame_message(payload)).collect();
                prop_assert_eq!(binary_records(Some(200), &split(&stream, &cuts)), Ok(payloads));
            }

            #[test]
            fn framing_carryover_messagepack_messages_above_the_maximum_size_fail(
                payload in prop::collection::vec(any::<u8>(), 9..64),
                cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..4),
            ) {
                let stream = frame_message(&payload);
                prop_assert_eq!(
                    binary_records(Some(8), &split(&stream, &cuts)),
                    Err(ErrorKind::Protocol)
                );
            }
        }
    }
}
