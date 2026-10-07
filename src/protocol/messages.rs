use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::error::Error;

pub const RECORD_SEPARATOR: &str = "\u{001E}";

pub struct MessageParser {}

impl MessageParser {
    pub fn to_json<T: ?Sized + Serialize>(value: &T) -> Result<String, serde_json::Error> {
        let serialized = serde_json::to_string(value)?;
        Ok(serialized + RECORD_SEPARATOR)
    }

    pub fn to_json_value<T: ?Sized + Serialize>(value: &T) -> Result<Value, serde_json::Error> {
        let serialized = serde_json::to_value(value)?;
        Ok(serialized)
    }

    pub fn strip_record_separator(input: &str) -> &str {
        input.trim_end_matches(RECORD_SEPARATOR)
    }

    /// Parses a hub message envelope; a record that does not parse is a protocol error.
    pub fn parse_message<T: DeserializeOwned>(message: &str) -> Result<T, Error> {
        serde_json::from_str::<T>(message)
            .map_err(|error| Error::protocol(format!("cannot parse the message: {error}")))
    }

    /// Decodes a JSON payload (a result, item or argument) into the requested type.
    pub fn decode_payload<T: DeserializeOwned>(payload: &str) -> Result<T, Error> {
        serde_json::from_str::<T>(payload).map_err(|error| {
            Error::serialization(format!(
                "cannot decode the payload into {}: {error}",
                std::any::type_name::<T>()
            ))
        })
    }
}
