use serde::{de::DeserializeOwned, Serialize};

use crate::error::Error;

pub const RECORD_SEPARATOR: &str = "\u{001E}";

pub struct MessageParser {}

impl MessageParser {
    pub fn to_json<T: ?Sized + Serialize>(value: &T) -> Result<String, serde_json::Error> {
        let serialized = serde_json::to_string(value)?;
        Ok(serialized + RECORD_SEPARATOR)
    }

    /// Parses a hub message envelope; a record that does not parse is a protocol error.
    pub fn parse_message<T: DeserializeOwned>(message: &str) -> Result<T, Error> {
        serde_json::from_str::<T>(message)
            .map_err(|error| Error::protocol(format!("cannot parse the message: {error}")))
    }
}
