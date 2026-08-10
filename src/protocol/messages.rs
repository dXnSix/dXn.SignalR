use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

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

    pub fn parse_message<T: DeserializeOwned>(message: &str) -> Result<T, String> {
        serde_json::from_str::<T>(message).map_err(|error| error.to_string())
    }
}
