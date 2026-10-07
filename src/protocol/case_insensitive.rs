//! A second chance for JSON payloads whose member names differ from the application's Serde names
//! only in case, as System.Text.Json's web defaults (the official .NET client's) read them: the
//! payload is decoded again through a deserializer that maps every object key onto the field name
//! the target struct expects, ignoring case. The exact names are always tried first.

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

use serde::{
    de::{
        self, DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor,
    },
    Deserializer,
};
use serde_json::{Map, Value};

/// Decodes `value` into `T`, matching object keys to `T`'s field names case-insensitively.
pub(crate) fn decode<T: DeserializeOwned>(value: Value) -> Result<T, serde_json::Error> {
    T::deserialize(CaseInsensitive(value))
}

struct CaseInsensitive(Value);

impl<'de> Deserializer<'de> for CaseInsensitive {
    type Error = serde_json::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.0 {
            Value::Array(items) => visitor.visit_seq(Items(items.into_iter())),
            Value::Object(map) => visitor.visit_map(Members::new(map, &[])),
            other => other.deserialize_any(visitor),
        }
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        match self.0 {
            Value::Object(map) => visitor.visit_map(Members::new(map, fields)),
            Value::Array(items) => visitor.visit_seq(Items(items.into_iter())),
            other => other.deserialize_struct(name, fields, visitor),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.0 {
            Value::Null => visitor.visit_none(),
            other => visitor.visit_some(CaseInsensitive(other)),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.0.deserialize_enum(name, variants, visitor)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        unit unit_struct seq tuple tuple_struct map identifier ignored_any
    }
}

struct Items(std::vec::IntoIter<Value>);

impl<'de> SeqAccess<'de> for Items {
    type Error = serde_json::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        self.0
            .next()
            .map(|value| seed.deserialize(CaseInsensitive(value)))
            .transpose()
    }
}

struct Members {
    entries: serde_json::map::IntoIter,
    fields: &'static [&'static str],
    value: Option<Value>,
}

impl Members {
    fn new(map: Map<String, Value>, fields: &'static [&'static str]) -> Self {
        Self {
            entries: map.into_iter(),
            fields,
            value: None,
        }
    }

    /// The expected field name `key` matches: itself when expected, else the field equal to it
    /// ignoring case.
    fn field_name(&self, key: String) -> String {
        if self.fields.contains(&key.as_str()) {
            return key;
        }
        let folded = |text: &str| {
            text.chars()
                .flat_map(char::to_lowercase)
                .collect::<String>()
        };
        let wanted = folded(&key);
        self.fields
            .iter()
            .find(|field| folded(field) == wanted)
            .map_or(key, |field| (*field).to_string())
    }
}

impl<'de> MapAccess<'de> for Members {
    type Error = serde_json::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        let Some((key, value)) = self.entries.next() else {
            return Ok(None);
        };
        self.value = Some(value);
        let key = self.field_name(key);
        let key: de::value::StringDeserializer<serde_json::Error> = key.into_deserializer();
        seed.deserialize(key).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let value = self
            .value
            .take()
            .ok_or_else(|| de::Error::custom("a map value was requested before its key"))?;
        seed.deserialize(CaseInsensitive(value))
    }
}

#[cfg(test)]
mod case_insensitive_payload {
    use serde::Deserialize;
    use serde_json::json;

    use super::decode;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct Order {
        order_id: u32,
        customer_name: String,
        lines: Vec<Line>,
        note: Option<String>,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct Line {
        product_code: String,
        quantity: u16,
    }

    #[test]
    fn case_insensitive_payload_pascal_case_members_reach_nested_fields() {
        let order: Order = decode(json!({
            "OrderId": 7,
            "CUSTOMERNAME": "Ana",
            "Lines": [{"ProductCode": "x-1", "Quantity": 2}],
            "Note": null,
        }))
        .unwrap();
        assert_eq!(
            order,
            Order {
                order_id: 7,
                customer_name: "Ana".into(),
                lines: vec![Line {
                    product_code: "x-1".into(),
                    quantity: 2
                }],
                note: None,
            }
        );
    }

    #[test]
    fn case_insensitive_payload_exact_names_and_unknown_members_are_kept() {
        let order: Order = decode(json!({
            "orderId": 1,
            "customerName": "Bo",
            "lines": [],
            "note": "n",
            "Extra": true,
        }))
        .unwrap();
        assert_eq!(order.note.as_deref(), Some("n"));
        // A value of the wrong type still fails.
        assert!(
            decode::<Order>(json!({"OrderId": "seven", "CustomerName": "x", "Lines": []})).is_err()
        );
    }
}
