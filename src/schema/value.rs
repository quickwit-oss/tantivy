use std::fmt;

use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Map;

use crate::schema::Facet;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;

/// Value represents the value of a any field.
/// It is an enum over all over all of the possible field type.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// The str type is used for any text information.
    Str(String),
    /// Pre-tokenized str type,
    PreTokStr(PreTokenizedString),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Signed 64-bits Date time stamp `date`
    Date(DateTime),
    /// Facet
    Facet(Facet),
    /// Arbitrarily sized byte array
    Bytes(Vec<u8>),
    /// Json object value.
    JsonObject(serde_json::Map<String, serde_json::Value>),
}

impl Eq for Value {}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        match *self {
            Value::Str(ref v) => serializer.serialize_str(v),
            Value::PreTokStr(ref v) => v.serialize(serializer),
            Value::U64(u) => serializer.serialize_u64(u),
            Value::I64(u) => serializer.serialize_i64(u),
            Value::F64(u) => serializer.serialize_f64(u),
            Value::Date(ref date) => serializer.serialize_str(&date.to_rfc3339()),
            Value::Facet(ref facet) => facet.serialize(serializer),
            Value::Bytes(ref bytes) => serializer.serialize_bytes(bytes),
            Value::JsonObject(ref obj) => obj.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string or u32")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(Value::I64(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Value::U64(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(Value::F64(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(Value::Str(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(Value::Str(v))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl Value {
    /// Returns the text value, provided the value is of the `Str` type.
    /// (Returns None if the value is not of the `Str` type).
    pub fn as_text(&self) -> Option<&str> {
        if let Value::Str(text) = self {
            Some(text)
        } else {
            None
        }
    }

    /// Returns the facet value, provided the value is of the `Facet` type.
    /// (Returns None if the value is not of the `Facet` type).
    pub fn as_facet(&self) -> Option<&Facet> {
        if let Value::Facet(facet) = self {
            Some(facet)
        } else {
            None
        }
    }

    /// Returns the tokenized text, provided the value is of the `PreTokStr` type.
    /// (Returns None if the value is not of the `PreTokStr` type.)
    pub fn tokenized_text(&self) -> Option<&PreTokenizedString> {
        if let Value::PreTokStr(tokenized_text) = self {
            Some(tokenized_text)
        } else {
            None
        }
    }

    /// Returns the u64-value, provided the value is of the `U64` type.
    /// (Returns None if the value is not of the `U64` type)
    pub fn as_u64(&self) -> Option<u64> {
        if let Value::U64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    /// Returns the i64-value, provided the value is of the `I64` type.
    ///
    /// Return None if the value is not of type `I64`.
    pub fn as_i64(&self) -> Option<i64> {
        if let Value::I64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    /// Returns the f64-value, provided the value is of the `F64` type.
    ///
    /// Return None if the value is not of type `F64`.
    pub fn as_f64(&self) -> Option<f64> {
        if let Value::F64(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    /// Returns the Date-value, provided the value is of the `Date` type.
    ///
    /// Returns None if the value is not of type `Date`.
    pub fn as_date(&self) -> Option<&DateTime> {
        if let Value::Date(date) = self {
            Some(date)
        } else {
            None
        }
    }

    /// Returns the Bytes-value, provided the value is of the `Bytes` type.
    ///
    /// Returns None if the value is not of type `Bytes`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Value::Bytes(bytes) = self {
            Some(bytes)
        } else {
            None
        }
    }

    /// Returns the json object, provided the value is of the JsonObject type.
    ///
    /// Returns None if the value is not of type JsonObject.
    pub fn as_json(&self) -> Option<&Map<String, serde_json::Value>> {
        if let Value::JsonObject(json) = self {
            Some(json)
        } else {
            None
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::Str(s)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Value {
        Value::U64(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Value {
        Value::I64(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Value {
        Value::F64(v)
    }
}

impl From<crate::DateTime> for Value {
    fn from(date_time: crate::DateTime) -> Value {
        Value::Date(date_time)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(s: &'a str) -> Value {
        Value::Str(s.to_string())
    }
}

impl<'a> From<&'a [u8]> for Value {
    fn from(bytes: &'a [u8]) -> Value {
        Value::Bytes(bytes.to_vec())
    }
}

impl<'a> From<Facet> for Value {
    fn from(facet: Facet) -> Value {
        Value::Facet(facet)
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Value {
        Value::Bytes(bytes)
    }
}

impl From<PreTokenizedString> for Value {
    fn from(pretokenized_string: PreTokenizedString) -> Value {
        Value::PreTokStr(pretokenized_string)
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for Value {
    fn from(json_object: serde_json::Map<String, serde_json::Value>) -> Value {
        Value::JsonObject(json_object)
    }
}

impl From<serde_json::Value> for Value {
    fn from(json_value: serde_json::Value) -> Value {
        match json_value {
            serde_json::Value::Object(json_object) => Value::JsonObject(json_object),
            _ => {
                panic!("Expected a json object.");
            }
        }
    }
}

mod binary_serialize {
    use std::io::{self, Read, Write};

    use chrono::{TimeZone, Utc};
    use common::{f64_to_u64, u64_to_f64, BinarySerializable};

    use super::Value;
    use crate::schema::Facet;
    use crate::tokenizer::PreTokenizedString;

    const TEXT_CODE: u8 = 0;
    const U64_CODE: u8 = 1;
    const I64_CODE: u8 = 2;
    const HIERARCHICAL_FACET_CODE: u8 = 3;
    const BYTES_CODE: u8 = 4;
    const DATE_CODE: u8 = 5;
    const F64_CODE: u8 = 6;
    const EXT_CODE: u8 = 7;
    const JSON_OBJ_CODE: u8 = 8;

    // extended types

    const TOK_STR_CODE: u8 = 0;

    impl BinarySerializable for Value {
        fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
            match *self {
                Value::Str(ref text) => {
                    TEXT_CODE.serialize(writer)?;
                    text.serialize(writer)
                }
                Value::PreTokStr(ref tok_str) => {
                    EXT_CODE.serialize(writer)?;
                    TOK_STR_CODE.serialize(writer)?;
                    if let Ok(text) = serde_json::to_string(tok_str) {
                        text.serialize(writer)
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Failed to dump Value::PreTokStr(_) to json.",
                        ))
                    }
                }
                Value::U64(ref val) => {
                    U64_CODE.serialize(writer)?;
                    val.serialize(writer)
                }
                Value::I64(ref val) => {
                    I64_CODE.serialize(writer)?;
                    val.serialize(writer)
                }
                Value::F64(ref val) => {
                    F64_CODE.serialize(writer)?;
                    f64_to_u64(*val).serialize(writer)
                }
                Value::Date(ref val) => {
                    DATE_CODE.serialize(writer)?;
                    val.timestamp().serialize(writer)
                }
                Value::Facet(ref facet) => {
                    HIERARCHICAL_FACET_CODE.serialize(writer)?;
                    facet.serialize(writer)
                }
                Value::Bytes(ref bytes) => {
                    BYTES_CODE.serialize(writer)?;
                    bytes.serialize(writer)
                }
                Value::JsonObject(ref map) => {
                    JSON_OBJ_CODE.serialize(writer)?;
                    serde_json::to_writer(writer, &map)?;
                    Ok(())
                }
            }
        }

        fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
            let type_code = u8::deserialize(reader)?;
            match type_code {
                TEXT_CODE => {
                    let text = String::deserialize(reader)?;
                    Ok(Value::Str(text))
                }
                U64_CODE => {
                    let value = u64::deserialize(reader)?;
                    Ok(Value::U64(value))
                }
                I64_CODE => {
                    let value = i64::deserialize(reader)?;
                    Ok(Value::I64(value))
                }
                F64_CODE => {
                    let value = u64_to_f64(u64::deserialize(reader)?);
                    Ok(Value::F64(value))
                }
                DATE_CODE => {
                    let timestamp = i64::deserialize(reader)?;
                    Ok(Value::Date(Utc.timestamp(timestamp, 0)))
                }
                HIERARCHICAL_FACET_CODE => Ok(Value::Facet(Facet::deserialize(reader)?)),
                BYTES_CODE => Ok(Value::Bytes(Vec::<u8>::deserialize(reader)?)),
                EXT_CODE => {
                    let ext_type_code = u8::deserialize(reader)?;
                    match ext_type_code {
                        TOK_STR_CODE => {
                            let str_val = String::deserialize(reader)?;
                            if let Ok(value) = serde_json::from_str::<PreTokenizedString>(&str_val)
                            {
                                Ok(Value::PreTokStr(value))
                            } else {
                                Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    "Failed to parse string data as Value::PreTokStr(_).",
                                ))
                            }
                        }
                        _ => Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "No extened field type is associated with code {:?}",
                                ext_type_code
                            ),
                        )),
                    }
                }
                JSON_OBJ_CODE => {
                    let map = serde_json::from_reader(reader)?;
                    Ok(Value::JsonObject(map))
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("No field type is associated with code {:?}", type_code),
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::Value;
    use crate::DateTime;

    #[test]
    fn test_serialize_date() {
        let value = Value::Date(DateTime::from_str("1996-12-20T00:39:57+00:00").unwrap());
        let serialized_value_json = serde_json::to_string_pretty(&value).unwrap();
        assert_eq!(serialized_value_json, r#""1996-12-20T00:39:57+00:00""#);
    }
}
