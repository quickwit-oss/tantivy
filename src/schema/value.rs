use crate::schema::Facet;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{cmp::Ordering, fmt};

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
    /// Hierarchical Facet
    Facet(Facet),
    /// Arbitrarily sized byte array
    Bytes(Vec<u8>),
}

impl Eq for Value {}
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Str(l), Value::Str(r)) => l.cmp(r),
            (Value::PreTokStr(l), Value::PreTokStr(r)) => l.cmp(r),
            (Value::U64(l), Value::U64(r)) => l.cmp(r),
            (Value::I64(l), Value::I64(r)) => l.cmp(r),
            (Value::Date(l), Value::Date(r)) => l.cmp(r),
            (Value::Facet(l), Value::Facet(r)) => l.cmp(r),
            (Value::Bytes(l), Value::Bytes(r)) => l.cmp(r),
            (Value::F64(l), Value::F64(r)) => {
                match (l.is_nan(), r.is_nan()) {
                    (false, false) => l.partial_cmp(r).unwrap(), // only fail on NaN
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Less, // we define NaN as less than -∞
                    (false, true) => Ordering::Greater,
                }
            }
            (Value::Str(_), _) => Ordering::Less,
            (_, Value::Str(_)) => Ordering::Greater,
            (Value::PreTokStr(_), _) => Ordering::Less,
            (_, Value::PreTokStr(_)) => Ordering::Greater,
            (Value::U64(_), _) => Ordering::Less,
            (_, Value::U64(_)) => Ordering::Greater,
            (Value::I64(_), _) => Ordering::Less,
            (_, Value::I64(_)) => Ordering::Greater,
            (Value::F64(_), _) => Ordering::Less,
            (_, Value::F64(_)) => Ordering::Greater,
            (Value::Date(_), _) => Ordering::Less,
            (_, Value::Date(_)) => Ordering::Greater,
            (Value::Facet(_), _) => Ordering::Less,
            (_, Value::Facet(_)) => Ordering::Greater,
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Value::Str(ref v) => serializer.serialize_str(v),
            Value::PreTokStr(ref v) => v.serialize(serializer),
            Value::U64(u) => serializer.serialize_u64(u),
            Value::I64(u) => serializer.serialize_i64(u),
            Value::F64(u) => serializer.serialize_f64(u),
            Value::Date(ref date) => serializer.serialize_str(&date.to_rfc3339()),
            Value::Facet(ref facet) => facet.serialize(serializer),
            Value::Bytes(ref bytes) => serializer.serialize_bytes(bytes),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
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
    pub fn text(&self) -> Option<&str> {
        if let Value::Str(text) = self {
            Some(text)
        } else {
            None
        }
    }

    /// Returns the path value, provided the value is of the `Facet` type.
    /// (Returns None if the value is not of the `Facet` type).
    pub fn path(&self) -> Option<String> {
        if let Value::Facet(facet) = self {
            Some(facet.to_path_string())
        } else {
            None
        }
    }

    /// Returns the tokenized text, provided the value is of the `PreTokStr` type.
    ///
    /// Returns None if the value is not of the `PreTokStr` type.
    pub fn tokenized_text(&self) -> Option<&PreTokenizedString> {
        if let Value::PreTokStr(tokenized_text) = self {
            Some(tokenized_text)
        } else {
            None
        }
    }

    /// Returns the u64-value, provided the value is of the `U64` type.
    ///
    /// Returns None if the value is not of the `U64` type.
    pub fn u64_value(&self) -> Option<u64> {
        if let Value::U64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    /// Returns the i64-value, provided the value is of the `I64` type.
    ///
    /// Return None if the value is not of type `I64`.
    pub fn i64_value(&self) -> Option<i64> {
        if let Value::I64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    /// Returns the f64-value, provided the value is of the `F64` type.
    ///
    /// Return None if the value is not of type `F64`.
    pub fn f64_value(&self) -> Option<f64> {
        if let Value::F64(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    /// Returns the Date-value, provided the value is of the `Date` type.
    ///
    /// Returns None if the value is not of type `Date`.
    pub fn date_value(&self) -> Option<&DateTime> {
        if let Value::Date(date) = self {
            Some(date)
        } else {
            None
        }
    }

    /// Returns the Bytes-value, provided the value is of the `Bytes` type.
    ///
    /// Returns None if the value is not of type `Bytes`.
    pub fn bytes_value(&self) -> Option<&[u8]> {
        if let Value::Bytes(bytes) = self {
            Some(bytes)
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

mod binary_serialize {
    use super::Value;
    use crate::common::{f64_to_u64, u64_to_f64, BinarySerializable};
    use crate::schema::Facet;
    use crate::tokenizer::PreTokenizedString;
    use chrono::{TimeZone, Utc};
    use std::io::{self, Read, Write};

    const TEXT_CODE: u8 = 0;
    const U64_CODE: u8 = 1;
    const I64_CODE: u8 = 2;
    const HIERARCHICAL_FACET_CODE: u8 = 3;
    const BYTES_CODE: u8 = 4;
    const DATE_CODE: u8 = 5;
    const F64_CODE: u8 = 6;
    const EXT_CODE: u8 = 7;

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
    use super::Value;
    use crate::DateTime;
    use std::str::FromStr;

    #[test]
    fn test_serialize_date() {
        let value = Value::Date(DateTime::from_str("1996-12-20T00:39:57+00:00").unwrap());
        let serialized_value_json = serde_json::to_string_pretty(&value).unwrap();
        assert_eq!(serialized_value_json, r#""1996-12-20T00:39:57+00:00""#);
    }
}
