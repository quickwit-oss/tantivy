use std::fmt;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Visitor;
use schema::Facet;

/// Value represents the value of a any field.
/// It is an enum over all over all of the possible field type.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Value {
    /// The str type is used for any text information.
    Str(String),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// Hierarchical Facet
    Facet(Facet),
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Value::Str(ref v) => serializer.serialize_str(v),
            Value::U64(u) => serializer.serialize_u64(u),
            Value::I64(u) => serializer.serialize_i64(u),
            Value::Facet(ref facet) => facet.serialize(serializer),
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

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or u32")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Value::U64(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(Value::I64(v))
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
    ///
    /// # Panics
    /// If the value is not of type `Str`
    pub fn text(&self) -> &str {
        match *self {
            Value::Str(ref text) => text,
            _ => panic!("This is not a text field."),
        }
    }

    /// Returns the u64-value, provided the value is of the `U64` type.
    ///
    /// # Panics
    /// If the value is not of type `U64`
    pub fn u64_value(&self) -> u64 {
        match *self {
            Value::U64(ref value) => *value,
            _ => panic!("This is not a text field."),
        }
    }

    /// Returns the i64-value, provided the value is of the `I64` type.
    ///
    /// # Panics
    /// If the value is not of type `I64`
    pub fn i64_value(&self) -> i64 {
        match *self {
            Value::I64(ref value) => *value,
            _ => panic!("This is not a text field."),
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

impl<'a> From<&'a str> for Value {
    fn from(s: &'a str) -> Value {
        Value::Str(s.to_string())
    }
}

impl<'a> From<Facet> for Value {
    fn from(facet: Facet) -> Value {
        Value::Facet(facet)
    }
}

mod binary_serialize {
    use common::BinarySerializable;
    use std::io::{self, Read, Write};
    use super::Value;
    use schema::Facet;

    const TEXT_CODE: u8 = 0;
    const U64_CODE: u8 = 1;
    const I64_CODE: u8 = 2;
    const HIERARCHICAL_FACET_CODE: u8 = 3;

    impl BinarySerializable for Value {
        fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
            match *self {
                Value::Str(ref text) => {
                    TEXT_CODE.serialize(writer)?;
                    text.serialize(writer)
                }
                Value::U64(ref val) => {
                    U64_CODE.serialize(writer)?;
                    val.serialize(writer)
                }
                Value::I64(ref val) => {
                    I64_CODE.serialize(writer)?;
                    val.serialize(writer)
                }
                Value::Facet(ref facet) => {
                    HIERARCHICAL_FACET_CODE.serialize(writer)?;
                    facet.serialize(writer)
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
                HIERARCHICAL_FACET_CODE => Ok(Value::Facet(Facet::deserialize(reader)?)),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("No field type is associated with code {:?}", type_code),
                )),
            }
        }
    }
}
