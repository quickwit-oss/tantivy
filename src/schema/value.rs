use std::collections::{btree_map, BTreeMap};
use std::fmt;
use std::net::Ipv6Addr;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::de::{MapAccess, SeqAccess};

use crate::schema::document::{
    ArrayAccess, DeserializeError, DocValue, ObjectAccess, ReferenceValue, ValueDeserialize,
    ValueDeserializer, ValueVisitor,
};
use crate::schema::Facet;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;

/// Value represents the value of a any field.
/// It is an enum over all over all of the possible field type.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A null value.
    Null,
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
    /// Bool value
    Bool(bool),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    /// Facet
    Facet(Facet),
    /// Arbitrarily sized byte array
    Bytes(Vec<u8>),
    /// A set of values.
    Array(Vec<Self>),
    /// Dynamic object value.
    Object(BTreeMap<String, Self>),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
}

impl<'a> DocValue<'a> for &'a Value {
    type ArrayIter = ArrayIter<'a>;
    type ObjectIter = ObjectMapIter<'a>;

    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match self {
            Value::Null => ReferenceValue::Null,
            Value::Str(val) => ReferenceValue::Str(val),
            Value::PreTokStr(val) => ReferenceValue::PreTokStr(val),
            Value::U64(val) => ReferenceValue::U64(*val),
            Value::I64(val) => ReferenceValue::I64(*val),
            Value::F64(val) => ReferenceValue::F64(*val),
            Value::Bool(val) => ReferenceValue::Bool(*val),
            Value::Date(val) => ReferenceValue::Date(*val),
            Value::Facet(val) => ReferenceValue::Facet(val),
            Value::Bytes(val) => ReferenceValue::Bytes(val),
            Value::IpAddr(val) => ReferenceValue::IpAddr(*val),
            Value::Array(array) => ReferenceValue::Array(ArrayIter(array.iter())),
            Value::Object(object) => ReferenceValue::Object(ObjectMapIter(object.iter())),
        }
    }
}

impl ValueDeserialize for Value {
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        struct Visitor;

        impl ValueVisitor for Visitor {
            type Value = Value;

            fn visit_null(&self) -> Result<Self::Value, DeserializeError> {
                Ok(Value::Null)
            }

            fn visit_string(&self, val: String) -> Result<Self::Value, DeserializeError> {
                Ok(Value::Str(val))
            }

            fn visit_u64(&self, val: u64) -> Result<Self::Value, DeserializeError> {
                Ok(Value::U64(val))
            }

            fn visit_i64(&self, val: i64) -> Result<Self::Value, DeserializeError> {
                Ok(Value::I64(val))
            }

            fn visit_f64(&self, val: f64) -> Result<Self::Value, DeserializeError> {
                Ok(Value::F64(val))
            }

            fn visit_bool(&self, val: bool) -> Result<Self::Value, DeserializeError> {
                Ok(Value::Bool(val))
            }

            fn visit_datetime(&self, val: DateTime) -> Result<Self::Value, DeserializeError> {
                Ok(Value::Date(val))
            }

            fn visit_ip_address(&self, val: Ipv6Addr) -> Result<Self::Value, DeserializeError> {
                Ok(Value::IpAddr(val))
            }

            fn visit_facet(&self, val: Facet) -> Result<Self::Value, DeserializeError> {
                Ok(Value::Facet(val))
            }

            fn visit_bytes(&self, val: Vec<u8>) -> Result<Self::Value, DeserializeError> {
                Ok(Value::Bytes(val))
            }

            fn visit_pre_tokenized_string(
                &self,
                val: PreTokenizedString,
            ) -> Result<Self::Value, DeserializeError> {
                Ok(Value::PreTokStr(val))
            }

            fn visit_array<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
            where A: ArrayAccess<'de> {
                let mut elements = Vec::with_capacity(access.size_hint());

                while let Some(value) = access.next_element()? {
                    elements.push(value);
                }

                Ok(Value::Array(elements))
            }

            fn visit_object<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
            where A: ObjectAccess<'de> {
                let mut elements = BTreeMap::new();

                while let Some((key, value)) = access.next_entry()? {
                    elements.insert(key, value);
                }

                Ok(Value::Object(elements))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl Eq for Value {}

impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match *self {
            Value::Null => serializer.serialize_unit(),
            Value::Str(ref v) => serializer.serialize_str(v),
            Value::PreTokStr(ref v) => v.serialize(serializer),
            Value::U64(u) => serializer.serialize_u64(u),
            Value::I64(u) => serializer.serialize_i64(u),
            Value::F64(u) => serializer.serialize_f64(u),
            Value::Bool(b) => serializer.serialize_bool(b),
            Value::Date(ref date) => time::serde::rfc3339::serialize(&date.into_utc(), serializer),
            Value::Facet(ref facet) => facet.serialize(serializer),
            Value::Bytes(ref bytes) => serializer.serialize_str(&BASE64.encode(bytes)),
            Value::Object(ref obj) => obj.serialize(serializer),
            Value::IpAddr(ref ip_v6) => {
                // Ensure IpV4 addresses get serialized as IpV4, but excluding IpV6 loopback.
                if let Some(ip_v4) = ip_v6.to_ipv4_mapped() {
                    ip_v4.serialize(serializer)
                } else {
                    ip_v6.serialize(serializer)
                }
            }
            Value::Array(ref array) => array.serialize(serializer),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        struct ValueVisitor;

        impl<'de> serde::de::Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string or u32")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
                Ok(Value::Bool(v))
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

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where E: serde::de::Error {
                Ok(Value::Null)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where A: SeqAccess<'de> {
                let mut elements = Vec::with_capacity(seq.size_hint().unwrap_or_default());

                while let Some(value) = seq.next_element()? {
                    elements.push(value);
                }

                Ok(Value::Array(elements))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where A: MapAccess<'de> {
                let mut object = BTreeMap::new();

                while let Some((key, value)) = map.next_entry()? {
                    object.insert(key, value);
                }

                Ok(Value::Object(object))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::Str(s)
    }
}

impl From<Ipv6Addr> for Value {
    fn from(v: Ipv6Addr) -> Value {
        Value::IpAddr(v)
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

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<DateTime> for Value {
    fn from(dt: DateTime) -> Value {
        Value::Date(dt)
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

impl From<Facet> for Value {
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

impl From<BTreeMap<String, Value>> for Value {
    fn from(object: BTreeMap<String, Value>) -> Value {
        Value::Object(object)
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(val) => Self::Bool(val),
            serde_json::Value::Number(number) => {
                if let Some(val) = number.as_u64() {
                    Self::U64(val)
                } else if let Some(val) = number.as_i64() {
                    Self::I64(val)
                } else if let Some(val) = number.as_f64() {
                    Self::F64(val)
                } else {
                    panic!("Unsupported serde_json number {number}");
                }
            }
            serde_json::Value::String(val) => Self::Str(val),
            serde_json::Value::Array(elements) => {
                let converted_elements = elements.into_iter().map(Self::from).collect();
                Self::Array(converted_elements)
            }
            serde_json::Value::Object(object) => Self::from(object),
        }
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for Value {
    fn from(map: serde_json::Map<String, serde_json::Value>) -> Self {
        let mut object = BTreeMap::new();

        for (key, value) in map {
            object.insert(key, Value::from(value));
        }

        Value::Object(object)
    }
}

/// A wrapper type for iterating over a serde_json array producing reference values.
pub struct ArrayIter<'a>(std::slice::Iter<'a, Value>);

impl<'a> Iterator for ArrayIter<'a> {
    type Item = ReferenceValue<'a, &'a Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.0.next()?;
        Some(value.as_value())
    }
}

/// A wrapper type for iterating over a serde_json object producing reference values.
pub struct ObjectMapIter<'a>(btree_map::Iter<'a, String, Value>);

impl<'a> Iterator for ObjectMapIter<'a> {
    type Item = (&'a str, ReferenceValue<'a, &'a Value>);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.0.next()?;
        Some((key.as_str(), value.as_value()))
    }
}

#[cfg(test)]
mod tests {
    use super::Value;
    use crate::schema::{BytesOptions, Schema};
    use crate::time::format_description::well_known::Rfc3339;
    use crate::time::OffsetDateTime;
    use crate::{DateTime, Document};

    #[test]
    fn test_parse_bytes_doc() {
        let mut schema_builder = Schema::builder();
        let bytes_options = BytesOptions::default();
        let bytes_field = schema_builder.add_bytes_field("my_bytes", bytes_options);
        let schema = schema_builder.build();
        let mut doc = Document::default();
        doc.add_bytes(bytes_field, "this is a test".as_bytes());
        let json_string = doc.to_json(&schema);
        assert_eq!(json_string, r#"{"my_bytes":["dGhpcyBpcyBhIHRlc3Q="]}"#);
    }

    #[test]
    fn test_parse_empty_bytes_doc() {
        let mut schema_builder = Schema::builder();
        let bytes_options = BytesOptions::default();
        let bytes_field = schema_builder.add_bytes_field("my_bytes", bytes_options);
        let schema = schema_builder.build();
        let mut doc = Document::default();
        doc.add_bytes(bytes_field, "".as_bytes());
        let json_string = doc.to_json(&schema);
        assert_eq!(json_string, r#"{"my_bytes":[""]}"#);
    }

    #[test]
    fn test_parse_many_bytes_doc() {
        let mut schema_builder = Schema::builder();
        let bytes_options = BytesOptions::default();
        let bytes_field = schema_builder.add_bytes_field("my_bytes", bytes_options);
        let schema = schema_builder.build();
        let mut doc = Document::default();
        doc.add_bytes(
            bytes_field,
            "A bigger test I guess\nspanning on multiple lines\nhoping this will work".as_bytes(),
        );
        let json_string = doc.to_json(&schema);
        assert_eq!(
            json_string,
            r#"{"my_bytes":["QSBiaWdnZXIgdGVzdCBJIGd1ZXNzCnNwYW5uaW5nIG9uIG11bHRpcGxlIGxpbmVzCmhvcGluZyB0aGlzIHdpbGwgd29yaw=="]}"#
        );
    }

    #[test]
    fn test_serialize_date() {
        let value = Value::from(DateTime::from_utc(
            OffsetDateTime::parse("1996-12-20T00:39:57+00:00", &Rfc3339).unwrap(),
        ));
        let serialized_value_json = serde_json::to_string_pretty(&value).unwrap();
        assert_eq!(serialized_value_json, r#""1996-12-20T00:39:57Z""#);
        let value = Value::from(DateTime::from_utc(
            OffsetDateTime::parse("1996-12-20T00:39:57-01:00", &Rfc3339).unwrap(),
        ));
        let serialized_value_json = serde_json::to_string_pretty(&value).unwrap();
        // The time zone information gets lost by conversion into `Value::Date` and
        // implicitly becomes UTC.
        assert_eq!(serialized_value_json, r#""1996-12-20T01:39:57Z""#);
    }
}
