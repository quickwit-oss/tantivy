//! Document binary deserialization API
//!
//! The deserialization API is strongly inspired by serde's API but with
//! some tweaks, mostly around some of the types being concrete (errors)
//! and some more specific types being visited (Ips, datetime, etc...)
//!
//! The motivation behind this API is to provide a easy to implement and
//! efficient way of deserializing a potentially arbitrarily nested object.

use std::cell::RefCell;
use std::fmt::Display;
use std::io;
use std::net::Ipv6Addr;
use std::sync::Arc;

use columnar::MonotonicallyMappableToU128;
use common::{u64_to_f64, BinaryRefDeserializable, DateTime, RefReader, VInt};

use crate::schema::document::type_codes;
use crate::schema::Field;
use crate::store::DocStoreVersion;

#[derive(Debug, thiserror::Error, Clone)]
/// An error which occurs while attempting to deserialize a given value
/// by using the provided value visitor.
pub enum DeserializeError {
    #[error("Unsupported Type: {0:?} cannot be deserialized from the given visitor")]
    /// The value cannot be deserialized from the given type.
    UnsupportedType(ValueType),
    #[error("Type Mismatch: Expected {expected:?} but found {actual:?}")]
    /// The value cannot be deserialized from the given type.
    TypeMismatch {
        /// The expected value type.
        expected: ValueType,
        /// The actual value type read.
        actual: ValueType,
    },
    #[error("The value could not be read: {0}")]
    /// The value was unable to be read due to the error.
    CorruptedValue(Arc<io::Error>),
    #[error("{0}")]
    /// A custom error message.
    Custom(String),
    #[error("Version {0}, Max version supported: {1}")]
    /// Unsupported version error.
    UnsupportedVersion(u32, u32),
}

impl DeserializeError {
    /// Creates a new custom deserialize error.
    pub fn custom(msg: impl Display) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl From<io::Error> for DeserializeError {
    fn from(error: io::Error) -> Self {
        Self::CorruptedValue(Arc::new(error))
    }
}

/// A convenience trait for deserializing a struct that does not contain borrowed values.
///
/// If your type contains references such as `&str` or `&[u8]` you should use `DocumentDeserialize`
/// instead.
pub trait DocumentDeserialize: Sized {
    /// Attempts to deserialize Self from a given document deserializer.
    fn deserialize<'de, D>(deserializer: &'de D) -> Result<Self, DeserializeError>
    where D: DocumentDeserializer<'de>;
}

/// The core trait for deserializing a document with borrowed values.
///
/// Automatically implemented for all types that implement `DocumentDeserialize`.
pub trait DocumentDeserializeRef<'de>: Sized {
    /// Attempts to deserialize Self from a given document deserializer.
    fn deserialize<'borrow, D>(deserializer: &'borrow D) -> Result<Self, DeserializeError>
    where
        D: DocumentDeserializer<'de>,
        'borrow: 'de;
}

// Implement `DocumentDeserializeRef` for all types that implement `DocumentDeserialize`.
// If a type implements `DocumentDeserialize` that means it owns all values so there is no
// difference between deserializing with owned and borrowed values.
impl<'de, T> DocumentDeserializeRef<'de> for T
where T: DocumentDeserialize
{
    fn deserialize<'borrow, D>(deserializer: &'borrow D) -> Result<Self, DeserializeError>
    where
        D: DocumentDeserializer<'de>,
        'borrow: 'de,
    {
        T::deserialize(deserializer)
    }
}

/// A deserializer that can walk through each entry in the document.
pub trait DocumentDeserializer<'de> {
    /// Resets the deserializer to the start of the document    
    fn reset_position(&self);

    /// A indicator as to how many values are in the document.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next field in the document.
    ///
    /// `RefValue` must be consumed before the next call as it may contain a reference to the
    /// internal position of the deserializer.
    /// This is required in order to allow for nested deserialization of Arrays and Objects
    fn next_field<'a>(&'de self) -> Result<Option<(Field, RefValue<'a>)>, DeserializeError>
    where 'de: 'a;

    /// Expose behaviour from `DocumentDeserializer` as an iterator for convenience.
    ///
    /// This will reset the position of the deserializer to the start of the document when called
    fn iter<T>(&'de self) -> impl Iterator<Item = Result<(Field, T), DeserializeError>>
    where
        T: TryFrom<RefValue<'de>>,
        T::Error: Into<DeserializeError>,
    {
        self.reset_position();
        std::iter::from_fn(|| match self.next_field() {
            Ok(Some((field, value))) => Some(Ok((field, value))),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        })
        .map(|r| r.and_then(|(f, v)| T::try_from(v).map(|t| (f, t)).map_err(Into::into)))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The type of the value attempting to be deserialized.
pub enum ValueType {
    /// A null value.
    Null,
    /// A string value.
    String,
    /// A u64 value.
    U64,
    /// A i64 value.
    I64,
    /// A f64 value.
    F64,
    /// A datetime value.
    DateTime,
    /// A facet value.
    Facet,
    /// A bytes value.
    Bytes,
    /// A IP address value.
    IpAddr,
    /// A boolean value.
    Bool,
    /// A pre-tokenized string value.
    PreTokStr,
    /// An array of value.
    Array,
    /// A dynamic object value.
    Object,
    /// A JSON object value. Deprecated.
    #[deprecated(note = "We keep this for backwards compatibility, use Object instead")]
    JSONObject,
}

/// A container for document field value deserialization.
///
/// Attempts to be efficient by avoiding unnecessary allocations and conversions.
pub enum RefValue<'a> {
    /// A null value.
    Null,
    /// Bool value
    Bool(bool),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
    /// Arbitrarily sized byte array
    Bytes(&'a [u8]),
    /// The str type is used for any text information.
    Str(&'a str),
    /// Facet string needs to match the format of
    /// [Facet::encoded_str](crate::schema::Facet::encoded_str).
    Facet(&'a str),
    /// Pre-tokenized str type
    /// Needs to be a valid JSON string. It will be deserialized as a
    /// [PreTokenizedString](crate::tokenizer::PreTokenizedString).
    PreTokStr(&'a str),
    /// An iterator over a list of values.
    Array(BinaryArrayDeserializer<'a>),
    /// An iterator over a list of key-value pairs.
    Object(BinaryObjectDeserializer<'a>),
    /// Legacy JSON object type.
    #[deprecated(note = "Kept for compatability, use Object instead")]
    JsonObject(serde_json::Map<String, serde_json::Value>),
}

/// Access to a sequence of values which can be deserialized.
pub trait ArrayAccess<'de> {
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next element in the sequence.
    fn next_element<'a>(&mut self) -> Result<Option<RefValue<'a>>, DeserializeError>
    where 'de: 'a;
}

/// TODO: Improve docs
pub trait ObjectAccess<'de> {
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next key-value pair in the object.
    fn next_entry<'a>(&mut self) -> Result<Option<(&'a str, RefValue<'a>)>, DeserializeError>
    where 'de: 'a;
}

/// The document deserializer used to read the tantivy documents serialized with
/// `BinarySerializable`.
///
/// This acts very similarly to serde's deserialize types and can incrementally
/// deserialize each field of the document.
///
/// The `RefReader` owns the bytes of the document and provides a way to incrementally read without
/// requiring a unique mutable reference or invalidating references to the underlying data. This
/// allows for zero-copy deserialization of reference types such as `&str` and `&[u8]`.
pub struct BinaryDocumentDeserializer {
    length: usize,
    position: RefCell<usize>,
    doc_store_version: DocStoreVersion,
    reader: RefReader,
}

impl BinaryDocumentDeserializer {
    /// Attempts to create a new document deserializer from a given reader.
    pub(crate) fn from_reader(
        reader: RefReader,
        doc_store_version: DocStoreVersion,
    ) -> Result<Self, DeserializeError> {
        let length = VInt::deserialize_from_ref(&reader)?;

        Ok(Self {
            length: length.val() as usize,
            position: RefCell::new(0),
            doc_store_version,
            reader,
        })
    }

    /// Returns true if the deserializer has deserialized all the entries
    /// within the document.
    fn is_complete(&self) -> bool {
        let pos = *self.position.borrow();
        pos >= self.length
    }
}

fn deserialize_value<'a, 'b>(
    reader: &'a RefReader,
    doc_store_version: DocStoreVersion,
) -> Result<RefValue<'b>, DeserializeError>
where
    'a: 'b,
{
    let type_code = u8::deserialize_from_ref(reader)?;

    let result = match type_code {
        // Simple types
        type_codes::NULL_CODE => Ok(RefValue::Null),
        type_codes::U64_CODE => u64::deserialize_from_ref(reader).map(RefValue::U64),
        type_codes::I64_CODE => i64::deserialize_from_ref(reader).map(RefValue::I64),
        type_codes::BOOL_CODE => bool::deserialize_from_ref(reader).map(RefValue::Bool),
        type_codes::F64_CODE => u64::deserialize_from_ref(reader)
            .map(u64_to_f64)
            .map(RefValue::F64),

        // Referenced types
        type_codes::BYTES_CODE => <&[u8]>::deserialize_from_ref(reader).map(RefValue::Bytes),
        type_codes::TEXT_CODE => <&str>::deserialize_from_ref(reader).map(RefValue::Str),
        type_codes::HIERARCHICAL_FACET_CODE => {
            <&str>::deserialize_from_ref(reader).map(RefValue::Facet)
        }

        // Types that require additional processing
        type_codes::IP_CODE => u128::deserialize_from_ref(reader)
            .map(Ipv6Addr::from_u128)
            .map(RefValue::IpAddr),

        type_codes::DATE_CODE => i64::deserialize_from_ref(reader)
            .map(|t| match doc_store_version {
                DocStoreVersion::V1 => DateTime::from_timestamp_micros(t),
                DocStoreVersion::V2 => DateTime::from_timestamp_nanos(t),
            })
            .map(RefValue::Date),

        // Extended types
        type_codes::EXT_CODE => {
            let ext_type_code = u8::deserialize_from_ref(reader)?;

            match ext_type_code {
                type_codes::TOK_STR_EXT_CODE => {
                    <&str>::deserialize_from_ref(reader).map(RefValue::PreTokStr)
                }
                _ => {
                    return Err(DeserializeError::from(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("No extended field type is associated with code {ext_type_code:?}"),
                    )))
                }
            }
        }

        // Nested types
        type_codes::ARRAY_CODE => {
            let array_reader = BinaryArrayDeserializer::from_reader(reader, doc_store_version)?;
            Ok(RefValue::Array(array_reader))
        }
        type_codes::OBJECT_CODE => {
            let object_reader = BinaryObjectDeserializer::from_reader(reader, doc_store_version)?;
            Ok(RefValue::Object(object_reader))
        }

        #[expect(deprecated)]
        type_codes::JSON_OBJ_CODE => {
            // This is a compatibility layer
            // The implementation is slow, but is temporary anyways

            // TODO: find a better way to do this, but this field type doesn't include a length
            // header to safely get a slice of the data

            // Make a mutable version of reader
            // SAFETY: reader already uses internal mutability, so there is no change in
            // semantics
            #[allow(invalid_reference_casting)]
            let reader = unsafe { &mut *(reader as *const RefReader as *mut RefReader) };

            let mut de = serde_json::Deserializer::from_reader(reader);
            let json_map =
                <serde_json::Map<String, serde_json::Value> as serde::Deserialize>::deserialize(
                    &mut de,
                )
                .map_err(|err| DeserializeError::Custom(err.to_string()))?;

            Ok(RefValue::JsonObject(json_map))
        }

        // Anything else
        _ => {
            return Err(DeserializeError::from(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("No field type is associated with code {type_code:?}"),
            )))
        }
    };

    result.map_err(DeserializeError::from)
}

impl<'de> DocumentDeserializer<'de> for BinaryDocumentDeserializer {
    #[inline]
    fn size_hint(&self) -> usize {
        self.length
    }

    fn next_field<'a>(&'de self) -> Result<Option<(Field, RefValue<'a>)>, DeserializeError>
    where 'de: 'a {
        if self.is_complete() {
            return Ok(None);
        }

        let field = Field::deserialize_from_ref(&self.reader).map_err(DeserializeError::from)?;
        let value = deserialize_value(&self.reader, self.doc_store_version)?;

        *self.position.borrow_mut() += 1;

        Ok(Some((field, value)))
    }

    fn reset_position(&self) {
        *self.position.borrow_mut() = 0;
    }
}

/// A deserializer for an array of values serialized with `BinarySerializable`.
/// TODO: Improve docs
pub struct BinaryArrayDeserializer<'de> {
    length: usize,
    position: usize,
    reader: &'de RefReader,
    doc_store_version: DocStoreVersion,
}

impl<'de> BinaryArrayDeserializer<'de> {
    /// Attempts to create a new array deserializer from a given reader.
    fn from_reader(
        reader: &'de RefReader,
        doc_store_version: DocStoreVersion,
    ) -> Result<Self, DeserializeError> {
        let length = VInt::deserialize_from_ref(reader)?;

        Ok(Self {
            length: length.val() as usize,
            position: 0,
            reader,
            doc_store_version,
        })
    }

    /// Returns true if the deserializer has deserialized all the elements
    /// within the array.
    fn is_complete(&self) -> bool {
        self.position >= self.length
    }
}

impl<'de> ArrayAccess<'de> for BinaryArrayDeserializer<'de> {
    #[inline]
    fn size_hint(&self) -> usize {
        self.length
    }

    fn next_element<'a>(&mut self) -> Result<Option<RefValue<'a>>, DeserializeError>
    where 'de: 'a {
        if self.is_complete() {
            return Ok(None);
        }

        let value = deserialize_value(self.reader, self.doc_store_version)?;

        // Advance the position cursor.
        self.position += 1;

        Ok(Some(value))
    }
}

/// A deserializer for a object consisting of key-value pairs.
pub struct BinaryObjectDeserializer<'de> {
    /// The inner deserializer.
    ///
    /// Internally an object is just represented by an array
    /// in the format of `[key, value, key, value, key, value]`.
    inner: BinaryArrayDeserializer<'de>,
}

impl<'de> BinaryObjectDeserializer<'de> {
    /// Attempts to create a new object deserializer from a given reader.
    fn from_reader(
        reader: &'de RefReader,
        doc_store_version: DocStoreVersion,
    ) -> Result<Self, DeserializeError> {
        let inner = BinaryArrayDeserializer::from_reader(reader, doc_store_version)?;
        Ok(Self { inner })
    }
}

impl<'de> ObjectAccess<'de> for BinaryObjectDeserializer<'de> {
    #[inline]
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize {
        // We divide by 2 here as we know our elements are going to be
        // in the format of `[key, value, key, value, key, value]`.
        self.inner.size_hint() / 2
    }

    /// Attempts to deserialize the next key-value pair in the object.
    fn next_entry<'a>(&mut self) -> Result<Option<(&'a str, RefValue<'a>)>, DeserializeError>
    where 'de: 'a {
        if self.inner.is_complete() {
            return Ok(None);
        }

        let key = self
            .inner
            .next_element()?
            .and_then(|k| match k {
                RefValue::Str(s) => Some(s),
                _ => None, // Handle unexpected types gracefully
            })
            .expect(
                "Deserializer should not be empty as it is not marked as complete, this is a bug",
            );
        let value = self.inner.next_element()?.expect(
            "Deserializer should not be empty as it is not marked as complete, this is a bug",
        );

        Ok(Some((key, value)))
    }
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use common::OwnedBytes;
    use serde_json::Number;
    use tokenizer_api::Token;

    use super::*;
    use crate::schema::document::existing_type_impls::JsonObjectIter;
    use crate::schema::document::se::BinaryValueSerializer;
    use crate::schema::document::{ReferenceValue, ReferenceValueLeaf};
    use crate::schema::{Facet, OwnedValue, Value};
    use crate::store::DOC_STORE_VERSION;
    use crate::tokenizer::PreTokenizedString;

    fn serialize_value<'a>(value: ReferenceValue<'a, &'a serde_json::Value>) -> Vec<u8> {
        let mut writer = Vec::new();

        let mut serializer = BinaryValueSerializer::new(&mut writer);
        serializer.serialize_value(value).expect("Serialize value");

        writer
    }

    fn serialize_owned_value<'a>(value: ReferenceValue<'a, &'a OwnedValue>) -> Vec<u8> {
        let mut writer = Vec::new();

        let mut serializer = BinaryValueSerializer::new(&mut writer);
        serializer.serialize_value(value).expect("Serialize value");

        writer
    }

    fn deserialize_value(buffer: Vec<u8>) -> crate::schema::OwnedValue {
        let reader = RefReader::new(OwnedBytes::new(buffer));
        super::deserialize_value(&reader, DOC_STORE_VERSION)
            .and_then(|v| v.try_into())
            .expect("Deserialize value")
    }

    #[test]
    fn test_simple_value_serialize() {
        let result = serialize_value(ReferenceValueLeaf::Null.into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::Null);

        let result = serialize_value(ReferenceValueLeaf::Str("hello, world").into());
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Str(String::from("hello, world"))
        );

        let result = serialize_value(ReferenceValueLeaf::U64(123).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::U64(123));

        let result = serialize_value(ReferenceValueLeaf::I64(-123).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::I64(-123));

        let result = serialize_value(ReferenceValueLeaf::F64(123.3845).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::F64(123.3845));

        let result = serialize_value(ReferenceValueLeaf::Bool(false).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::Bool(false));

        let result =
            serialize_value(ReferenceValueLeaf::Date(DateTime::from_timestamp_micros(100)).into());
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Date(DateTime::from_timestamp_micros(100))
        );

        let facet = Facet::from_text("/hello/world").unwrap();
        let result = serialize_value(ReferenceValueLeaf::Facet(facet.encoded_str()).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::Facet(facet));

        let pre_tok_str = PreTokenizedString {
            text: "hello, world".to_string(),
            tokens: vec![Token::default(), Token::default()],
        };
        let result =
            serialize_value(ReferenceValueLeaf::PreTokStr(pre_tok_str.clone().into()).into());
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::PreTokStr(pre_tok_str));
    }

    #[test]
    fn test_array_serialize() {
        let elements = [serde_json::Value::Null, serde_json::Value::Null];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Array(vec![
                crate::schema::OwnedValue::Null,
                crate::schema::OwnedValue::Null,
            ]),
        );

        let elements = [
            serde_json::Value::String("Hello, world".into()),
            serde_json::Value::String("Some demo".into()),
        ];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Array(vec![
                crate::schema::OwnedValue::Str(String::from("Hello, world")),
                crate::schema::OwnedValue::Str(String::from("Some demo")),
            ]),
        );

        let elements = [];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let value = deserialize_value(result);
        assert_eq!(value, crate::schema::OwnedValue::Array(vec![]));

        let elements = [
            serde_json::Value::Null,
            serde_json::Value::String("Hello, world".into()),
            serde_json::Value::Number(12345.into()),
        ];
        let result = serialize_value(ReferenceValue::Array(elements.iter()));
        let value = deserialize_value(result);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Array(vec![
                crate::schema::OwnedValue::Null,
                crate::schema::OwnedValue::Str(String::from("Hello, world")),
                crate::schema::OwnedValue::I64(12345),
            ]),
        );
    }

    #[test]
    fn test_object_serialize() {
        let mut object = serde_json::Map::new();
        object.insert(
            "my-first-key".into(),
            serde_json::Value::String("Hello".into()),
        );
        object.insert("my-second-key".into(), serde_json::Value::Null);
        object.insert(
            "my-third-key".into(),
            serde_json::Value::Number(Number::from_f64(123.0).unwrap()),
        );
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);

        let mut expected_object = BTreeMap::new();
        expected_object.insert(
            "my-first-key".to_string(),
            crate::schema::OwnedValue::Str(String::from("Hello")),
        );
        expected_object.insert("my-second-key".to_string(), crate::schema::OwnedValue::Null);
        expected_object.insert(
            "my-third-key".to_string(),
            crate::schema::OwnedValue::F64(123.0),
        );
        assert_eq!(
            value,
            crate::schema::OwnedValue::Object(expected_object.into_iter().collect())
        );

        let object = serde_json::Map::new();
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);
        let expected_object = BTreeMap::new();
        assert_eq!(
            value,
            crate::schema::OwnedValue::Object(expected_object.into_iter().collect())
        );

        let mut object = serde_json::Map::new();
        object.insert("my-first-key".into(), serde_json::Value::Null);
        object.insert("my-second-key".into(), serde_json::Value::Null);
        object.insert("my-third-key".into(), serde_json::Value::Null);
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);
        let mut expected_object = BTreeMap::new();
        expected_object.insert("my-first-key".to_string(), crate::schema::OwnedValue::Null);
        expected_object.insert("my-second-key".to_string(), crate::schema::OwnedValue::Null);
        expected_object.insert("my-third-key".to_string(), crate::schema::OwnedValue::Null);
        assert_eq!(
            value,
            crate::schema::OwnedValue::Object(expected_object.into_iter().collect())
        );
    }

    #[test]
    fn test_json_compat() {
        let data = [
            8, 123, 34, 107, 101, 121, 97, 58, 34, 58, 34, 98, 108, 117, 98, 34, 44, 34, 118, 97,
            108, 115, 34, 58, 123, 34, 104, 101, 121, 34, 58, 34, 104, 111, 34, 125, 125,
        ]
        .to_vec();
        let expected = json!({
            "keya:": "blub",
            "vals": {
                "hey": "ho"
            }
        });
        let expected_val: OwnedValue = expected.clone().into();

        let value = deserialize_value(data);
        assert_eq!(value, expected_val);
    }

    #[test]
    fn test_nested_date_precision() {
        let object = OwnedValue::Object(vec![(
            "my-date".into(),
            OwnedValue::Date(DateTime::from_timestamp_nanos(323456)),
        )]);
        let result = serialize_owned_value((&object).as_value());
        let value = deserialize_value(result);
        assert_eq!(value, object);
    }

    #[test]
    fn test_nested_serialize() {
        let mut object = serde_json::Map::new();
        object.insert(
            "my-array".into(),
            serde_json::Value::Array(vec![
                serde_json::Value::Null,
                serde_json::Value::String(String::from("bobby of the sea")),
            ]),
        );
        object.insert(
            "my-object".into(),
            serde_json::Value::Object(
                vec![
                    (
                        "inner-1".to_string(),
                        serde_json::Value::Number((-123i64).into()),
                    ),
                    (
                        "inner-2".to_string(),
                        serde_json::Value::String(String::from("bobby of the sea 2")),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        );
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);

        let mut expected_object = BTreeMap::new();
        expected_object.insert(
            "my-array".to_string(),
            crate::schema::OwnedValue::Array(vec![
                crate::schema::OwnedValue::Null,
                crate::schema::OwnedValue::Str(String::from("bobby of the sea")),
            ]),
        );
        expected_object.insert(
            "my-object".to_string(),
            crate::schema::OwnedValue::Object(
                vec![
                    (
                        "inner-1".to_string(),
                        crate::schema::OwnedValue::I64(-123i64),
                    ),
                    (
                        "inner-2".to_string(),
                        crate::schema::OwnedValue::Str(String::from("bobby of the sea 2")),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        );
        assert_eq!(
            value,
            crate::schema::OwnedValue::Object(expected_object.into_iter().collect())
        );

        // Some more extreme nesting that might behave weirdly
        let mut object = serde_json::Map::new();
        object.insert(
            "my-array".into(),
            serde_json::Value::Array(vec![serde_json::Value::Array(vec![
                serde_json::Value::Array(vec![]),
                serde_json::Value::Array(vec![serde_json::Value::Null]),
            ])]),
        );
        let result = serialize_value(ReferenceValue::Object(JsonObjectIter(object.iter())));
        let value = deserialize_value(result);

        let mut expected_object = BTreeMap::new();
        expected_object.insert(
            "my-array".to_string(),
            OwnedValue::Array(vec![OwnedValue::Array(vec![
                OwnedValue::Array(vec![]),
                OwnedValue::Array(vec![OwnedValue::Null]),
            ])]),
        );
        assert_eq!(
            value,
            OwnedValue::Object(expected_object.into_iter().collect())
        );
    }
}
