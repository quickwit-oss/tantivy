use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::io;
use std::io::Read;
use std::marker::PhantomData;
use std::net::Ipv6Addr;

use columnar::MonotonicallyMappableToU128;
use common::{u64_to_f64, BinarySerializable, DateTime, VInt};

use crate::schema::document::type_codes;
use crate::schema::{Facet, Field};
use crate::tokenizer::PreTokenizedString;

#[derive(Debug, thiserror::Error)]
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
    CorruptedValue(io::Error),
    #[error("{0}")]
    /// A custom error message.
    Custom(String),
}

impl DeserializeError {
    /// Creates a new custom deserialize error.
    pub fn custom(msg: impl Display) -> Self {
        Self::Custom(msg.to_string())
    }
}

/// The core trait for deserializing a document.
pub trait DocumentDeserialize {
    /// Attempts to deserialize Self from a given document deserializer.
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: DocumentDeserializer<'de>;
}

pub trait DocumentDeserializer<'de> {
    /// A indicator as to how many values are in the document.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next field in the document.
    fn next_field<V: ValueDeserialize>(&mut self) -> Result<(Field, V), DeserializeError>;
}

/// The core trait for deserializing values.
pub trait ValueDeserialize {
    /// Attempts to deserialize Self from a given value deserializer.
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de>;
}

/// A value deserializer.
trait ValueDeserializer<'de> {
    /// Attempts to deserialize a null value from the deserializer.
    fn deserialize_null(self) -> Result<(), DeserializeError>;

    /// Attempts to deserialize a string value from the deserializer.
    fn deserialize_string(self) -> Result<String, DeserializeError>;

    /// Attempts to deserialize a u64 value from the deserializer.
    fn deserialize_u64(self) -> Result<u64, DeserializeError>;

    /// Attempts to deserialize a i64 value from the deserializer.
    fn deserialize_i64(self) -> Result<i64, DeserializeError>;

    /// Attempts to deserialize a f64 value from the deserializer.
    fn deserialize_f64(self) -> Result<f64, DeserializeError>;

    /// Attempts to deserialize a datetime value from the deserializer.
    fn deserialize_datetime(self) -> Result<DateTime, DeserializeError>;

    /// Attempts to deserialize a facet value from the deserializer.
    fn deserialize_facet(self) -> Result<Facet, DeserializeError>;

    /// Attempts to deserialize a bytes value from the deserializer.
    fn deserialize_bytes(self) -> Result<Vec<u8>, DeserializeError>;

    /// Attempts to deserialize a IP address value from the deserializer.
    fn deserialize_ip_address(self) -> Result<Ipv6Addr, DeserializeError>;

    /// Attempts to deserialize a bool value from the deserializer.
    fn deserialize_bool(self) -> Result<bool, DeserializeError>;

    /// Attempts to deserialize a pre-tokenized string value from the deserializer.
    fn deserialize_pre_tokenized_string(self) -> Result<PreTokenizedString, DeserializeError>;

    /// Attempts to deserialize the value using a given visitor.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, DeserializeError>
    where V: ValueVisitor;
}

#[derive(Debug, Copy, Clone)]
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
}

/// A value visitor for deserializing a document value.
///
/// This is strongly inspired by serde but has a few extra types.
pub trait ValueVisitor {
    /// The value produced by the visitor.
    type Value;

    #[inline]
    /// Called when the deserializer visits a string value.
    fn visit_null(&self) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Null))
    }

    #[inline]
    /// Called when the deserializer visits a string value.
    fn visit_string(&self, _val: String) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::String))
    }

    #[inline]
    /// Called when the deserializer visits a u64 value.
    fn visit_u64(&self, _val: u64) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::U64))
    }

    #[inline]
    /// Called when the deserializer visits a i64 value.
    fn visit_i64(&self, _val: i64) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::I64))
    }

    #[inline]
    /// Called when the deserializer visits a f64 value.
    fn visit_f64(&self, _val: f64) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::F64))
    }

    #[inline]
    /// Called when the deserializer visits a bool value.
    fn visit_bool(&self, _val: bool) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Bool))
    }

    #[inline]
    /// Called when the deserializer visits a datetime value.
    fn visit_datetime(&self, _val: DateTime) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::DateTime))
    }

    #[inline]
    /// Called when the deserializer visits a IP address value.
    fn visit_ip_address(&self, _val: Ipv6Addr) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::IpAddr))
    }

    #[inline]
    /// Called when the deserializer visits a facet value.
    fn visit_facet(&self, _val: Facet) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Facet))
    }

    #[inline]
    /// Called when the deserializer visits a bytes value.
    fn visit_bytes(&self, _val: Vec<u8>) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Bytes))
    }

    #[inline]
    /// Called when the deserializer visits a pre-tokenized string value.
    fn visit_pre_tokenized_string(
        &self,
        _val: PreTokenizedString,
    ) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::PreTokStr))
    }

    #[inline]
    /// Called when the deserializer visits an array.
    fn visit_array<'de, A>(&self, _access: A) -> Result<Self::Value, DeserializeError>
    where A: ArrayAccess<'de> {
        Err(DeserializeError::UnsupportedType(ValueType::Array))
    }

    #[inline]
    /// Called when the deserializer visits a object value.
    fn visit_object<'de, A>(&self, _access: A) -> Result<Self::Value, DeserializeError>
    where A: ObjectAccess<'de> {
        Err(DeserializeError::UnsupportedType(ValueType::Object))
    }
}

/// Access to a sequence of values which can be deserialized.
trait ArrayAccess<'de> {
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next element in the sequence.
    fn next_element<V: ValueDeserialize>(&mut self) -> Result<V, DeserializeError>;
}

pub trait ObjectAccess<'de> {
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next key-value pair in the object.
    fn next_entry<V: ValueDeserialize>(&mut self) -> Result<Option<(String, V)>, DeserializeError>;
}

pub struct GenericDocumentDeserializer<'de, R> {
    length: usize,
    position: usize,
    reader: &'de mut R,
}

impl<'de, R> GenericDocumentDeserializer<'de, R>
where
    R: Read
{
    /// Attempts to create a new document deserializer from a given reader.
    fn from_reader(reader: &'de mut R) -> Result<Self, DeserializeError> {
        let length = VInt::deserialize(reader)
            .map_err(DeserializeError::CorruptedValue)?;

        Ok(Self {
            length: length.val() as usize,
            position: 0,
            reader,
        })
    }

    /// Returns true if the deserializer has deserialized all the entries
    /// within the document.
    fn is_complete(&self) -> bool {
        self.position >= self.length
    }
}

impl<'de, R> DocumentDeserializer<'de> for GenericDocumentDeserializer<'de, R>
where
    R: Read
{
    #[inline]
    fn size_hint(&self) -> usize {
        self.length
    }

    fn next_field<V: ValueDeserialize>(&mut self) -> Result<Option<(Field, V)>, DeserializeError> {
        if self.is_complete() {
            return Ok(None)
        }

        let field = Field::deserialize(self.reader)?;

        let deserializer = GenericValueDeserializer::from_reader(self.reader)?;
        let value = V::deserialize(deserializer)?;

        self.position += 1;

        Ok(Some((field, value)))
    }
}

/// A single value deserializer for a given reader.
pub struct GenericValueDeserializer<'de, R> {
    value_type: ValueType,
    reader: &'de mut R,
}

impl<'de, R> GenericValueDeserializer<'de, R>
where
    R: Read
{
    /// Attempts to create a new value deserializer from a given reader.
    fn from_reader(reader: &'de mut R) -> Result<Self, DeserializeError> {
        let type_code = <u8 as BinarySerializable>::deserialize(reader)
            .map_err(DeserializeError::CorruptedValue)?;

        let value_type = match type_code {
            type_codes::TEXT_CODE => ValueType::String,
            type_codes::U64_CODE => ValueType::U64,
            type_codes::I64_CODE => ValueType::I64,
            type_codes::F64_CODE => ValueType::F64,
            type_codes::BOOL_CODE => ValueType::Bool,
            type_codes::DATE_CODE => ValueType::DateTime,
            type_codes::HIERARCHICAL_FACET_CODE => ValueType::Facet,
            type_codes::BYTES_CODE => ValueType::Bytes,
            type_codes::EXT_CODE => {
                let ext_type_code = <u8 as BinarySerializable>::deserialize(reader)
                    .map_err(DeserializeError::CorruptedValue)?;

                match ext_type_code {
                    type_codes::TOK_STR_EXT_CODE => ValueType::PreTokStr,
                    _ => {
                        return Err(DeserializeError::CorruptedValue(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "No extended field type is associated with code {ext_type_code:?}"
                            ),
                        )))
                    }
                }
            }
            type_codes::IP_CODE => ValueType::IpAddr,
            type_codes::NULL_CODE => ValueType::Null,
            type_codes::ARRAY_CODE => ValueType::Array,
            type_codes::OBJECT_CODE => ValueType::Object,
            _ => {
                return Err(DeserializeError::CorruptedValue(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("No field type is associated with code {type_code:?}"),
                )))
            }
        };

        Ok(Self { value_type, reader })
    }

    fn validate_type(&self, expected_type: ValueType) -> Result<(), DeserializeError> {
        if self.value_type == expected_type {
            Ok(())
        } else {
            Err(DeserializeError::TypeMismatch {
                expected: expected_type,
                actual: self.value_type,
            })
        }
    }
}

impl<'de, R> ValueDeserializer<'de> for GenericValueDeserializer<'de, R>
where
    R: Read
{
    fn deserialize_null(self) -> Result<(), DeserializeError> {
        self.validate_type(ValueType::Null)?;
        Ok(())
    }

    fn deserialize_string(self) -> Result<String, DeserializeError> {
        self.validate_type(ValueType::String)?;
        <String as BinarySerializable>::deserialize(self.reader)
            .map_err(DeserializeError::CorruptedValue)
    }

    fn deserialize_u64(self) -> Result<u64, DeserializeError> {
        self.validate_type(ValueType::U64)?;
        <u64 as BinarySerializable>::deserialize(self.reader)
            .map_err(DeserializeError::CorruptedValue)
    }

    fn deserialize_i64(self) -> Result<i64, DeserializeError> {
        self.validate_type(ValueType::I64)?;
        <i64 as BinarySerializable>::deserialize(self.reader)
            .map_err(DeserializeError::CorruptedValue)
    }

    fn deserialize_f64(self) -> Result<f64, DeserializeError> {
        self.validate_type(ValueType::F64)?;
        <u64 as BinarySerializable>::deserialize(self.reader)
            .map(u64_to_f64)
            .map_err(DeserializeError::CorruptedValue)
    }

    fn deserialize_datetime(self) -> Result<DateTime, DeserializeError> {
        self.validate_type(ValueType::DateTime)?;
        <DateTime as BinarySerializable>::deserialize(self.reader)
            .map_err(DeserializeError::CorruptedValue)
    }

    fn deserialize_facet(self) -> Result<Facet, DeserializeError> {
        self.validate_type(ValueType::Facet)?;
        <Facet as BinarySerializable>::deserialize(self.reader)
            .map_err(DeserializeError::CorruptedValue)
    }

    fn deserialize_bytes(self) -> Result<Vec<u8>, DeserializeError> {
        self.validate_type(ValueType::Bytes)?;
        <Vec<u8> as BinarySerializable>::deserialize(self.reader)
            .map_err(DeserializeError::CorruptedValue)
    }

    fn deserialize_ip_address(self) -> Result<Ipv6Addr, DeserializeError> {
        self.validate_type(ValueType::IpAddr)?;
        <u128 as BinarySerializable>::deserialize(self.reader)
            .map(Ipv6Addr::from_u128)
            .map_err(DeserializeError::CorruptedValue)
    }

    fn deserialize_bool(self) -> Result<bool, DeserializeError> {
        self.validate_type(ValueType::Bool)?;
        <bool as BinarySerializable>::deserialize(self.reader)
            .map_err(DeserializeError::CorruptedValue)
    }

    fn deserialize_pre_tokenized_string(self) -> Result<PreTokenizedString, DeserializeError> {
        self.validate_type(ValueType::PreTokStr)?;
        let json_text = <String as BinarySerializable>::deserialize(self.reader)
            .map_err(DeserializeError::CorruptedValue)?;

        if let Ok(value) = serde_json::from_str(&json_text) {
            Ok(value)
        } else {
            Err(DeserializeError::CorruptedValue(io::Error::new(
                io::ErrorKind::Other,
                "Failed to parse string data as PreTokenizedString.",
            )))
        }
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, DeserializeError>
    where V: ValueVisitor {
        match self.value_type {
            ValueType::Null => visitor.visit_null(),
            ValueType::String => {
                let val = self.deserialize_string()?;
                visitor.visit_null(val)
            }
            ValueType::U64 => {
                let val = self.deserialize_u64()?;
                visitor.visit_null(val)
            }
            ValueType::I64 => {
                let val = self.deserialize_i64()?;
                visitor.visit_null(val)
            }
            ValueType::F64 => {
                let val = self.deserialize_f64()?;
                visitor.visit_null(val)
            }
            ValueType::DateTime => {
                let val = self.deserialize_datetime()?;
                visitor.visit_null(val)
            }
            ValueType::Facet => {
                let val = self.deserialize_facet()?;
                visitor.visit_null(val)
            }
            ValueType::Bytes => {
                let val = self.deserialize_bytes()?;
                visitor.visit_null(val)
            }
            ValueType::IpAddr => {
                let val = self.deserialize_ip_address()?;
                visitor.visit_null(val)
            }
            ValueType::Bool => {
                let val = self.deserialize_bool()?;
                visitor.visit_null(val)
            }
            ValueType::PreTokStr => {
                let val = self.deserialize_pre_tokenized_string()?;
                visitor.visit_null(val)
            }
            ValueType::Array => {
                let access = ArrayDeserializer::from_reader(self.reader)?;
                visitor.visit_array(access)
            }
            ValueType::Object => {
                let access = ObjectDeserializer::from_reader(self.reader)?;
                visitor.visit_object(access)
            }
        }
    }
}

/// A deserializer for an array of values.
pub struct ArrayDeserializer<'de, R> {
    length: usize,
    position: usize,
    reader: &'de mut R,
}

impl<'de, R> ArrayDeserializer<'de, R>
where R: Read
{
    /// Attempts to create a new array deserializer from a given reader.
    fn from_reader(reader: &'de mut R) -> Result<Self, DeserializeError> {
        let length = <VInt as BinarySerializable>::deserialize(reader)
            .map_err(DeserializeError::CorruptedValue)?;

        Ok(Self {
            length: length.val() as usize,
            position: 0,
            reader,
        })
    }

    /// Returns true if the deserializer has deserialized all the elements
    /// within the array.
    fn is_complete(&self) -> bool {
        self.position >= self.length
    }
}

impl<'de, R> ArrayAccess<'de> for ArrayDeserializer<'de, R>
where R: Read
{
    #[inline]
    fn size_hint(&self) -> usize {
        self.length
    }

    fn next_element<V: ValueDeserialize>(&mut self) -> Result<V, DeserializeError> {
        let deserializer = GenericValueDeserializer::from_reader(self.reader)?;
        let value = V::deserialize(deserializer)?;

        // Advance the position cursor.
        self.position += 1;

        Ok(value)
    }
}

/// A deserializer for a object consisting of key-value pairs.
pub struct ObjectDeserializer<'de, R> {
    /// The inner deserializer.
    ///
    /// Internally an object is just represented by an array
    /// in the format of `[key, value, key, value, key, value]`.
    inner: ArrayDeserializer<'de, R>,
}

impl<'de, R> ObjectDeserializer<'de, R>
where R: Read
{
    /// Attempts to create a new object deserializer from a given reader.
    fn from_reader(reader: &'de mut R) -> Result<Self, DeserializeError> {
        let inner = ArrayDeserializer::from_reader(reader)?;
        Ok(Self { inner })
    }
}

impl<'de, R> ObjectAccess<'de> for ObjectDeserializer<'de, R>
where R: Read
{
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
    fn next_entry<V: ValueDeserialize>(&mut self) -> Result<Option<(String, V)>, DeserializeError> {
        if self.inner.is_complete() {
            return Ok(None);
        }

        let key = self.inner.next_element()?;
        let value = self.inner.next_element()?;

        Ok(Some((key, value)))
    }
}

// Core type implementations

impl ValueDeserialize for String {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_string()
    }
}

impl ValueDeserialize for u64 {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_u64()
    }
}

impl ValueDeserialize for i64 {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_i64()
    }
}

impl ValueDeserialize for f64 {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_f64()
    }
}

impl ValueDeserialize for DateTime {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_datetime()
    }
}

impl ValueDeserialize for Ipv6Addr {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_ip_address()
    }
}

impl ValueDeserialize for Facet {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_facet()
    }
}

impl ValueDeserialize for Vec<u8> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_bytes()
    }
}

impl ValueDeserialize for PreTokenizedString {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_pre_tokenized_string()
    }
}

// Collections kind of suck, but can't think of a nicer way of doing this generically
// without quite literally cloning serde entirely...

#[derive(Default)]
struct VecVisitor<T: ValueDeserialize>(PhantomData<T>);
impl<T: ValueDeserialize> ValueVisitor for VecVisitor<T> {
    type Value = Vec<T>;

    fn visit_array<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
    where A: ArrayAccess<'de> {
        let mut entries = Vec::with_capacity(access.size_hint());
        while let Some(value) = access.next_element()? {
            entries.push(value);
        }
        Ok(entries)
    }
}
impl<T: ValueDeserialize> ValueDeserialize for Vec<T> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_any(VecVisitor::default())
    }
}

#[derive(Default)]
struct BTreeMapVisitor<T: ValueDeserialize>(PhantomData<T>);
impl<T: ValueDeserialize> ValueVisitor for BTreeMapVisitor<T> {
    type Value = BTreeMap<String, T>;

    fn visit_object<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
    where A: ObjectAccess<'de> {
        let mut entries = BTreeMap::new();
        while let Some((key, value)) = access.next_entry()? {
            entries.insert(key, value);
        }
        Ok(entries)
    }
}
impl<T: ValueDeserialize> ValueDeserialize for BTreeMap<String, T> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_any(BTreeMapVisitor::default())
    }
}

#[derive(Default)]
struct HashMapVisitor<T: ValueDeserialize>(PhantomData<T>);
impl<T: ValueDeserialize> ValueVisitor for HashMapVisitor<T> {
    type Value = HashMap<String, T>;

    fn visit_object<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
    where A: ObjectAccess<'de> {
        let mut entries = HashMap::with_capacity(access.size_hint());
        while let Some((key, value)) = access.next_entry()? {
            entries.insert(key, value);
        }
        Ok(entries)
    }
}
impl<T: ValueDeserialize> ValueDeserialize for HashMap<String, T> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_any(HashMapVisitor::default())
    }
}

#[derive(Default)]
struct KeyValuesVecVisitor<T: ValueDeserialize>(PhantomData<T>);
impl<T: ValueDeserialize> ValueVisitor for KeyValuesVecVisitor<T> {
    type Value = Vec<(String, T)>;

    fn visit_object<'de, A>(&self, mut access: A) -> Result<Self::Value, DeserializeError>
    where A: ObjectAccess<'de> {
        let mut entries = Vec::with_capacity(access.size_hint());
        while let Some(entry) = access.next_entry()? {
            entries.push(entry);
        }
        Ok(entries)
    }
}
impl<T: ValueDeserialize> ValueDeserialize for Vec<(String, T)> {
    #[inline]
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where D: ValueDeserializer<'de> {
        deserializer.deserialize_any(KeyValuesVecVisitor::default())
    }
}