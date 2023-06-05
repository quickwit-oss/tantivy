use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::mem;
use std::net::Ipv6Addr;

use common::{BinarySerializable, VInt};
use serde::ser::Error;
use serde::Serializer;
use serde_json::Map;

use super::*;
use crate::schema::field_value::FieldValueIter;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;

/// The core trait representing a document within the index.
pub trait DocumentAccess: Send + Sync + 'static {
    /// The value of the field.
    type Value<'a>: DocValue<'a>
    where Self: 'a;
    /// The owned version of a value type.
    ///
    /// It's possible that this is the same type as the borrowed
    /// variant simply by using things like a `Cow`, but it may
    /// be beneficial to implement them as separate types for
    /// some use cases.
    type OwnedValue: ValueDeserialize + Debug;
    /// The iterator over all of the fields and values within the doc.
    type FieldsValuesIter<'a>: Iterator<Item = (Field, Self::Value<'a>)>
    where Self: 'a;

    /// Returns the number of fields within the document.
    fn len(&self) -> usize;

    /// Returns true if the document contains no fields.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get an iterator iterating over all fields and values in a document.
    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_>;

    /// Create a new document from a given stream of fields.
    fn from_fields(fields: Vec<(Field, Self::OwnedValue)>) -> Self;
}

pub trait DocValue<'a>: Debug {
    /// The visitor used to walk through the key-value pairs
    /// of the provided JSON object.
    type JsonVisitor: JsonVisitor<'a>;

    /// Get the string contents of the value if applicable.
    ///
    /// If the value is not a string, `None` should be returned.
    fn as_str(&self) -> Option<&str>;
    /// Get the facet contents of the value if applicable.
    ///
    /// If the value is not a string, `None` should be returned.
    fn as_facet(&self) -> Option<&Facet>;
    /// Get the u64 contents of the value if applicable.
    ///
    /// If the value is not a u64, `None` should be returned.
    fn as_u64(&self) -> Option<u64>;
    /// Get the i64 contents of the value if applicable.
    ///
    /// If the value is not a i64, `None` should be returned.
    fn as_i64(&self) -> Option<i64>;
    /// Get the f64 contents of the value if applicable.
    ///
    /// If the value is not a f64, `None` should be returned.
    fn as_f64(&self) -> Option<f64>;
    /// Get the date contents of the value if applicable.
    ///
    /// If the value is not a date, `None` should be returned.
    fn as_date(&self) -> Option<DateTime>;
    /// Get the bool contents of the value if applicable.
    ///
    /// If the value is not a boolean, `None` should be returned.
    fn as_bool(&self) -> Option<bool>;
    /// Get the IP addr contents of the value if applicable.
    ///
    /// If the value is not an IP addr, `None` should be returned.
    fn as_ip_addr(&self) -> Option<Ipv6Addr>;
    /// Get the bytes contents of the value if applicable.
    ///
    /// If the value is not bytes, `None` should be returned.
    fn as_bytes(&self) -> Option<&[u8]>;
    /// Get the pre-tokenized string contents of the value if applicable.
    ///
    /// If the value is not pre-tokenized string, `None` should be returned.
    fn as_tokenized_text(&self) -> Option<&PreTokenizedString>;
    /// Get the JSON contents of the value if applicable.
    ///
    /// If the value is not pre-tokenized string, `None` should be returned.
    fn as_json(&self) -> Option<Self::JsonVisitor>;
}

/// The deserializer trait for deserialization of a value from the
/// doc store.
pub trait ValueDeserialize {
    /// Create a new value from a given string.
    fn from_str(s: String) -> Self;

    /// Create a new value from a given u64.
    fn from_u64(v: u64) -> Self;

    /// Create a new value from a given i64.
    fn from_i64(v: i64) -> Self;

    /// Create a new value from a given f64.
    fn from_f64(v: f64) -> Self;

    /// Create a new value from a given boolean.
    fn from_bool(v: bool) -> Self;

    /// Create a new value from a given datetime.
    fn from_date(dt: DateTime) -> Self;

    /// Create a new value from a facet.
    fn from_facet(facet: Facet) -> Self;

    /// Create a new value from a pre-tokenized string.
    fn from_pre_tok_str(v: PreTokenizedString) -> Self;

    // TODO: Add this probably is better off being passed a serde deserializer ->
    //       Requires a re-work of the binary serialization trait first though in order
    //       for error handling to be nicer.
    /// Create a new value from a given JSON map.
    fn from_json(map: Map<String, serde_json::Value>) -> Self;

    /// Create a new value from a given IP addresses.
    fn from_ip_addr(v: Ipv6Addr) -> Self;

    /// Create a new value from some bytes.
    fn from_bytes(bytes: Vec<u8>) -> Self;
}

/// A trait representing a JSON value.
///
/// This allows for access to the JSON value without having it as a
/// concrete type.
///
/// This is largely a sub-set of the `Value<'a>` trait.
pub trait JsonValueVisitor<'a> {
    /// The iterator for walking through the element within the array.
    type ArrayIter: Iterator<Item = Self>;
    /// The visitor for walking through the key-value pairs within
    /// the object.
    type ObjectVisitor: JsonVisitor<'a>;

    /// Checks if the value is `null` or not.
    fn is_null(&self) -> bool;
    /// Get the string contents of the value if applicable.
    ///
    /// If the value is not a string, `None` should be returned.
    fn as_str(&self) -> Option<&str>;
    /// Get the i64 contents of the value if applicable.
    ///
    /// If the value is not a i64, `None` should be returned.
    fn as_i64(&self) -> Option<i64>;
    /// Get the u64 contents of the value if applicable.
    ///
    /// If the value is not a u64, `None` should be returned.
    fn as_u64(&self) -> Option<u64>;
    /// Get the f64 contents of the value if applicable.
    ///
    /// If the value is not a f64, `None` should be returned.
    fn as_f64(&self) -> Option<f64>;
    /// Get the bool contents of the value if applicable.
    ///
    /// If the value is not a boolean, `None` should be returned.
    fn as_bool(&self) -> Option<bool>;
    /// Get the array contents of the value if applicable.
    ///
    /// If the value is not an array, `None` should be returned.
    fn as_array(&self) -> Option<Self::ArrayIter>;
    /// Get the object contents of the value if applicable.
    ///
    /// If the value is not an object, `None` should be returned.
    fn as_object(&self) -> Option<Self::ObjectVisitor>;
}

/// A trait representing a JSON key-value object.
///
/// This allows for access to the JSON object without having it as a
/// concrete type.
pub trait JsonVisitor<'a> {
    /// The visitor for each value within the object.
    type ValueVisitor: JsonValueVisitor<'a>;

    #[inline]
    /// The size hint of the iterator length.
    fn size_hint(&self) -> usize {
        0
    }

    /// Get the next key-value pair in the object or return `None`
    /// if the element is empty.
    fn next_key_value(&mut self) -> Option<(&'a str, Self::ValueVisitor)>;
}

pub mod doc_binary_wrappers {
    use super::*;

    /// Serializes stored field values.
    pub fn serialize_stored<T, W>(document: &T, schema: &Schema, writer: &mut W) -> io::Result<()>
    where
        T: DocumentAccess,
        W: Write + ?Sized,
    {
        let stored_field_values = || {
            document
                .iter_fields_and_values()
                .filter(|(field, _)| schema.get_field_entry(*field).is_stored())
        };

        let num_field_values = stored_field_values().count();

        VInt(num_field_values as u64).serialize(writer)?;
        for (field, value) in stored_field_values() {
            let value = value as <T as DocumentAccess>::Value<'_>;

            field.serialize(writer)?;
            value::serialize(&value, writer)?;
        }

        Ok(())
    }

    pub fn serialize<T, W>(value: &T, writer: &mut W) -> io::Result<()>
    where
        T: DocumentAccess,
        W: Write + ?Sized,
    {
        VInt(value.len() as u64).serialize(writer)?;

        for (field, value) in value.iter_fields_and_values() {
            field.serialize(writer)?;
            value::serialize(&value, writer)?;
        }

        Ok(())
    }

    pub fn deserialize<T, R>(reader: &mut R) -> io::Result<T>
    where
        T: DocumentAccess,
        R: Read,
    {
        let num_field_values = VInt::deserialize(reader)?.val() as usize;

        let mut field_values = Vec::with_capacity(num_field_values);
        for _ in 0..num_field_values {
            let field = Field::deserialize(reader)?;
            let value: T::OwnedValue = value::deserialize(reader)?;

            field_values.push((field, value));
        }

        Ok(T::from_fields(field_values))
    }
}

/// Tantivy's Document is the object that can
/// be indexed and then searched for.
///
/// Documents are fundamentally a collection of unordered couples `(field, value)`.
/// In this list, one field may appear more than once.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct Document {
    field_values: Vec<FieldValue>,
}

impl DocumentAccess for Document {
    type Value<'a> = &'a Value;
    type OwnedValue = Value;
    type FieldsValuesIter<'a> = FieldValueIter<'a>;

    fn len(&self) -> usize {
        self.field_values.len()
    }

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldValueIter(self.field_values.iter())
    }

    fn from_fields(fields: Vec<(Field, Self::OwnedValue)>) -> Self {
        Self {
            field_values: fields
                .into_iter()
                .map(|(field, value)| FieldValue::new(field, value))
                .collect(),
        }
    }
}

impl From<Vec<FieldValue>> for Document {
    fn from(field_values: Vec<FieldValue>) -> Self {
        Self { field_values }
    }
}

impl PartialEq for Document {
    fn eq(&self, other: &Self) -> bool {
        // super slow, but only here for tests
        let convert_to_comparable_map = |field_values: &[FieldValue]| {
            let mut field_value_set: HashMap<Field, HashSet<String>> = Default::default();
            for field_value in field_values.iter() {
                let value = serde_json::to_string(field_value.value()).unwrap();
                field_value_set
                    .entry(field_value.field())
                    .or_default()
                    .insert(value);
            }
            field_value_set
        };
        let self_field_values: HashMap<Field, HashSet<String>> =
            convert_to_comparable_map(&self.field_values);
        let other_field_values: HashMap<Field, HashSet<String>> =
            convert_to_comparable_map(&other.field_values);
        self_field_values.eq(&other_field_values)
    }
}

impl Eq for Document {}

impl IntoIterator for Document {
    type Item = FieldValue;

    type IntoIter = std::vec::IntoIter<FieldValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.field_values.into_iter()
    }
}

impl Document {
    /// Creates a new, empty document object
    pub fn new() -> Document {
        Document::default()
    }

    /// Adding a facet to the document.
    pub fn add_facet<F>(&mut self, field: Field, path: F)
    where Facet: From<F> {
        let facet = Facet::from(path);
        let value = Value::Facet(facet);
        self.add_field_value(field, value);
    }

    /// Add a text field.
    pub fn add_text<S: ToString>(&mut self, field: Field, text: S) {
        let value = Value::Str(text.to_string());
        self.add_field_value(field, value);
    }

    /// Add a pre-tokenized text field.
    pub fn add_pre_tokenized_text(&mut self, field: Field, pre_tokenized_text: PreTokenizedString) {
        self.add_field_value(field, pre_tokenized_text);
    }

    /// Add a u64 field
    pub fn add_u64(&mut self, field: Field, value: u64) {
        self.add_field_value(field, value);
    }

    /// Add a IP address field. Internally only Ipv6Addr is used.
    pub fn add_ip_addr(&mut self, field: Field, value: Ipv6Addr) {
        self.add_field_value(field, value);
    }

    /// Add a i64 field
    pub fn add_i64(&mut self, field: Field, value: i64) {
        self.add_field_value(field, value);
    }

    /// Add a f64 field
    pub fn add_f64(&mut self, field: Field, value: f64) {
        self.add_field_value(field, value);
    }

    /// Add a bool field
    pub fn add_bool(&mut self, field: Field, value: bool) {
        self.add_field_value(field, value);
    }

    /// Add a date field with unspecified time zone offset
    pub fn add_date(&mut self, field: Field, value: DateTime) {
        self.add_field_value(field, value);
    }

    /// Add a bytes field
    pub fn add_bytes<T: Into<Vec<u8>>>(&mut self, field: Field, value: T) {
        self.add_field_value(field, value.into());
    }

    /// Add a JSON field
    pub fn add_json_object(&mut self, field: Field, json_object: Map<String, serde_json::Value>) {
        self.add_field_value(field, json_object);
    }

    /// Add a (field, value) to the document.
    pub fn add_field_value<T: Into<Value>>(&mut self, field: Field, typed_val: T) {
        let value = typed_val.into();
        let field_value = FieldValue { field, value };
        self.field_values.push(field_value);
    }

    /// field_values accessor
    pub fn field_values(&self) -> &[FieldValue] {
        &self.field_values
    }

    /// Sort and groups the field_values by field.
    ///
    /// The result of this method is not cached and is
    /// computed on the fly when this method is called.
    pub fn get_sorted_field_values(&self) -> Vec<(Field, Vec<&Value>)> {
        let mut field_values: Vec<&FieldValue> = self.field_values().iter().collect();
        field_values.sort_by_key(|field_value| field_value.field());

        let mut field_values_it = field_values.into_iter();

        let first_field_value = if let Some(first_field_value) = field_values_it.next() {
            first_field_value
        } else {
            return Vec::new();
        };

        let mut grouped_field_values = vec![];
        let mut current_field = first_field_value.field();
        let mut current_group = vec![first_field_value.value()];

        for field_value in field_values_it {
            if field_value.field() == current_field {
                current_group.push(field_value.value());
            } else {
                grouped_field_values.push((
                    current_field,
                    mem::replace(&mut current_group, vec![field_value.value()]),
                ));
                current_field = field_value.field();
            }
        }

        grouped_field_values.push((current_field, current_group));
        grouped_field_values
    }

    /// Returns all of the `FieldValue`s associated the given field
    pub fn get_all(&self, field: Field) -> impl Iterator<Item = &Value> {
        self.field_values
            .iter()
            .filter(move |field_value| field_value.field() == field)
            .map(FieldValue::value)
    }

    /// Returns the first `FieldValue` associated the given field
    pub fn get_first(&self, field: Field) -> Option<&Value> {
        self.get_all(field).next()
    }
}

// Visitor implementations for compatibility with
// the owned document approach.
impl<'a> JsonVisitor<'a> for serde_json::map::Iter<'a> {
    type ValueVisitor = &'a serde_json::Value;

    #[inline]
    fn size_hint(&self) -> usize {
        self.len()
    }

    #[inline]
    fn next_key_value(&mut self) -> Option<(&'a str, Self::ValueVisitor)> {
        self.next().map(|v| (v.0.as_str(), v.1))
    }
}

impl<'a> JsonValueVisitor<'a> for &'a serde_json::Value {
    type ArrayIter = std::slice::Iter<'a, serde_json::Value>;
    type ObjectVisitor = serde_json::map::Iter<'a>;

    #[inline]
    fn is_null(&self) -> bool {
        <serde_json::Value>::is_null(self)
    }

    #[inline]
    fn as_str(&self) -> Option<&str> {
        <serde_json::Value>::as_str(self)
    }

    #[inline]
    fn as_i64(&self) -> Option<i64> {
        <serde_json::Value>::as_i64(self)
    }

    #[inline]
    fn as_u64(&self) -> Option<u64> {
        <serde_json::Value>::as_u64(self)
    }

    #[inline]
    fn as_f64(&self) -> Option<f64> {
        <serde_json::Value>::as_f64(self)
    }

    #[inline]
    fn as_bool(&self) -> Option<bool> {
        <serde_json::Value>::as_bool(self)
    }

    #[inline]
    fn as_array(&self) -> Option<Self::ArrayIter> {
        Some(<serde_json::Value>::as_array(self)?.iter())
    }

    #[inline]
    fn as_object(&self) -> Option<Self::ObjectVisitor> {
        Some(<serde_json::Value>::as_object(self)?.iter())
    }
}

// Utility wrappers
//

// Copied from serde:
// We only use our own error type; no need for From conversions provided by the
// standard library's try! macro. This reduces lines of LLVM IR by 4%.
macro_rules! tri {
    ($e:expr $(,)?) => {
        match $e {
            core::result::Result::Ok(val) => val,
            core::result::Result::Err(err) => return core::result::Result::Err(err),
        }
    };
}

/// A wrapper type that supports serializing any value which implements
/// the JSON visitor trait.
pub struct SerializeJsonWrapper<'a, T: JsonValueVisitor<'a>> {
    visitor: T,
    phantom: PhantomData<&'a ()>,
}

impl<'a, T: JsonValueVisitor<'a>> From<T> for SerializeJsonWrapper<'a, T> {
    fn from(visitor: T) -> Self {
        Self {
            visitor,
            phantom: PhantomData,
        }
    }
}

impl<'a, T: JsonValueVisitor<'a>> serde::Serialize for SerializeJsonWrapper<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        use serde::ser::{SerializeMap, SerializeSeq};

        if self.visitor.is_null() {
            return serializer.serialize_none();
        }

        if let Some(v) = self.visitor.as_str() {
            return <str as serde::Serialize>::serialize(&v, serializer);
        }

        if let Some(v) = self.visitor.as_u64() {
            return <u64 as serde::Serialize>::serialize(&v, serializer);
        }

        if let Some(v) = self.visitor.as_i64() {
            return <i64 as serde::Serialize>::serialize(&v, serializer);
        }

        if let Some(v) = self.visitor.as_f64() {
            return <f64 as serde::Serialize>::serialize(&v, serializer);
        }

        if let Some(v) = self.visitor.as_bool() {
            return <bool as serde::Serialize>::serialize(&v, serializer);
        }

        if let Some(elements) = self.visitor.as_array() {
            let mut seq = tri!(serializer.serialize_seq(elements.size_hint().1));

            for value in elements {
                let wrapper = SerializeJsonWrapper::from(value);
                tri!(seq.serialize_element(&wrapper));
            }

            return seq.end();
        }

        if let Some(mut object) = self.visitor.as_object() {
            let mut map = tri!(serializer.serialize_map(Some(object.size_hint())));
            while let Some((key, value)) = object.next_key_value() {
                let wrapped = SerializeJsonWrapper::from(value);
                tri!(map.serialize_entry(key, &wrapped))
            }

            return map.end();
        }

        Err(Error::custom("Visitor provided no serializable value"))
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::document::doc_binary_wrappers;
    use crate::schema::*;

    #[test]
    fn test_doc() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("title", TEXT);
        let mut doc = Document::default();
        doc.add_text(text_field, "My title");
        assert_eq!(doc.field_values().len(), 1);
    }

    #[test]
    fn test_doc_serialization_issue() {
        let mut doc = Document::default();
        doc.add_json_object(
            Field::from_field_id(0),
            serde_json::json!({"key": 2u64})
                .as_object()
                .unwrap()
                .clone(),
        );
        doc.add_text(Field::from_field_id(1), "hello");
        assert_eq!(doc.field_values().len(), 2);
        let mut payload: Vec<u8> = Vec::new();
        doc_binary_wrappers::serialize(&doc, &mut payload).unwrap();
        assert_eq!(payload.len(), 26);
        doc_binary_wrappers::deserialize::<Document, _>(&mut &payload[..]).unwrap();
    }
}
