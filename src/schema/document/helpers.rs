//! Implementations of some of the core traits on varius types to improve the ergonomics
//! of the API when providing custom documents.
//!
//! This allows users a bit more freedom and ergonomics if they want a simple API
//! and don't care about some of the more specialised types or only want to customise
//! part of the document structure.
use std::collections::{btree_map, hash_map, BTreeMap, HashMap};

use crate::schema::document::{
    DeserializeError, DocValue, DocumentAccess, DocumentDeserialize, DocumentDeserializer,
    ReferenceValue,
};
use crate::schema::Field;

// Serde compatibility support.
impl<'a> DocValue<'a> for &'a serde_json::Value {
    type ArrayIter = JsonArrayIter<'a>;
    type ObjectIter = JsonObjectIter<'a>;

    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match self {
            serde_json::Value::Null => ReferenceValue::Null,
            serde_json::Value::Bool(value) => ReferenceValue::Bool(*value),
            serde_json::Value::Number(number) => {
                if let Some(val) = number.as_u64() {
                    ReferenceValue::U64(val)
                } else if let Some(val) = number.as_i64() {
                    ReferenceValue::I64(val)
                } else if let Some(val) = number.as_f64() {
                    ReferenceValue::F64(val)
                } else {
                    panic!("Unsupported serde_json number {number}");
                }
            }
            serde_json::Value::String(val) => ReferenceValue::Str(val),
            serde_json::Value::Array(elements) => {
                ReferenceValue::Array(JsonArrayIter(elements.iter()))
            }
            serde_json::Value::Object(object) => {
                ReferenceValue::Object(JsonObjectIter(object.iter()))
            }
        }
    }
}

/// A wrapper struct for an interator producing [Value]s.
pub struct JsonArrayIter<'a>(std::slice::Iter<'a, serde_json::Value>);

impl<'a> Iterator for JsonArrayIter<'a> {
    type Item = ReferenceValue<'a, &'a serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.0.next()?;
        Some(value.as_value())
    }
}

/// A wrapper struct for an interator producing [Value]s.
pub struct JsonObjectIter<'a>(serde_json::map::Iter<'a>);

impl<'a> Iterator for JsonObjectIter<'a> {
    type Item = (&'a str, ReferenceValue<'a, &'a serde_json::Value>);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.0.next()?;
        Some((key, value.as_value()))
    }
}

// Custom document types

// BTreeMap based documents
impl DocumentAccess for BTreeMap<Field, crate::schema::Value> {
    type Value<'a> = &'a crate::schema::Value;
    type FieldsValuesIter<'a> = FieldCopyingIterator<
        'a,
        btree_map::Iter<'a, Field, crate::schema::Value>,
        crate::schema::Value,
    >;

    fn len(&self) -> usize {
        self.len()
    }

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldCopyingIterator(self.iter())
    }
}
impl DocumentDeserialize for BTreeMap<Field, crate::schema::Value> {
    fn deserialize<'de, D>(mut deserializer: D) -> Result<Self, DeserializeError>
    where D: DocumentDeserializer<'de> {
        let mut document = BTreeMap::new();

        while let Some((field, value)) = deserializer.next_field()? {
            document.insert(field, value);
        }

        Ok(document)
    }
}

// HashMap based documents
impl DocumentAccess for HashMap<Field, crate::schema::Value> {
    type Value<'a> = &'a crate::schema::Value;
    type FieldsValuesIter<'a> = FieldCopyingIterator<
        'a,
        hash_map::Iter<'a, Field, crate::schema::Value>,
        crate::schema::Value,
    >;

    fn len(&self) -> usize {
        self.len()
    }

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldCopyingIterator(self.iter())
    }
}
impl DocumentDeserialize for HashMap<Field, crate::schema::Value> {
    fn deserialize<'de, D>(mut deserializer: D) -> Result<Self, DeserializeError>
    where D: DocumentDeserializer<'de> {
        let mut document = HashMap::with_capacity(deserializer.size_hint());

        while let Some((field, value)) = deserializer.next_field()? {
            document.insert(field, value);
        }

        Ok(document)
    }
}

pub struct FieldCopyingIterator<'a, I, V>(I)
where
    V: 'a,
    I: Iterator<Item = (&'a Field, &'a V)>;

impl<'a, I, V> Iterator for FieldCopyingIterator<'a, I, V>
where
    V: 'a,
    I: Iterator<Item = (&'a Field, &'a V)>,
{
    type Item = (Field, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let (field, value) = self.0.next()?;
        Some((*field, value))
    }
}
