use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::Ipv6Addr;

use common::DateTime;
use serde_json::Map;

use crate::schema::document::{
    DeserializeError, Document, DocumentDeserialize, DocumentDeserializer,
};
use crate::schema::field_type::ValueParsingError;
use crate::schema::field_value::FieldValueIter;
use crate::schema::{Facet, Field, FieldValue, NamedFieldDocument, OwnedValue, Schema};
use crate::tokenizer::PreTokenizedString;

/// Tantivy's Document is the object that can be indexed and then searched for.
/// It provides a default implementation of the `Document` trait.
///
/// Documents are fundamentally a collection of unordered couples `(field, value)`.
/// In this list, one field may appear more than once.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct TantivyDocument {
    field_values: Vec<FieldValue>,
}

impl Document for TantivyDocument {
    type Value<'a> = &'a OwnedValue;
    type FieldsValuesIter<'a> = FieldValueIter<'a>;

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldValueIter(self.field_values.iter())
    }
}

impl DocumentDeserialize for TantivyDocument {
    fn deserialize<'de, D>(mut deserializer: D) -> Result<Self, DeserializeError>
    where D: DocumentDeserializer<'de> {
        let mut field_values = Vec::with_capacity(deserializer.size_hint());

        while let Some((field, value)) = deserializer.next_field()? {
            field_values.push(FieldValue::new(field, value));
        }

        Ok(Self { field_values })
    }
}

impl From<Vec<FieldValue>> for TantivyDocument {
    fn from(field_values: Vec<FieldValue>) -> Self {
        Self { field_values }
    }
}

impl PartialEq for TantivyDocument {
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

impl Eq for TantivyDocument {}

impl IntoIterator for TantivyDocument {
    type Item = FieldValue;

    type IntoIter = std::vec::IntoIter<FieldValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.field_values.into_iter()
    }
}

impl TantivyDocument {
    /// Creates a new, empty document object
    pub fn new() -> TantivyDocument {
        TantivyDocument::default()
    }

    /// Returns the length of the document.
    pub fn len(&self) -> usize {
        self.field_values.len()
    }

    /// Adding a facet to the document.
    pub fn add_facet<F>(&mut self, field: Field, path: F)
    where Facet: From<F> {
        let facet = Facet::from(path);
        let value = OwnedValue::Facet(facet);
        self.add_field_value(field, value);
    }

    /// Add a text field.
    pub fn add_text<S: ToString>(&mut self, field: Field, text: S) {
        let value = OwnedValue::Str(text.to_string());
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

    /// Add a dynamic object field
    pub fn add_object(&mut self, field: Field, object: BTreeMap<String, OwnedValue>) {
        self.add_field_value(field, object);
    }

    /// Add a (field, value) to the document.
    pub fn add_field_value<T: Into<OwnedValue>>(&mut self, field: Field, typed_val: T) {
        let value = typed_val.into();
        let field_value = FieldValue { field, value };
        self.field_values.push(field_value);
    }

    /// field_values accessor
    pub fn field_values(&self) -> &[FieldValue] {
        &self.field_values
    }

    /// Returns all of the `FieldValue`s associated the given field
    pub fn get_all(&self, field: Field) -> impl Iterator<Item = &OwnedValue> {
        self.field_values
            .iter()
            .filter(move |field_value| field_value.field() == field)
            .map(FieldValue::value)
    }

    /// Returns the first `FieldValue` associated the given field
    pub fn get_first(&self, field: Field) -> Option<&OwnedValue> {
        self.get_all(field).next()
    }

    /// Create document from a named doc.
    pub fn convert_named_doc(
        schema: &Schema,
        named_doc: NamedFieldDocument,
    ) -> Result<TantivyDocument, DocParsingError> {
        let mut document = TantivyDocument::new();
        for (field_name, values) in named_doc.0 {
            if let Ok(field) = schema.get_field(&field_name) {
                for value in values {
                    document.add_field_value(field, value);
                }
            }
        }
        Ok(document)
    }

    /// Create a named document from the doc.
    pub fn to_named_doc(&self, schema: &Schema) -> NamedFieldDocument {
        let mut field_map = BTreeMap::new();
        for (field, field_values) in self.get_sorted_field_values() {
            let field_name = schema.get_field_name(field);
            let values: Vec<OwnedValue> = field_values.into_iter().cloned().collect();
            field_map.insert(field_name.to_string(), values);
        }
        NamedFieldDocument(field_map)
    }

    /// Encode the schema in JSON.
    ///
    /// Encoding a document cannot fail.
    pub fn to_json(&self, schema: &Schema) -> String {
        serde_json::to_string(&self.to_named_doc(schema))
            .expect("doc encoding failed. This is a bug")
    }

    /// Build a document object from a json-object.
    pub fn parse_json(schema: &Schema, doc_json: &str) -> Result<TantivyDocument, DocParsingError> {
        let json_obj: Map<String, serde_json::Value> =
            serde_json::from_str(doc_json).map_err(|_| DocParsingError::invalid_json(doc_json))?;
        Self::from_json_object(schema, json_obj)
    }

    /// Build a document object from a json-object.
    pub fn from_json_object(
        schema: &Schema,
        json_obj: Map<String, serde_json::Value>,
    ) -> Result<TantivyDocument, DocParsingError> {
        let mut doc = TantivyDocument::default();
        for (field_name, json_value) in json_obj {
            if let Ok(field) = schema.get_field(&field_name) {
                let field_entry = schema.get_field_entry(field);
                let field_type = field_entry.field_type();
                match json_value {
                    serde_json::Value::Array(json_items) => {
                        for json_item in json_items {
                            let value = field_type
                                .value_from_json(json_item)
                                .map_err(|e| DocParsingError::ValueError(field_name.clone(), e))?;
                            doc.add_field_value(field, value);
                        }
                    }
                    _ => {
                        let value = field_type
                            .value_from_json(json_value)
                            .map_err(|e| DocParsingError::ValueError(field_name.clone(), e))?;
                        doc.add_field_value(field, value);
                    }
                }
            }
        }
        Ok(doc)
    }
}

/// Error that may happen when deserializing
/// a document from JSON.
#[derive(Debug, Error, PartialEq)]
pub enum DocParsingError {
    /// The payload given is not valid JSON.
    #[error("The provided string is not valid JSON")]
    InvalidJson(String),
    /// One of the value node could not be parsed.
    #[error("The field '{0:?}' could not be parsed: {1:?}")]
    ValueError(String, ValueParsingError),
}

impl DocParsingError {
    /// Builds a NotJson DocParsingError
    fn invalid_json(invalid_json: &str) -> Self {
        let sample = invalid_json.chars().take(20).collect();
        DocParsingError::InvalidJson(sample)
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::document::default_doc_type::TantivyDocument;
    use crate::schema::*;

    #[test]
    fn test_doc() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("title", TEXT);
        let mut doc = TantivyDocument::default();
        doc.add_text(text_field, "My title");
        assert_eq!(doc.field_values().len(), 1);
    }

    // TODO: Should this be re-added with the serialize method
    //       technically this is no longer useful since the doc types
    //       do not implement BinarySerializable due to orphan rules.
    // #[test]
    // fn test_doc_serialization_issue() {
    //     let mut doc = Document::default();
    //     doc.add_json_object(
    //         Field::from_field_id(0),
    //         serde_json::json!({"key": 2u64})
    //             .as_object()
    //             .unwrap()
    //             .clone(),
    //     );
    //     doc.add_text(Field::from_field_id(1), "hello");
    //     assert_eq!(doc.field_values().len(), 2);
    //     let mut payload: Vec<u8> = Vec::new();
    //     doc_binary_wrappers::serialize(&doc, &mut payload).unwrap();
    //     assert_eq!(payload.len(), 26);
    //     doc_binary_wrappers::deserialize::<Document, _>(&mut &payload[..]).unwrap();
    // }
}
