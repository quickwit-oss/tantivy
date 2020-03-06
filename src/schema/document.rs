use super::*;
use crate::common::BinarySerializable;
use crate::common::VInt;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;
use itertools::Itertools;
use serde;
use std::io::{self, Read, Write};

/// Tantivy's Document is the object that can
/// be indexed and then searched for.
///
/// Documents are fundamentally a collection of unordered couple `(field, value)`.
/// In this list, one field may appear more than once.
///
///

/// Documents are really just a list of couple `(field, value)`.
/// In this list, one field may appear more than once.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct Document {
    field_values: Vec<FieldValue>,
}

impl From<Vec<FieldValue>> for Document {
    fn from(field_values: Vec<FieldValue>) -> Self {
        Document { field_values }
    }
}

impl PartialEq for Document {
    fn eq(&self, other: &Document) -> bool {
        // super slow, but only here for tests
        let mut self_field_values: Vec<&_> = self.field_values.iter().collect();
        let mut other_field_values: Vec<&_> = other.field_values.iter().collect();
        self_field_values.sort();
        other_field_values.sort();
        self_field_values.eq(&other_field_values)
    }
}

impl Eq for Document {}

impl Document {
    /// Creates a new, empty document object
    pub fn new() -> Document {
        Document::default()
    }

    /// Returns the number of `(field, value)` pairs.
    pub fn len(&self) -> usize {
        self.field_values.len()
    }

    /// Returns true iff the document contains no fields.
    pub fn is_empty(&self) -> bool {
        self.field_values.is_empty()
    }

    /// Retain only the field that are matching the
    /// predicate given in argument.
    pub fn filter_fields<P: Fn(Field) -> bool>(&mut self, predicate: P) {
        self.field_values
            .retain(|field_value| predicate(field_value.field()));
    }

    /// Adding a facet to the document.
    pub fn add_facet<F>(&mut self, field: Field, path: F)
    where
        Facet: From<F>,
    {
        let facet = Facet::from(path);
        let value = Value::Facet(facet);
        self.add(FieldValue::new(field, value));
    }

    /// Add a text field.
    pub fn add_text(&mut self, field: Field, text: &str) {
        let value = Value::Str(String::from(text));
        self.add(FieldValue::new(field, value));
    }

    /// Add a pre-tokenized text field.
    pub fn add_pre_tokenized_text(
        &mut self,
        field: Field,
        pre_tokenized_text: &PreTokenizedString,
    ) {
        let value = Value::PreTokStr(pre_tokenized_text.clone());
        self.add(FieldValue::new(field, value));
    }

    /// Add a u64 field
    pub fn add_u64(&mut self, field: Field, value: u64) {
        self.add(FieldValue::new(field, Value::U64(value)));
    }

    /// Add a i64 field
    pub fn add_i64(&mut self, field: Field, value: i64) {
        self.add(FieldValue::new(field, Value::I64(value)));
    }

    /// Add a f64 field
    pub fn add_f64(&mut self, field: Field, value: f64) {
        self.add(FieldValue::new(field, Value::F64(value)));
    }

    /// Add a date field
    pub fn add_date(&mut self, field: Field, value: &DateTime) {
        self.add(FieldValue::new(field, Value::Date(*value)));
    }

    /// Add a bytes field
    pub fn add_bytes(&mut self, field: Field, value: Vec<u8>) {
        self.add(FieldValue::new(field, Value::Bytes(value)))
    }

    /// Add a field value
    pub fn add(&mut self, field_value: FieldValue) {
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
    pub fn get_sorted_field_values(&self) -> Vec<(Field, Vec<&FieldValue>)> {
        let mut field_values: Vec<&FieldValue> = self.field_values().iter().collect();
        field_values.sort_by_key(|field_value| field_value.field());
        field_values
            .into_iter()
            .group_by(|field_value| field_value.field())
            .into_iter()
            .map(|(key, group)| (key, group.collect()))
            .collect::<Vec<(Field, Vec<&FieldValue>)>>()
    }

    /// Returns all of the `FieldValue`s associated the given field
    pub fn get_all(&self, field: Field) -> Vec<&Value> {
        self.field_values
            .iter()
            .filter(|field_value| field_value.field() == field)
            .map(FieldValue::value)
            .collect()
    }

    /// Returns the first `FieldValue` associated the given field
    pub fn get_first(&self, field: Field) -> Option<&Value> {
        self.field_values
            .iter()
            .find(|field_value| field_value.field() == field)
            .map(FieldValue::value)
    }

    /// Prepares Document for being stored in the document store
    ///
    /// Method transforms PreTokenizedString values into String
    /// values.
    pub fn prepare_for_store(&mut self) {
        for field_value in &mut self.field_values {
            if let Value::PreTokStr(pre_tokenized_text) = field_value.value() {
                *field_value = FieldValue::new(
                    field_value.field(),
                    Value::Str(pre_tokenized_text.text.clone()), //< TODO somehow remove .clone()
                );
            }
        }
    }
}

impl BinarySerializable for Document {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let field_values = self.field_values();
        VInt(field_values.len() as u64).serialize(writer)?;
        for field_value in field_values {
            field_value.serialize(writer)?;
        }
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let num_field_values = VInt::deserialize(reader)?.val() as usize;
        let field_values = (0..num_field_values)
            .map(|_| FieldValue::deserialize(reader))
            .collect::<io::Result<Vec<FieldValue>>>()?;
        Ok(Document::from(field_values))
    }
}

#[cfg(test)]
mod tests {

    use crate::schema::*;
    use crate::tokenizer::{PreTokenizedString, Token};

    #[test]
    fn test_doc() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("title", TEXT);
        let mut doc = Document::default();
        doc.add_text(text_field, "My title");
        assert_eq!(doc.field_values().len(), 1);
    }

    #[test]
    fn test_prepare_for_store() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("title", TEXT);
        let mut doc = Document::default();

        let pre_tokenized_text = PreTokenizedString {
            text: String::from("A"),
            tokens: vec![Token {
                offset_from: 0,
                offset_to: 1,
                position: 0,
                text: String::from("A"),
                position_length: 1,
            }],
        };

        doc.add_pre_tokenized_text(text_field, &pre_tokenized_text);
        doc.add_text(text_field, "title");
        doc.prepare_for_store();

        assert_eq!(doc.field_values().len(), 2);

        match doc.field_values()[0].value() {
            Value::Str(ref text) => assert_eq!(text, "A"),
            _ => panic!("Incorrect variant of Value"),
        }

        match doc.field_values()[1].value() {
            Value::Str(ref text) => assert_eq!(text, "title"),
            _ => panic!("Incorrect variant of Value"),
        }
    }
}
