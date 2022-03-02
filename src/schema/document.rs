use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::mem;

use common::{BinarySerializable, VInt};

use super::*;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;

/// Tantivy's Document is the object that can
/// be indexed and then searched for.
///
/// Documents are fundamentally a collection of unordered couple `(field, value)`.
/// In this list, one field may appear more than once.

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
        let convert_to_comparable_map = |field_values: &[FieldValue]| {
            let mut field_value_set: HashMap<Field, HashSet<String>> = Default::default();
            for field_value in field_values.iter() {
                let json_val = serde_json::to_string(field_value.value()).unwrap();
                field_value_set
                    .entry(field_value.field())
                    .or_default()
                    .insert(json_val);
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

    /// Returns the number of `(field, value)` pairs.
    pub fn len(&self) -> usize {
        self.field_values.len()
    }

    /// Returns true if the document contains no fields.
    pub fn is_empty(&self) -> bool {
        self.field_values.is_empty()
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

    /// Add a i64 field
    pub fn add_i64(&mut self, field: Field, value: i64) {
        self.add_field_value(field, value);
    }

    /// Add a f64 field
    pub fn add_f64(&mut self, field: Field, value: f64) {
        self.add_field_value(field, value);
    }

    /// Add a date field
    pub fn add_date(&mut self, field: Field, value: DateTime) {
        self.add_field_value(field, value);
    }

    /// Add a bytes field
    pub fn add_bytes<T: Into<Vec<u8>>>(&mut self, field: Field, value: T) {
        self.add_field_value(field, value.into());
    }

    /// Add a bytes field
    pub fn add_json_object(
        &mut self,
        field: Field,
        json_object: serde_json::Map<String, serde_json::Value>,
    ) {
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

    #[test]
    fn test_doc() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("title", TEXT);
        let mut doc = Document::default();
        doc.add_text(text_field, "My title");
        assert_eq!(doc.field_values().len(), 1);
    }
}
