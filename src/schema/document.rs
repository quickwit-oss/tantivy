use super::*;
use itertools::Itertools;

/// Tantivy's Document is the object that can
/// be indexed and then searched for.
///
/// Documents are fundamentally a collection of unordered couple `(field, value)`.
/// In this list, one field may appear more than once.
///
///

/// Documents are really just a list of couple `(field, value)`.
/// In this list, one field may appear more than once.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Document {
    field_values: Vec<FieldValue>,
}

impl PartialEq for Document {
    fn eq(&self, other: &Document) -> bool {
        // super slow, but only here for tests
        let mut self_field_values = self.field_values.clone();
        let mut other_field_values = other.field_values.clone();
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

    /// Add a text field.
    pub fn add_text(&mut self, field: Field, text: &str) {
        let value = Value::Str(String::from(text));
        self.add(FieldValue::new(field, value));
    }

    /// Add a u64 field
    pub fn add_u64(&mut self, field: Field, value: u64) {
        self.add(FieldValue::new(field, Value::U64(value)));
    }

    /// Add a u64 field
    pub fn add_i64(&mut self, field: Field, value: i64) {
        self.add(FieldValue::new(field, Value::I64(value)));
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
            .map(|(key, group)| (key, group.into_iter().collect()))
            .collect::<Vec<(Field, Vec<&FieldValue>)>>()
    }

    /// Returns all of the `FieldValue`s associated the given field
    pub fn get_all(&self, field: Field) -> Vec<&Value> {
        self.field_values
            .iter()
            .filter(|field_value| field_value.field() == field)
            .map(|field_value| field_value.value())
            .collect()
    }

    /// Returns the first `FieldValue` associated the given field
    pub fn get_first(&self, field: Field) -> Option<&Value> {
        self.field_values
            .iter()
            .find(|field_value| field_value.field() == field)
            .map(|field_value| field_value.value())
    }
}


impl From<Vec<FieldValue>> for Document {
    fn from(field_values: Vec<FieldValue>) -> Document {
        Document { field_values: field_values }
    }
}


#[cfg(test)]
mod tests {

    use schema::*;

    #[test]
    fn test_doc() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("title", TEXT);
        let mut doc = Document::default();
        doc.add_text(text_field, "My title");
        assert_eq!(doc.field_values().len(), 1);
    }

}
