use std::collections::HashMap;
use std::collections::BTreeMap;
use schema::field_type::ValueParsingError;
use std::sync::Arc;

use serde_json::{self, Map as JsonObject, Value as JsonValue};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::ser::SerializeSeq;
use serde::de::{SeqAccess, Visitor};
use super::*;
use std::fmt;

/// Tantivy has a very strict schema.
/// You need to specify in advance whether a field is indexed or not,
/// stored or not, and RAM-based or not.
///
/// This is done by creating a schema object, and
/// setting up the fields one by one.
/// It is for the moment impossible to remove fields.
///
/// # Examples
///
/// ```
/// use tantivy::schema::*;
///
/// let mut schema_builder = SchemaBuilder::default();
/// let id_field = schema_builder.add_text_field("id", STRING);
/// let title_field = schema_builder.add_text_field("title", TEXT);
/// let body_field = schema_builder.add_text_field("body", TEXT);
/// let schema = schema_builder.build();
///
/// ```
pub struct SchemaBuilder {
    fields: Vec<FieldEntry>,
    fields_map: HashMap<String, Field>,
}

impl SchemaBuilder {
    /// Create a new `SchemaBuilder`
    pub fn new() -> SchemaBuilder {
        SchemaBuilder::default()
    }

    /// Adds a new u64 field.
    /// Returns the associated field handle
    ///
    /// # Caution
    ///
    /// Appending two fields with the same name
    /// will result in the shadowing of the first
    /// by the second one.
    /// The first field will get a field id
    /// but only the second one will be indexed
    pub fn add_u64_field(&mut self, field_name_str: &str, field_options: IntOptions) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_u64(field_name, field_options);
        self.add_field(field_entry)
    }

    /// Adds a new i64 field.
    /// Returns the associated field handle
    ///
    /// # Caution
    ///
    /// Appending two fields with the same name
    /// will result in the shadowing of the first
    /// by the second one.
    /// The first field will get a field id
    /// but only the second one will be indexed
    pub fn add_i64_field(&mut self, field_name_str: &str, field_options: IntOptions) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_i64(field_name, field_options);
        self.add_field(field_entry)
    }

    /// Adds a new text field.
    /// Returns the associated field handle
    ///
    /// # Caution
    ///
    /// Appending two fields with the same name
    /// will result in the shadowing of the first
    /// by the second one.
    /// The first field will get a field id
    /// but only the second one will be indexed
    pub fn add_text_field(&mut self, field_name_str: &str, field_options: TextOptions) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_text(field_name, field_options);
        self.add_field(field_entry)
    }

    /// Adds a facet field to the schema.
    pub fn add_facet_field(&mut self, field_name: &str) -> Field {
        let field_entry = FieldEntry::new_facet(field_name.to_string());
        self.add_field(field_entry)
    }

    /// Adds a field entry to the schema in build.
    fn add_field(&mut self, field_entry: FieldEntry) -> Field {
        let field = Field(self.fields.len() as u32);
        let field_name = field_entry.name().to_string();
        self.fields.push(field_entry);
        self.fields_map.insert(field_name, field);
        field
    }

    /// Finalize the creation of a `Schema`
    /// This will consume your `SchemaBuilder`
    pub fn build(self) -> Schema {
        Schema(Arc::new(InnerSchema {
            fields: self.fields,
            fields_map: self.fields_map,
        }))
    }
}

impl Default for SchemaBuilder {
    fn default() -> SchemaBuilder {
        SchemaBuilder {
            fields: Vec::new(),
            fields_map: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct InnerSchema {
    fields: Vec<FieldEntry>,
    fields_map: HashMap<String, Field>, // transient
}

/// Tantivy has a very strict schema.
/// You need to specify in advance, whether a field is indexed or not,
/// stored or not, and RAM-based or not.
///
/// This is done by creating a schema object, and
/// setting up the fields one by one.
/// It is for the moment impossible to remove fields.
///
/// # Examples
///
/// ```
/// use tantivy::schema::*;
///
/// let mut schema_builder = SchemaBuilder::default();
/// let id_field = schema_builder.add_text_field("id", STRING);
/// let title_field = schema_builder.add_text_field("title", TEXT);
/// let body_field = schema_builder.add_text_field("body", TEXT);
/// let schema = schema_builder.build();
///
/// ```
#[derive(Clone)]
pub struct Schema(Arc<InnerSchema>);

impl Schema {
    /// Return the `FieldEntry` associated to a `Field`.
    pub fn get_field_entry(&self, field: Field) -> &FieldEntry {
        &self.0.fields[field.0 as usize]
    }

    /// Return the field name for a given `Field`.
    pub fn get_field_name(&self, field: Field) -> &str {
        self.get_field_entry(field).name()
    }

    /// Return the list of all the `Field`s.
    pub fn fields(&self) -> &[FieldEntry] {
        &self.0.fields
    }

    /// Returns the field options associated with a given name.
    ///
    /// # Panics
    /// Panics if the field name does not exist.
    /// It is meant as an helper for user who created
    /// and control the content of their schema.
    ///
    /// If panicking is not an option for you,
    /// you may use `get(&self, field_name: &str)`.
    pub fn get_field(&self, field_name: &str) -> Option<Field> {
        self.0.fields_map.get(field_name).cloned()
    }

    /// Create a named document off the doc.
    pub fn to_named_doc(&self, doc: &Document) -> NamedFieldDocument {
        let mut field_map = BTreeMap::new();
        for (field, field_values) in doc.get_sorted_field_values() {
            let field_name = self.get_field_name(field);
            let values: Vec<Value> = field_values
                .into_iter()
                .map(|field_val| field_val.value())
                .cloned()
                .collect();
            field_map.insert(field_name.to_string(), values);
        }
        NamedFieldDocument(field_map)
    }

    /// Encode the schema in JSON.
    ///
    /// Encoding a document cannot fail.
    pub fn to_json(&self, doc: &Document) -> String {
        serde_json::to_string(&self.to_named_doc(doc)).expect("doc encoding failed. This is a bug")
    }

    /// Build a document object from a json-object.
    pub fn parse_document(&self, doc_json: &str) -> Result<Document, DocParsingError> {
        let json_obj: JsonObject<String, JsonValue> =
            serde_json::from_str(doc_json).map_err(|_| {
                let doc_json_sample: String = if doc_json.len() < 20 {
                    String::from(doc_json)
                } else {
                    format!("{:?}...", &doc_json[0..20])
                };
                DocParsingError::NotJSON(doc_json_sample)
            })?;

        let mut doc = Document::default();
        for (field_name, json_value) in json_obj.iter() {
            match self.get_field(field_name) {
                Some(field) => {
                    let field_entry = self.get_field_entry(field);
                    let field_type = field_entry.field_type();
                    match *json_value {
                        JsonValue::Array(ref json_items) => for json_item in json_items {
                            let value = field_type
                                .value_from_json(json_item)
                                .map_err(|e| DocParsingError::ValueError(field_name.clone(), e))?;
                            doc.add(FieldValue::new(field, value));
                        },
                        _ => {
                            let value = field_type
                                .value_from_json(json_value)
                                .map_err(|e| DocParsingError::ValueError(field_name.clone(), e))?;
                            doc.add(FieldValue::new(field, value));
                        }
                    }
                }
                None => return Err(DocParsingError::NoSuchFieldInSchema(field_name.clone())),
            }
        }
        Ok(doc)
    }
}

impl fmt::Debug for Schema {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.fields.len()))?;
        for e in &self.0.fields {
            seq.serialize_element(e)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SchemaVisitor;

        impl<'de> Visitor<'de> for SchemaVisitor {
            type Value = Schema;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Schema")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut schema = SchemaBuilder {
                    fields: Vec::with_capacity(seq.size_hint().unwrap_or(0)),
                    fields_map: HashMap::with_capacity(seq.size_hint().unwrap_or(0)),
                };

                while let Some(value) = seq.next_element()? {
                    schema.add_field(value);
                }

                Ok(schema.build())
            }
        }

        deserializer.deserialize_seq(SchemaVisitor)
    }
}

impl From<SchemaBuilder> for Schema {
    fn from(schema_builder: SchemaBuilder) -> Schema {
        schema_builder.build()
    }
}

/// Error that may happen when deserializing
/// a document from JSON.
#[derive(Debug)]
pub enum DocParsingError {
    /// The payload given is not valid JSON.
    NotJSON(String),
    /// One of the value node could not be parsed.
    ValueError(String, ValueParsingError),
    /// The json-document contains a field that is not declared in the schema.
    NoSuchFieldInSchema(String),
}

#[cfg(test)]
mod tests {

    use schema::*;
    use serde_json;
    use schema::field_type::ValueParsingError;
    use schema::schema::DocParsingError::NotJSON;

    #[test]
    pub fn is_indexed_test() {
        let mut schema_builder = SchemaBuilder::default();
        let field_str = schema_builder.add_text_field("field_str", STRING);
        let schema = schema_builder.build();
        assert!(schema.get_field_entry(field_str).is_indexed());
    }

    #[test]
    pub fn test_schema_serialization() {
        let mut schema_builder = SchemaBuilder::default();
        let count_options = IntOptions::default()
            .set_stored()
            .set_fast(Cardinality::SingleValue);
        let popularity_options = IntOptions::default()
            .set_stored()
            .set_fast(Cardinality::SingleValue);
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("author", STRING);
        schema_builder.add_u64_field("count", count_options);
        schema_builder.add_i64_field("popularity", popularity_options);
        let schema = schema_builder.build();
        let schema_json = serde_json::to_string_pretty(&schema).unwrap();
        let expected = r#"[
  {
    "name": "title",
    "type": "text",
    "options": {
      "indexing": {
        "record": "position",
        "tokenizer": "default"
      },
      "stored": false
    }
  },
  {
    "name": "author",
    "type": "text",
    "options": {
      "indexing": {
        "record": "basic",
        "tokenizer": "raw"
      },
      "stored": false
    }
  },
  {
    "name": "count",
    "type": "u64",
    "options": {
      "indexed": false,
      "fast": "single",
      "stored": true
    }
  },
  {
    "name": "popularity",
    "type": "i64",
    "options": {
      "indexed": false,
      "fast": "single",
      "stored": true
    }
  }
]"#;
        assert_eq!(schema_json, expected);

        let schema: Schema = serde_json::from_str(expected).unwrap();

        let mut fields = schema.fields().iter();

        assert_eq!("title", fields.next().unwrap().name());
        assert_eq!("author", fields.next().unwrap().name());
        assert_eq!("count", fields.next().unwrap().name());
        assert_eq!("popularity", fields.next().unwrap().name());
    }

    #[test]
    pub fn test_document_to_json() {
        let mut schema_builder = SchemaBuilder::default();
        let count_options = IntOptions::default()
            .set_stored()
            .set_fast(Cardinality::SingleValue);
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("author", STRING);
        schema_builder.add_u64_field("count", count_options);
        let schema = schema_builder.build();
        let doc_json = r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 4
        }"#;
        let doc = schema.parse_document(doc_json).unwrap();

        let doc_serdeser = schema.parse_document(&schema.to_json(&doc)).unwrap();
        assert_eq!(doc, doc_serdeser);
    }

    #[test]
    pub fn test_parse_document() {
        let mut schema_builder = SchemaBuilder::default();
        let count_options = IntOptions::default()
            .set_stored()
            .set_fast(Cardinality::SingleValue);
        let popularity_options = IntOptions::default()
            .set_stored()
            .set_fast(Cardinality::SingleValue);
        let title_field = schema_builder.add_text_field("title", TEXT);
        let author_field = schema_builder.add_text_field("author", STRING);
        let count_field = schema_builder.add_u64_field("count", count_options);
        let popularity_field = schema_builder.add_i64_field("popularity", popularity_options);
        let schema = schema_builder.build();
        {
            let doc = schema.parse_document("{}").unwrap();
            assert!(doc.field_values().is_empty());
        }
        {
            let doc = schema
                .parse_document(
                    r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 4,
                "popularity": 10
            }"#,
                )
                .unwrap();
            assert_eq!(doc.get_first(title_field).unwrap().text(), "my title");
            assert_eq!(doc.get_first(author_field).unwrap().text(), "fulmicoton");
            assert_eq!(doc.get_first(count_field).unwrap().u64_value(), 4);
            assert_eq!(doc.get_first(popularity_field).unwrap().i64_value(), 10);
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 4,
                "popularity": 10,
                "jambon": "bayonne"
            }"#,
            );
            match json_err {
                Err(DocParsingError::NoSuchFieldInSchema(field_name)) => {
                    assert_eq!(field_name, "jambon");
                }
                _ => {
                    panic!("expected additional field 'jambon' to fail but didn't");
                }
            }
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": "5",
                "popularity": "10",
                "jambon": "bayonne"
            }"#,
            );
            match json_err {
                Err(DocParsingError::ValueError(_, ValueParsingError::TypeError(_))) => {
                    assert!(true);
                }
                _ => {
                    panic!("expected string of 5 to fail but didn't");
                }
            }
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": -5,
                "popularity": 10
            }"#,
            );
            match json_err {
                Err(DocParsingError::ValueError(_, ValueParsingError::OverflowError(_))) => {
                    assert!(true);
                }
                _ => {
                    panic!("expected -5 to fail but didn't");
                }
            }
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 9223372036854775808,
                "popularity": 10
            }"#,
            );
            match json_err {
                Err(DocParsingError::ValueError(_, ValueParsingError::OverflowError(_))) => {
                    panic!("expected 9223372036854775808 to fit into u64, but it didn't");
                }
                _ => {
                    assert!(true);
                }
            }
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 50,
                "popularity": 9223372036854775808
            }"#,
            );
            match json_err {
                Err(DocParsingError::ValueError(_, ValueParsingError::OverflowError(_))) => {
                    assert!(true);
                }
                _ => {
                    panic!("expected 9223372036854775808 to overflow i64, but it didn't");
                }
            }
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 50,
            }"#,
            );
            match json_err {
                Err(NotJSON(_)) => {
                    assert!(true);
                }
                _ => {
                    panic!("expected invalid JSON to fail parsing, but it didn't");
                }
            }
        }
    }
}
