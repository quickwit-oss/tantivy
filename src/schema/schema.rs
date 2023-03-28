use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{self, Value as JsonValue};

use super::ip_options::IpAddrOptions;
use super::*;
use crate::schema::bytes_options::BytesOptions;
use crate::schema::field_type::ValueParsingError;
use crate::TantivyError;

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
/// let mut schema_builder = Schema::builder();
/// let id_field = schema_builder.add_text_field("id", STRING);
/// let title_field = schema_builder.add_text_field("title", TEXT);
/// let body_field = schema_builder.add_text_field("body", TEXT);
/// let schema = schema_builder.build();
/// ```
#[derive(Default)]
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
    /// # Panics
    ///
    /// Panics when field already exists.
    pub fn add_u64_field<T: Into<NumericOptions>>(
        &mut self,
        field_name_str: &str,
        field_options: T,
    ) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_u64(field_name, field_options.into());
        self.add_field(field_entry)
    }

    /// Adds a new i64 field.
    /// Returns the associated field handle
    ///
    /// # Panics
    ///
    /// Panics when field already exists.
    pub fn add_i64_field<T: Into<NumericOptions>>(
        &mut self,
        field_name_str: &str,
        field_options: T,
    ) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_i64(field_name, field_options.into());
        self.add_field(field_entry)
    }

    /// Adds a new f64 field.
    /// Returns the associated field handle
    ///
    /// # Panics
    ///
    /// Panics when field already exists.
    pub fn add_f64_field<T: Into<NumericOptions>>(
        &mut self,
        field_name_str: &str,
        field_options: T,
    ) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_f64(field_name, field_options.into());
        self.add_field(field_entry)
    }

    /// Adds a new bool field.
    /// Returns the associated field handle
    ///
    /// # Panics
    ///
    /// Panics when field already exists.
    pub fn add_bool_field<T: Into<NumericOptions>>(
        &mut self,
        field_name_str: &str,
        field_options: T,
    ) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_bool(field_name, field_options.into());
        self.add_field(field_entry)
    }

    /// Adds a new date field.
    /// Returns the associated field handle
    /// Internally, Tantivy simply stores dates as i64 UTC timestamps,
    /// while the user supplies DateTime values for convenience.
    ///
    /// # Panics
    ///
    /// Panics when field already exists.
    pub fn add_date_field<T: Into<DateOptions>>(
        &mut self,
        field_name_str: &str,
        field_options: T,
    ) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_date(field_name, field_options.into());
        self.add_field(field_entry)
    }

    /// Adds a ip field.
    /// Returns the associated field handle.
    ///
    /// # Panics
    ///
    /// Panics when field already exists.
    pub fn add_ip_addr_field<T: Into<IpAddrOptions>>(
        &mut self,
        field_name_str: &str,
        field_options: T,
    ) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_ip_addr(field_name, field_options.into());
        self.add_field(field_entry)
    }

    /// Adds a new text field.
    /// Returns the associated field handle
    ///
    /// # Panics
    ///
    /// Panics when field already exists.
    pub fn add_text_field<T: Into<TextOptions>>(
        &mut self,
        field_name_str: &str,
        field_options: T,
    ) -> Field {
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_text(field_name, field_options.into());
        self.add_field(field_entry)
    }

    /// Adds a facet field to the schema.
    pub fn add_facet_field(
        &mut self,
        field_name: &str,
        facet_options: impl Into<FacetOptions>,
    ) -> Field {
        let field_entry = FieldEntry::new_facet(field_name.to_string(), facet_options.into());
        self.add_field(field_entry)
    }

    /// Adds a fast bytes field to the schema.
    ///
    /// Bytes field are not searchable and are only used
    /// as fast field, to associate any kind of payload
    /// to a document.
    ///
    /// For instance, learning-to-rank often requires to access
    /// some document features at scoring time.
    /// These can be serializing and stored as a bytes field to
    /// get access rapidly when scoring each document.
    pub fn add_bytes_field<T: Into<BytesOptions>>(
        &mut self,
        field_name: &str,
        field_options: T,
    ) -> Field {
        let field_entry = FieldEntry::new_bytes(field_name.to_string(), field_options.into());
        self.add_field(field_entry)
    }

    /// Adds a json object field to the schema.
    pub fn add_json_field<T: Into<JsonObjectOptions>>(
        &mut self,
        field_name: &str,
        field_options: T,
    ) -> Field {
        let field_entry = FieldEntry::new_json(field_name.to_string(), field_options.into());
        self.add_field(field_entry)
    }

    /// Adds a field entry to the schema in build.
    pub fn add_field(&mut self, field_entry: FieldEntry) -> Field {
        let field = Field::from_field_id(self.fields.len() as u32);
        let field_name = field_entry.name().to_string();
        if let Some(_previous_value) = self.fields_map.insert(field_name, field) {
            panic!("Field already exists in schema {}", field_entry.name());
        };
        self.fields.push(field_entry);
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
#[derive(Debug)]
struct InnerSchema {
    fields: Vec<FieldEntry>,
    fields_map: HashMap<String, Field>, // transient
}

impl PartialEq for InnerSchema {
    fn eq(&self, other: &InnerSchema) -> bool {
        self.fields == other.fields
    }
}

impl Eq for InnerSchema {}

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
/// let mut schema_builder = Schema::builder();
/// let id_field = schema_builder.add_text_field("id", STRING);
/// let title_field = schema_builder.add_text_field("title", TEXT);
/// let body_field = schema_builder.add_text_field("body", TEXT);
/// let schema = schema_builder.build();
/// ```
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Schema(Arc<InnerSchema>);

// Returns the position (in byte offsets) of the unescaped '.' in the `field_path`.
//
// This function operates directly on bytes (as opposed to codepoint), relying
// on a encoding property of utf-8 for its correctness.
fn locate_splitting_dots(field_path: &str) -> Vec<usize> {
    let mut splitting_dots_pos = Vec::new();
    let mut escape_state = false;
    for (pos, b) in field_path.bytes().enumerate() {
        if escape_state {
            escape_state = false;
            continue;
        }
        match b {
            b'\\' => {
                escape_state = true;
            }
            b'.' => {
                splitting_dots_pos.push(pos);
            }
            _ => {}
        }
    }
    splitting_dots_pos
}

impl Schema {
    /// Return the `FieldEntry` associated with a `Field`.
    pub fn get_field_entry(&self, field: Field) -> &FieldEntry {
        &self.0.fields[field.field_id() as usize]
    }

    /// Return the field name for a given `Field`.
    pub fn get_field_name(&self, field: Field) -> &str {
        self.get_field_entry(field).name()
    }

    /// Returns the number of fields in the schema.
    pub fn num_fields(&self) -> usize {
        self.0.fields.len()
    }

    /// Return the list of all the `Field`s.
    pub fn fields(&self) -> impl Iterator<Item = (Field, &FieldEntry)> {
        self.0
            .fields
            .iter()
            .enumerate()
            .map(|(field_id, field_entry)| (Field::from_field_id(field_id as u32), field_entry))
    }

    /// Creates a new builder.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::default()
    }

    /// Returns the field option associated with a given name.
    pub fn get_field(&self, field_name: &str) -> crate::Result<Field> {
        self.0
            .fields_map
            .get(field_name)
            .cloned()
            .ok_or_else(|| TantivyError::FieldNotFound(field_name.to_string()))
    }

    /// Create document from a named doc.
    pub fn convert_named_doc(
        &self,
        named_doc: NamedFieldDocument,
    ) -> Result<Document, DocParsingError> {
        let mut document = Document::new();
        for (field_name, values) in named_doc.0 {
            if let Ok(field) = self.get_field(&field_name) {
                for value in values {
                    document.add_field_value(field, value);
                }
            }
        }
        Ok(document)
    }

    /// Create a named document from the doc.
    pub fn to_named_doc(&self, doc: &Document) -> NamedFieldDocument {
        let mut field_map = BTreeMap::new();
        for (field, field_values) in doc.get_sorted_field_values() {
            let field_name = self.get_field_name(field);
            let values: Vec<Value> = field_values.into_iter().cloned().collect();
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
        let json_obj: serde_json::Map<String, JsonValue> =
            serde_json::from_str(doc_json).map_err(|_| DocParsingError::invalid_json(doc_json))?;
        self.json_object_to_doc(json_obj)
    }

    /// Build a document object from a json-object.
    pub fn json_object_to_doc(
        &self,
        json_obj: serde_json::Map<String, JsonValue>,
    ) -> Result<Document, DocParsingError> {
        let mut doc = Document::default();
        for (field_name, json_value) in json_obj {
            if let Ok(field) = self.get_field(&field_name) {
                let field_entry = self.get_field_entry(field);
                let field_type = field_entry.field_type();
                match json_value {
                    JsonValue::Array(json_items) => {
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

    /// Searches for a full_path in the schema, returning the field name and a JSON path.
    ///
    /// This function works by checking if the field exists for the exact given full_path.
    /// If it's not, it splits the full_path at non-escaped '.' chars and tries to match the
    /// prefix with the field names, favoring the longest field names.
    ///
    /// This does not check if field is a JSON field. It is possible for this functions to
    /// return a non-empty JSON path with a non-JSON field.
    pub fn find_field<'a>(&self, full_path: &'a str) -> Option<(Field, &'a str)> {
        if let Some(field) = self.0.fields_map.get(full_path) {
            return Some((*field, ""));
        }
        let mut splitting_period_pos: Vec<usize> = locate_splitting_dots(full_path);
        while let Some(pos) = splitting_period_pos.pop() {
            let (prefix, suffix) = full_path.split_at(pos);
            if let Some(field) = self.0.fields_map.get(prefix) {
                return Some((*field, &suffix[1..]));
            }
        }
        None
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut seq = serializer.serialize_seq(Some(self.0.fields.len()))?;
        for e in &self.0.fields {
            seq.serialize_element(e)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct SchemaVisitor;

        impl<'de> Visitor<'de> for SchemaVisitor {
            type Value = Schema;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("struct Schema")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where A: SeqAccess<'de> {
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

    use std::collections::BTreeMap;

    use matches::{assert_matches, matches};
    use pretty_assertions::assert_eq;
    use serde_json;

    use crate::schema::field_type::ValueParsingError;
    use crate::schema::schema::DocParsingError::InvalidJson;
    use crate::schema::*;

    #[test]
    fn test_locate_splitting_dots() {
        assert_eq!(&super::locate_splitting_dots("a.b.c"), &[1, 3]);
        assert_eq!(&super::locate_splitting_dots(r#"a\.b.c"#), &[4]);
        assert_eq!(&super::locate_splitting_dots(r#"a\..b.c"#), &[3, 5]);
    }

    #[test]
    pub fn is_indexed_test() {
        let mut schema_builder = Schema::builder();
        let field_str = schema_builder.add_text_field("field_str", STRING);
        let schema = schema_builder.build();
        assert!(schema.get_field_entry(field_str).is_indexed());
    }

    #[test]
    pub fn test_schema_serialization() {
        let mut schema_builder = Schema::builder();
        let count_options = NumericOptions::default().set_stored().set_fast();
        let popularity_options = NumericOptions::default().set_stored().set_fast();
        let score_options = NumericOptions::default()
            .set_indexed()
            .set_fieldnorm()
            .set_fast();
        let is_read_options = NumericOptions::default().set_stored().set_fast();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field(
            "author",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("raw")
                    .set_fieldnorms(false),
            ),
        );
        schema_builder.add_u64_field("count", count_options);
        schema_builder.add_i64_field("popularity", popularity_options);
        schema_builder.add_f64_field("score", score_options);
        schema_builder.add_bool_field("is_read", is_read_options);
        let schema = schema_builder.build();
        let schema_json = serde_json::to_string_pretty(&schema).unwrap();
        let expected = r#"[
  {
    "name": "title",
    "type": "text",
    "options": {
      "indexing": {
        "record": "position",
        "fieldnorms": true,
        "tokenizer": "default"
      },
      "stored": false,
      "fast": false
    }
  },
  {
    "name": "author",
    "type": "text",
    "options": {
      "indexing": {
        "record": "basic",
        "fieldnorms": false,
        "tokenizer": "raw"
      },
      "stored": false,
      "fast": false
    }
  },
  {
    "name": "count",
    "type": "u64",
    "options": {
      "indexed": false,
      "fieldnorms": false,
      "fast": true,
      "stored": true
    }
  },
  {
    "name": "popularity",
    "type": "i64",
    "options": {
      "indexed": false,
      "fieldnorms": false,
      "fast": true,
      "stored": true
    }
  },
  {
    "name": "score",
    "type": "f64",
    "options": {
      "indexed": true,
      "fieldnorms": true,
      "fast": true,
      "stored": false
    }
  },
  {
    "name": "is_read",
    "type": "bool",
    "options": {
      "indexed": false,
      "fieldnorms": false,
      "fast": true,
      "stored": true
    }
  }
]"#;
        assert_eq!(schema_json, expected);

        let schema: Schema = serde_json::from_str(expected).unwrap();

        let mut fields = schema.fields();
        {
            let (field, field_entry) = fields.next().unwrap();
            assert_eq!("title", field_entry.name());
            assert_eq!(0, field.field_id());
        }
        {
            let (field, field_entry) = fields.next().unwrap();
            assert_eq!("author", field_entry.name());
            assert_eq!(1, field.field_id());
        }
        {
            let (field, field_entry) = fields.next().unwrap();
            assert_eq!("count", field_entry.name());
            assert_eq!(2, field.field_id());
        }
        {
            let (field, field_entry) = fields.next().unwrap();
            assert_eq!("popularity", field_entry.name());
            assert_eq!(3, field.field_id());
        }
        {
            let (field, field_entry) = fields.next().unwrap();
            assert_eq!("score", field_entry.name());
            assert_eq!(4, field.field_id());
        }
        {
            let (field, field_entry) = fields.next().unwrap();
            assert_eq!("is_read", field_entry.name());
            assert_eq!(5, field.field_id());
        }
        assert!(fields.next().is_none());
    }

    #[test]
    pub fn test_document_to_json() {
        let mut schema_builder = Schema::builder();
        let count_options = NumericOptions::default().set_stored().set_fast();
        let is_read_options = NumericOptions::default().set_stored().set_fast();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("author", STRING);
        schema_builder.add_u64_field("count", count_options);
        schema_builder.add_ip_addr_field("ip", FAST | STORED);
        schema_builder.add_bool_field("is_read", is_read_options);
        let schema = schema_builder.build();
        let doc_json = r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 4,
                "ip": "127.0.0.1",
                "is_read": true
        }"#;
        let doc = schema.parse_document(doc_json).unwrap();

        let doc_serdeser = schema.parse_document(&schema.to_json(&doc)).unwrap();
        assert_eq!(doc, doc_serdeser);
    }

    #[test]
    pub fn test_document_to_ipv4_json() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_ip_addr_field("ip", FAST | STORED);
        let schema = schema_builder.build();

        // IpV4 loopback
        let doc_json = r#"{
                "ip": "127.0.0.1"
        }"#;
        let doc = schema.parse_document(doc_json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&schema.to_json(&doc)).unwrap();
        assert_eq!(value["ip"][0], "127.0.0.1");

        // Special case IpV6 loopback. We don't want to map that to IPv4
        let doc_json = r#"{
                "ip": "::1"
        }"#;
        let doc = schema.parse_document(doc_json).unwrap();

        let value: serde_json::Value = serde_json::from_str(&schema.to_json(&doc)).unwrap();
        assert_eq!(value["ip"][0], "::1");

        // testing ip address of every router in the world
        let doc_json = r#"{
                "ip": "192.168.0.1"
        }"#;
        let doc = schema.parse_document(doc_json).unwrap();

        let value: serde_json::Value = serde_json::from_str(&schema.to_json(&doc)).unwrap();
        assert_eq!(value["ip"][0], "192.168.0.1");
    }

    #[test]
    pub fn test_document_from_nameddoc() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let val = schema_builder.add_i64_field("val", INDEXED);
        let schema = schema_builder.build();
        let mut named_doc_map = BTreeMap::default();
        named_doc_map.insert(
            "title".to_string(),
            vec![Value::from("title1"), Value::from("title2")],
        );
        named_doc_map.insert(
            "val".to_string(),
            vec![Value::from(14u64), Value::from(-1i64)],
        );
        let doc = schema
            .convert_named_doc(NamedFieldDocument(named_doc_map))
            .unwrap();
        assert_eq!(
            doc.get_all(title).collect::<Vec<_>>(),
            vec![
                &Value::from("title1".to_string()),
                &Value::from("title2".to_string())
            ]
        );
        assert_eq!(
            doc.get_all(val).collect::<Vec<_>>(),
            vec![&Value::from(14u64), &Value::from(-1i64)]
        );
    }

    #[test]
    pub fn test_document_missing_field_no_error() {
        let schema = Schema::builder().build();
        let mut named_doc_map = BTreeMap::default();
        named_doc_map.insert(
            "title".to_string(),
            vec![Value::from("title1"), Value::from("title2")],
        );
        schema
            .convert_named_doc(NamedFieldDocument(named_doc_map))
            .unwrap();
    }

    #[test]
    pub fn test_parse_document() {
        let mut schema_builder = Schema::builder();
        let count_options = NumericOptions::default().set_stored().set_fast();
        let popularity_options = NumericOptions::default().set_stored().set_fast();
        let score_options = NumericOptions::default().set_indexed().set_fast();
        let title_field = schema_builder.add_text_field("title", TEXT);
        let author_field = schema_builder.add_text_field("author", STRING);
        let count_field = schema_builder.add_u64_field("count", count_options);
        let popularity_field = schema_builder.add_i64_field("popularity", popularity_options);
        let score_field = schema_builder.add_f64_field("score", score_options);
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
                "popularity": 10,
                "score": 80.5
            }"#,
                )
                .unwrap();
            assert_eq!(
                doc.get_first(title_field).unwrap().as_text(),
                Some("my title")
            );
            assert_eq!(
                doc.get_first(author_field).unwrap().as_text(),
                Some("fulmicoton")
            );
            assert_eq!(doc.get_first(count_field).unwrap().as_u64(), Some(4));
            assert_eq!(doc.get_first(popularity_field).unwrap().as_i64(), Some(10));
            assert_eq!(doc.get_first(score_field).unwrap().as_f64(), Some(80.5f64));
        }
        {
            let res = schema.parse_document(
                r#"{
                "thisfieldisnotdefinedintheschema": "my title",
                "title": "my title",
                "author": "fulmicoton",
                "count": 4,
                "popularity": 10,
                "score": 80.5,
                "jambon": "bayonne"
            }"#,
            );
            assert!(res.is_ok());
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": "5",
                "popularity": "10",
                "score": "80.5",
                "jambon": "bayonne"
            }"#,
            );
            assert_matches!(
                json_err,
                Err(DocParsingError::ValueError(
                    _,
                    ValueParsingError::TypeError { .. }
                ))
            );
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": -5,
                "popularity": 10,
                "score": 80.5
            }"#,
            );
            assert_matches!(
                json_err,
                Err(DocParsingError::ValueError(
                    _,
                    ValueParsingError::OverflowError { .. }
                ))
            );
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 9223372036854775808,
                "popularity": 10,
                "score": 80.5
            }"#,
            );
            assert!(!matches!(
                json_err,
                Err(DocParsingError::ValueError(
                    _,
                    ValueParsingError::OverflowError { .. }
                ))
            ));
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 50,
                "popularity": 9223372036854775808,
                "score": 80.5
            }"#,
            );
            assert_matches!(
                json_err,
                Err(DocParsingError::ValueError(
                    _,
                    ValueParsingError::OverflowError { .. }
                ))
            );
        }
        {
            // Short JSON, under the 20 char take.
            let json_err = schema.parse_document(r#"{"count": 50,}"#);
            assert_matches!(json_err, Err(InvalidJson(_)));
        }
        {
            let json_err = schema.parse_document(
                r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 50,
            }"#,
            );
            assert_matches!(json_err, Err(InvalidJson(_)));
        }
    }

    #[test]
    pub fn test_schema_add_field() {
        let mut schema_builder = SchemaBuilder::default();
        let id_options = TextOptions::default().set_stored().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::Basic),
        );
        let timestamp_options = DateOptions::default()
            .set_stored()
            .set_indexed()
            .set_fieldnorm()
            .set_fast();
        schema_builder.add_text_field("_id", id_options);
        schema_builder.add_date_field("_timestamp", timestamp_options);

        let schema_content = r#"[
  {
    "name": "text",
    "type": "text",
    "options": {
      "indexing": {
        "record": "position",
        "fieldnorms": true,
        "tokenizer": "default"
      },
      "stored": false,
      "fast": false
    }
  },
  {
    "name": "popularity",
    "type": "i64",
    "options": {
      "indexed": false,
      "fieldnorms": false,
      "fast": true,
      "stored": true
    }
  }
]"#;
        let tmp_schema: Schema =
            serde_json::from_str(schema_content).expect("error while reading json");
        for (_field, field_entry) in tmp_schema.fields() {
            schema_builder.add_field(field_entry.clone());
        }

        let schema = schema_builder.build();
        let schema_json = serde_json::to_string_pretty(&schema).unwrap();
        let expected = r#"[
  {
    "name": "_id",
    "type": "text",
    "options": {
      "indexing": {
        "record": "basic",
        "fieldnorms": true,
        "tokenizer": "raw"
      },
      "stored": true,
      "fast": false
    }
  },
  {
    "name": "_timestamp",
    "type": "date",
    "options": {
      "indexed": true,
      "fieldnorms": true,
      "fast": true,
      "stored": true,
      "precision": "seconds"
    }
  },
  {
    "name": "text",
    "type": "text",
    "options": {
      "indexing": {
        "record": "position",
        "fieldnorms": true,
        "tokenizer": "default"
      },
      "stored": false,
      "fast": false
    }
  },
  {
    "name": "popularity",
    "type": "i64",
    "options": {
      "indexed": false,
      "fieldnorms": false,
      "fast": true,
      "stored": true
    }
  }
]"#;
        assert_eq!(schema_json, expected);
    }

    #[test]
    fn test_find_field() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_json_field("foo", STRING);

        schema_builder.add_text_field("bar", STRING);
        schema_builder.add_text_field("foo.bar", STRING);
        schema_builder.add_text_field("foo.bar.baz", STRING);
        schema_builder.add_text_field("bar.a.b.c", STRING);
        let schema = schema_builder.build();

        assert_eq!(
            schema.find_field("foo.bar"),
            Some((schema.get_field("foo.bar").unwrap(), ""))
        );
        assert_eq!(
            schema.find_field("foo.bar.bar"),
            Some((schema.get_field("foo.bar").unwrap(), "bar"))
        );
        assert_eq!(
            schema.find_field("foo.bar.baz"),
            Some((schema.get_field("foo.bar.baz").unwrap(), ""))
        );
        assert_eq!(
            schema.find_field("foo.toto"),
            Some((schema.get_field("foo").unwrap(), "toto"))
        );
        assert_eq!(
            schema.find_field("foo.bar"),
            Some((schema.get_field("foo.bar").unwrap(), ""))
        );
        assert_eq!(
            schema.find_field("bar.toto.titi"),
            Some((schema.get_field("bar").unwrap(), "toto.titi"))
        );

        assert_eq!(schema.find_field("hello"), None);
        assert_eq!(schema.find_field(""), None);
        assert_eq!(schema.find_field("thiswouldbeareallylongfieldname"), None);
        assert_eq!(schema.find_field("baz.bar.foo"), None);
    }
}
