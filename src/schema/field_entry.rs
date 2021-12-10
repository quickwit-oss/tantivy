use crate::schema::FacetOptions;
use crate::schema::TextOptions;
use crate::schema::{is_valid_field_name, IntOptions};

use crate::schema::bytes_options::BytesOptions;
use crate::schema::FieldType;
use serde::{Deserialize, Serialize};

/// A `FieldEntry` represents a field and its configuration.
/// `Schema` are a collection of `FieldEntry`
///
/// It consists of
/// - a field name
/// - a field type, itself wrapping up options describing
/// how the field should be indexed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldEntry {
    name: String,
    #[serde(flatten)]
    field_type: FieldType,
}

impl FieldEntry {
    /// Creates a new field entry given a name and a field type
    pub fn new(field_name: String, field_type: FieldType) -> FieldEntry {
        assert!(is_valid_field_name(&field_name));
        FieldEntry {
            name: field_name,
            field_type,
        }
    }

    /// Creates a new u64 field entry in the schema, given
    /// a name, and some options.
    pub fn new_text(field_name: String, text_options: TextOptions) -> FieldEntry {
        assert!(is_valid_field_name(&field_name));
        FieldEntry {
            name: field_name,
            field_type: FieldType::Str(text_options),
        }
    }

    /// Creates a new u64 field entry in the schema, given
    /// a name, and some options.
    pub fn new_u64(field_name: String, field_type: IntOptions) -> FieldEntry {
        assert!(is_valid_field_name(&field_name));
        FieldEntry {
            name: field_name,
            field_type: FieldType::U64(field_type),
        }
    }

    /// Creates a new i64 field entry in the schema, given
    /// a name, and some options.
    pub fn new_i64(field_name: String, field_type: IntOptions) -> FieldEntry {
        assert!(is_valid_field_name(&field_name));
        FieldEntry {
            name: field_name,
            field_type: FieldType::I64(field_type),
        }
    }

    /// Creates a new f64 field entry in the schema, given
    /// a name, and some options.
    pub fn new_f64(field_name: String, field_type: IntOptions) -> FieldEntry {
        assert!(is_valid_field_name(&field_name));
        FieldEntry {
            name: field_name,
            field_type: FieldType::F64(field_type),
        }
    }

    /// Creates a new date field entry in the schema, given
    /// a name, and some options.
    pub fn new_date(field_name: String, field_type: IntOptions) -> FieldEntry {
        assert!(is_valid_field_name(&field_name));
        FieldEntry {
            name: field_name,
            field_type: FieldType::Date(field_type),
        }
    }

    /// Creates a field entry for a facet.
    pub fn new_facet(field_name: String, field_type: FacetOptions) -> FieldEntry {
        assert!(is_valid_field_name(&field_name));
        FieldEntry {
            name: field_name,
            field_type: FieldType::HierarchicalFacet(field_type),
        }
    }

    /// Creates a field entry for a bytes field
    pub fn new_bytes(field_name: String, bytes_type: BytesOptions) -> FieldEntry {
        FieldEntry {
            name: field_name,
            field_type: FieldType::Bytes(bytes_type),
        }
    }

    /// Returns the name of the field
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the field type
    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    /// Returns true iff the field is indexed.
    ///
    /// An indexed field is searchable.
    pub fn is_indexed(&self) -> bool {
        self.field_type.is_indexed()
    }

    /// Returns true iff the field is normed
    pub fn is_normed(&self) -> bool {
        self.field_type.is_normed()
    }

    /// Returns true iff the field is a int (signed or unsigned) fast field
    pub fn is_fast(&self) -> bool {
        match self.field_type {
            FieldType::U64(ref options)
            | FieldType::I64(ref options)
            | FieldType::Date(ref options)
            | FieldType::F64(ref options) => options.is_fast(),
            _ => false,
        }
    }

    /// Returns true iff the field is stored
    pub fn is_stored(&self) -> bool {
        match self.field_type {
            FieldType::U64(ref options)
            | FieldType::I64(ref options)
            | FieldType::F64(ref options)
            | FieldType::Date(ref options) => options.is_stored(),
            FieldType::Str(ref options) => options.is_stored(),
            FieldType::HierarchicalFacet(ref options) => options.is_stored(),
            FieldType::Bytes(ref options) => options.is_stored(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TEXT;
    use serde_json;

    #[test]
    #[should_panic]
    fn test_invalid_field_name_should_panic() {
        FieldEntry::new_text("-hello".to_string(), TEXT);
    }

    #[test]
    fn test_json_serialization() {
        let field_value = FieldEntry::new_text(String::from("title"), TEXT);

        let expected = r#"{
  "name": "title",
  "type": "text",
  "options": {
    "indexing": {
      "record": "position",
      "tokenizer": "default"
    },
    "stored": false
  }
}"#;
        let field_value_json = serde_json::to_string_pretty(&field_value).unwrap();

        assert_eq!(expected, &field_value_json);

        let field_value: FieldEntry = serde_json::from_str(expected).unwrap();

        assert_eq!("title", field_value.name);

        match field_value.field_type {
            FieldType::Str(_) => {}
            _ => panic!("expected FieldType::Str"),
        }
    }

    #[test]
    fn test_json_deserialization() {
        let json_str = r#"{
  "name": "title",
  "options": {
    "indexing": {
      "record": "position",
      "tokenizer": "default"
    },
    "stored": false
  },
  "type": "text"
}"#;
        let field_entry: FieldEntry = serde_json::from_str(json_str).unwrap();
        match field_entry.field_type {
            FieldType::Str(_) => {}
            _ => panic!("expected FieldType::Str"),
        }
    }
}
