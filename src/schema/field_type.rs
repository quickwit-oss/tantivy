use base64::decode;

use crate::schema::{IntOptions, TextOptions};

use crate::schema::Facet;
use crate::schema::IndexRecordOption;
use crate::schema::TextFieldIndexing;
use crate::schema::Value;
use serde_json::Value as JsonValue;

/// Possible error that may occur while parsing a field value
/// At this point the JSON is known to be valid.
#[derive(Debug, PartialEq)]
pub enum ValueParsingError {
    /// Encountered a numerical value that overflows or underflow its integer type.
    OverflowError(String),
    /// The json node is not of the correct type.
    /// (e.g. 3 for a `Str` type or `"abc"` for a u64 type)
    /// Tantivy will try to autocast values.
    TypeError(String),
    /// The json node is a string but contains json that is
    /// not valid base64.
    InvalidBase64(String),
}

/// Type of the value that a field can take.
///
/// Contrary to FieldType, this does
/// not include the way the field must be indexed.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Type {
    /// `&str`
    Str,
    /// `u64`
    U64,
    /// `i64`
    I64,
    /// `f64`
    F64,
    /// `date(i64) timestamp`
    Date,
    /// `tantivy::schema::Facet`. Passed as a string in JSON.
    HierarchicalFacet,
    /// `Vec<u8>`
    Bytes,
}

/// A `FieldType` describes the type (text, u64) of a field as well as
/// how it should be handled by tantivy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldType {
    /// String field type configuration
    Str(TextOptions),
    /// Unsigned 64-bits integers field type configuration
    U64(IntOptions),
    /// Signed 64-bits integers 64 field type configuration
    I64(IntOptions),
    /// 64-bits float 64 field type configuration
    F64(IntOptions),
    /// Signed 64-bits Date 64 field type configuration,
    Date(IntOptions),
    /// Hierachical Facet
    HierarchicalFacet,
    /// Bytes (one per document)
    Bytes,
}

impl FieldType {
    /// Returns the value type associated for this field.
    pub fn value_type(&self) -> Type {
        match *self {
            FieldType::Str(_) => Type::Str,
            FieldType::U64(_) => Type::U64,
            FieldType::I64(_) => Type::I64,
            FieldType::F64(_) => Type::F64,
            FieldType::Date(_) => Type::Date,
            FieldType::HierarchicalFacet => Type::HierarchicalFacet,
            FieldType::Bytes => Type::Bytes,
        }
    }

    /// returns true iff the field is indexed.
    pub fn is_indexed(&self) -> bool {
        match *self {
            FieldType::Str(ref text_options) => text_options.get_indexing_options().is_some(),
            FieldType::U64(ref int_options)
            | FieldType::I64(ref int_options)
            | FieldType::F64(ref int_options) => int_options.is_indexed(),
            FieldType::Date(ref date_options) => date_options.is_indexed(),
            FieldType::HierarchicalFacet => true,
            FieldType::Bytes => false,
        }
    }

    /// Given a field configuration, return the maximal possible
    /// `IndexRecordOption` available.
    ///
    /// If the field is not indexed, then returns `None`.
    pub fn get_index_record_option(&self) -> Option<IndexRecordOption> {
        match *self {
            FieldType::Str(ref text_options) => text_options
                .get_indexing_options()
                .map(TextFieldIndexing::index_option),
            FieldType::U64(ref int_options)
            | FieldType::I64(ref int_options)
            | FieldType::F64(ref int_options)
            | FieldType::Date(ref int_options) => {
                if int_options.is_indexed() {
                    Some(IndexRecordOption::Basic)
                } else {
                    None
                }
            }
            FieldType::HierarchicalFacet => Some(IndexRecordOption::Basic),
            FieldType::Bytes => None,
        }
    }

    /// Parses a field value from json, given the target FieldType.
    ///
    /// Tantivy will not try to cast values.
    /// For instance, If the json value is the integer `3` and the
    /// target field is a `Str`, this method will return an Error.
    pub fn value_from_json(&self, json: &JsonValue) -> Result<Value, ValueParsingError> {
        match *json {
            JsonValue::String(ref field_text) => match *self {
                FieldType::Str(_) => Ok(Value::Str(field_text.clone())),
                FieldType::U64(_) | FieldType::I64(_) | FieldType::F64(_) | FieldType::Date(_) => {
                    Err(ValueParsingError::TypeError(format!(
                        "Expected an integer, got {:?}",
                        json
                    )))
                }
                FieldType::HierarchicalFacet => Ok(Value::Facet(Facet::from(field_text))),
                FieldType::Bytes => decode(field_text).map(Value::Bytes).map_err(|_| {
                    ValueParsingError::InvalidBase64(format!(
                        "Expected base64 string, got {:?}",
                        field_text
                    ))
                }),
            },
            JsonValue::Number(ref field_val_num) => match *self {
                FieldType::I64(_) | FieldType::Date(_) => {
                    if let Some(field_val_i64) = field_val_num.as_i64() {
                        Ok(Value::I64(field_val_i64))
                    } else {
                        let msg = format!("Expected an i64 int, got {:?}", json);
                        Err(ValueParsingError::OverflowError(msg))
                    }
                }
                FieldType::U64(_) => {
                    if let Some(field_val_u64) = field_val_num.as_u64() {
                        Ok(Value::U64(field_val_u64))
                    } else {
                        let msg = format!("Expected a u64 int, got {:?}", json);
                        Err(ValueParsingError::OverflowError(msg))
                    }
                }
                FieldType::F64(_) => {
                    if let Some(field_val_f64) = field_val_num.as_f64() {
                        Ok(Value::F64(field_val_f64))
                    } else {
                        let msg = format!("Expected a f64 int, got {:?}", json);
                        Err(ValueParsingError::OverflowError(msg))
                    }
                }
                FieldType::Str(_) | FieldType::HierarchicalFacet | FieldType::Bytes => {
                    let msg = format!("Expected a string, got {:?}", json);
                    Err(ValueParsingError::TypeError(msg))
                }
            },
            _ => {
                let msg = format!(
                    "Json value not supported error {:?}. Expected {:?}",
                    json, self
                );
                Err(ValueParsingError::TypeError(msg))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FieldType;
    use crate::schema::field_type::ValueParsingError;
    use crate::schema::Value;

    #[test]
    fn test_bytes_value_from_json() {
        let result = FieldType::Bytes
            .value_from_json(&json!("dGhpcyBpcyBhIHRlc3Q="))
            .unwrap();
        assert_eq!(result, Value::Bytes("this is a test".as_bytes().to_vec()));

        let result = FieldType::Bytes.value_from_json(&json!(521));
        match result {
            Err(ValueParsingError::TypeError(_)) => {}
            _ => panic!("Expected parse failure for wrong type"),
        }

        let result = FieldType::Bytes.value_from_json(&json!("-"));
        match result {
            Err(ValueParsingError::InvalidBase64(_)) => {}
            _ => panic!("Expected parse failure for invalid base64"),
        }
    }
}
