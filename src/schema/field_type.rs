use chrono::{FixedOffset, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::schema::bytes_options::BytesOptions;
use crate::schema::facet_options::FacetOptions;
use crate::schema::{
    Facet, IndexRecordOption, JsonObjectOptions, NumericOptions, TextFieldIndexing, TextOptions,
    Value,
};
use crate::tokenizer::PreTokenizedString;

/// Possible error that may occur while parsing a field value
/// At this point the JSON is known to be valid.
#[derive(Debug, PartialEq, Error)]
pub enum ValueParsingError {
    #[error("Overflow error. Expected {expected}, got {json}")]
    OverflowError {
        expected: &'static str,
        json: serde_json::Value,
    },
    #[error("Type error. Expected {expected}, got {json}")]
    TypeError {
        expected: &'static str,
        json: serde_json::Value,
    },
    #[error("Invalid base64: {base64}")]
    InvalidBase64 { base64: String },
}

/// Type of the value that a field can take.
///
/// Contrary to FieldType, this does
/// not include the way the field must be indexed.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Type {
    /// `&str`
    Str = b's',
    /// `u64`
    U64 = b'u',
    /// `i64`
    I64 = b'i',
    /// `f64`
    F64 = b'f',
    /// `date(i64) timestamp`
    Date = b'd',
    /// `tantivy::schema::Facet`. Passed as a string in JSON.
    Facet = b'h',
    /// `Vec<u8>`
    Bytes = b'b',
    /// Leaf in a Json object.
    Json = b'j',
}

const ALL_TYPES: [Type; 8] = [
    Type::Str,
    Type::U64,
    Type::I64,
    Type::F64,
    Type::Date,
    Type::Facet,
    Type::Bytes,
    Type::Json,
];

impl Type {
    /// Returns an iterator over the different values
    /// the Type enum can tape.
    pub fn iter_values() -> impl Iterator<Item = Type> {
        ALL_TYPES.iter().cloned()
    }

    /// Returns a 1 byte code used to identify the type.
    pub fn to_code(&self) -> u8 {
        *self as u8
    }

    /// Returns a human readable name for the Type.
    pub fn name(&self) -> &'static str {
        match self {
            Type::Str => "Str",
            Type::U64 => "U64",
            Type::I64 => "I64",
            Type::F64 => "F64",
            Type::Date => "Date",
            Type::Facet => "Facet",
            Type::Bytes => "Bytes",
            Type::Json => "Json",
        }
    }

    /// Interprets a 1byte code as a type.
    /// Returns None if the code is invalid.
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            b's' => Some(Type::Str),
            b'u' => Some(Type::U64),
            b'i' => Some(Type::I64),
            b'f' => Some(Type::F64),
            b'd' => Some(Type::Date),
            b'h' => Some(Type::Facet),
            b'b' => Some(Type::Bytes),
            b'j' => Some(Type::Json),
            _ => None,
        }
    }
}

/// A `FieldType` describes the type (text, u64) of a field as well as
/// how it should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "options")]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    /// String field type configuration
    #[serde(rename = "text")]
    Str(TextOptions),
    /// Unsigned 64-bits integers field type configuration
    U64(NumericOptions),
    /// Signed 64-bits integers 64 field type configuration
    I64(NumericOptions),
    /// 64-bits float 64 field type configuration
    F64(NumericOptions),
    /// Signed 64-bits Date 64 field type configuration,
    Date(NumericOptions),
    /// Hierachical Facet
    Facet(FacetOptions),
    /// Bytes (one per document)
    Bytes(BytesOptions),
    /// Json object
    JsonObject(JsonObjectOptions),
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
            FieldType::Facet(_) => Type::Facet,
            FieldType::Bytes(_) => Type::Bytes,
            FieldType::JsonObject(_) => Type::Json,
        }
    }

    /// returns true if the field is indexed.
    pub fn is_indexed(&self) -> bool {
        match *self {
            FieldType::Str(ref text_options) => text_options.get_indexing_options().is_some(),
            FieldType::U64(ref int_options)
            | FieldType::I64(ref int_options)
            | FieldType::F64(ref int_options) => int_options.is_indexed(),
            FieldType::Date(ref date_options) => date_options.is_indexed(),
            FieldType::Facet(ref _facet_options) => true,
            FieldType::Bytes(ref bytes_options) => bytes_options.is_indexed(),
            FieldType::JsonObject(ref json_object_options) => json_object_options.is_indexed(),
        }
    }

    /// Returns the index record option for the field.
    ///
    /// If the field is not indexed, returns `None`.
    pub fn index_record_option(&self) -> Option<IndexRecordOption> {
        match self {
            FieldType::Str(text_options) => text_options
                .get_indexing_options()
                .map(|text_indexing| text_indexing.index_option()),
            FieldType::JsonObject(json_object_options) => json_object_options
                .get_text_indexing_options()
                .map(|text_indexing| text_indexing.index_option()),
            field_type => {
                if field_type.is_indexed() {
                    Some(IndexRecordOption::Basic)
                } else {
                    None
                }
            }
        }
    }

    /// returns true if the field is normed.
    pub fn has_fieldnorms(&self) -> bool {
        match *self {
            FieldType::Str(ref text_options) => text_options
                .get_indexing_options()
                .map(|options| options.fieldnorms())
                .unwrap_or(false),
            FieldType::U64(ref int_options)
            | FieldType::I64(ref int_options)
            | FieldType::F64(ref int_options)
            | FieldType::Date(ref int_options) => int_options.fieldnorms(),
            FieldType::Facet(_) => false,
            FieldType::Bytes(ref bytes_options) => bytes_options.fieldnorms(),
            FieldType::JsonObject(ref _json_object_options) => false,
        }
    }

    /// Given a field configuration, return the maximal possible
    /// `IndexRecordOption` available.
    ///
    /// For the Json object, this does not necessarily mean it is the index record
    /// option level is available for all terms.
    /// (Non string terms have the Basic indexing option at most.)
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
            FieldType::Facet(ref _facet_options) => Some(IndexRecordOption::Basic),
            FieldType::Bytes(ref bytes_options) => {
                if bytes_options.is_indexed() {
                    Some(IndexRecordOption::Basic)
                } else {
                    None
                }
            }
            FieldType::JsonObject(ref json_obj_options) => json_obj_options
                .get_text_indexing_options()
                .map(TextFieldIndexing::index_option),
        }
    }

    /// Parses a field value from json, given the target FieldType.
    ///
    /// Tantivy will not try to cast values.
    /// For instance, If the json value is the integer `3` and the
    /// target field is a `Str`, this method will return an Error.
    pub fn value_from_json(&self, json: JsonValue) -> Result<Value, ValueParsingError> {
        match json {
            JsonValue::String(field_text) => match *self {
                FieldType::Date(_) => {
                    let dt_with_fixed_tz: chrono::DateTime<FixedOffset> =
                        chrono::DateTime::parse_from_rfc3339(&field_text).map_err(|_err| {
                            ValueParsingError::TypeError {
                                expected: "rfc3339 format",
                                json: JsonValue::String(field_text),
                            }
                        })?;
                    Ok(Value::Date(dt_with_fixed_tz.with_timezone(&Utc)))
                }
                FieldType::Str(_) => Ok(Value::Str(field_text)),
                FieldType::U64(_) | FieldType::I64(_) | FieldType::F64(_) => {
                    Err(ValueParsingError::TypeError {
                        expected: "an integer",
                        json: JsonValue::String(field_text),
                    })
                }
                FieldType::Facet(_) => Ok(Value::Facet(Facet::from(&field_text))),
                FieldType::Bytes(_) => base64::decode(&field_text)
                    .map(Value::Bytes)
                    .map_err(|_| ValueParsingError::InvalidBase64 { base64: field_text }),
                FieldType::JsonObject(_) => Err(ValueParsingError::TypeError {
                    expected: "a json object",
                    json: JsonValue::String(field_text),
                }),
            },
            JsonValue::Number(field_val_num) => match self {
                FieldType::I64(_) | FieldType::Date(_) => {
                    if let Some(field_val_i64) = field_val_num.as_i64() {
                        Ok(Value::I64(field_val_i64))
                    } else {
                        Err(ValueParsingError::OverflowError {
                            expected: "an i64 int",
                            json: JsonValue::Number(field_val_num),
                        })
                    }
                }
                FieldType::U64(_) => {
                    if let Some(field_val_u64) = field_val_num.as_u64() {
                        Ok(Value::U64(field_val_u64))
                    } else {
                        Err(ValueParsingError::OverflowError {
                            expected: "u64",
                            json: JsonValue::Number(field_val_num),
                        })
                    }
                }
                FieldType::F64(_) => {
                    if let Some(field_val_f64) = field_val_num.as_f64() {
                        Ok(Value::F64(field_val_f64))
                    } else {
                        Err(ValueParsingError::OverflowError {
                            expected: "a f64",
                            json: JsonValue::Number(field_val_num),
                        })
                    }
                }
                FieldType::Str(_) | FieldType::Facet(_) | FieldType::Bytes(_) => {
                    Err(ValueParsingError::TypeError {
                        expected: "a string",
                        json: JsonValue::Number(field_val_num),
                    })
                }
                FieldType::JsonObject(_) => Err(ValueParsingError::TypeError {
                    expected: "a json object",
                    json: JsonValue::Number(field_val_num),
                }),
            },
            JsonValue::Object(json_map) => match self {
                FieldType::Str(_) => {
                    if let Ok(tok_str_val) = serde_json::from_value::<PreTokenizedString>(
                        serde_json::Value::Object(json_map.clone()),
                    ) {
                        Ok(Value::PreTokStr(tok_str_val))
                    } else {
                        Err(ValueParsingError::TypeError {
                            expected: "a string or an pretokenized string",
                            json: JsonValue::Object(json_map),
                        })
                    }
                }
                FieldType::JsonObject(_) => Ok(Value::JsonObject(json_map)),
                _ => Err(ValueParsingError::TypeError {
                    expected: self.value_type().name(),
                    json: JsonValue::Object(json_map),
                }),
            },
            _ => Err(ValueParsingError::TypeError {
                expected: self.value_type().name(),
                json: json.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use serde_json::json;

    use super::FieldType;
    use crate::schema::field_type::ValueParsingError;
    use crate::schema::{Schema, TextOptions, Type, Value, INDEXED};
    use crate::tokenizer::{PreTokenizedString, Token};
    use crate::{DateTime, Document};

    #[test]
    fn test_deserialize_json_date() {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let schema = schema_builder.build();
        let doc_json = r#"{"date": "2019-10-12T07:20:50.52+02:00"}"#;
        let doc = schema.parse_document(doc_json).unwrap();
        let date = doc.get_first(date_field).unwrap();
        assert_eq!(format!("{:?}", date), "Date(2019-10-12T05:20:50.520Z)");
    }

    #[test]
    fn test_serialize_json_date() {
        let mut doc = Document::new();
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let schema = schema_builder.build();
        let naive_date = NaiveDate::from_ymd(1982, 9, 17);
        let naive_time = NaiveTime::from_hms(13, 20, 00);
        let date_time = DateTime::from_utc(NaiveDateTime::new(naive_date, naive_time), Utc);
        doc.add_date(date_field, date_time);
        let doc_json = schema.to_json(&doc);
        assert_eq!(doc_json, r#"{"date":["1982-09-17T13:20:00+00:00"]}"#);
    }

    #[test]
    fn test_bytes_value_from_json() {
        let result = FieldType::Bytes(Default::default())
            .value_from_json(json!("dGhpcyBpcyBhIHRlc3Q="))
            .unwrap();
        assert_eq!(result, Value::Bytes("this is a test".as_bytes().to_vec()));

        let result = FieldType::Bytes(Default::default()).value_from_json(json!(521));
        match result {
            Err(ValueParsingError::TypeError { .. }) => {}
            _ => panic!("Expected parse failure for wrong type"),
        }

        let result = FieldType::Bytes(Default::default()).value_from_json(json!("-"));
        match result {
            Err(ValueParsingError::InvalidBase64 { .. }) => {}
            _ => panic!("Expected parse failure for invalid base64"),
        }
    }

    #[test]
    fn test_pre_tok_str_value_from_json() {
        let pre_tokenized_string_json = r#"{
  "text": "The Old Man",
  "tokens": [
    {
      "offset_from": 0,
      "offset_to": 3,
      "position": 0,
      "text": "The",
      "position_length": 1
    },
    {
      "offset_from": 4,
      "offset_to": 7,
      "position": 1,
      "text": "Old",
      "position_length": 1
    },
    {
      "offset_from": 8,
      "offset_to": 11,
      "position": 2,
      "text": "Man",
      "position_length": 1
    }
  ]
}"#;

        let expected_value = Value::PreTokStr(PreTokenizedString {
            text: String::from("The Old Man"),
            tokens: vec![
                Token {
                    offset_from: 0,
                    offset_to: 3,
                    position: 0,
                    text: String::from("The"),
                    position_length: 1,
                },
                Token {
                    offset_from: 4,
                    offset_to: 7,
                    position: 1,
                    text: String::from("Old"),
                    position_length: 1,
                },
                Token {
                    offset_from: 8,
                    offset_to: 11,
                    position: 2,
                    text: String::from("Man"),
                    position_length: 1,
                },
            ],
        });

        let deserialized_value = FieldType::Str(TextOptions::default())
            .value_from_json(serde_json::from_str(pre_tokenized_string_json).unwrap())
            .unwrap();

        assert_eq!(deserialized_value, expected_value);

        let serialized_value_json = serde_json::to_string_pretty(&expected_value).unwrap();

        assert_eq!(serialized_value_json, pre_tokenized_string_json);
    }

    #[test]
    fn test_type_codes() {
        for type_val in Type::iter_values() {
            let code = type_val.to_code();
            assert_eq!(Type::from_code(code), Some(type_val));
        }
        assert_eq!(Type::from_code(b'z'), None);
    }
}
