use std::net::IpAddr;
use std::str::FromStr;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use columnar::{ColumnType, NumericalType};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use super::ip_options::IpAddrOptions;
use super::IntoIpv6Addr;
use crate::schema::bytes_options::BytesOptions;
use crate::schema::facet_options::FacetOptions;
use crate::schema::{
    DateOptions, Facet, IndexRecordOption, JsonObjectOptions, NumericOptions, OwnedValue,
    TextFieldIndexing, TextOptions,
};
use crate::time::format_description::well_known::Rfc3339;
use crate::time::OffsetDateTime;
use crate::tokenizer::PreTokenizedString;
use crate::DateTime;

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
    #[error("Parse  error on {json}: {error}")]
    ParseError {
        error: String,
        json: serde_json::Value,
    },
    #[error("Invalid base64: {base64}")]
    InvalidBase64 { base64: String },
}

/// Type of the value that a field can take.
///
/// Contrary to FieldType, this does
/// not include the way the field must be indexed.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
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
    /// `bool`
    Bool = b'o',
    /// `date(i64) timestamp`
    Date = b'd',
    /// `tantivy::schema::Facet`. Passed as a string in JSON.
    Facet = b'h',
    /// `Vec<u8>`
    Bytes = b'b',
    /// Leaf in a Json object.
    Json = b'j',
    /// IpAddr
    IpAddr = b'p',
}

impl From<ColumnType> for Type {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Str => Self::Str,
            ColumnType::U64 => Self::U64,
            ColumnType::I64 => Self::I64,
            ColumnType::F64 => Self::F64,
            ColumnType::Bool => Self::Bool,
            ColumnType::DateTime => Self::Date,
            ColumnType::Bytes => Self::Bytes,
            ColumnType::IpAddr => Self::IpAddr,
        }
    }
}

const ALL_TYPES: [Type; 10] = [
    Type::Str,
    Type::U64,
    Type::I64,
    Type::F64,
    Type::Bool,
    Type::Date,
    Type::Facet,
    Type::Bytes,
    Type::Json,
    Type::IpAddr,
];

impl Type {
    /// Returns the numerical type if applicable
    /// It does not do any mapping, e.g. Date is None although it's also stored as I64 in the
    /// column store
    pub fn numerical_type(&self) -> Option<NumericalType> {
        match self {
            Self::I64 => Some(NumericalType::I64),
            Self::U64 => Some(NumericalType::U64),
            Self::F64 => Some(NumericalType::F64),
            _ => None,
        }
    }

    /// Returns an iterator over the different values
    /// the Type enum can tape.
    pub fn iter_values() -> impl Iterator<Item = Self> {
        ALL_TYPES.iter().cloned()
    }

    /// Returns a 1 byte code used to identify the type.
    #[inline]
    pub fn to_code(&self) -> u8 {
        *self as u8
    }

    /// Returns a human readable name for the Type.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Str => "Str",
            Self::U64 => "U64",
            Self::I64 => "I64",
            Self::F64 => "F64",
            Self::Bool => "Bool",
            Self::Date => "Date",
            Self::Facet => "Facet",
            Self::Bytes => "Bytes",
            Self::Json => "Json",
            Self::IpAddr => "IpAddr",
        }
    }

    /// Interprets a 1byte code as a type.
    /// Returns `None` if the code is invalid.
    #[inline]
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            b's' => Some(Self::Str),
            b'u' => Some(Self::U64),
            b'i' => Some(Self::I64),
            b'f' => Some(Self::F64),
            b'o' => Some(Self::Bool),
            b'd' => Some(Self::Date),
            b'h' => Some(Self::Facet),
            b'b' => Some(Self::Bytes),
            b'j' => Some(Self::Json),
            b'p' => Some(Self::IpAddr),
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
    /// Bool field type configuration
    Bool(NumericOptions),
    /// Signed 64-bits Date 64 field type configuration,
    Date(DateOptions),
    /// Hierarchical Facet
    Facet(FacetOptions),
    /// Bytes (one per document)
    Bytes(BytesOptions),
    /// Json object
    JsonObject(JsonObjectOptions),
    /// IpAddr field
    IpAddr(IpAddrOptions),
}

impl FieldType {
    /// Returns the value type associated for this field.
    pub fn value_type(&self) -> Type {
        match *self {
            Self::Str(_) => Type::Str,
            Self::U64(_) => Type::U64,
            Self::I64(_) => Type::I64,
            Self::F64(_) => Type::F64,
            Self::Bool(_) => Type::Bool,
            Self::Date(_) => Type::Date,
            Self::Facet(_) => Type::Facet,
            Self::Bytes(_) => Type::Bytes,
            Self::JsonObject(_) => Type::Json,
            Self::IpAddr(_) => Type::IpAddr,
        }
    }

    /// returns true if this is an json field
    pub fn is_json(&self) -> bool {
        matches!(self, Self::JsonObject(_))
    }

    /// returns true if this is an ip address field
    pub fn is_ip_addr(&self) -> bool {
        matches!(self, Self::IpAddr(_))
    }

    /// returns true if this is an str field
    pub fn is_str(&self) -> bool {
        matches!(self, Self::Str(_))
    }

    /// returns true if this is an date field
    pub fn is_date(&self) -> bool {
        matches!(self, Self::Date(_))
    }

    /// returns true if the field is indexed.
    pub fn is_indexed(&self) -> bool {
        match *self {
            Self::Str(ref text_options) => text_options.get_indexing_options().is_some(),
            Self::U64(ref int_options)
            | Self::I64(ref int_options)
            | Self::F64(ref int_options)
            | Self::Bool(ref int_options) => int_options.is_indexed(),
            Self::Date(ref date_options) => date_options.is_indexed(),
            Self::Facet(ref _facet_options) => true,
            Self::Bytes(ref bytes_options) => bytes_options.is_indexed(),
            Self::JsonObject(ref json_object_options) => json_object_options.is_indexed(),
            Self::IpAddr(ref ip_addr_options) => ip_addr_options.is_indexed(),
        }
    }

    /// Returns the index record option for the field.
    ///
    /// If the field is not indexed, returns `None`.
    pub fn index_record_option(&self) -> Option<IndexRecordOption> {
        match self {
            Self::Str(text_options) => text_options
                .get_indexing_options()
                .map(|text_indexing| text_indexing.index_option()),
            Self::JsonObject(json_object_options) => json_object_options
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

    /// returns true if the field is fast.
    pub fn is_fast(&self) -> bool {
        match *self {
            Self::Bytes(ref bytes_options) => bytes_options.is_fast(),
            Self::Str(ref text_options) => text_options.is_fast(),
            Self::U64(ref int_options)
            | Self::I64(ref int_options)
            | Self::F64(ref int_options)
            | Self::Bool(ref int_options) => int_options.is_fast(),
            Self::Date(ref date_options) => date_options.is_fast(),
            Self::IpAddr(ref ip_addr_options) => ip_addr_options.is_fast(),
            Self::Facet(_) => true,
            Self::JsonObject(ref json_object_options) => json_object_options.is_fast(),
        }
    }

    /// returns true if the field is normed (see [fieldnorms](crate::fieldnorm)).
    pub fn has_fieldnorms(&self) -> bool {
        match *self {
            Self::Str(ref text_options) => text_options
                .get_indexing_options()
                .map(|options| options.fieldnorms())
                .unwrap_or(false),
            Self::U64(ref int_options)
            | Self::I64(ref int_options)
            | Self::F64(ref int_options)
            | Self::Bool(ref int_options) => int_options.fieldnorms(),
            Self::Date(ref date_options) => date_options.fieldnorms(),
            Self::Facet(_) => false,
            Self::Bytes(ref bytes_options) => bytes_options.fieldnorms(),
            Self::JsonObject(ref _json_object_options) => false,
            Self::IpAddr(ref ip_addr_options) => ip_addr_options.fieldnorms(),
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
            Self::Str(ref text_options) => text_options
                .get_indexing_options()
                .map(TextFieldIndexing::index_option),
            Self::U64(ref int_options)
            | Self::I64(ref int_options)
            | Self::F64(ref int_options)
            | Self::Bool(ref int_options) => {
                if int_options.is_indexed() {
                    Some(IndexRecordOption::Basic)
                } else {
                    None
                }
            }
            Self::Date(ref date_options) => {
                if date_options.is_indexed() {
                    Some(IndexRecordOption::Basic)
                } else {
                    None
                }
            }
            Self::Facet(ref _facet_options) => Some(IndexRecordOption::Basic),
            Self::Bytes(ref bytes_options) => {
                if bytes_options.is_indexed() {
                    Some(IndexRecordOption::Basic)
                } else {
                    None
                }
            }
            Self::JsonObject(ref json_obj_options) => json_obj_options
                .get_text_indexing_options()
                .map(TextFieldIndexing::index_option),
            Self::IpAddr(ref ip_addr_options) => {
                if ip_addr_options.is_indexed() {
                    Some(IndexRecordOption::Basic)
                } else {
                    None
                }
            }
        }
    }

    /// Parses a field value from json, given the target FieldType.
    ///
    /// Tantivy will try to cast values only with the coerce option.
    /// For instance, If the json value is the integer `3` and the
    /// target field is a `Str`, this method will return an Error if `coerce`
    /// is not enabled.
    pub fn value_from_json(&self, json: JsonValue) -> Result<OwnedValue, ValueParsingError> {
        match json {
            JsonValue::String(field_text) => {
                match self {
                    Self::Date(_) => {
                        let dt_with_fixed_tz = OffsetDateTime::parse(&field_text, &Rfc3339)
                            .map_err(|_err| ValueParsingError::TypeError {
                                expected: "rfc3339 format",
                                json: JsonValue::String(field_text),
                            })?;
                        Ok(DateTime::from_utc(dt_with_fixed_tz).into())
                    }
                    Self::Str(_) => Ok(OwnedValue::Str(field_text)),
                    Self::U64(opt) => {
                        if opt.should_coerce() {
                            Ok(OwnedValue::U64(field_text.parse().map_err(|_| {
                                ValueParsingError::TypeError {
                                    expected: "a u64 or a u64 as string",
                                    json: JsonValue::String(field_text),
                                }
                            })?))
                        } else {
                            Err(ValueParsingError::TypeError {
                                expected: "a u64",
                                json: JsonValue::String(field_text),
                            })
                        }
                    }
                    Self::I64(opt) => {
                        if opt.should_coerce() {
                            Ok(OwnedValue::I64(field_text.parse().map_err(|_| {
                                ValueParsingError::TypeError {
                                    expected: "a i64 or a i64 as string",
                                    json: JsonValue::String(field_text),
                                }
                            })?))
                        } else {
                            Err(ValueParsingError::TypeError {
                                expected: "a i64",
                                json: JsonValue::String(field_text),
                            })
                        }
                    }
                    Self::F64(opt) => {
                        if opt.should_coerce() {
                            Ok(OwnedValue::F64(field_text.parse().map_err(|_| {
                                ValueParsingError::TypeError {
                                    expected: "a f64 or a f64 as string",
                                    json: JsonValue::String(field_text),
                                }
                            })?))
                        } else {
                            Err(ValueParsingError::TypeError {
                                expected: "a f64",
                                json: JsonValue::String(field_text),
                            })
                        }
                    }
                    Self::Bool(opt) => {
                        if opt.should_coerce() {
                            Ok(OwnedValue::Bool(field_text.parse().map_err(|_| {
                                ValueParsingError::TypeError {
                                    expected: "a i64 or a bool as string",
                                    json: JsonValue::String(field_text),
                                }
                            })?))
                        } else {
                            Err(ValueParsingError::TypeError {
                                expected: "a boolean",
                                json: JsonValue::String(field_text),
                            })
                        }
                    }
                    Self::Facet(_) => Ok(OwnedValue::Facet(Facet::from(&field_text))),
                    Self::Bytes(_) => BASE64
                        .decode(&field_text)
                        .map(OwnedValue::Bytes)
                        .map_err(|_| ValueParsingError::InvalidBase64 { base64: field_text }),
                    Self::JsonObject(_) => Err(ValueParsingError::TypeError {
                        expected: "a json object",
                        json: JsonValue::String(field_text),
                    }),
                    Self::IpAddr(_) => {
                        let ip_addr: IpAddr = IpAddr::from_str(&field_text).map_err(|err| {
                            ValueParsingError::ParseError {
                                error: err.to_string(),
                                json: JsonValue::String(field_text),
                            }
                        })?;

                        Ok(OwnedValue::IpAddr(ip_addr.into_ipv6_addr()))
                    }
                }
            }
            JsonValue::Number(field_val_num) => match self {
                Self::I64(_) | Self::Date(_) => {
                    if let Some(field_val_i64) = field_val_num.as_i64() {
                        Ok(OwnedValue::I64(field_val_i64))
                    } else {
                        Err(ValueParsingError::OverflowError {
                            expected: "an i64 int",
                            json: JsonValue::Number(field_val_num),
                        })
                    }
                }
                Self::U64(_) => {
                    if let Some(field_val_u64) = field_val_num.as_u64() {
                        Ok(OwnedValue::U64(field_val_u64))
                    } else {
                        Err(ValueParsingError::OverflowError {
                            expected: "u64",
                            json: JsonValue::Number(field_val_num),
                        })
                    }
                }
                Self::F64(_) => {
                    if let Some(field_val_f64) = field_val_num.as_f64() {
                        Ok(OwnedValue::F64(field_val_f64))
                    } else {
                        Err(ValueParsingError::OverflowError {
                            expected: "a f64",
                            json: JsonValue::Number(field_val_num),
                        })
                    }
                }
                Self::Bool(_) => Err(ValueParsingError::TypeError {
                    expected: "a boolean",
                    json: JsonValue::Number(field_val_num),
                }),
                Self::Str(opt) => {
                    if opt.should_coerce() {
                        Ok(OwnedValue::Str(field_val_num.to_string()))
                    } else {
                        Err(ValueParsingError::TypeError {
                            expected: "a string",
                            json: JsonValue::Number(field_val_num),
                        })
                    }
                }
                Self::Facet(_) | Self::Bytes(_) => Err(ValueParsingError::TypeError {
                    expected: "a string",
                    json: JsonValue::Number(field_val_num),
                }),
                Self::JsonObject(_) => Err(ValueParsingError::TypeError {
                    expected: "a json object",
                    json: JsonValue::Number(field_val_num),
                }),
                Self::IpAddr(_) => Err(ValueParsingError::TypeError {
                    expected: "a string with an ip addr",
                    json: JsonValue::Number(field_val_num),
                }),
            },
            JsonValue::Object(json_map) => match self {
                Self::Str(_) => {
                    if let Ok(tok_str_val) = serde_json::from_value::<PreTokenizedString>(
                        serde_json::Value::Object(json_map.clone()),
                    ) {
                        Ok(OwnedValue::PreTokStr(tok_str_val))
                    } else {
                        Err(ValueParsingError::TypeError {
                            expected: "a string or an pretokenized string",
                            json: JsonValue::Object(json_map),
                        })
                    }
                }
                Self::JsonObject(_) => Ok(OwnedValue::from(json_map)),
                _ => Err(ValueParsingError::TypeError {
                    expected: self.value_type().name(),
                    json: JsonValue::Object(json_map),
                }),
            },
            JsonValue::Bool(json_bool_val) => match self {
                Self::Bool(_) => Ok(OwnedValue::Bool(json_bool_val)),
                Self::Str(opt) => {
                    if opt.should_coerce() {
                        Ok(OwnedValue::Str(json_bool_val.to_string()))
                    } else {
                        Err(ValueParsingError::TypeError {
                            expected: "a string",
                            json: JsonValue::Bool(json_bool_val),
                        })
                    }
                }
                _ => Err(ValueParsingError::TypeError {
                    expected: self.value_type().name(),
                    json: JsonValue::Bool(json_bool_val),
                }),
            },
            // Could also just filter them
            JsonValue::Null => match self {
                Self::Str(opt) => {
                    if opt.should_coerce() {
                        Ok(OwnedValue::Str("null".to_string()))
                    } else {
                        Err(ValueParsingError::TypeError {
                            expected: "a string",
                            json: JsonValue::Null,
                        })
                    }
                }
                _ => Err(ValueParsingError::TypeError {
                    expected: self.value_type().name(),
                    json: JsonValue::Null,
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
    use serde_json::json;

    use super::FieldType;
    use crate::schema::field_type::ValueParsingError;
    use crate::schema::{
        Document, NumericOptions, OwnedValue, Schema, TextOptions, Type, COERCE, INDEXED,
    };
    use crate::time::{Date, Month, PrimitiveDateTime, Time};
    use crate::tokenizer::{PreTokenizedString, Token};
    use crate::{DateTime, TantivyDocument};

    #[test]
    fn test_to_string_coercion() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("id", COERCE);
        let schema = schema_builder.build();
        let doc = TantivyDocument::parse_json(&schema, r#"{"id": 100}"#).unwrap();
        assert_eq!(
            OwnedValue::Str("100".to_string()),
            doc.get_first(text_field).unwrap().into()
        );

        let doc = TantivyDocument::parse_json(&schema, r#"{"id": true}"#).unwrap();
        assert_eq!(
            OwnedValue::Str("true".to_string()),
            doc.get_first(text_field).unwrap().into()
        );

        // Not sure if this null coercion is the best approach
        let doc = TantivyDocument::parse_json(&schema, r#"{"id": null}"#).unwrap();
        assert_eq!(
            OwnedValue::Str("null".to_string()),
            doc.get_first(text_field).unwrap().into()
        );
    }

    #[test]
    fn test_to_number_coercion() {
        let mut schema_builder = Schema::builder();
        let i64_field = schema_builder.add_i64_field("i64", COERCE);
        let u64_field = schema_builder.add_u64_field("u64", COERCE);
        let f64_field = schema_builder.add_f64_field("f64", COERCE);
        let schema = schema_builder.build();
        let doc_json = r#"{"i64": "100", "u64": "100", "f64": "100"}"#;
        let doc = TantivyDocument::parse_json(&schema, doc_json).unwrap();
        assert_eq!(
            OwnedValue::I64(100),
            doc.get_first(i64_field).unwrap().into()
        );
        assert_eq!(
            OwnedValue::U64(100),
            doc.get_first(u64_field).unwrap().into()
        );
        assert_eq!(
            OwnedValue::F64(100.0),
            doc.get_first(f64_field).unwrap().into()
        );
    }

    #[test]
    fn test_to_bool_coercion() {
        let mut schema_builder = Schema::builder();
        let bool_field = schema_builder.add_bool_field("bool", COERCE);
        let schema = schema_builder.build();
        let doc_json = r#"{"bool": "true"}"#;
        let doc = TantivyDocument::parse_json(&schema, doc_json).unwrap();
        assert_eq!(
            OwnedValue::Bool(true),
            doc.get_first(bool_field).unwrap().into()
        );

        let doc_json = r#"{"bool": "false"}"#;
        let doc = TantivyDocument::parse_json(&schema, doc_json).unwrap();
        assert_eq!(
            OwnedValue::Bool(false),
            doc.get_first(bool_field).unwrap().into()
        );
    }

    #[test]
    fn test_to_number_no_coercion() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("i64", NumericOptions::default());
        schema_builder.add_u64_field("u64", NumericOptions::default());
        schema_builder.add_f64_field("f64", NumericOptions::default());
        let schema = schema_builder.build();
        assert!(TantivyDocument::parse_json(&schema, r#"{"u64": "100"}"#)
            .unwrap_err()
            .to_string()
            .contains("a u64"));

        assert!(TantivyDocument::parse_json(&schema, r#"{"i64": "100"}"#)
            .unwrap_err()
            .to_string()
            .contains("a i64"));

        assert!(TantivyDocument::parse_json(&schema, r#"{"f64": "100"}"#)
            .unwrap_err()
            .to_string()
            .contains("a f64"));
    }

    #[test]
    fn test_deserialize_json_date() {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let schema = schema_builder.build();
        let doc_json = r#"{"date": "2019-10-12T07:20:50.52+02:00"}"#;
        let doc = TantivyDocument::parse_json(&schema, doc_json).unwrap();
        let date = OwnedValue::from(doc.get_first(date_field).unwrap());
        // Time zone is converted to UTC
        assert_eq!("Date(2019-10-12T05:20:50.52Z)", format!("{date:?}"));
    }

    #[test]
    fn test_serialize_json_date() {
        let mut doc = TantivyDocument::new();
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let schema = schema_builder.build();
        let naive_date = Date::from_calendar_date(1982, Month::September, 17).unwrap();
        let naive_time = Time::from_hms(13, 20, 0).unwrap();
        let date_time = PrimitiveDateTime::new(naive_date, naive_time);
        doc.add_date(date_field, DateTime::from_primitive(date_time));
        let doc_json = doc.to_json(&schema);
        assert_eq!(doc_json, r#"{"date":["1982-09-17T13:20:00Z"]}"#);
    }

    #[test]
    fn test_bytes_value_from_json() {
        let result = FieldType::Bytes(Default::default())
            .value_from_json(json!("dGhpcyBpcyBhIHRlc3Q="))
            .unwrap();
        assert_eq!(
            result,
            OwnedValue::Bytes("this is a test".as_bytes().to_vec())
        );

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

        let expected_value = OwnedValue::PreTokStr(PreTokenizedString {
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
