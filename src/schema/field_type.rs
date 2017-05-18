use schema::{TextOptions, IntOptions};

use serde_json::Value as JsonValue;
use schema::Value;
use postings::SegmentPostingsOption;
use schema::TextIndexingOptions;

/// Possible error that may occur while parsing a field value
/// At this point the JSON is known to be valid.
#[derive(Debug)]
pub enum ValueParsingError {
    /// Encounterred a numerical value that overflows or underflow its integer type.
    OverflowError(String),
    /// The json node is not of the correct type.
    /// (e.g. 3 for a `Str` type or `"abc"` for a u64 type)
    /// Tantivy will try to autocast values.
    TypeError(String),
}


/// A `FieldType` describes the type (text, u64) of a field as well as
/// how it should be handled by tantivy.
#[derive(Clone, Debug)]
pub enum FieldType {
    /// String field type configuration
    Str(TextOptions),
    /// Unsigned 64-bits integers field type configuration
    U64(IntOptions),
    /// Signed 64-bits integers 64 field type configuration
    I64(IntOptions),
}

impl FieldType {
    /// returns true iff the field is indexed.
    pub fn is_indexed(&self) -> bool {
        match *self {
            FieldType::Str(ref text_options) => text_options.get_indexing_options().is_indexed(),
            FieldType::U64(ref int_options) |
            FieldType::I64(ref int_options) => int_options.is_indexed(),
        }
    }

    /// Given a field configuration, return the maximal possible
    /// `SegmentPostingsOption` available.
    ///
    /// If the field is not indexed, then returns `None`.
    pub fn get_segment_postings_option(&self) -> Option<SegmentPostingsOption> {
        match *self {
            FieldType::Str(ref text_options) => {
                match text_options.get_indexing_options() {
                    TextIndexingOptions::Untokenized |
                    TextIndexingOptions::TokenizedNoFreq   => Some(SegmentPostingsOption::NoFreq),
                    TextIndexingOptions::TokenizedWithFreq => Some(SegmentPostingsOption::Freq),
                    TextIndexingOptions::TokenizedWithFreqAndPosition => {
                        Some(SegmentPostingsOption::FreqAndPositions)
                    }
                    TextIndexingOptions::Unindexed => None,
                }
            }
            FieldType::U64(ref int_options) |
            FieldType::I64(ref int_options) => {
                if int_options.is_indexed() {
                    Some(SegmentPostingsOption::NoFreq)
                } else {
                    None
                }
            }
        }
    }

    /// Parses a field value from json, given the target FieldType.
    ///
    /// Tantivy will not try to cast values.
    /// For instance, If the json value is the integer `3` and the
    /// target field is a `Str`, this method will return an Error.
    pub fn value_from_json(&self, json: &JsonValue) -> Result<Value, ValueParsingError> {
        match *json {
            JsonValue::String(ref field_text) => {
                match *self {
                    FieldType::Str(_) => Ok(Value::Str(field_text.clone())),
                    FieldType::U64(_) |
                    FieldType::I64(_) => {
                        Err(ValueParsingError::TypeError(format!("Expected an integer, got {:?}",
                                                                 json)))
                    }
                }
            }
            JsonValue::Number(ref field_val_num) => {
                match *self {
                    FieldType::I64(_) => {
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
                    FieldType::Str(_) => {
                        let msg = format!("Expected a string, got {:?}", json);
                        Err(ValueParsingError::TypeError(msg))
                    }
                }
            }
            _ => {
                let msg = format!("Json value not supported error {:?}. Expected {:?}",
                                  json,
                                  self);
                Err(ValueParsingError::TypeError(msg))
            }
        }
    }
}
