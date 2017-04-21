use schema::TextOptions;
use schema::IntOptions;

use rustc_serialize::json::Json;
use schema::Value;


/// Possible error that may occur while parsing a field value
/// At this point the JSON is known to be valid.
#[derive(Debug)]
pub enum ValueParsingError {
    /// Encounterred a numerical value that overflows or underflow its integer type.
    OverflowError(String),
    /// The json node is not of the correct type. (e.g. 3 for a `Str` type or `"abc"` for a u64 type)
    /// Tantivy will try to autocast values.  
    TypeError(String),
}


/// A `FieldType` describes the type (text, u64) of a field as well as 
/// how it should be handled by tantivy.
#[derive(Clone, Debug, RustcDecodable, RustcEncodable)]
pub enum FieldType {
    /// String field type configuration
    Str(TextOptions),
    /// Unsigned 64-bits integers field type configuration
    U64(IntOptions),
    // /// Signed 64-bits integers 64 field type configuration
    // I64(IntOptions),
}

impl FieldType {
    
    /// Parses a field value from json, given the target FieldType.
    ///
    /// Tantivy will not try to cast values.
    /// For instance, If the json value is the integer `3` and the 
    /// target field is a `Str`, this method will return an Error. 
    pub fn value_from_json(&self, json: &Json) -> Result<Value, ValueParsingError> {
        match *json {
            Json::String(ref field_text) => {
                match *self {
                    FieldType::Str(_) => {
                        Ok(Value::Str(field_text.clone()))
                    }
                    FieldType::U64(_) => {
                        Err(ValueParsingError::TypeError(format!("Expected a u64 int, got {:?}", json)))
                    }
                }
            }
            Json::U64(ref field_val_u64) => {
                match *self {
                    FieldType::U64(_) => {
                        Ok(Value::U64(*field_val_u64 as u64))
                    }
                    _ => {
                        Err(ValueParsingError::TypeError(format!("Expected a string, got {:?}", json)))
                    }
                }
            },
            _ => {
                Err(ValueParsingError::TypeError(format!("Expected a string or a u64, got {:?}", json)))
            }
        }
    }
}