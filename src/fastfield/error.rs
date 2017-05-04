use std::result;
use schema::FieldEntry;

/// FastFieldNotAvailableError is returned when the 
/// user requested for a fast field reader, and the field was not
/// defined in the schema as a fast field.
#[derive(Debug)]
pub struct FastFieldNotAvailableError {
    field_name: String,
}

impl FastFieldNotAvailableError {
    pub fn new(field_entry: &FieldEntry) -> FastFieldNotAvailableError {
        FastFieldNotAvailableError {
            field_name: field_entry.name().clone(),
        }
    }
}


/// Result when trying to access a fast field reader.
pub type Result<R> = result::Result<R, FastFieldNotAvailableError>; 
