use schema::FieldEntry;
use std::result;

/// `FastFieldNotAvailableError` is returned when the
/// user requested for a fast field reader, and the field was not
/// defined in the schema as a fast field.
#[derive(Debug, Fail)]
#[fail(display = "field not available: '{:?}'", field_name)]
pub struct FastFieldNotAvailableError {
    field_name: String,
}

impl FastFieldNotAvailableError {
    /// Creates a `FastFieldNotAvailable` error.
    /// `field_entry` is the configuration of the field
    /// for which fast fields are not available.
    pub fn new(field_entry: &FieldEntry) -> FastFieldNotAvailableError {
        FastFieldNotAvailableError {
            field_name: field_entry.name().to_string(),
        }
    }
}

/// Result when trying to access a fast field reader.
pub type Result<R> = result::Result<R, FastFieldNotAvailableError>;
