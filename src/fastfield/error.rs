use std::result;
use schema::FieldEntry;

#[derive(Debug)]
pub struct FastFieldNotAvailableError {
    field_name: String,
}

pub type Result<R> = result::Result<R, FastFieldNotAvailableError>; 

impl FastFieldNotAvailableError {

    pub fn new(field_entry: &FieldEntry) -> FastFieldNotAvailableError {
        FastFieldNotAvailableError {
            field_name: field_entry.name().clone(),
        }
    }

}