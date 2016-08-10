mod schema;
mod term;
mod document;

mod field_entry;
mod field_value;

mod text_options;
mod u32_options;
mod field;


pub use self::schema::Schema;
pub use self::document::Document;
// TODO change to FieldId
pub use self::field::Field;
pub use self::term::Term;

pub use self::field_entry::FieldEntry;
pub use self::field_value::FieldValue;

pub use self::text_options::TextOptions;
pub use self::text_options::TEXT;
pub use self::text_options::STRING;
pub use self::text_options::STORED;
pub use self::text_options::TextIndexingOptions;

pub use self::u32_options::U32Options;
pub use self::u32_options::FAST;

use regex::Regex;



pub fn is_valid_field_name(field_name: &str) -> bool {
    lazy_static! {
        static ref FIELD_NAME_PTN: Regex = Regex::new("[_a-zA-Z0-9]+").unwrap();
    }
    FIELD_NAME_PTN.is_match(field_name)
}