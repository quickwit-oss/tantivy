use serde::{Deserialize, Serialize};

use crate::schema::TextFieldIndexing;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonObjectOptions {
    stored: bool,
    // If set to some, int, date, f64 and text will be indexed.
    // Text will use the TextFieldIndexing setting for indexing.
    pub(crate) indexing: Option<TextFieldIndexing>,
}

impl JsonObjectOptions {
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    pub fn is_indexed(&self) -> bool {
        self.indexing.is_some()
    }

    pub fn get_text_indexing_option(&self) -> Option<&TextFieldIndexing> {
        self.indexing.as_ref()
    }
}
