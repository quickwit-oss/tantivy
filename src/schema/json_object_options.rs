use serde::{Serialize, Deserialize};

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
}
