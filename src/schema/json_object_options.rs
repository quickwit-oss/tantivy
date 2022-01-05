use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonObjectOptions {
    stored: bool,
    indexed: bool,
}

impl JsonObjectOptions {
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    pub fn is_indexed(&self) -> bool {
        self.indexed
    }
}
