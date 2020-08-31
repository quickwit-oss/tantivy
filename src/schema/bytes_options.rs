use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BytesOptions {
    indexed: bool,
    stored: bool,
}

impl BytesOptions {

    pub fn is_stored(&self) -> bool {
        self.stored
    }

    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    pub fn set_stored(mut self) -> BytesOptions {
        self.stored = true;
        self
    }

    pub fn set_indexed(mut self) -> BytesOptions {
        self.indexed = true;
        self
    }

}

impl Default for BytesOptions {
    fn default() -> BytesOptions {
        BytesOptions {
            indexed: false,
            stored: false,
        }
    }
}