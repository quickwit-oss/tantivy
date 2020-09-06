use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BytesOptions {
    indexed: bool,
    fast: bool,
    stored: bool,
}

impl BytesOptions {
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    pub fn is_fast(&self) -> bool {
        self.fast
    }

    pub fn is_stored(&self) -> bool {
        self.stored
    }

    pub fn set_indexed(mut self) -> BytesOptions {
        self.indexed = true;
        self
    }

    pub fn set_fast(mut self) -> BytesOptions {
        self.fast = true;
        self
    }

    pub fn set_stored(mut self) -> BytesOptions {
        self.stored = true;
        self
    }
}

impl Default for BytesOptions {
    fn default() -> BytesOptions {
        BytesOptions {
            indexed: false,
            fast: false,
            stored: false,
        }
    }
}
