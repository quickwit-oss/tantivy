use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use crate::schema::flags::{SchemaFlagList, StoredFlag};
use crate::schema::{TextFieldIndexing, TextOptions};

/// The `JsonObjectOptions` make it possible to
/// configure how a json object field should be indexed and stored.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonObjectOptions {
    stored: bool,
    // If set to some, int, date, f64 and text will be indexed.
    // Text will use the TextFieldIndexing setting for indexing.
    indexing: Option<TextFieldIndexing>,
}

impl JsonObjectOptions {
    /// Returns `true` if the json object should be stored.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns `true` iff the json object should be indexed.
    pub fn is_indexed(&self) -> bool {
        self.indexing.is_some()
    }

    /// Returns the text indexing options.
    ///
    /// If set to `Some` then both int and str values will be indexed.
    /// The inner `TextFieldIndexing` will however, only apply to the str values
    /// in the json object.
    pub fn get_text_indexing_options(&self) -> Option<&TextFieldIndexing> {
        self.indexing.as_ref()
    }
}

impl From<StoredFlag> for JsonObjectOptions {
    fn from(_stored_flag: StoredFlag) -> Self {
        JsonObjectOptions {
            stored: true,
            indexing: None,
        }
    }
}

impl From<()> for JsonObjectOptions {
    fn from(_: ()) -> Self {
        Self::default()
    }
}

impl<T: Into<JsonObjectOptions>> BitOr<T> for JsonObjectOptions {
    type Output = JsonObjectOptions;

    fn bitor(self, other: T) -> Self {
        let other = other.into();
        JsonObjectOptions {
            indexing: self.indexing.or(other.indexing),
            stored: self.stored | other.stored,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for JsonObjectOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}

impl From<TextOptions> for JsonObjectOptions {
    fn from(text_options: TextOptions) -> Self {
        JsonObjectOptions {
            stored: text_options.is_stored(),
            indexing: text_options.get_indexing_options().cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{STORED, TEXT};

    #[test]
    fn test_json_options() {
        {
            let json_options: JsonObjectOptions = (STORED | TEXT).into();
            assert!(json_options.is_stored());
            assert!(json_options.is_indexed());
        }
        {
            let json_options: JsonObjectOptions = TEXT.into();
            assert!(!json_options.is_stored());
            assert!(json_options.is_indexed());
        }
        {
            let json_options: JsonObjectOptions = STORED.into();
            assert!(json_options.is_stored());
            assert!(!json_options.is_indexed());
        }
    }
}
