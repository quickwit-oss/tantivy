use crate::schema::flags::SchemaFlagList;
use crate::schema::flags::StoredFlag;
use crate::schema::IndexRecordOption;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::ops::BitOr;

/// Define how a text field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextOptions {
    indexing: Option<TextFieldIndexing>,
    stored: bool,
}

impl TextOptions {
    /// Returns the indexing options.
    pub fn get_indexing_options(&self) -> Option<&TextFieldIndexing> {
        self.indexing.as_ref()
    }

    /// Returns true iff the text is to be stored.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Sets the field as stored
    pub fn set_stored(mut self) -> TextOptions {
        self.stored = true;
        self
    }

    /// Sets the field as indexed, with the specific indexing options.
    pub fn set_indexing_options(mut self, indexing: TextFieldIndexing) -> TextOptions {
        self.indexing = Some(indexing);
        self
    }
}

impl Default for TextOptions {
    fn default() -> TextOptions {
        TextOptions {
            indexing: None,
            stored: false,
        }
    }
}

/// Configuration defining indexing for a text field.
///
/// It defines
/// - the amount of information that should be stored about the presence of a term in a document.
/// Essentially, should we store the term frequency and/or the positions (See [`IndexRecordOption`](./enum.IndexRecordOption.html)).
/// - the name of the `Tokenizer` that should be used to process the field.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct TextFieldIndexing {
    record: IndexRecordOption,
    tokenizer: Cow<'static, str>,
}

impl Default for TextFieldIndexing {
    fn default() -> TextFieldIndexing {
        TextFieldIndexing {
            tokenizer: Cow::Borrowed("default"),
            record: IndexRecordOption::Basic,
        }
    }
}

impl TextFieldIndexing {
    /// Sets the tokenizer to be used for a given field.
    pub fn set_tokenizer(mut self, tokenizer_name: &str) -> TextFieldIndexing {
        self.tokenizer = Cow::Owned(tokenizer_name.to_string());
        self
    }

    /// Returns the tokenizer that will be used for this field.
    pub fn tokenizer(&self) -> &str {
        &self.tokenizer
    }

    /// Sets which information should be indexed with the tokens.
    ///
    /// See [IndexRecordOption](./enum.IndexRecordOption.html) for more detail.
    pub fn set_index_option(mut self, index_option: IndexRecordOption) -> TextFieldIndexing {
        self.record = index_option;
        self
    }

    /// Returns the indexing options associated to this field.
    ///
    /// See [IndexRecordOption](./enum.IndexRecordOption.html) for more detail.
    pub fn index_option(&self) -> IndexRecordOption {
        self.record
    }
}

/// The field will be untokenized and indexed
pub const STRING: TextOptions = TextOptions {
    indexing: Some(TextFieldIndexing {
        tokenizer: Cow::Borrowed("raw"),
        record: IndexRecordOption::Basic,
    }),
    stored: false,
};

/// The field will be tokenized and indexed
pub const TEXT: TextOptions = TextOptions {
    indexing: Some(TextFieldIndexing {
        tokenizer: Cow::Borrowed("default"),
        record: IndexRecordOption::WithFreqsAndPositions,
    }),
    stored: false,
};

impl<T: Into<TextOptions>> BitOr<T> for TextOptions {
    type Output = TextOptions;

    fn bitor(self, other: T) -> TextOptions {
        let other = other.into();
        TextOptions {
            indexing: self.indexing.or(other.indexing),
            stored: self.stored | other.stored,
        }
    }
}

impl From<()> for TextOptions {
    fn from(_: ()) -> TextOptions {
        TextOptions::default()
    }
}

impl From<StoredFlag> for TextOptions {
    fn from(_: StoredFlag) -> TextOptions {
        TextOptions {
            indexing: None,
            stored: true,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for TextOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::*;

    #[test]
    fn test_field_options() {
        let field_options = STORED | TEXT;
        assert!(field_options.is_stored());
        assert!(field_options.get_indexing_options().is_some());
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let field = schema.get_field("body").unwrap();
        let field_entry = schema.get_field_entry(field);
        assert!(matches!(field_entry.field_type(),
                &FieldType::Str(ref text_options)
                if text_options.get_indexing_options().unwrap().tokenizer() == "default"));
    }

    #[test]
    fn test_cmp_index_record_option() {
        assert!(IndexRecordOption::WithFreqsAndPositions > IndexRecordOption::WithFreqs);
        assert!(IndexRecordOption::WithFreqs > IndexRecordOption::Basic);
    }
}
