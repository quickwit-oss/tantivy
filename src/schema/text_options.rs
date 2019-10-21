use crate::schema::flags::SchemaFlagList;
use crate::schema::flags::StoredFlag;
use crate::schema::IndexRecordOption;
use std::borrow::Cow;
use std::ops::BitOr;

/// Define how a text field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextOptions {
    indexing: Option<TextFieldIndexing>,
    stored: bool,
    tokenized: bool,
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

    /// Returns true if the text is already tokenized in the form of TokenString
    pub fn is_tokenized(&self) -> bool {
        self.tokenized
    }

    /// Sets the field as already tokenized
    pub fn set_tokenized(mut self) -> TextOptions {
        self.tokenized = true;
        self
    }
}

impl Default for TextOptions {
    fn default() -> TextOptions {
        TextOptions {
            indexing: None,
            stored: false,
            tokenized: false,
        }
    }
}

/// Configuration defining indexing for a text field.
///
/// It defines
/// - the amount of information that should be stored about the presence of a term in a document.
/// Essentially, should we store the term frequency and/or the positions (See [`IndexRecordOption`](./enum.IndexRecordOption.html)).
/// - the name of the `Tokenizer` that should be used to process the field.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
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
    tokenized: false,
};

/// The field will be tokenized and indexed
pub const TEXT: TextOptions = TextOptions {
    indexing: Some(TextFieldIndexing {
        tokenizer: Cow::Borrowed("default"),
        record: IndexRecordOption::WithFreqsAndPositions,
    }),
    stored: false,
    tokenized: false,
};

/// The field is already tokenized, should come as TokenizedString
pub const TOKENIZED: TextOptions = TextOptions {
    indexing: None,
    stored: false,
    tokenized: true,
};

impl<T: Into<TextOptions>> BitOr<T> for TextOptions {
    type Output = TextOptions;

    fn bitor(self, other: T) -> TextOptions {
        let other = other.into();
        let mut res = TextOptions::default();
        res.indexing = self.indexing.or(other.indexing);
        res.stored = self.stored | other.stored;
        res.tokenized = self.tokenized | other.tokenized;
        res
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
            tokenized: false,
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
        {
            let field_options = STORED | TEXT;
            assert!(field_options.is_stored());
            assert!(!field_options.is_tokenized());
            assert!(field_options.get_indexing_options().is_some());
        }
        {
            let field_options = STORED | TOKENIZED;
            assert!(field_options.is_stored());
            assert!(field_options.is_tokenized());
        }
        {
            let mut schema_builder = Schema::builder();
            schema_builder.add_text_field("body", TEXT);
            let schema = schema_builder.build();
            let field = schema.get_field("body").unwrap();
            let field_entry = schema.get_field_entry(field);
            match field_entry.field_type() {
                &FieldType::Str(ref text_options) => {
                    assert!(text_options.get_indexing_options().is_some());
                    assert_eq!(
                        text_options.get_indexing_options().unwrap().tokenizer(),
                        "default"
                    );
                }
                _ => {
                    panic!("");
                }
            }
        }
    }

    #[test]
    fn test_cmp_index_record_option() {
        assert!(IndexRecordOption::WithFreqsAndPositions > IndexRecordOption::WithFreqs);
        assert!(IndexRecordOption::WithFreqs > IndexRecordOption::Basic);
    }
}
