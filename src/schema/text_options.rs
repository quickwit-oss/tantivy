use schema::IndexRecordOption;
use std::borrow::Cow;
use std::ops::BitOr;

/// Define how a text field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
};

/// The field will be tokenized and indexed
pub const TEXT: TextOptions = TextOptions {
    indexing: Some(TextFieldIndexing {
        tokenizer: Cow::Borrowed("default"),
        record: IndexRecordOption::WithFreqsAndPositions,
    }),
    stored: false,
};

/// A stored fields of a document can be retrieved given its `DocId`.
/// Stored field are stored together and LZ4 compressed.
/// Reading the stored fields of a document is relatively slow.
/// (100 microsecs)
pub const STORED: TextOptions = TextOptions {
    indexing: None,
    stored: true,
};

impl BitOr for TextOptions {
    type Output = TextOptions;

    fn bitor(self, other: TextOptions) -> TextOptions {
        let mut res = TextOptions::default();
        res.indexing = self.indexing.or(other.indexing);
        res.stored = self.stored | other.stored;
        res
    }
}

#[cfg(test)]
mod tests {
    use schema::*;

    #[test]
    fn test_field_options() {
        {
            let field_options = STORED | TEXT;
            assert!(field_options.is_stored());
            assert!(field_options.get_indexing_options().is_some());
        }
        {
            let mut schema_builder = SchemaBuilder::default();
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
