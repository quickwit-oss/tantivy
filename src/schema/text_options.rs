use std::ops::BitOr;
use std::borrow::Cow;
use schema::IndexRecordOption;

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


#[derive(Clone,  PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct TextFieldIndexing {
    record: IndexRecordOption,
    analyzer: Cow<'static, str>,
}


impl Default for TextFieldIndexing {
    fn default() -> TextFieldIndexing {
        TextFieldIndexing {
            analyzer: Cow::Borrowed("default"),
            record: IndexRecordOption::Basic,
        }
    }
}

impl TextFieldIndexing {
    pub fn set_analyzer(mut self, analyzer_name: &str) -> TextFieldIndexing {
        self.analyzer = Cow::Owned(analyzer_name.to_string());
        self
    }

    pub fn analyzer(&self) -> &str {
        &self.analyzer
    }

    pub fn set_index_option(mut self, index_option: IndexRecordOption) -> TextFieldIndexing {
        self.record = index_option;
        self
    }

    pub fn index_option(&self) -> IndexRecordOption {
        self.record
    }
}

/// The field will be untokenized and indexed
pub const STRING: TextOptions = TextOptions {
    indexing: Some(
        TextFieldIndexing {
            analyzer: Cow::Borrowed("raw"),
            record: IndexRecordOption::Basic,
        }),
    stored: false,
};


/// The field will be tokenized and indexed
pub const TEXT: TextOptions = TextOptions {
    indexing: Some(
        TextFieldIndexing {
            analyzer: Cow::Borrowed("default"),
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
                    assert_eq!(text_options.get_indexing_options().unwrap().analyzer(), "default");
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


