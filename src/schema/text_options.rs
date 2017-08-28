use std::ops::BitOr;


/// Define how a text field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextOptions {
    indexing: TextIndexingOptions,
    stored: bool,
}

impl TextOptions {
    /// Returns the indexing options.
    pub fn get_indexing_options(&self) -> TextIndexingOptions {
        self.indexing
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
    pub fn set_indexing_options(mut self, indexing: TextIndexingOptions) -> TextOptions {
        self.indexing = indexing;
        self
    }
}

impl Default for TextOptions {
    fn default() -> TextOptions {
        TextOptions {
            indexing: TextIndexingOptions::Unindexed,
            stored: false,
        }
    }
}




/// Describe how a field should be indexed
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub enum TextIndexingOptions {
    /// Unindexed fields will not generate any postings. They will not be searchable either.
    #[serde(rename = "unindexed")]
    Unindexed,
    /// Untokenized means that the field text will not be split into tokens before being indexed.
    /// A field with the value "Hello world", will have the document suscribe to one single
    /// postings, the postings associated to the string "Hello world".
    ///
    /// It will **not** be searchable if the user enter "hello" for instance.
    /// This can be useful for tags, or ids for instance.
    #[serde(rename = "untokenized")]
    Untokenized,
    /// TokenizedNoFreq will tokenize the field value, and append the document doc id
    /// to the posting lists associated to all of the tokens.
    /// The frequence of appearance of the term in the document however will be lost.
    /// The term frequency used in the TfIdf formula will always be 1.
    #[serde(rename = "tokenize")]
    TokenizedNoFreq,
    /// TokenizedWithFreq will tokenize the field value, and encode
    /// both the docid and the term frequency in the posting lists associated to all
    #[serde(rename = "freq")]
    TokenizedWithFreq,
    /// Like TokenizedWithFreq, but also encodes the positions of the
    /// terms in a separate file. This option is required for phrase queries.
    /// Don't use this if you are certain you won't need it, the term positions file
    /// can be very big.
    #[serde(rename = "position")]
    TokenizedWithFreqAndPosition,
}

impl TextIndexingOptions {
    /// Returns true iff the term frequency will be encoded.
    pub fn is_termfreq_enabled(&self) -> bool {
        match *self {
            TextIndexingOptions::TokenizedWithFreq |
            TextIndexingOptions::TokenizedWithFreqAndPosition => true,
            _ => false,
        }
    }

    /// Returns true iff the term is tokenized before being indexed
    pub fn is_tokenized(&self) -> bool {
        match *self {
            TextIndexingOptions::TokenizedNoFreq |
            TextIndexingOptions::TokenizedWithFreq |
            TextIndexingOptions::TokenizedWithFreqAndPosition => true,
            _ => false,
        }
    }


    /// Returns true iff the term will generate some posting lists.
    pub fn is_indexed(&self) -> bool {
        match *self {
            TextIndexingOptions::Unindexed => false,
            _ => true,
        }
    }

    /// Returns true iff the term positions within the document are stored as well.
    pub fn is_position_enabled(&self) -> bool {
        match *self {
            TextIndexingOptions::TokenizedWithFreqAndPosition => true,
            _ => false,
        }
    }
}


impl BitOr for TextIndexingOptions {
    type Output = TextIndexingOptions;

    fn bitor(self, other: TextIndexingOptions) -> TextIndexingOptions {
        use super::TextIndexingOptions::*;
        if self == Unindexed {
            other
        } else if other == Unindexed || self == other {
            self
        } else {
            // make it possible
            panic!(format!("Combining {:?} and {:?} is ambiguous", self, other));
        }
    }
}


/// The field will be untokenized and indexed
pub const STRING: TextOptions = TextOptions {
    indexing: TextIndexingOptions::Untokenized,
    stored: false,
};


/// The field will be tokenized and indexed
pub const TEXT: TextOptions = TextOptions {
    indexing: TextIndexingOptions::TokenizedWithFreqAndPosition,
    stored: false,
};

/// A stored fields of a document can be retrieved given its `DocId`.
/// Stored field are stored together and LZ4 compressed.
/// Reading the stored fields of a document is relatively slow.
/// (100 microsecs)
pub const STORED: TextOptions = TextOptions {
    indexing: TextIndexingOptions::Unindexed,
    stored: true,
};


impl BitOr for TextOptions {
    type Output = TextOptions;

    fn bitor(self, other: TextOptions) -> TextOptions {
        let mut res = TextOptions::default();
        res.indexing = self.indexing | other.indexing;
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
            assert!(field_options.get_indexing_options().is_tokenized());
        }
        {
            let mut schema_builder = SchemaBuilder::default();
            schema_builder.add_text_field("body", TEXT);
            let schema = schema_builder.build();
            let field = schema.get_field("body").unwrap();
            let field_entry = schema.get_field_entry(field);
            match field_entry.field_type() {
                &FieldType::Str(ref text_options) => {
                    assert!(text_options.get_indexing_options().is_tokenized());
                }
                _ => {
                    panic!("");
                }
            }
        }
    }
}
