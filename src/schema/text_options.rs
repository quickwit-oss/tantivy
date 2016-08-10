use std::ops::BitOr;


#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct TextOptions {
    indexing_options: TextIndexingOptions,
    stored: bool,
}

impl TextOptions {
    
    pub fn get_indexing_options(&self,) -> TextIndexingOptions {
        self.indexing_options
    }

    pub fn is_stored(&self,) -> bool {
        self.stored
    }

    pub fn set_stored(mut self,) -> TextOptions {
        self.stored = true;
        self
    }

    pub fn set_indexing_options(mut self, indexing_options: TextIndexingOptions) -> TextOptions {
        self.indexing_options = indexing_options;
        self
    }

    pub fn new() -> TextOptions {
        TextOptions {
            indexing_options: TextIndexingOptions::Unindexed,
            stored: false,
        }
    }
}

#[derive(Clone,Copy,Debug,PartialEq,PartialOrd,Eq,Hash, RustcDecodable, RustcEncodable)]
pub enum TextIndexingOptions {
    Unindexed,
    Untokenized,
    TokenizedNoFreq,
    TokenizedWithFreq,
    TokenizedWithFreqAndPosition,
}

impl TextIndexingOptions {
    pub fn is_termfreq_enabled(&self) -> bool {
        match *self {
            TextIndexingOptions::TokenizedWithFreq => true,
            TextIndexingOptions::TokenizedWithFreqAndPosition => true,
            _ => false,
        }
    }
    
    pub fn is_tokenized(&self,) -> bool {
        match *self {
            TextIndexingOptions::TokenizedNoFreq => true,
            TextIndexingOptions::TokenizedWithFreq => true,
            TextIndexingOptions::TokenizedWithFreqAndPosition => true,
            _ => false,
        }
    }
    
    pub fn is_indexed(&self,) -> bool {
        match *self {
            TextIndexingOptions::Unindexed => false,
            _ => true,
        }
    } 
    
    pub fn is_position_enabled(&self,) -> bool {
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
        }
        else if other == Unindexed {
            self
        }
        else if self == other {
            self
        }
        else {
            // make it possible
            panic!("Combining {:?} and {:?} is ambiguous");
        }
    }
}


/// The field will be untokenized and indexed
pub const STRING: TextOptions = TextOptions {
    indexing_options: TextIndexingOptions::Untokenized,
    stored: false,
};


/// The field will be tokenized and indexed
pub const TEXT: TextOptions = TextOptions {
    indexing_options: TextIndexingOptions::TokenizedWithFreqAndPosition,
    stored: false,
};

/// A stored fields of a document can be retrieved given its DocId.
/// Stored field are stored together and LZ4 compressed.
/// Reading the stored fields of a document is relatively slow.
/// (100 microsecs)
pub const STORED: TextOptions = TextOptions {
    indexing_options: TextIndexingOptions::Unindexed,
    stored: true,
};


impl BitOr for TextOptions {

    type Output = TextOptions;

    fn bitor(self, other: TextOptions) -> TextOptions {
        let mut res = TextOptions::new();
        res.indexing_options = self.indexing_options | other.indexing_options;
        res.stored = self.stored || other.stored;
        res
    }
}


#[cfg(test)]
mod tests {
    use schema::Schema;
    use schema::Field;
    use schema::FieldEntry;
    use super::*;
    
    #[test]
    fn test_field_options() {
        {
            let field_options = STORED | TEXT;
            assert!(field_options.is_stored());
            assert!(field_options.get_indexing_options().is_tokenized());
        }
        {
            let mut schema = Schema::new();
            let _body_field: Field = schema.add_text_field("body", TEXT);
            let field = schema.get_field("body").unwrap();
            let field_entry = schema.get_field_entry(field);
            match field_entry {
                &FieldEntry::Text(_, ref text_options) => {
                    assert!(text_options.get_indexing_options().is_tokenized());
                }
                _ => {
                    panic!("");
                }
            }
        }
    }
}
