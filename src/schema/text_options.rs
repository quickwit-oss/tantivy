use std::ops::BitOr;
use rustc_serialize::Decodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;

#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct TextOptions {
    indexing: TextIndexingOptions,
    stored: bool,
}

impl TextOptions {
    
    pub fn get_indexing_options(&self,) -> TextIndexingOptions {
        self.indexing
    }

    pub fn is_stored(&self,) -> bool {
        self.stored
    }

    pub fn set_stored(mut self,) -> TextOptions {
        self.stored = true;
        self
    }

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

#[derive(Clone,Copy,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub enum TextIndexingOptions {
    Unindexed,
    Untokenized,
    TokenizedNoFreq,
    TokenizedWithFreq,
    TokenizedWithFreqAndPosition,
}

impl Encodable for TextIndexingOptions {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let name = match *self {
          TextIndexingOptions::Unindexed => {
              "unindexed"
          }
          TextIndexingOptions::Untokenized => {
              "untokenized"
          }
          TextIndexingOptions::TokenizedNoFreq => {
              "tokenize"
          }
          TextIndexingOptions::TokenizedWithFreq => {
              "freq"
          }
          TextIndexingOptions::TokenizedWithFreqAndPosition => {
              "position"
          }
        };
        s.emit_str(name)
    }
}

impl Decodable for TextIndexingOptions {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        use self::TextIndexingOptions::*;
        let option_name: String = try!(d.read_str());
        Ok(match option_name.as_ref() {
            "unindexed" => Unindexed,
            "untokenized" => Untokenized,
            "tokenize" => TokenizedNoFreq,
            "freq" => TokenizedWithFreq,
            "position" => TokenizedWithFreqAndPosition,
            _ => {
                return Err(d.error(&format!("Encoding option {:?} unknown", option_name)));
            }
        })
    }
}

impl TextIndexingOptions {
    pub fn is_termfreq_enabled(&self) -> bool {
        match *self {
            TextIndexingOptions::TokenizedWithFreq | TextIndexingOptions::TokenizedWithFreqAndPosition => true,
            _ => false,
        }
    }
    
    pub fn is_tokenized(&self,) -> bool {
        match *self {
            TextIndexingOptions::TokenizedNoFreq 
            | TextIndexingOptions::TokenizedWithFreq 
            | TextIndexingOptions::TokenizedWithFreqAndPosition=> true,
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
        else if other == Unindexed || self == other {
            self
        }
        else {
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
        res.stored = self.stored || other.stored;
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
