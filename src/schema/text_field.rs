use std::io::Write;
use std::io;

use std::io::Read;
use common::BinarySerializable;
use rustc_serialize::Decoder;
use rustc_serialize::Encoder;
use std::ops::BitOr;

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct TextField(pub u8);

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash, RustcDecodable, RustcEncodable)]
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

#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct TextOptions {
    indexing_options: TextIndexingOptions,
    stored: bool,
    fast: bool,
}

impl TextOptions {
    
    pub fn indexing_options(&self,) -> TextIndexingOptions {
        self.indexing_options.clone()
    }

    pub fn is_stored(&self,) -> bool {
        self.stored
    }

    pub fn is_fast(&self,) -> bool {
        self.fast
    }

    pub fn set_stored(mut self,) -> TextOptions {
        self.stored = true;
        self
    }

    pub fn set_fast(mut self,) -> TextOptions {
        self.fast = true;
        self
    }

    pub fn set_indexing_options(mut self, indexing_options: TextIndexingOptions) -> TextOptions {
        self.indexing_options = indexing_options;
        self
    }

    pub fn new() -> TextOptions {
        TextOptions {
            fast: false,
            indexing_options: TextIndexingOptions::Unindexed,
            stored: false,
        }
    }
}


impl BinarySerializable for TextField {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let TextField(field_id) = *self;
        field_id.serialize(writer)
    }

    fn deserialize(reader: &mut Read) -> io::Result<TextField> {
        u8::deserialize(reader).map(TextField)
    }
}


impl BinarySerializable for TextFieldValue {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        Ok(
            try!(self.field.serialize(writer)) +
            try!(self.text.serialize(writer))
        )
    }
    fn deserialize(reader: &mut Read) -> io::Result<Self> {
        let field = try!(TextField::deserialize(reader));
        let text = try!(String::deserialize(reader));
        Ok(TextFieldValue {
            field: field,
            text: text,
        })
    }
}



#[derive(Clone,Debug,PartialEq,PartialOrd,Eq)]
pub struct TextFieldValue {
    pub field: TextField,
    pub text: String,
}


/// The field will be untokenized and indexed
pub const STRING: TextOptions = TextOptions {
    indexing_options: TextIndexingOptions::Untokenized,
    stored: false,
    fast: false,
};


/// The field will be tokenized and indexed
pub const TEXT: TextOptions = TextOptions {
    indexing_options: TextIndexingOptions::TokenizedWithFreqAndPosition,
    stored: false,
    fast: false,
};

/// A stored fields of a document can be retrieved given its DocId.
/// Stored field are stored together and LZ4 compressed.
/// Reading the stored fields of a document is relatively slow.
/// (100 microsecs)
pub const STORED: TextOptions = TextOptions {
    indexing_options: TextIndexingOptions::Unindexed,
    stored: true,
    fast: false,
};

/// Fast field are used for field you need to access many times during
/// collection. (e.g: for sort, aggregates).
pub const FAST: TextOptions = TextOptions {
    indexing_options: TextIndexingOptions::Unindexed,
    stored: false,
    fast: true
};


impl BitOr for TextOptions {

    type Output = TextOptions;

    fn bitor(self, other: TextOptions) -> TextOptions {
        let mut res = TextOptions::new();
        res.indexing_options = self.indexing_options | other.indexing_options;
        res.stored = self.stored || other.stored;
        res.fast = self.fast || other.fast;
        res
    }
}


#[cfg(test)]
mod tests {
    use schema::Schema;
    use super::*;

    #[test]
    fn test_field_options() {
        {
            let field_options = STORED | FAST;
            assert!(field_options.is_stored());
            assert!(field_options.is_fast());
            assert!(!field_options.indexing_options().is_tokenized());
        }
        {
            let field_options = STORED | TEXT;
            assert!(field_options.is_stored());
            assert!(!field_options.is_fast());
            assert!(field_options.indexing_options().is_tokenized());
        }
        {
            let mut schema = Schema::new();
            let _body_field: TextField = schema.add_text_field("body", &TEXT);
            let field = schema.text_field("body");
            assert!(schema.text_field_options(&field).indexing_options().is_tokenized());
        }
    }
}
