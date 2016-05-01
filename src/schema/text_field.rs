use std::io::Write;
use std::io;

use std::io::Read;
use core::serialize::BinarySerializable;
use rustc_serialize::Encodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encoder;
use std::ops::BitOr;

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct TextField(pub u8);


#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct TextOptions {
    tokenized_indexed: bool,
    stored: bool,
    fast: bool,
}

impl TextOptions {
    pub fn is_tokenized_indexed(&self,) -> bool {
        self.tokenized_indexed
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

    pub fn set_tokenized_indexed(mut self,) -> TextOptions {
        self.tokenized_indexed = true;
        self
    }

    pub fn new() -> TextOptions {
        TextOptions {
            fast: false,
            tokenized_indexed: false,
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





/// The field will be tokenized and indexed
pub const TEXT: TextOptions = TextOptions {
    tokenized_indexed: true,
    stored: false,
    fast: false,
};

/// A stored fields of a document can be retrieved given its DocId.
/// Stored field are stored together and LZ4 compressed.
/// Reading the stored fields of a document is relatively slow.
/// (100 microsecs)
pub const STORED: TextOptions = TextOptions {
    tokenized_indexed: false,
    stored: true,
    fast: false,
};

/// Fast field are used for field you need to access many times during
/// collection. (e.g: for sort, aggregates).
pub const FAST: TextOptions = TextOptions {
    tokenized_indexed: false,
    stored: false,
    fast: true
};


impl BitOr for TextOptions {

    type Output = TextOptions;

    fn bitor(self, other: TextOptions) -> TextOptions {
        let mut res = TextOptions::new();
        res.tokenized_indexed = self.tokenized_indexed || other.tokenized_indexed;
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
            assert!(!field_options.is_tokenized_indexed());
        }
        {
            let field_options = STORED | TEXT;
            assert!(field_options.is_stored());
            assert!(!field_options.is_fast());
            assert!(field_options.is_tokenized_indexed());
        }
        {
            let mut schema = Schema::new();
            let _body_field: TextField = schema.add_text_field("body", &TEXT);
            let field = schema.text_field("body");
            assert!(schema.text_field_options(&field).is_tokenized_indexed());
        }
    }
}
