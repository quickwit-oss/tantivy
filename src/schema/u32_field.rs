use std::io;
use std::io::Write;
use std::io::Read;

use common::BinarySerializable;
use rustc_serialize::Encodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encoder;

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct U32Field(pub u8);

impl BinarySerializable for U32Field {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let U32Field(field_id) = *self;
        field_id.serialize(writer)
    }

    fn deserialize(reader: &mut Read) -> io::Result<U32Field> {
        u8::deserialize(reader).map(U32Field)
    }
}

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq)]
pub struct U32FieldValue {
    pub field: U32Field,
    pub value: u32,
}


#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct U32Options {
    indexed: bool,
    fast: bool,
    stored: bool,
}


impl U32Options {

    pub fn new() -> U32Options {
        U32Options {
            fast: false,
            indexed: false,
            stored: false,
        }
    }

    pub fn is_indexed(&self,) -> bool {
        self.indexed
    }

    pub fn set_indexed(mut self,) -> U32Options {
        self.indexed = true;
        self
    }

    pub fn is_fast(&self,) -> bool {
        self.fast
    }

    pub fn set_fast(mut self,) -> U32Options {
        self.fast = true;
        self
    }
}




/// The field will be tokenized and indexed
pub const FAST_U32: U32Options = U32Options {
    indexed: false,
    stored: false,
    fast: true,
};
