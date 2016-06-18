use std::io::Write;
use std::io;

use std::io::Read;
use common::BinarySerializable;
use rustc_serialize::Decoder;
use rustc_serialize::Encoder;
use std::ops::BitOr;


// TODO impl Copy trait

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct Field(pub u8);

impl BinarySerializable for Field {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        self.0.serialize(writer)
    }

    fn deserialize(reader: &mut Read) -> io::Result<Field> {
        u8::deserialize(reader).map(Field)
    }
}
    