use std::io;
use std::io::Write;
use std::io::Read;
use common::BinarySerializable;

#[derive(Copy,Clone,Debug,PartialEq,PartialOrd,Eq,Ord,Hash)]
pub struct Field(pub u8);

impl BinarySerializable for Field {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        self.0.serialize(writer)
    }

    fn deserialize(reader: &mut Read) -> io::Result<Field> {
        u8::deserialize(reader).map(Field)
    }
}
    