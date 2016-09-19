use std::io;
use std::io::Write;
use std::io::Read;
use common::BinarySerializable;


/// `Field` is actually a `u8` identifying a `Field`
/// The schema is in charge of holding mapping between field names
/// to `Field` objects.
/// 
/// Because the field id is a `u8`, tantivy can only have at most `256` fields
#[derive(Copy,Clone,Debug,PartialEq,PartialOrd,Eq,Ord,Hash, RustcEncodable, RustcDecodable)]
pub struct Field(pub u8);

impl BinarySerializable for Field {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        self.0.serialize(writer)
    }

    fn deserialize(reader: &mut Read) -> io::Result<Field> {
        u8::deserialize(reader).map(Field)
    }
}

