use crate::common::BinarySerializable;
use std::io;
use std::io::Read;
use std::io::Write;

/// `Field` is represented by an unsigned 32-bit integer type
/// The schema holds the mapping between field names and `Field` objects.
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Field(pub u32);

impl BinarySerializable for Field {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.0.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Field> {
        u32::deserialize(reader).map(Field)
    }
}
