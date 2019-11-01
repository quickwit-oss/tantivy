use crate::common::BinarySerializable;
use std::io;
use std::io::Read;
use std::io::Write;

/// `Field` is actually a `u8` identifying a `Field`
/// The schema is in charge of holding mapping between field names
/// to `Field` objects.
///
/// Because the field id is a `u8`, tantivy can only have at most `255` fields.
/// Value 255 is reserved.
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Field(pub u32);

impl Field {
    /// Create a new field object for the given FieldId.
    pub fn from_field_id(field_id: u32) -> Field {
        Field(field_id)
    }

    /// Returns a u32 identifying uniquely a field within a schema.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn field_id(&self) -> u32 {
        self.0
    }
}

impl BinarySerializable for Field {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.0.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Field> {
        u32::deserialize(reader).map(Field)
    }
}
