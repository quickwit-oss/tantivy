use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

use crate::common::BinarySerializable;

use crate::common::CompositeWrite;
use crate::directory::WritePtr;
use crate::schema::Field;
use std::io;
use std::io::Write;

/// The fieldnorms serializer is in charge of
/// the serialization of field norms for all fields.
pub struct FieldNormsSerializer {
    composite_write: CompositeWrite,
}

impl FieldNormsSerializer {
    /// Constructor
    pub fn from_write(write: WritePtr) -> io::Result<FieldNormsSerializer> {
        // just making room for the pointer to header.
        let composite_write = CompositeWrite::wrap(write);
        Ok(FieldNormsSerializer { composite_write })
    }

    /// Serialize the given field
    pub fn serialize_field(
        &mut self,
        field: Field,
        fieldnorms_data: &[u8],
        max_value: u8,
    ) -> io::Result<()> {
        let write = self.composite_write.for_field(field);
        max_value.serialize(write)?;
        (fieldnorms_data.len() as u32).serialize(write)?; // num_docs
        let mut bitpacker = BitPacker::new();
        let num_bits = compute_num_bits(max_value as u64);
        for val in fieldnorms_data {
            bitpacker.write(*val as u64, num_bits, write)?;
        }
        bitpacker.close(write)?;
        write.flush()?;
        Ok(())
    }

    /// Clean up / flush / close
    pub fn close(self) -> io::Result<()> {
        self.composite_write.close()?;
        Ok(())
    }
}
