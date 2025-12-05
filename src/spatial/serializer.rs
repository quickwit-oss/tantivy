//! HUSH
use std::io;
use std::io::Write;

use crate::directory::{CompositeWrite, WritePtr};
use crate::schema::Field;
use crate::spatial::bkd::write_block_kd_tree;
use crate::spatial::triangle::Triangle;

/// The fieldnorms serializer is in charge of
/// the serialization of field norms for all fields.
pub struct SpatialSerializer {
    composite_write: CompositeWrite,
}

impl SpatialSerializer {
    /// Create a composite file from the write pointer.
    pub fn from_write(write: WritePtr) -> io::Result<SpatialSerializer> {
        // just making room for the pointer to header.
        let composite_write = CompositeWrite::wrap(write);
        Ok(SpatialSerializer { composite_write })
    }

    /// Serialize the given field
    pub fn serialize_field(&mut self, field: Field, triangles: &mut [Triangle]) -> io::Result<()> {
        let write = self.composite_write.for_field(field);
        write_block_kd_tree(triangles, write)?;
        write.flush()?;
        Ok(())
    }

    /// Clean up, flush, and close.
    pub fn close(self) -> io::Result<()> {
        self.composite_write.close()?;
        Ok(())
    }
}
