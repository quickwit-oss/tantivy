use common::CompositeWrite;
use directory::WritePtr;
use schema::Field;
use std::io;
use std::io::Write;

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

    pub fn serialize_field(&mut self, field: Field, fieldnorms_data: &[u8]) -> io::Result<()> {
        let write = self.composite_write.for_field(field);
        write.write_all(fieldnorms_data)?;
        write.flush()?;
        Ok(())
    }

    pub fn close(self) -> io::Result<()> {
        self.composite_write.close()?;
        Ok(())
    }
}
