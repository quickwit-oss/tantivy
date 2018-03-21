use directory::WritePtr;
use std::io;
use common::CompositeWrite;


pub struct FieldNormSerializer {
    composite_write: CompositeWrite,
}

impl FieldNormSerializer {

    /// Constructor
    pub fn from_write(write: WritePtr) -> io::Result<FieldNormSerializer> {
        // just making room for the pointer to header.
        let composite_write = CompositeWrite::wrap(write);
        Ok(FieldNormSerializer {
            composite_write
        })
    }

}

