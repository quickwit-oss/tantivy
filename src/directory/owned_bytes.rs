use crate::directory::FileHandle;
use std::io;
use std::ops::Range;

pub use ownedbytes::OwnedBytes;

impl FileHandle for OwnedBytes {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        Ok(self.slice(range))
    }
}
