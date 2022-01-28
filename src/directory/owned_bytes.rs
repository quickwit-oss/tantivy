use std::io;
use std::ops::Range;

pub use ownedbytes::OwnedBytes;

use crate::directory::FileHandle;

impl FileHandle for OwnedBytes {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        Ok(self.slice(range))
    }
}
