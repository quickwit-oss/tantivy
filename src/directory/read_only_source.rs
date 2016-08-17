use fst::raw::MmapReadOnly;
use std::ops::Deref;
use std::io::Cursor;
use super::SharedVecSlice;

////////////////////////////////////////
// Read only source.


pub enum ReadOnlySource {
    Mmap(MmapReadOnly),
    Anonymous(SharedVecSlice),
}

impl Deref for ReadOnlySource {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl ReadOnlySource {

    pub fn len(&self,) -> usize {
        self.as_slice().len()
    }

    pub fn empty() -> ReadOnlySource {
        ReadOnlySource::Anonymous(SharedVecSlice::empty())
    }

    pub fn as_slice(&self,) -> &[u8] {
        match *self {
            ReadOnlySource::Mmap(ref mmap_read_only) => unsafe { 
                mmap_read_only.as_slice()
            },
            ReadOnlySource::Anonymous(ref shared_vec) => {
                shared_vec.as_slice()
            },
        }
    }

    pub fn cursor<'a>(&'a self) -> Cursor<&'a [u8]> {
        Cursor::new(&self.deref())
    }

    pub fn slice(&self, from_offset:usize, to_offset:usize) -> ReadOnlySource {
        match *self {
            ReadOnlySource::Mmap(ref mmap_read_only) => {
                let sliced_mmap = mmap_read_only.range(from_offset, to_offset - from_offset);
                ReadOnlySource::Mmap(sliced_mmap)
            }
            ReadOnlySource::Anonymous(ref shared_vec) => {
                ReadOnlySource::Anonymous(shared_vec.slice(from_offset, to_offset))
            },
        }
    }
}

impl Clone for ReadOnlySource {
    fn clone(&self) -> Self {
        self.slice(0, self.len())
    }
}
