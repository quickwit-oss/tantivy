use fst::raw::MmapReadOnly;
use std::ops::Deref;
use super::shared_vec_slice::SharedVecSlice;
use common::HasLen;


/// Read object that represents files in tantivy.
///
/// These read objects are only in charge to deliver
/// the data in the form of a constant read-only `&[u8]`.
/// Whatever happens to the directory file, the data
/// hold by this object should never be altered or destroyed.
pub enum ReadOnlySource {
    /// Mmap source of data
    Mmap(MmapReadOnly),
    /// Wrapping a `Vec<u8>`
    Anonymous(SharedVecSlice),
}

impl Deref for ReadOnlySource {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl ReadOnlySource {
    /// Creates an empty ReadOnlySource
    pub fn empty() -> ReadOnlySource {
        ReadOnlySource::Anonymous(SharedVecSlice::empty())
    }

    /// Returns the data underlying the ReadOnlySource object.
    pub fn as_slice(&self) -> &[u8] {
        match *self {
            ReadOnlySource::Mmap(ref mmap_read_only) => unsafe { mmap_read_only.as_slice() },
            ReadOnlySource::Anonymous(ref shared_vec) => shared_vec.as_slice(),
        }
    }

    /// Creates a ReadOnlySource that is just a
    /// view over a slice of the data.
    ///
    /// Keep in mind that any living slice extends
    /// the lifetime of the original ReadOnlySource,
    ///
    /// For instance, if `ReadOnlySource` wraps 500MB
    /// worth of data in anonymous memory, and only a
    /// 1KB slice is remaining, the whole `500MBs`
    /// are retained in memory.
    pub fn slice(&self, from_offset: usize, to_offset: usize) -> ReadOnlySource {
        match *self {
            ReadOnlySource::Mmap(ref mmap_read_only) => {
                let sliced_mmap = mmap_read_only.range(from_offset, to_offset - from_offset);
                ReadOnlySource::Mmap(sliced_mmap)
            }
            ReadOnlySource::Anonymous(ref shared_vec) => {
                ReadOnlySource::Anonymous(shared_vec.slice(from_offset, to_offset))
            }
        }
    }
}

impl HasLen for ReadOnlySource {
    fn len(&self) -> usize {
        self.as_slice().len()
    }
}

impl Clone for ReadOnlySource {
    fn clone(&self) -> Self {
        self.slice(0, self.len())
    }
}
