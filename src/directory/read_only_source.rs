use super::shared_vec_slice::SharedVecSlice;
use common::HasLen;
#[cfg(feature = "mmap")]
use fst::raw::MmapReadOnly;
use stable_deref_trait::{CloneStableDeref, StableDeref};
use std::io::{self, Read};
use std::ops::Deref;
use std::slice;

/// Read object that represents files in tantivy.
///
/// These read objects are only in charge to deliver
/// the data in the form of a constant read-only `&[u8]`.
/// Whatever happens to the directory file, the data
/// hold by this object should never be altered or destroyed.
pub enum ReadOnlySource {
    /// Mmap source of data
    #[cfg(feature = "mmap")]
    Mmap(MmapReadOnly),
    /// Wrapping a `Vec<u8>`
    Anonymous(SharedVecSlice),
}

unsafe impl StableDeref for ReadOnlySource {}
unsafe impl CloneStableDeref for ReadOnlySource {}

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
            #[cfg(feature = "mmap")]
            ReadOnlySource::Mmap(ref mmap_read_only) => mmap_read_only.as_slice(),
            ReadOnlySource::Anonymous(ref shared_vec) => shared_vec.as_slice(),
        }
    }

    /// Splits into 2 `ReadOnlySource`, at the offset given
    /// as an argument.
    pub fn split(self, addr: usize) -> (ReadOnlySource, ReadOnlySource) {
        let left = self.slice(0, addr);
        let right = self.slice_from(addr);
        (left, right)
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
        assert!(
            from_offset <= to_offset,
            "Requested negative slice [{}..{}]",
            from_offset,
            to_offset
        );
        match *self {
            #[cfg(feature = "mmap")]
            ReadOnlySource::Mmap(ref mmap_read_only) => {
                let sliced_mmap = mmap_read_only.range(from_offset, to_offset - from_offset);
                ReadOnlySource::Mmap(sliced_mmap)
            }
            ReadOnlySource::Anonymous(ref shared_vec) => {
                ReadOnlySource::Anonymous(shared_vec.slice(from_offset, to_offset))
            }
        }
    }

    /// Like `.slice(...)` but enforcing only the `from`
    /// boundary.
    ///
    /// Equivalent to `.slice(from_offset, self.len())`
    pub fn slice_from(&self, from_offset: usize) -> ReadOnlySource {
        let len = self.len();
        self.slice(from_offset, len)
    }

    /// Like `.slice(...)` but enforcing only the `to`
    /// boundary.
    ///
    /// Equivalent to `.slice(0, to_offset)`
    pub fn slice_to(&self, to_offset: usize) -> ReadOnlySource {
        self.slice(0, to_offset)
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

impl From<Vec<u8>> for ReadOnlySource {
    fn from(data: Vec<u8>) -> ReadOnlySource {
        let shared_data = SharedVecSlice::from(data);
        ReadOnlySource::Anonymous(shared_data)
    }
}

/// Acts as a owning cursor over the data backed up by a `ReadOnlySource`
pub(crate) struct SourceRead {
    _data_owner: ReadOnlySource,
    cursor: &'static [u8],
}

impl SourceRead {
    // Advance the cursor by a given number of bytes.
    pub fn advance(&mut self, len: usize) {
        self.cursor = &self.cursor[len..];
    }

    pub fn slice_from(&self, start: usize) -> &[u8] {
        &self.cursor[start..]
    }

    pub fn get(&self, idx: usize) -> u8 {
        self.cursor[idx]
    }
}

impl AsRef<[u8]> for SourceRead {
    fn as_ref(&self) -> &[u8] {
        self.cursor
    }
}

impl From<ReadOnlySource> for SourceRead {
    // Creates a new `SourceRead` from a given `ReadOnlySource`
    fn from(source: ReadOnlySource) -> SourceRead {
        let len = source.len();
        let slice_ptr = source.as_slice().as_ptr();
        let static_slice = unsafe { slice::from_raw_parts(slice_ptr, len) };
        SourceRead {
            _data_owner: source,
            cursor: static_slice,
        }
    }
}

impl Read for SourceRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.cursor.read(buf)
    }
}
