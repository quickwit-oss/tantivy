use crate::common::HasLen;
use crate::directory::OwnedBytes;
use stable_deref_trait::{CloneStableDeref, StableDeref};
use std::sync::Arc;
use std::{io, ops::Deref};

pub type BoxedData = Box<dyn Deref<Target = [u8]> + Send + Sync + 'static>;

/// Objects that represents files sections in tantivy.
///
/// These read objects are only in charge to deliver
/// the data in the form of a constant read-only `&[u8]`.
/// Whatever happens to the directory file, the data
/// hold by this object should never be altered or destroyed.
pub trait FileSliceTrait: 'static + Send + Sync + HasLen {
    fn read_bytes(&self) -> io::Result<OwnedBytes>;
    fn slice(&self, from: usize, to: usize) -> FileSlice;
}

impl FileSliceTrait for &'static [u8] {
    fn read_bytes(&self) -> io::Result<OwnedBytes> {
        Ok(OwnedBytes::new(*self))
    }

    fn slice(&self, from: usize, to: usize) -> FileSlice {
        FileSlice::from(&self[from..to])
    }
}

impl HasLen for &'static [u8] {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

/// Logical slice of read only file in tantivy.
//
/// In other words, it is more or less equivalent to the triplet `(file, start_byteoffset, stop_offset)`.
///
/// FileSlice is a simple wrapper over an `Arc<Box<dyn FileSliceTrait>>`. It can
/// be cloned cheaply.
///
/// The underlying behavior is therefore specific to the `Directory` that created it.
/// Despite its name, a `FileSlice` may or may not directly map to an actual file
/// on the filesystem.
#[derive(Clone)]
pub struct FileSlice(Arc<Box<dyn FileSliceTrait>>);

impl FileSlice {
    /// Creates a FileSlice, wrapping over a FileSliceTrait.
    pub fn new<D>(data: D) -> Self
    where
        D: Deref<Target = [u8]> + Send + Sync + 'static,
    {
        FileSlice::from(SlicedDeref::new(data))
    }

    /// Creates an empty FileSlice
    pub fn empty() -> FileSlice {
        let data: &'static [u8] = &[];
        FileSlice::from(data)
    }

    /// Returns a `OwnedBytes` with all of the data in the `FileSlice`.
    ///
    /// The behavior is strongly dependant on the implementation of the underlying
    /// `Directory` and the `FileSliceTrait` it creates.
    /// In particular, it is  up to the `Directory` implementation
    /// to handle caching if needed.
    pub fn read_bytes(&self) -> io::Result<OwnedBytes> {
        self.0.read_bytes()
    }

    /// Splits the file slice at the given offset and return two file slices.
    /// `file_slice[..split_offset]` and `file_slice[split_offset..]`.
    ///
    /// This operation is cheap and must not copy any underlying data.
    pub fn split(self, left_len: usize) -> (FileSlice, FileSlice) {
        let left = self.slice_to(left_len);
        let right = self.slice_from(left_len);
        (left, right)
    }

    /// Splits the file slice at the given offset and return two file slices.
    /// `file_slice[..split_offset]` and `file_slice[split_offset..]`.
    pub fn split_from_end(self, right_len: usize) -> (FileSlice, FileSlice) {
        let left_len = self.len() - right_len;
        self.split(left_len)
    }

    /// Creates a FileSlice that is just a view over a slice of the data.
    pub fn slice(&self, start: usize, stop: usize) -> FileSlice {
        assert!(
            start <= stop,
            "Requested negative slice [{}..{}]",
            start,
            stop
        );
        assert!(stop <= self.len());
        self.0.slice(start, stop)
    }

    /// Like `.slice(...)` but enforcing only the `from`
    /// boundary.
    ///
    /// Equivalent to `.slice(from_offset, self.len())`
    pub fn slice_from(&self, from_offset: usize) -> FileSlice {
        self.slice(from_offset, self.len())
    }

    /// Like `.slice(...)` but enforcing only the `to`
    /// boundary.
    ///
    /// Equivalent to `.slice(0, to_offset)`
    pub fn slice_to(&self, to_offset: usize) -> FileSlice {
        self.slice(0, to_offset)
    }
}

impl HasLen for FileSlice {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<S: FileSliceTrait> From<S> for FileSlice {
    fn from(file: S) -> Self {
        FileSlice(Arc::new(Box::new(file)))
    }
}

impl From<Arc<BoxedData>> for FileSlice {
    fn from(data: Arc<BoxedData>) -> Self {
        let slice_deref: SlicedDeref = SlicedDeref::from(data);
        FileSlice::from(slice_deref)
    }
}

/// `SliceDeref` wraps an `Arc<BoxData>` to implement `FileSliceTrait` .
/// It keeps track of (start, stop) boundaries.
#[derive(Clone)]
pub struct SlicedDeref {
    data: Arc<BoxedData>,
    start: usize,
    stop: usize,
}

impl SlicedDeref {
    /// Wraps a new `Deref<Target = [u8]>`
    pub fn new<D>(data: D) -> Self
    where
        D: Deref<Target = [u8]> + 'static + Send + Sync,
    {
        let len = data.len();
        SlicedDeref {
            data: Arc::new(Box::new(data)),
            start: 0,
            stop: len,
        }
    }
}

impl From<Arc<BoxedData>> for SlicedDeref {
    fn from(data: Arc<BoxedData>) -> Self {
        let len = data.len();
        SlicedDeref {
            data,
            start: 0,
            stop: len,
        }
    }
}

unsafe impl StableDeref for SlicedDeref {}
unsafe impl CloneStableDeref for SlicedDeref {}

impl FileSliceTrait for SlicedDeref {
    fn read_bytes(&self) -> io::Result<OwnedBytes> {
        Ok(OwnedBytes::new(self.clone()))
    }

    fn slice(&self, from: usize, to: usize) -> FileSlice {
        assert!(to <= self.len());
        FileSlice::from(SlicedDeref {
            data: self.data.clone(),
            start: self.start + from,
            stop: self.start + to,
        })
    }
}

impl HasLen for SlicedDeref {
    fn len(&self) -> usize {
        self.stop - self.start
    }
}

impl Deref for SlicedDeref {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data.deref()[self.start..self.stop]
    }
}

#[cfg(test)]
mod tests {
    use super::{FileSlice, FileSliceTrait, SlicedDeref};
    use crate::common::HasLen;
    use std::io;

    #[test]
    fn test_file_slice() -> io::Result<()> {
        let file_slice = FileSlice::new(b"abcdef".as_ref());
        assert_eq!(file_slice.len(), 6);
        assert_eq!(file_slice.slice_from(2).read_bytes()?.as_slice(), b"cdef");
        assert_eq!(file_slice.slice_to(2).read_bytes()?.as_slice(), b"ab");
        assert_eq!(
            file_slice
                .slice_from(1)
                .slice_to(2)
                .read_bytes()?
                .as_slice(),
            b"bc"
        );
        {
            let (left, right) = file_slice.clone().split(0);
            assert_eq!(left.read_bytes()?.as_slice(), b"");
            assert_eq!(right.read_bytes()?.as_slice(), b"abcdef");
        }
        {
            let (left, right) = file_slice.clone().split(2);
            assert_eq!(left.read_bytes()?.as_slice(), b"ab");
            assert_eq!(right.read_bytes()?.as_slice(), b"cdef");
        }
        {
            let (left, right) = file_slice.clone().split_from_end(0);
            assert_eq!(left.read_bytes()?.as_slice(), b"abcdef");
            assert_eq!(right.read_bytes()?.as_slice(), b"");
        }
        {
            let (left, right) = file_slice.clone().split_from_end(2);
            assert_eq!(left.read_bytes()?.as_slice(), b"abcd");
            assert_eq!(right.read_bytes()?.as_slice(), b"ef");
        }
        Ok(())
    }

    #[test]
    fn test_file_slice_trait_slice_len() {
        let blop: &'static [u8] = b"abc";
        let owned_bytes: Box<dyn FileSliceTrait> = Box::new(blop);
        assert_eq!(owned_bytes.len(), 3);
    }

    #[test]
    fn test_slice_deref() -> io::Result<()> {
        let slice_deref = SlicedDeref::new(&b"abcdef"[..]);
        assert_eq!(slice_deref.len(), 6);
        assert_eq!(slice_deref.read_bytes()?.as_ref(), b"abcdef");
        assert_eq!(slice_deref.slice(1, 4).read_bytes()?.as_ref(), b"bcd");
        Ok(())
    }
}
