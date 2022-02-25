use std::ops::{Deref, Range};
use std::sync::{Arc, Weak};
use std::{fmt, io};

use async_trait::async_trait;
use common::HasLen;
use stable_deref_trait::StableDeref;

use crate::directory::OwnedBytes;

pub type ArcBytes = Arc<dyn Deref<Target = [u8]> + Send + Sync + 'static>;
pub type WeakArcBytes = Weak<dyn Deref<Target = [u8]> + Send + Sync + 'static>;

/// Objects that represents files sections in tantivy.
///
/// By contract, whatever happens to the directory file, as long as a FileHandle
/// is alive, the data associated with it cannot be altered or destroyed.
///
/// The underlying behavior is therefore specific to the `Directory` that created it.
/// Despite its name, a `FileSlice` may or may not directly map to an actual file
/// on the filesystem.

#[async_trait]
pub trait FileHandle: 'static + Send + Sync + HasLen + fmt::Debug {
    /// Reads a slice of bytes.
    ///
    /// This method may panic if the range requested is invalid.
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes>;

    #[cfg(feature = "quickwit")]
    #[doc(hidden)]
    async fn read_bytes_async(
        &self,
        _byte_range: Range<usize>,
    ) -> crate::AsyncIoResult<OwnedBytes> {
        Err(crate::error::AsyncIoError::AsyncUnsupported)
    }
}

#[async_trait]
impl FileHandle for &'static [u8] {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let bytes = &self[range];
        Ok(OwnedBytes::new(bytes))
    }

    #[cfg(feature = "quickwit")]
    async fn read_bytes_async(&self, byte_range: Range<usize>) -> crate::AsyncIoResult<OwnedBytes> {
        Ok(self.read_bytes(byte_range)?)
    }
}

impl<B> From<B> for FileSlice
where B: StableDeref + Deref<Target = [u8]> + 'static + Send + Sync
{
    fn from(bytes: B) -> FileSlice {
        FileSlice::new(Box::new(OwnedBytes::new(bytes)))
    }
}

/// Logical slice of read only file in tantivy.
///
/// It can be cloned and sliced cheaply.
#[derive(Clone)]
pub struct FileSlice {
    data: Arc<dyn FileHandle>,
    range: Range<usize>,
}

impl fmt::Debug for FileSlice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSlice({:?}, {:?})", &self.data, self.range)
    }
}

impl FileSlice {
    /// Wraps a FileHandle.
    pub fn new(file_handle: Box<dyn FileHandle>) -> Self {
        let num_bytes = file_handle.len();
        FileSlice::new_with_num_bytes(file_handle, num_bytes)
    }

    /// Wraps a FileHandle.
    #[doc(hidden)]
    #[must_use]
    pub fn new_with_num_bytes(file_handle: Box<dyn FileHandle>, num_bytes: usize) -> Self {
        FileSlice {
            data: Arc::from(file_handle),
            range: 0..num_bytes,
        }
    }

    /// Creates a fileslice that is just a view over a slice of the data.
    ///
    /// # Panics
    ///
    /// Panics if `byte_range.end` exceeds the filesize.
    #[must_use]
    pub fn slice(&self, byte_range: Range<usize>) -> FileSlice {
        assert!(byte_range.end <= self.len());
        FileSlice {
            data: self.data.clone(),
            range: self.range.start + byte_range.start..self.range.start + byte_range.end,
        }
    }

    /// Creates an empty FileSlice
    pub fn empty() -> FileSlice {
        const EMPTY_SLICE: &[u8] = &[];
        FileSlice::from(EMPTY_SLICE)
    }

    /// Returns a `OwnedBytes` with all of the data in the `FileSlice`.
    ///
    /// The behavior is strongly dependant on the implementation of the underlying
    /// `Directory` and the `FileSliceTrait` it creates.
    /// In particular, it is  up to the `Directory` implementation
    /// to handle caching if needed.
    pub fn read_bytes(&self) -> io::Result<OwnedBytes> {
        self.data.read_bytes(self.range.clone())
    }

    #[cfg(feature = "quickwit")]
    #[doc(hidden)]
    pub async fn read_bytes_async(&self) -> crate::AsyncIoResult<OwnedBytes> {
        self.data.read_bytes_async(self.range.clone()).await
    }

    /// Reads a specific slice of data.
    ///
    /// This is equivalent to running `file_slice.slice(from, to).read_bytes()`.
    pub fn read_bytes_slice(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        assert!(
            range.end <= self.len(),
            "end of requested range exceeds the fileslice length ({} > {})",
            range.end,
            self.len()
        );
        self.data
            .read_bytes(self.range.start + range.start..self.range.start + range.end)
    }

    #[cfg(feature = "quickwit")]
    #[doc(hidden)]
    pub async fn read_bytes_slice_async(
        &self,
        byte_range: Range<usize>,
    ) -> crate::AsyncIoResult<OwnedBytes> {
        assert!(
            self.range.start + byte_range.end <= self.range.end,
            "`to` exceeds the fileslice length"
        );
        self.data
            .read_bytes_async(
                self.range.start + byte_range.start..self.range.start + byte_range.end,
            )
            .await
    }

    /// Splits the FileSlice at the given offset and return two file slices.
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

    /// Like `.slice(...)` but enforcing only the `from`
    /// boundary.
    ///
    /// Equivalent to `.slice(from_offset, self.len())`
    #[must_use]
    pub fn slice_from(&self, from_offset: usize) -> FileSlice {
        self.slice(from_offset..self.len())
    }

    /// Returns a slice from the end.
    ///
    /// Equivalent to `.slice(self.len() - from_offset, self.len())`
    #[must_use]
    pub fn slice_from_end(&self, from_offset: usize) -> FileSlice {
        self.slice(self.len() - from_offset..self.len())
    }

    /// Like `.slice(...)` but enforcing only the `to`
    /// boundary.
    ///
    /// Equivalent to `.slice(0, to_offset)`
    #[must_use]
    pub fn slice_to(&self, to_offset: usize) -> FileSlice {
        self.slice(0..to_offset)
    }
}

#[async_trait]
impl FileHandle for FileSlice {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.read_bytes_slice(range)
    }

    #[cfg(feature = "quickwit")]
    async fn read_bytes_async(&self, byte_range: Range<usize>) -> crate::AsyncIoResult<OwnedBytes> {
        self.read_bytes_slice_async(byte_range).await
    }
}

impl HasLen for FileSlice {
    fn len(&self) -> usize {
        self.range.len()
    }
}

#[async_trait]
impl FileHandle for OwnedBytes {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        Ok(self.slice(range))
    }

    #[cfg(feature = "quickwit")]
    async fn read_bytes_async(&self, range: Range<usize>) -> crate::AsyncIoResult<OwnedBytes> {
        let bytes = self.read_bytes(range)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use common::HasLen;

    use super::{FileHandle, FileSlice};

    #[test]
    fn test_file_slice() -> io::Result<()> {
        let file_slice = FileSlice::new(Box::new(b"abcdef".as_ref()));
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
            let (left, right) = file_slice.split_from_end(2);
            assert_eq!(left.read_bytes()?.as_slice(), b"abcd");
            assert_eq!(right.read_bytes()?.as_slice(), b"ef");
        }
        Ok(())
    }

    #[test]
    fn test_file_slice_trait_slice_len() {
        let blop: &'static [u8] = b"abc";
        let owned_bytes: Box<dyn FileHandle> = Box::new(blop);
        assert_eq!(owned_bytes.len(), 3);
    }

    #[test]
    fn test_slice_simple_read() -> io::Result<()> {
        let slice = FileSlice::new(Box::new(&b"abcdef"[..]));
        assert_eq!(slice.len(), 6);
        assert_eq!(slice.read_bytes()?.as_ref(), b"abcdef");
        assert_eq!(slice.slice(1..4).read_bytes()?.as_ref(), b"bcd");
        Ok(())
    }

    #[test]
    fn test_slice_read_slice() -> io::Result<()> {
        let slice_deref = FileSlice::new(Box::new(&b"abcdef"[..]));
        assert_eq!(slice_deref.read_bytes_slice(1..4)?.as_ref(), b"bcd");
        Ok(())
    }

    #[test]
    #[should_panic(expected = "end of requested range exceeds the fileslice length (10 > 6)")]
    fn test_slice_read_slice_invalid_range_exceeds() {
        let slice_deref = FileSlice::new(Box::new(&b"abcdef"[..]));
        assert_eq!(
            slice_deref.read_bytes_slice(0..10).unwrap().as_ref(),
            b"bcd"
        );
    }
}
