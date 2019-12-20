use crate::common::HasLen;
use std::cmp;
use std::convert::TryInto;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::ops::{Deref};
use std::sync::{Arc, Weak};

pub struct InnerBoxedData(Arc<Box<dyn Deref<Target = [u8]> + Send + Sync + 'static>>);

pub struct BoxedData(Cursor<InnerBoxedData>);

impl Deref for InnerBoxedData {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for InnerBoxedData {
    fn as_ref(&self) -> &[u8] {
        &self.0.as_ref()
    }
}

impl InnerBoxedData {
    pub fn new(data: Arc<Box<dyn Deref<Target = [u8]> + Send + Sync + 'static>>) -> Self {
        InnerBoxedData(data)
    }

    pub(crate) fn downgrade(&self) -> Weak<Box<dyn Deref<Target = [u8]> + Send + Sync + 'static>> {
        Arc::downgrade(&self.0)
    }
}

impl Clone for InnerBoxedData {
    fn clone(&self) -> Self {
        InnerBoxedData(self.0.clone())
    }
}

impl BoxedData {
    pub fn new(data: Arc<Box<dyn Deref<Target = [u8]> + Send + Sync + 'static>>) -> Self {
        BoxedData(Cursor::new(InnerBoxedData::new(data)))
    }
}

impl Read for BoxedData {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Seek for BoxedData {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.0.seek(pos)
    }
}

impl Deref for BoxedData {
    type Target = InnerBoxedData;
    fn deref(&self) -> &InnerBoxedData {
        self.0.get_ref()
    }
}

impl Clone for BoxedData {
    fn clone(&self) -> Self {
        BoxedData(Cursor::new(self.0.get_ref().clone()))
    }
}

impl HasLen for BoxedData {
    fn len(&self) -> usize {
        self.0.get_ref().len()
    }
}

/// A trait that is used as the underlying data source for `ReadOnlySource`
pub trait ReadOnlyData: Read + Seek + HasLen + Sync + Send {
    /// Create a new view of the data source. This is essentially a clone that
    /// gets boxed.
    fn snapshot(&self) -> Box<dyn ReadOnlyData>;
}

impl ReadOnlyData for BoxedData {
    fn snapshot(&self) -> Box<dyn ReadOnlyData> {
        Box::new(self.clone())
    }
}

impl ReadOnlyData for ReadOnlySource {
    fn snapshot(&self) -> Box<dyn ReadOnlyData> {
        Box::new(self.clone())
    }
}

/// Read object that represents files in tantivy.
///
/// These read objects are only in charge to deliver
/// the data in the form of a constant read-only `&[u8]`.
/// Whatever happens to the directory file, the data
/// hold by this object should never be altered or destroyed.
pub struct ReadOnlySource {
    data: Box<dyn ReadOnlyData>,
    start: usize,
    stop: usize,
    pos: usize,
}

impl From<BoxedData> for ReadOnlySource {
    fn from(data: BoxedData) -> Self {
        let len = data.len();
        ReadOnlySource {
            data: Box::new(data),
            start: 0,
            stop: len,
            pos: 0,
        }
    }
}

/// A version of ReadOnlySource that slices forward as you read from it.
pub struct AdvancingReadOnlySource(ReadOnlySource);

impl AdvancingReadOnlySource {
    /// Create an empty AdvancingReadOnlySource.
    pub fn empty() -> AdvancingReadOnlySource {
        AdvancingReadOnlySource(ReadOnlySource::empty())
    }

    /// Slice this current source forward.
    /// # Arguments
    /// `clip_len` - How many bytes should the source slice forward.
    pub fn advance(&mut self, clip_len: usize) {
        self.0.start += clip_len;
        self.0
            .seek(SeekFrom::Start(0))
            .expect("Can't seek while advancing");
    }


    /// Splits into 2 `AdvancingReadOnlySource`, at the offset given
    /// as an argument.
    pub fn split(self, addr: usize) -> (AdvancingReadOnlySource, AdvancingReadOnlySource) {
        let (left, right) = self.0.split(addr);
        (
            AdvancingReadOnlySource::from(left),
            AdvancingReadOnlySource::from(right),
        )
    }

    /// Get a single byte out of the source without advancing the cursor.
    /// # Arguments
    /// `idx` - The position of the byte that should be retrieved.
    pub fn get(&mut self, idx: usize) -> u8 {
        self.0.get(idx).expect("Can't get a byte out of the reader")
    }

    /// Read the whole source into a vector. This reads from the current
    /// position to the end and seeks back to the start.
    pub fn read_all(&mut self) -> std::io::Result<Vec<u8>> {
        self.0.read_all()
    }

    /// Read data into the provided buffer without slicing forward.
    /// Afther the read this method seeks back to the cursor position before the
    /// read.
    pub fn read_without_advancing(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let current_location = self.0.seek(SeekFrom::Current(0))?;
        let n = self.0.read(buf)?;
        self.0.seek(SeekFrom::Start(current_location))?;
        Ok(n)
    }

    /// Is the source empty.
    pub fn is_empty(&self) -> bool {
        self.0.start == self.0.stop
    }
}

impl From<ReadOnlySource> for AdvancingReadOnlySource {
    fn from(source: ReadOnlySource) -> AdvancingReadOnlySource {
        AdvancingReadOnlySource(source)
    }
}

impl From<Vec<u8>> for AdvancingReadOnlySource {
    fn from(data: Vec<u8>) -> AdvancingReadOnlySource {
        AdvancingReadOnlySource::from(ReadOnlySource::from(data))
    }
}

impl Clone for AdvancingReadOnlySource {
    fn clone(&self) -> Self {
        AdvancingReadOnlySource(self.0.slice_from(0))
    }
}

impl Read for AdvancingReadOnlySource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.0.read(buf)?;
        self.advance(n);
        Ok(n)
    }
}

impl ReadOnlySource {
    /// Create a new ReadOnlySource from a data source.
    pub fn new<D>(data: D) -> ReadOnlySource
    where
        D: ReadOnlyData + 'static,
    {
        let len = data.len();
        ReadOnlySource {
            data: Box::new(data),
            start: 0,
            stop: len,
            pos: 0,
        }
    }

    /// Creates an empty ReadOnlySource
    pub fn empty() -> ReadOnlySource {
        ReadOnlySource::from(Vec::new())
    }

    /// Splits into 2 `ReadOnlySource`, at the offset given
    /// as an argument.
    pub fn split(self, addr: usize) -> (ReadOnlySource, ReadOnlySource) {
        let left = self.slice(0, addr);
        let right = self.slice_from(addr);
        (left, right)
    }

    /// Splits into 2 `ReadOnlySource`, at the offset `end - right_len`.
    pub fn split_from_end(self, right_len: usize) -> (ReadOnlySource, ReadOnlySource) {
        let left_len = self.len() - right_len;
        self.split(left_len)
    }

    /// Read the whole source into a vector. This reads from the current
    /// position to the end and seeks back to the start.
    pub fn read_all(&mut self) -> std::io::Result<Vec<u8>> {
        let mut ret = Vec::new();
        self.read_to_end(&mut ret)?;
        self.seek(SeekFrom::Start(0))?;
        Ok(ret)
    }

    /// Get a single byte out of the source without advancing the cursor.
    /// # Arguments
    /// `idx` - The position of the byte that should be retrieved.
    pub fn get(&mut self, idx: usize) -> std::io::Result<u8> {
        let current_location = self.seek(SeekFrom::Current(0))?;
        let mut ret = vec![0u8; 1];
        self.seek(SeekFrom::Start(idx as u64))?;
        self.read_exact(&mut ret)?;
        self.seek(SeekFrom::Start(current_location))?;
        Ok(ret[0])
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
    pub fn slice(&self, start: usize, stop: usize) -> ReadOnlySource {
        assert!(
            start <= stop,
            "Requested negative slice [{}..{}]",
            start,
            stop
        );
        assert!(stop <= self.len());

        let mut data = self.data.snapshot();
        let pos = data.seek(SeekFrom::Start(
            (self.start + start)
                .try_into()
                .expect("Can't convert seek start position while slicing"),
        ))
        .expect("Can't seek while slicing");

        ReadOnlySource {
            data,
            start: self.start + start,
            stop: self.start + stop,
            pos: pos as usize,
        }
    }

    /// Like `.slice(...)` but enforcing only the `from`
    /// boundary.
    ///
    /// Equivalent to `.slice(from_offset, self.len())`
    pub fn slice_from(&self, from_offset: usize) -> ReadOnlySource {
        self.slice(from_offset, self.len())
    }

    /// Like `.slice(...)` but enforcing only the `to`
    /// boundary.
    ///
    /// Equivalent to `.slice(0, to_offset)`
    pub fn slice_to(&self, to_offset: usize) -> ReadOnlySource {
        self.slice(0, to_offset)
    }
}

impl Read for ReadOnlySource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let max = cmp::min(buf.len(), self.stop - self.pos);

        let n = self.data.read(&mut buf[..max])?;
        self.pos += n;
        Ok(n)
    }
}

impl Seek for ReadOnlySource {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let seek = match pos {
            SeekFrom::Start(n) => {
                let n = n.checked_add(self.start as u64).ok_or_else(|| std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                ))?;
                SeekFrom::Start(n)
            }
            SeekFrom::End(n) => {
                if n > 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "invalid seek beyond the source",
                    ));
                } else if n == 0 {
                    SeekFrom::End(n)
                } else {
                    let true_end = self.data.seek(SeekFrom::End(0))?;
                    let offset = true_end - self.stop as u64;
                    let offset: i64 = n.checked_add(offset as i64).ok_or_else(|| std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "invalid seek to a negative or overflowing position",
                    ))?;
                    SeekFrom::End(offset)
                }
            },

            SeekFrom::Current(n) => SeekFrom::Current(n),
        };

        let pos = self.data.seek(seek)?;
        self.pos = pos as usize;

        Ok(pos - self.start as u64)
    }
}

impl HasLen for ReadOnlySource {
    fn len(&self) -> usize {
        self.stop - self.start
    }
}

impl Clone for ReadOnlySource {
    fn clone(&self) -> Self {
        self.slice_from(0)
    }
}

impl From<Vec<u8>> for ReadOnlySource {
    fn from(data: Vec<u8>) -> ReadOnlySource {
        ReadOnlySource::new(BoxedData::new(Arc::new(Box::new(data))))
    }
}
