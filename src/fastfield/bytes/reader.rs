use crate::directory::{FileSlice, OwnedBytes};
use crate::fastfield::{DynamicFastFieldReader, FastFieldReader, MultiValueLength};
use crate::DocId;

/// Reader for byte array fast fields
///
/// The reader is implemented as a `u64` fast field and a separate collection of bytes.
///
/// The `vals_reader` will access the concatenated list of all values for all documents.
///
/// The `idx_reader` associates, for each document, the index of its first value.
///
/// Reading the value for a document is done by reading the start index for it,
/// and the start index for the next document, and keeping the bytes in between.
#[derive(Clone)]
pub struct BytesFastFieldReader {
    idx_reader: DynamicFastFieldReader<u64>,
    values: OwnedBytes,
}

impl BytesFastFieldReader {
    pub(crate) fn open(
        idx_reader: DynamicFastFieldReader<u64>,
        values_file: FileSlice,
    ) -> crate::Result<BytesFastFieldReader> {
        let values = values_file.read_bytes()?;
        Ok(BytesFastFieldReader { idx_reader, values })
    }

    fn range(&self, doc: DocId) -> (usize, usize) {
        let start = self.idx_reader.get(doc) as usize;
        let stop = self.idx_reader.get(doc + 1) as usize;
        (start, stop)
    }

    /// Returns the bytes associated to the given `doc`
    pub fn get_bytes(&self, doc: DocId) -> &[u8] {
        let (start, stop) = self.range(doc);
        &self.values.as_slice()[start..stop]
    }

    /// Returns the length of the bytes associated to the given `doc`
    pub fn num_bytes(&self, doc: DocId) -> usize {
        let (start, stop) = self.range(doc);
        stop - start
    }

    /// Returns the overall number of bytes in this bytes fast field.
    pub fn total_num_bytes(&self) -> usize {
        self.values.len()
    }
}

impl MultiValueLength for BytesFastFieldReader {
    fn get_len(&self, doc_id: DocId) -> u64 {
        self.num_bytes(doc_id) as u64
    }
    fn get_total_len(&self) -> u64 {
        self.total_num_bytes() as u64
    }
}
