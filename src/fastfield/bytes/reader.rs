use std::ops::Range;
use std::sync::Arc;

use fastfield_codecs::Column;

use crate::directory::{FileSlice, OwnedBytes};
use crate::fastfield::MultiValueLength;
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
    idx_reader: Arc<dyn Column<u64>>,
    values: OwnedBytes,
}

impl BytesFastFieldReader {
    pub(crate) fn open(
        idx_reader: Arc<dyn Column<u64>>,
        values_file: FileSlice,
    ) -> crate::Result<BytesFastFieldReader> {
        let values = values_file.read_bytes()?;
        Ok(BytesFastFieldReader { idx_reader, values })
    }

    fn range(&self, doc: DocId) -> Range<u32> {
        let start = self.idx_reader.get_val(doc) as u32;
        let end = self.idx_reader.get_val(doc + 1) as u32;
        start..end
    }

    /// Returns the bytes associated with the given `doc`
    pub fn get_bytes(&self, doc: DocId) -> &[u8] {
        let range = self.range(doc);
        &self.values.as_slice()[range.start as usize..range.end as usize]
    }

    /// Returns the length of the bytes associated with the given `doc`
    pub fn num_bytes(&self, doc: DocId) -> u64 {
        let range = self.range(doc);
        (range.end - range.start) as u64
    }

    /// Returns the overall number of bytes in this bytes fast field.
    pub fn total_num_bytes(&self) -> u64 {
        self.values.len() as u64
    }
}

impl MultiValueLength for BytesFastFieldReader {
    fn get_range(&self, doc_id: DocId) -> std::ops::Range<u32> {
        self.range(doc_id)
    }
    fn get_len(&self, doc_id: DocId) -> u64 {
        self.num_bytes(doc_id)
    }
    fn get_total_len(&self) -> u64 {
        self.total_num_bytes()
    }
}
