use std::sync::Arc;

use fastfield_codecs::Column;

use crate::directory::{FileSlice, OwnedBytes};
use crate::fastfield::MultiValueIndex;
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
    idx_reader: MultiValueIndex,
    values: OwnedBytes,
}

impl BytesFastFieldReader {
    pub(crate) fn open(
        idx_reader: Arc<dyn Column<u64>>,
        values_file: FileSlice,
    ) -> crate::Result<BytesFastFieldReader> {
        let values = values_file.read_bytes()?;
        Ok(BytesFastFieldReader {
            idx_reader: MultiValueIndex::new(idx_reader),
            values,
        })
    }

    /// returns the multivalue index
    pub fn get_index_reader(&self) -> &MultiValueIndex {
        &self.idx_reader
    }

    /// Returns the bytes associated with the given `doc`
    pub fn get_bytes(&self, doc: DocId) -> &[u8] {
        let range = self.idx_reader.range(doc);
        &self.values.as_slice()[range.start as usize..range.end as usize]
    }

    /// Returns the length of the bytes associated with the given `doc`
    pub fn num_bytes(&self, doc: DocId) -> u64 {
        let range = self.idx_reader.range(doc);
        (range.end - range.start) as u64
    }

    /// Returns the overall number of bytes in this bytes fast field.
    pub fn total_num_bytes(&self) -> u32 {
        self.values.len() as u32
    }
}
