use owning_ref::OwningRef;

use directory::ReadOnlySource;
use fastfield::FastFieldReader;
use DocId;

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
    idx_reader: FastFieldReader<u64>,
    values: OwningRef<ReadOnlySource, [u8]>,
}

impl BytesFastFieldReader {
    pub(crate) fn open(
        idx_reader: FastFieldReader<u64>,
        values_source: ReadOnlySource,
    ) -> BytesFastFieldReader {
        let values = OwningRef::new(values_source).map(|source| &source[..]);
        BytesFastFieldReader { idx_reader, values }
    }

    fn range(&self, doc: DocId) -> (usize, usize) {
        let start = self.idx_reader.get(doc) as usize;
        let stop = self.idx_reader.get(doc + 1) as usize;
        (start, stop)
    }

    /// Returns the bytes associated to the given `doc`
    pub fn get_bytes(&self, doc: DocId) -> &[u8] {
        let (start, stop) = self.range(doc);
        &self.values[start..stop]
    }

    /// Returns the overall number of bytes in this bytes fast field.
    pub fn total_num_bytes(&self) -> usize {
        self.values.len()
    }
}
