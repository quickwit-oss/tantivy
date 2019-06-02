use super::Scorer;
use core::SegmentReader;
use query::Explanation;
use {DocId, Result};

/// A Weight is the specialization of a Query
/// for a given set of segments.
///
/// See [`Query`](./trait.Query.html).
pub trait Weight: Send + Sync + 'static {
    /// Returns the scorer for the given segment.
    /// See [`Query`](./trait.Query.html).
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>>;

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> Result<Explanation>;

    /// Returns the number documents within the given `SegmentReader`.
    fn count(&self, reader: &SegmentReader) -> Result<u32> {
        let mut scorer = self.scorer(reader)?;
        if let Some(delete_bitset) = reader.delete_bitset() {
            Ok(scorer.count(delete_bitset))
        } else {
            Ok(scorer.count_including_deleted())
        }
    }
}
