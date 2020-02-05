use super::Scorer;
use crate::core::SegmentReader;
use crate::query::Explanation;
use crate::DocId;

/// A Weight is the specialization of a Query
/// for a given set of segments.
///
/// See [`Query`](./trait.Query.html).
pub trait Weight: Send + Sync + 'static {
    /// Returns the scorer for the given segment.
    ///
    /// `boost` is a multiplier to apply to the score.
    ///
    /// See [`Query`](./trait.Query.html).
    fn scorer(&self, reader: &SegmentReader, boost: f32) -> crate::Result<Box<dyn Scorer>>;

    /// Returns an `Explanation` for the given document.
    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation>;

    /// Returns the number documents within the given `SegmentReader`.
    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        let mut scorer = self.scorer(reader, 1.0f32)?;
        if let Some(delete_bitset) = reader.delete_bitset() {
            Ok(scorer.count(delete_bitset))
        } else {
            Ok(scorer.count_including_deleted())
        }
    }
}
