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

pub(crate) struct BoostWeight {
    weight: Box<dyn Weight>,
    boost: f32,
}

impl BoostWeight {
    pub fn new(weight: Box<dyn Weight>, boost: f32) -> Self {
        BoostWeight { weight, boost }
    }
}

impl Weight for BoostWeight {
    fn scorer(&self, reader: &SegmentReader, boost: f32) -> crate::Result<Box<dyn Scorer>> {
        self.weight.scorer(reader, boost * self.boost)
    }

    fn explain(&self, reader: &SegmentReader, doc: u32) -> crate::Result<Explanation> {
        self.weight.explain(reader, doc) // TODO Add boost
    }

    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        self.weight.count(reader)
    }
}
