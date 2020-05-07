use super::Scorer;
use crate::core::SegmentReader;
use crate::query::Explanation;
use crate::{DocId, Score};

pub trait PruningScorer {

    fn doc(&self) -> DocId;

    fn score(&self) -> Score;

    /// Advance to the next document that has a score strictly greater than
    /// `lower_bound_score`.
    fn advance_with_pruning(&mut self, score_lower_bound: f32) -> bool;
    fn advance(&mut self) -> bool {
        self.advance_with_pruning(std::f32::NEG_INFINITY)
    }
}

pub enum PruningScorerIfPossible {
    Pruning(Box<dyn PruningScorer>),
    NonPruning(Box<dyn Scorer>)
}

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

    fn pruning_scorer(&self, reader: &SegmentReader, boost: f32) -> crate::Result<PruningScorerIfPossible> {
        let scorer = self.scorer(reader, boost)?;
        Ok(PruningScorerIfPossible::NonPruning(Box::new(scorer)))
    }

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
