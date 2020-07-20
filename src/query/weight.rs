use super::Scorer;
use crate::core::SegmentReader;
use crate::query::Explanation;
use crate::{DocId, Score, TERMINATED};

/// Iterates through all of the document matched by the DocSet
/// `DocSet` and push the scored documents to the collector.
pub(crate) fn for_each_scorer<TScorer: Scorer + ?Sized>(
    scorer: &mut TScorer,
    callback: &mut dyn FnMut(DocId, Score),
) {
    let mut doc = scorer.doc();
    while doc != TERMINATED {
        callback(doc, scorer.score());
        doc = scorer.advance();
    }
}

/// Calls `callback` with all of the `(doc, score)` for which score
/// is exceeding a given threshold.
///
/// This method is useful for the TopDocs collector.
/// For all docsets, the blanket implementation has the benefit
/// of prefiltering (doc, score) pairs, avoiding the
/// virtual dispatch cost.
///
/// More importantly, it makes it possible for scorers to implement
/// important optimization (e.g. BlockWAND for union).
pub(crate) fn for_each_pruning_scorer<TScorer: Scorer + ?Sized>(
    scorer: &mut TScorer,
    mut threshold: Score,
    callback: &mut dyn FnMut(DocId, Score) -> Score,
) {
    let mut doc = scorer.doc();
    while doc != TERMINATED {
        let score = scorer.score();
        if score > threshold {
            threshold = callback(doc, score);
        }
        doc = scorer.advance();
    }
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
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>>;

    /// Returns an `Explanation` for the given document.
    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation>;

    /// Returns the number documents within the given `SegmentReader`.
    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if let Some(delete_bitset) = reader.delete_bitset() {
            Ok(scorer.count(delete_bitset))
        } else {
            Ok(scorer.count_including_deleted())
        }
    }

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    fn for_each(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score),
    ) -> crate::Result<()> {
        let mut scorer = self.scorer(reader, 1.0)?;
        for_each_scorer(scorer.as_mut(), callback);
        Ok(())
    }

    /// Calls `callback` with all of the `(doc, score)` for which score
    /// is exceeding a given threshold.
    ///
    /// This method is useful for the TopDocs collector.
    /// For all docsets, the blanket implementation has the benefit
    /// of prefiltering (doc, score) pairs, avoiding the
    /// virtual dispatch cost.
    ///
    /// More importantly, it makes it possible for scorers to implement
    /// important optimization (e.g. BlockWAND for union).
    fn for_each_pruning(
        &self,
        threshold: Score,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> crate::Result<()> {
        let mut scorer = self.scorer(reader, 1.0)?;
        for_each_pruning_scorer(scorer.as_mut(), threshold, callback);
        Ok(())
    }
}
