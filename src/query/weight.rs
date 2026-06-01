use super::Scorer;
use crate::docset::COLLECT_BLOCK_BUFFER_LEN;
use crate::index::SegmentReader;
use crate::query::Explanation;
use crate::{DocId, DocSet, Score, TERMINATED};

/// Iterates through all of the documents and scores matched by the DocSet
/// `DocSet`.
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

/// Number of `COLLECT_BLOCK_BUFFER_LEN`-sized windows accumulated into the large
/// buffer before it is flushed to the collector via `collect_block`.
const NUM_WINDOWS_PER_BLOCK: usize = 32;
/// Size of the buffer accumulated before invoking the callback (2_048 = 32 * 64).
/// `fill_buffer` keeps writing `COLLECT_BLOCK_BUFFER_LEN`-sized windows; this only
/// changes how much we accumulate before flushing.
const LARGE_COLLECT_BUFFER_LEN: usize = COLLECT_BLOCK_BUFFER_LEN * NUM_WINDOWS_PER_BLOCK;

/// Iterates through all of the documents matched by the `DocSet`, flushing
/// blocks of up to `LARGE_COLLECT_BUFFER_LEN` doc ids to `callback`.
///
/// `fill_buffer` only ever writes `COLLECT_BLOCK_BUFFER_LEN` doc ids at a time,
/// so we accumulate several such windows into a single larger buffer before
/// handing it to the collector. This amortizes the per-`collect_block` overhead
/// (virtual dispatch, aggregation setup) over more documents.
#[inline]
pub(crate) fn for_each_docset_buffered<T: DocSet + ?Sized>(
    docset: &mut T,
    mut callback: impl FnMut(&[DocId]),
) {
    // Heap-allocated once per call (i.e. once per segment in the no-score path).
    // `new_zeroed_slice` zeroes directly on the heap, avoiding a 2_048-element
    // stack temporary.
    // SAFETY: an all-zero bit pattern is a valid value for every `DocId` (u32),
    // so the zeroed slice is fully initialized.
    let mut buffer: Box<[DocId]> =
        unsafe { Box::new_zeroed_slice(LARGE_COLLECT_BUFFER_LEN).assume_init() };
    loop {
        let mut filled = 0;
        let mut reached_end = false;
        // Fill the large buffer one `COLLECT_BLOCK_BUFFER_LEN` window at a time.
        // `chunks_exact_mut` yields windows of exactly `COLLECT_BLOCK_BUFFER_LEN`
        // because `LARGE_COLLECT_BUFFER_LEN` is a multiple of it (empty remainder).
        // The windows are contiguous and filled in order, so the doc ids always
        // occupy the contiguous prefix `buffer[..filled]`.
        for window in buffer.chunks_exact_mut(COLLECT_BLOCK_BUFFER_LEN) {
            // SAFETY: each `window` is a slice of exactly `COLLECT_BLOCK_BUFFER_LEN`
            // elements, so reinterpreting its start pointer as a fixed-size array
            // reference of that length is valid.
            let window: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN] =
                unsafe { &mut *window.as_mut_ptr().cast::<[DocId; COLLECT_BLOCK_BUFFER_LEN]>() };
            let num_items = docset.fill_buffer(window);
            filled += num_items;
            if num_items != COLLECT_BLOCK_BUFFER_LEN {
                reached_end = true;
                break;
            }
        }
        callback(&buffer[..filled]);
        if reached_end {
            break;
        }
    }
}

/// Calls `callback` with all of the `(doc, score)` for which score
/// is exceeding a given threshold.
///
/// This method is useful for the [`TopDocs`](crate::collector::TopDocs) collector.
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

/// A Weight is the specialization of a `Query`
/// for a given set of segments.
///
/// See [`Query`](crate::query::Query).
pub trait Weight: Send + Sync + 'static {
    /// Returns the scorer for the given segment.
    ///
    /// `boost` is a multiplier to apply to the score.
    ///
    /// See [`Query`](crate::query::Query).
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>>;

    /// Returns an [`Explanation`] for the given document.
    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation>;

    /// Returns the number documents within the given [`SegmentReader`].
    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if let Some(alive_bitset) = reader.alive_bitset() {
            Ok(scorer.count(alive_bitset))
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

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    fn for_each_no_score(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(&[DocId]),
    ) -> crate::Result<()> {
        let mut docset = self.scorer(reader, 1.0)?;
        for_each_docset_buffered(&mut docset, callback);
        Ok(())
    }

    /// Calls `callback` with all of the `(doc, score)` for which score
    /// is exceeding a given threshold.
    ///
    /// This method is useful for the [`TopDocs`](crate::collector::TopDocs) collector.
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
