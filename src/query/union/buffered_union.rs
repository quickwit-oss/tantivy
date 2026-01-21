use common::TinySet;

use crate::docset::{DocSet, TERMINATED};
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner};
use crate::query::size_hint::estimate_union;
use crate::query::Scorer;
use crate::{DocId, Score};

// The buffered union looks ahead within a fixed-size sliding window
// of upcoming document IDs (the "horizon").
const HORIZON_NUM_TINYBITSETS: usize = HORIZON as usize / 64;
const HORIZON: u32 = 64u32 * 64u32;

// `drain_filter` is not stable yet.
// This function is similar except that it does is not unstable, and
// it does not keep the original vector ordering.
//
// Elements are dropped and not yielded.
fn unordered_drain_filter<T, P>(v: &mut Vec<T>, mut predicate: P)
where P: FnMut(&mut T) -> bool {
    let mut i = 0;
    while i < v.len() {
        if predicate(&mut v[i]) {
            v.swap_remove(i);
        } else {
            i += 1;
        }
    }
}

/// Creates a `DocSet` that iterate through the union of two or more `DocSet`s.
pub struct BufferedUnionScorer<TScorer, TScoreCombiner = DoNothingCombiner> {
    /// Active scorers (already filtered of `TERMINATED`).
    scorers: Vec<TScorer>,
    /// Sliding window presence map for upcoming docs.
    ///
    /// There are `HORIZON_NUM_TINYBITSETS` buckets, each covering
    /// a span of 64 doc IDs. Bucket `i` represents the range
    /// `[window_start_doc + i*64, window_start_doc + (i+1)*64)`.
    bitsets: Box<[TinySet; HORIZON_NUM_TINYBITSETS]>,
    // Index of the current TinySet bucket within the sliding window.
    bucket_idx: usize,
    /// Per-doc score combiners for the current window.
    ///
    /// these accumulators merge contributions from all scorers that
    /// hit the same doc within the buffered window.
    scores: Box<[TScoreCombiner; HORIZON as usize]>,
    /// Start doc ID (inclusive) of the current sliding window.
    /// None if the window is not loaded yet. This is true for a freshly created
    /// BufferedUnionScorer.
    window_start_doc: DocId,
    /// Current doc ID of the union.
    doc: DocId,
    /// Combined score for current `doc` as produced by `TScoreCombiner`.
    score: Score,
    /// Number of documents in the segment.
    num_docs: u32,
}

fn refill<TScorer: Scorer, TScoreCombiner: ScoreCombiner>(
    scorers: &mut Vec<TScorer>,
    bitsets: &mut [TinySet; HORIZON_NUM_TINYBITSETS],
    score_combiner: &mut [TScoreCombiner; HORIZON as usize],
    min_doc: DocId,
) {
    unordered_drain_filter(scorers, |scorer| {
        let horizon = min_doc + HORIZON;
        loop {
            let doc = scorer.doc();
            if doc >= horizon {
                return false;
            }
            // add this document
            let delta = doc - min_doc;
            bitsets[(delta / 64) as usize].insert_mut(delta % 64u32);
            score_combiner[delta as usize].update(scorer);
            if scorer.advance() == TERMINATED {
                // remove the docset, it has been entirely consumed.
                return true;
            }
        }
    });
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> BufferedUnionScorer<TScorer, TScoreCombiner> {
    /// Returns the underlying scorers in the union.
    pub fn into_scorers(self) -> Vec<TScorer> {
        self.scorers
    }

    /// Accessor for the underlying scorers in the union.
    pub fn scorers(&self) -> &[TScorer] {
        &self.scorers[..]
    }

    /// num_docs is the number of documents in the segment.
    pub(crate) fn build(
        docsets: Vec<TScorer>,
        score_combiner_fn: impl FnOnce() -> TScoreCombiner,
        num_docs: u32,
    ) -> BufferedUnionScorer<TScorer, TScoreCombiner> {
        let score_combiner = score_combiner_fn();
        let mut non_empty_docsets: Vec<TScorer> = docsets
            .into_iter()
            .filter(|docset| docset.doc() != TERMINATED)
            .collect();

        let first_doc: DocId = non_empty_docsets
            .iter()
            .map(|docset| docset.doc())
            .min()
            .unwrap_or(TERMINATED);
        let mut score_combiner_cloned = score_combiner.clone();
        let mut i = 0;
        while i < non_empty_docsets.len() {
            let should_remove_docset: bool = {
                let non_empty_docset = &mut non_empty_docsets[i];
                if non_empty_docset.doc() != first_doc {
                    false
                } else {
                    score_combiner_cloned.update(non_empty_docset);
                    if non_empty_docsets[i].advance() == TERMINATED {
                        true
                    } else {
                        false
                    }
                }
            };
            if should_remove_docset {
                non_empty_docsets.swap_remove(i);
            } else {
                i += 1;
            }
        }
        let first_score: Score = score_combiner_cloned.score();
        let union = BufferedUnionScorer {
            scorers: non_empty_docsets,
            bitsets: Box::new([TinySet::empty(); HORIZON_NUM_TINYBITSETS]),
            scores: Box::new([score_combiner; HORIZON as usize]),
            bucket_idx: HORIZON_NUM_TINYBITSETS,
            // That way we will be detected as outside the window,
            window_start_doc: u32::MAX - HORIZON,
            doc: first_doc,
            score: first_score,
            num_docs,
        };
        union
    }

    fn refill(&mut self) -> bool {
        let Some(min_doc) = self.scorers.iter().map(DocSet::doc).min() else {
            return false;
        };
        // Reset the sliding window to start at the smallest doc
        // across all scorers and prebuffer within the horizon.
        self.window_start_doc = min_doc;
        self.bucket_idx = 0;
        self.doc = min_doc;
        refill(
            &mut self.scorers,
            &mut self.bitsets,
            &mut self.scores,
            min_doc,
        );
        true
    }

    #[inline]
    fn advance_buffered(&mut self) -> bool {
        while self.bucket_idx < HORIZON_NUM_TINYBITSETS {
            if let Some(val) = self.bitsets[self.bucket_idx].pop_lowest() {
                let delta = val + (self.bucket_idx as u32) * 64;
                self.doc = self.window_start_doc + delta;
                let score_combiner = &mut self.scores[delta as usize];
                self.score = score_combiner.score();
                score_combiner.clear();
                return true;
            } else {
                self.bucket_idx += 1;
            }
        }
        false
    }

    fn is_in_horizon(&self, target: DocId) -> bool {
        // wrapping_sub, because target may be < window_start_doc
        // in particular during initialization.
        let gap = target.wrapping_sub(self.window_start_doc);
        gap < HORIZON
    }
}

impl<TScorer, TScoreCombiner> DocSet for BufferedUnionScorer<TScorer, TScoreCombiner>
where
    TScorer: Scorer,
    TScoreCombiner: ScoreCombiner,
{
    #[inline]
    fn advance(&mut self) -> DocId {
        if self.advance_buffered() {
            return self.doc;
        }
        if !self.refill() {
            self.doc = TERMINATED;
            return TERMINATED;
        }
        if !self.advance_buffered() {
            return TERMINATED;
        }
        self.doc
    }

    fn seek(&mut self, target: DocId) -> DocId {
        if self.doc >= target {
            return self.doc;
        }
        if self.is_in_horizon(target) {
            // Our value is within the buffered horizon.
            // Skipping to corresponding bucket.
            let gap = target.wrapping_sub(self.window_start_doc);
            let new_bucket_idx = gap as usize / 64;
            for obsolete_tinyset in &mut self.bitsets[self.bucket_idx..new_bucket_idx] {
                obsolete_tinyset.clear();
            }
            for score_combiner in &mut self.scores[self.bucket_idx * 64..new_bucket_idx * 64] {
                score_combiner.clear();
            }
            self.bucket_idx = new_bucket_idx;

            // Advancing until we reach the end of the bucket
            // or we reach a doc greater or equal to the target.
            let mut doc = self.doc();
            while doc < target {
                doc = self.advance();
            }
            doc
        } else {
            // clear the buffered info.
            self.bitsets.fill(TinySet::empty());
            for score_combiner in self.scores.iter_mut() {
                score_combiner.clear();
            }

            // The target is outside of the buffered horizon.
            // advance all docsets to a doc >= to the target.
            unordered_drain_filter(&mut self.scorers, |docset| {
                if docset.doc() < target {
                    docset.seek(target);
                }
                docset.doc() == TERMINATED
            });

            // at this point all of the docsets
            // are positioned on a doc >= to the target.
            if !self.refill() {
                self.doc = TERMINATED;
                return TERMINATED;
            }
            self.advance()
        }
    }

    fn seek_into_the_danger_zone(&mut self, target: DocId) -> bool {
        if self.is_in_horizon(target) {
            // Our value is within the buffered horizon and the docset may already have been
            // processed and removed, so we need to use seek, which uses the regular advance.
            self.seek(target) == target
        } else {
            // The docsets are not in the buffered range, so we can use seek_into_the_danger_zone
            // of the underlying docsets
            let is_hit = self
                .scorers
                .iter_mut()
                .any(|docset| docset.seek_into_the_danger_zone(target));

            // The API requires the DocSet to be in a valid state when `seek_into_the_danger_zone`
            // returns true.
            if is_hit {
                self.seek(target);
            }
            is_hit
        }
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        estimate_union(self.scorers.iter().map(DocSet::size_hint), self.num_docs)
    }

    fn cost(&self) -> u64 {
        self.scorers.iter().map(|docset| docset.cost()).sum()
    }

    // TODO Also implement `count` with deletes efficiently.
    fn count_including_deleted(&mut self) -> u32 {
        if self.doc == TERMINATED {
            return 0;
        }
        let mut count = self.bitsets[self.bucket_idx..HORIZON_NUM_TINYBITSETS]
            .iter()
            .map(|bitset| bitset.len())
            .sum::<u32>()
            + 1;
        for bitset in self.bitsets.iter_mut() {
            bitset.clear();
        }
        while self.refill() {
            count += self.bitsets.iter().map(|bitset| bitset.len()).sum::<u32>();
            for bitset in self.bitsets.iter_mut() {
                bitset.clear();
            }
        }
        self.bucket_idx = HORIZON_NUM_TINYBITSETS;
        count
    }
}

impl<TScorer, TScoreCombiner> Scorer for BufferedUnionScorer<TScorer, TScoreCombiner>
where
    TScoreCombiner: ScoreCombiner,
    TScorer: Scorer,
{
    #[inline]
    fn score(&mut self) -> Score {
        self.score
    }
}
