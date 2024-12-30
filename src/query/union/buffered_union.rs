use common::TinySet;

use crate::docset::{DocSet, TERMINATED};
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner};
use crate::query::size_hint::estimate_union;
use crate::query::Scorer;
use crate::{DocId, Score};

const HORIZON_NUM_TINYBITSETS: usize = 64;
const HORIZON: u32 = 64u32 * HORIZON_NUM_TINYBITSETS as u32;

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
    docsets: Vec<TScorer>,
    bitsets: Box<[TinySet; HORIZON_NUM_TINYBITSETS]>,
    scores: Box<[TScoreCombiner; HORIZON as usize]>,
    cursor: usize,
    offset: DocId,
    doc: DocId,
    score: Score,
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
    pub(crate) fn build(
        docsets: Vec<TScorer>,
        score_combiner_fn: impl FnOnce() -> TScoreCombiner,
        num_docs: u32,
    ) -> BufferedUnionScorer<TScorer, TScoreCombiner> {
        let non_empty_docsets: Vec<TScorer> = docsets
            .into_iter()
            .filter(|docset| docset.doc() != TERMINATED)
            .collect();
        let mut union = BufferedUnionScorer {
            docsets: non_empty_docsets,
            bitsets: Box::new([TinySet::empty(); HORIZON_NUM_TINYBITSETS]),
            scores: Box::new([score_combiner_fn(); HORIZON as usize]),
            cursor: HORIZON_NUM_TINYBITSETS,
            offset: 0,
            doc: 0,
            score: 0.0,
            num_docs,
        };
        if union.refill() {
            union.advance();
        } else {
            union.doc = TERMINATED;
        }
        union
    }

    fn refill(&mut self) -> bool {
        if let Some(min_doc) = self.docsets.iter().map(DocSet::doc).min() {
            self.offset = min_doc;
            self.cursor = 0;
            self.doc = min_doc;
            refill(
                &mut self.docsets,
                &mut self.bitsets,
                &mut self.scores,
                min_doc,
            );
            true
        } else {
            false
        }
    }

    fn advance_buffered(&mut self) -> bool {
        while self.cursor < HORIZON_NUM_TINYBITSETS {
            if let Some(val) = self.bitsets[self.cursor].pop_lowest() {
                let delta = val + (self.cursor as u32) * 64;
                self.doc = self.offset + delta;
                let score_combiner = &mut self.scores[delta as usize];
                self.score = score_combiner.score();
                score_combiner.clear();
                return true;
            } else {
                self.cursor += 1;
            }
        }
        false
    }

    fn is_in_horizon(&self, target: DocId) -> bool {
        let gap = target - self.offset;
        gap < HORIZON
    }
}

impl<TScorer, TScoreCombiner> DocSet for BufferedUnionScorer<TScorer, TScoreCombiner>
where
    TScorer: Scorer,
    TScoreCombiner: ScoreCombiner,
{
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
            let gap = target - self.offset;
            let new_cursor = gap as usize / 64;
            for obsolete_tinyset in &mut self.bitsets[self.cursor..new_cursor] {
                obsolete_tinyset.clear();
            }
            for score_combiner in &mut self.scores[self.cursor * 64..new_cursor * 64] {
                score_combiner.clear();
            }
            self.cursor = new_cursor;

            // Advancing until we reach the end of the bucket
            // or we reach a doc greater or equal to the target.
            let mut doc = self.doc();
            while doc < target {
                doc = self.advance();
            }
            doc
        } else {
            // clear the buffered info.
            for obsolete_tinyset in self.bitsets.iter_mut() {
                *obsolete_tinyset = TinySet::empty();
            }
            for score_combiner in self.scores.iter_mut() {
                score_combiner.clear();
            }

            // The target is outside of the buffered horizon.
            // advance all docsets to a doc >= to the target.
            unordered_drain_filter(&mut self.docsets, |docset| {
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
                .docsets
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

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        estimate_union(self.docsets.iter().map(DocSet::size_hint), self.num_docs)
    }

    fn cost(&self) -> u64 {
        self.docsets.iter().map(DocSet::cost).sum()
    }

    // TODO Also implement `count` with deletes efficiently.
    fn count_including_deleted(&mut self) -> u32 {
        if self.doc == TERMINATED {
            return 0;
        }
        let mut count = self.bitsets[self.cursor..HORIZON_NUM_TINYBITSETS]
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
        self.cursor = HORIZON_NUM_TINYBITSETS;
        count
    }
}

impl<TScorer, TScoreCombiner> Scorer for BufferedUnionScorer<TScorer, TScoreCombiner>
where
    TScoreCombiner: ScoreCombiner,
    TScorer: Scorer,
{
    fn score(&mut self) -> Score {
        self.score
    }
}
