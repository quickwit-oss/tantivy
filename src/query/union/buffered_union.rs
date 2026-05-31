use common::TinySet;

use crate::docset::{DocSet, SeekDangerResult, COLLECT_BLOCK_BUFFER_LEN, TERMINATED};
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner};
use crate::query::size_hint::estimate_union;
use crate::query::Scorer;
use crate::{DocId, Score};

// The buffered union looks ahead within a fixed-size sliding window
// of upcoming document IDs (the "horizon").
const HORIZON_NUM_TINYBITSETS: usize = HORIZON as usize / 64;
const HORIZON: u32 = 64u32 * 64u32;
const GROUPED_INSERT_MAX_BUCKET_SPAN: u32 = 2;

/// Creates a `DocSet` that iterate through the union of two or more `DocSet`s.
pub struct BufferedUnionScorer<TScorer, TScoreCombiner = DoNothingCombiner> {
    /// Active scorers (already filtered of `TERMINATED`).
    docsets: Vec<TScorer>,
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
    window_start_doc: DocId,
    /// Current doc ID of the union.
    doc: DocId,
    /// Combined score for current `doc` as produced by `TScoreCombiner`.
    score: Score,
    /// Number of documents in the segment.
    num_docs: u32,
    /// Scratch buffer for block-based refill.
    refill_docs: [DocId; COLLECT_BLOCK_BUFFER_LEN],
    /// Scratch buffer for term frequencies matching `refill_docs`.
    refill_term_freqs: [u32; COLLECT_BLOCK_BUFFER_LEN],
    /// Whether all children support scoring buffered docs after advancing.
    use_score_doc_refill: bool,
}

#[inline]
fn union_bucket(
    bitsets: &mut [TinySet; HORIZON_NUM_TINYBITSETS],
    bucket_pos: u32,
    tinyset: TinySet,
) {
    debug_assert!((bucket_pos as usize) < HORIZON_NUM_TINYBITSETS);
    // `bucket` comes from a doc delta below `HORIZON`; there are exactly
    // `HORIZON / 64` buckets in the refill window.
    bitsets[bucket_pos as usize] = bitsets[bucket_pos as usize].union(tinyset);
}

#[inline]
fn insert_delta(bitsets: &mut [TinySet; HORIZON_NUM_TINYBITSETS], delta: DocId) {
    debug_assert!(delta < HORIZON);
    // `delta < HORIZON`, so `delta / 64` is in the bitset array. The bit
    // offset is reduced modulo 64 before being inserted in the TinySet.
    bitsets[delta as usize / 64].insert_mut(delta % 64u32);
}

fn insert_and_score_full_buffer<TScorer: Scorer, TScoreCombiner: ScoreCombiner>(
    scorer: &mut TScorer,
    docs: &[DocId; COLLECT_BLOCK_BUFFER_LEN],
    term_freqs: &[u32; COLLECT_BLOCK_BUFFER_LEN],
    bitsets: &mut [TinySet; HORIZON_NUM_TINYBITSETS],
    score_combiner: &mut [TScoreCombiner; HORIZON as usize],
    min_doc: DocId,
) {
    debug_assert!(docs.windows(2).all(|pair| pair[0] < pair[1]));
    debug_assert!(docs[COLLECT_BLOCK_BUFFER_LEN - 1] - min_doc < HORIZON);

    let first_delta = docs[0] - min_doc;
    let last_delta = docs[COLLECT_BLOCK_BUFFER_LEN - 1] - min_doc;
    let first_bucket = first_delta / 64;
    let last_bucket = last_delta / 64;

    // Common for very dense scorers: 64 distinct doc ids in one 64-doc bucket
    // means all bits in that bucket are present.
    if first_bucket == last_bucket {
        union_bucket(bitsets, first_bucket, TinySet::full());
        score_full_buffer(scorer, docs, term_freqs, score_combiner, min_doc);
        return;
    }

    // 64 sorted distinct integers spanning exactly 64 values are consecutive.
    // If they cross a TinySet boundary, this is just the suffix of the first
    // bucket plus the prefix of the second bucket.
    if last_delta - first_delta == COLLECT_BLOCK_BUFFER_LEN as u32 - 1 {
        union_bucket(
            bitsets,
            first_bucket,
            TinySet::range_greater_or_equal(first_delta % 64u32),
        );
        union_bucket(
            bitsets,
            last_bucket,
            TinySet::range_lower((last_delta + 1) % 64u32),
        );
        score_full_buffer(scorer, docs, term_freqs, score_combiner, min_doc);
        return;
    }

    // Grouping wins only for very dense buffers that hit the same TinySet many
    // times. Once the 64 docs are spread farther, a straight pass is cheaper.
    if last_bucket - first_bucket <= GROUPED_INSERT_MAX_BUCKET_SPAN {
        let mut bucket = first_bucket;
        let mut tinyset = TinySet::empty();
        for (&doc, &term_freq) in docs.iter().zip(term_freqs.iter()) {
            let delta = doc - min_doc;
            let delta_bucket = delta / 64;
            if delta_bucket != bucket {
                union_bucket(bitsets, bucket, tinyset);
                bucket = delta_bucket;
                tinyset = TinySet::empty();
            }
            tinyset.insert_mut(delta % 64u32);
            let score = scorer.score_doc(doc, term_freq);
            update_score_combiner(score_combiner, delta, doc, score);
        }
        union_bucket(bitsets, bucket, tinyset);
    } else {
        for (&doc, &term_freq) in docs.iter().zip(term_freqs.iter()) {
            let delta = doc - min_doc;
            insert_delta(bitsets, delta);
            // TODO: score_doc access the field_norm reader for each _term_, instead of once per
            // doc. We could optimize this by caching the field norm for the doc, and
            // reusing it for all terms in the doc.
            let score = scorer.score_doc(doc, term_freq);
            update_score_combiner(score_combiner, delta, doc, score);
        }
    }
}

#[inline]
fn update_score_combiner<TScoreCombiner: ScoreCombiner>(
    score_combiner: &mut [TScoreCombiner; HORIZON as usize],
    delta: DocId,
    doc: DocId,
    score: Score,
) {
    debug_assert!(delta < HORIZON);
    // Full and partial refill only buffer docs below `horizon`, so their
    // deltas are always in the score-combiner window.
    score_combiner[delta as usize].update_score(doc, score);
}

fn score_full_buffer<TScorer: Scorer, TScoreCombiner: ScoreCombiner>(
    scorer: &mut TScorer,
    docs: &[DocId; COLLECT_BLOCK_BUFFER_LEN],
    term_freqs: &[u32; COLLECT_BLOCK_BUFFER_LEN],
    score_combiner: &mut [TScoreCombiner; HORIZON as usize],
    min_doc: DocId,
) {
    for (&doc, &term_freq) in docs.iter().zip(term_freqs.iter()) {
        let score = scorer.score_doc(doc, term_freq);
        update_score_combiner(score_combiner, doc - min_doc, doc, score);
    }
}

fn refill_scorer_with_score_docs<TScorer: Scorer, TScoreCombiner: ScoreCombiner>(
    scorer: &mut TScorer,
    bitsets: &mut [TinySet; HORIZON_NUM_TINYBITSETS],
    score_combiner: &mut [TScoreCombiner; HORIZON as usize],
    docs: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN],
    term_freqs: &mut [u32; COLLECT_BLOCK_BUFFER_LEN],
    min_doc: DocId,
    horizon: DocId,
) {
    loop {
        let len = scorer.fill_buffer_up_to_with_term_freqs(horizon, docs, term_freqs);
        if len == COLLECT_BLOCK_BUFFER_LEN {
            debug_assert!(docs[COLLECT_BLOCK_BUFFER_LEN - 1] != TERMINATED);
            debug_assert!(docs[COLLECT_BLOCK_BUFFER_LEN - 1] < horizon);
            insert_and_score_full_buffer(
                scorer,
                docs,
                term_freqs,
                bitsets,
                score_combiner,
                min_doc,
            );
        } else {
            for (&doc, &term_freq) in docs[..len].iter().zip(term_freqs[..len].iter()) {
                let delta = doc - min_doc;
                insert_delta(bitsets, delta);
                let score = scorer.score_doc(doc, term_freq);
                update_score_combiner(score_combiner, delta, doc, score);
            }
            break;
        }
    }
}

fn refill_scorer_from_current_doc<TScorer: Scorer, TScoreCombiner: ScoreCombiner>(
    scorer: &mut TScorer,
    bitsets: &mut [TinySet; HORIZON_NUM_TINYBITSETS],
    score_combiner: &mut [TScoreCombiner; HORIZON as usize],
    min_doc: DocId,
    horizon: DocId,
) {
    loop {
        let doc = scorer.doc();
        if doc >= horizon {
            break;
        }
        let delta = doc - min_doc;
        insert_delta(bitsets, delta);
        debug_assert!(delta < HORIZON);
        score_combiner[delta as usize].update(scorer);
        scorer.advance();
    }
}

fn refill<TScorer: Scorer, TScoreCombiner: ScoreCombiner>(
    scorers: &mut Vec<TScorer>,
    bitsets: &mut [TinySet; HORIZON_NUM_TINYBITSETS],
    score_combiner: &mut [TScoreCombiner; HORIZON as usize],
    docs: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN],
    term_freqs: &mut [u32; COLLECT_BLOCK_BUFFER_LEN],
    min_doc: DocId,
    use_score_doc_refill: bool,
) {
    let horizon = min_doc + HORIZON;
    for scorer in scorers.iter_mut() {
        if use_score_doc_refill {
            refill_scorer_with_score_docs(
                scorer,
                bitsets,
                score_combiner,
                docs,
                term_freqs,
                min_doc,
                horizon,
            );
        } else {
            refill_scorer_from_current_doc(scorer, bitsets, score_combiner, min_doc, horizon);
        }
    }
    scorers.retain(|scorer| scorer.doc() != TERMINATED);
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> BufferedUnionScorer<TScorer, TScoreCombiner> {
    /// num_docs is the number of documents in the segment.
    pub(crate) fn build(
        docsets: Vec<TScorer>,
        score_combiner_fn: impl FnOnce() -> TScoreCombiner,
        num_docs: u32,
    ) -> BufferedUnionScorer<TScorer, TScoreCombiner> {
        let use_score_doc_refill = docsets.iter().all(Scorer::can_score_doc);
        let non_empty_docsets: Vec<TScorer> = docsets
            .into_iter()
            .filter(|docset| docset.doc() != TERMINATED)
            .collect();
        let mut union = BufferedUnionScorer {
            docsets: non_empty_docsets,
            bitsets: Box::new([TinySet::empty(); HORIZON_NUM_TINYBITSETS]),
            scores: Box::new([score_combiner_fn(); HORIZON as usize]),
            bucket_idx: HORIZON_NUM_TINYBITSETS,
            window_start_doc: 0,
            doc: 0,
            score: 0.0,
            num_docs,
            refill_docs: [TERMINATED; COLLECT_BLOCK_BUFFER_LEN],
            refill_term_freqs: [1u32; COLLECT_BLOCK_BUFFER_LEN],
            use_score_doc_refill,
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
            // Reset the sliding window to start at the smallest doc
            // across all scorers and prebuffer within the horizon.
            self.window_start_doc = min_doc;
            self.bucket_idx = 0;
            self.doc = min_doc;
            refill(
                &mut self.docsets,
                &mut self.bitsets,
                &mut self.scores,
                &mut self.refill_docs,
                &mut self.refill_term_freqs,
                min_doc,
                self.use_score_doc_refill,
            );
            true
        } else {
            false
        }
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

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        if self.doc == TERMINATED {
            return 0;
        }
        // The current doc (self.doc) has already been popped from the bitsets,
        // so the loop below won't yield it. Emit it here first.
        buffer[0] = self.doc;
        let mut count = 1;

        loop {
            // Drain docs directly from the pre-computed bitsets.
            while self.bucket_idx < HORIZON_NUM_TINYBITSETS {
                // Move bitset to a local variable to avoid read/store on self.bitsets while
                // iterating through the bits.
                let mut tinyset: TinySet = self.bitsets[self.bucket_idx];

                while let Some(val) = tinyset.pop_lowest() {
                    let delta = val + (self.bucket_idx as u32) * 64;
                    self.doc = self.window_start_doc + delta;

                    if count >= COLLECT_BLOCK_BUFFER_LEN {
                        // Buffer full; put remaining bits back.
                        self.bitsets[self.bucket_idx] = tinyset;
                        return COLLECT_BLOCK_BUFFER_LEN;
                    }
                    buffer[count] = self.doc;
                    count += 1;
                }
                self.bitsets[self.bucket_idx] = TinySet::empty();
                self.bucket_idx += 1;
            }

            // Current window exhausted, refill.
            if !self.refill() {
                self.doc = TERMINATED;
                return count;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        if self.doc >= target {
            return self.doc;
        }
        let gap = target - self.window_start_doc;
        if gap < HORIZON {
            // Our value is within the buffered horizon.

            // Skipping to corresponding bucket.
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
            for obsolete_tinyset in self.bitsets.iter_mut() {
                *obsolete_tinyset = TinySet::empty();
            }
            for score_combiner in self.scores.iter_mut() {
                score_combiner.clear();
            }

            // The target is outside of the buffered horizon.
            // advance all docsets to a doc >= to the target.
            for docset in &mut self.docsets {
                if docset.doc() < target {
                    docset.seek(target);
                }
            }
            self.docsets.retain(|docset| docset.doc() != TERMINATED);

            // at this point all of the docsets
            // are positioned on a doc >= to the target.
            if !self.refill() {
                self.doc = TERMINATED;
                return TERMINATED;
            }
            self.advance()
        }
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        if target >= TERMINATED {
            return SeekDangerResult::SeekLowerBound(TERMINATED);
        }
        if self.is_in_horizon(target) {
            // Our value is within the buffered horizon and the docset may already have been
            // processed and removed, so we need to use seek, which uses the regular advance.
            let seek_doc = self.seek(target);
            if seek_doc == target {
                return SeekDangerResult::Found;
            } else {
                return SeekDangerResult::SeekLowerBound(seek_doc);
            };
        }

        // The docsets are not in the buffered range, so we can use seek_into_the_danger_zone
        // of the underlying docsets
        let mut is_hit = false;
        let mut min_new_target = TERMINATED;

        for docset in self.docsets.iter_mut() {
            match docset.seek_danger(target) {
                SeekDangerResult::Found => {
                    is_hit = true;
                    break;
                }
                SeekDangerResult::SeekLowerBound(new_target) => {
                    min_new_target = min_new_target.min(new_target);
                }
            }
        }

        // The API requires the DocSet to be in a valid state when `seek_into_the_danger_zone`
        // returns Found.
        if is_hit {
            // The doc is found. Let's make sure we position the union on the target
            // to bring it back to a valid state.
            self.seek(target);
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(min_new_target)
        }
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        estimate_union(self.docsets.iter().map(DocSet::size_hint), self.num_docs)
    }

    fn cost(&self) -> u64 {
        self.docsets.iter().map(|docset| docset.cost()).sum()
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
