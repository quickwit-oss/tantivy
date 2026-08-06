use crate::fieldnorm::FieldNormReader;
use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
use crate::query::scorer::PruningScorer;
use crate::query::term_query::TermScorer;
use crate::query::{Bm25Weight, Scorer};
use crate::{DocId, DocSet, Score, TERMINATED};

#[cfg(test)]
pub(crate) fn block_wand_intersection(
    scorers: Vec<TermScorer>,
    threshold: Score,
    callback: &mut dyn FnMut(DocId, Score) -> Score,
) {
    use crate::query::weight::for_each_pruning_scorer;

    let mut scorer = BlockWandIntersectionScorer::new(scorers, threshold);
    for_each_pruning_scorer(&mut scorer, callback);
}

/// Block-max pruning for top-K over intersection of term scorers.
///
/// Uses the least-frequent term as "leader" to define 128-doc processing windows.
/// For each window, the sum of block_max_scores is compared to the current threshold;
/// if the block can't beat it, the entire block is skipped.
///
/// Within non-skipped blocks, individual documents are pruned by checking whether
/// leader_score + sum(secondary block_max_scores) can exceed the threshold before
/// performing the expensive intersection membership check (seeking into secondary scorers).
///
/// # Preconditions
/// - `scorers` has at least 2 elements
/// - All scorers read frequencies (`FreqReadingOption::ReadFreq`)
pub struct BlockWandIntersectionScorer {
    leader: TermScorer,
    secondaries: Vec<TermScorer>,

    maximum_possible_score: Score,
    secondary_block_max_scores: Box<[f32]>,
    secondary_suffix_block_max: Box<[f32]>,
    fieldnorm_reader: FieldNormReader,
    bm25_weight: Bm25Weight,

    candidate_doc_ids: [u32; COMPRESSION_BLOCK_SIZE],
    candidate_scores: [f32; COMPRESSION_BLOCK_SIZE],
    num_candidates: usize,
    candidate_idx: usize,

    threshold: Score,
    current: (DocId, Score),
    internal_doc: DocId,
    window_end: DocId,
}
impl BlockWandIntersectionScorer {
    /// Construction positions `current` on the first match
    pub fn new(mut scorers: Vec<TermScorer>, threshold: Score) -> Self {
        assert!(scorers.len() >= 2);

        // Sort by cost (ascending). scorers[0] becomes the "leader" (rarest term).
        scorers.sort_by_key(TermScorer::size_hint);
        let leader = scorers.remove(0);
        let secondaries = scorers;
        let secondaries_len = secondaries.len();

        let secondaries_global_max_sum: Score = secondaries.iter().map(TermScorer::max_score).sum();
        let maximum_possible_score = leader.max_score() + secondaries_global_max_sum;

        // Borrow fieldnorm reader and BM25 weight before the main loop.
        // These are immutable references to disjoint fields from block_cursor,
        // but Rust's borrow checker can't see through method calls, so we
        // extract them once upfront.
        let fieldnorm_reader = leader.fieldnorm_reader().clone();
        let bm25_weight = leader.bm25_weight().clone();

        let internal_doc = leader.doc();

        let mut scorer = Self {
            leader,
            secondaries,
            maximum_possible_score,
            secondary_block_max_scores: vec![0.0f32; secondaries_len].into_boxed_slice(),
            secondary_suffix_block_max: vec![0.0f32; secondaries_len].into_boxed_slice(),
            fieldnorm_reader,
            bm25_weight,
            candidate_doc_ids: [0u32; COMPRESSION_BLOCK_SIZE],
            candidate_scores: [0f32; COMPRESSION_BLOCK_SIZE],
            num_candidates: 0,
            candidate_idx: 0,
            threshold,
            current: (0, Score::MIN),
            internal_doc,
            window_end: 0,
        };
        scorer.advance();
        scorer
    }

    fn handle_candidates(&mut self) -> Option<DocId> {
        // Pass 2: Check intersection membership only for survivors.
        // score_threshold may be stale (threshold can increase from callbacks),
        // but that's conservative — we may check a few extra candidates, never miss one.
        'next_candidate: while self.candidate_idx < self.num_candidates {
            let candidate_doc = self.candidate_doc_ids[self.candidate_idx];
            let mut total_score: Score = self.candidate_scores[self.candidate_idx];

            for (secondary_idx, secondary) in self.secondaries.iter_mut().enumerate() {
                // If a previous candidate already advanced this secondary past
                // candidate_doc, the candidate can't be in the intersection.
                if secondary.doc() > candidate_doc {
                    self.candidate_idx += 1;
                    continue 'next_candidate;
                }
                let seek_result = secondary.seek(candidate_doc);
                if seek_result != candidate_doc {
                    self.candidate_idx += 1;
                    continue 'next_candidate;
                }
                total_score += secondary.score();

                // Prune: even if all remaining secondaries score at their block max,
                // can we still beat the threshold?
                if total_score + self.secondary_suffix_block_max[secondary_idx] <= self.threshold {
                    self.candidate_idx += 1;
                    continue 'next_candidate;
                }
            }

            // All secondaries matched.
            if total_score > self.threshold {
                self.current = (candidate_doc, total_score);
                self.candidate_idx += 1;
                return Some(candidate_doc);
            }
            self.candidate_idx += 1;
        }
        None
    }
}
impl Scorer for BlockWandIntersectionScorer {
    #[inline]
    fn score(&mut self) -> Score {
        self.current.1
    }
}
impl PruningScorer for BlockWandIntersectionScorer {
    #[inline]
    fn set_threshold(&mut self, score: Score) {
        self.threshold = score;
    }
}
impl DocSet for BlockWandIntersectionScorer {
    fn advance(&mut self) -> DocId {
        if self.maximum_possible_score <= self.threshold {
            self.current = (TERMINATED, Score::MIN);
            return TERMINATED;
        }

        // check for leftover candidates to handle
        if self.num_candidates > 0 {
            if let Some(doc_id) = self.handle_candidates() {
                return doc_id;
            } else {
                // no remaining candidates, so reset and advance the internal doc
                self.num_candidates = 0;
                self.internal_doc = self.window_end + 1;
            }
        }

        while self.internal_doc < TERMINATED {
            // --- Phase 1: Block-level pruning ---
            //
            // Position all skip readers on the block containing `doc`.
            // seek_block is cheap: it only advances the skip reader, no block decompression.
            self.leader.seek_block(self.internal_doc);
            let leader_block_max: Score = self.leader.block_max_score();

            // Compute the window end as the minimum last_doc_in_block across all scorers.
            // This ensures the block_max values are valid for all docs in [doc, window_end].
            // Different scorers have independently aligned blocks, so we must use the
            // smallest window where all block_max values hold.
            self.window_end = self.leader.last_doc_in_block();

            let mut secondary_block_max_sum: Score = 0.0;
            let num_secondaries = self.secondaries.len();
            for (idx, secondary) in self.secondaries.iter_mut().enumerate() {
                secondary.block_cursor().seek_block(self.internal_doc);
                if !secondary.block_cursor().has_remaining_docs() {
                    self.current = (TERMINATED, Score::MIN);
                    return TERMINATED;
                }
                self.window_end = self.window_end.min(secondary.last_doc_in_block());
                let bms = secondary.block_max_score();
                self.secondary_block_max_scores[idx] = bms;
                secondary_block_max_sum += bms;
            }

            if leader_block_max + secondary_block_max_sum <= self.threshold {
                // The entire window cannot beat the threshold. Skip past it.
                self.internal_doc = self.window_end + 1;
                continue;
            }

            // --- Phase 2: Batch processing within the window ---
            //
            // Score-first approach: decode the leader's block, filter by threshold,
            // then check intersection membership only for survivors. This avoids expensive
            // secondary seeks for docs that can't beat the threshold.
            let block_cursor = self.leader.block_cursor();
            // seek loads the block and returns the in-block index of the first doc >= `doc`.
            let start_idx = block_cursor.seek(self.internal_doc);

            // Use the branchless binary search on the doc decoder to find the first
            // index past window_end.
            let end_idx = block_cursor
                .doc_decoder
                .seek_within_block(self.window_end + 1)
                .min(block_cursor.block_len());

            let block_docs = &block_cursor.doc_decoder.output_array()[start_idx..end_idx];
            let block_freqs = &block_cursor.freq_output_array()[start_idx..end_idx];

            // Pass 1: Batch-compute leader BM25 scores and branchlessly filter
            // candidates that can't beat the threshold.
            //
            // The trick: always write to the buffer at `num_candidates`, then
            // conditionally advance the count. The compiler can turn this into
            // a cmov instead of a branch, avoiding misprediction costs.
            let score_threshold = self.threshold - secondary_block_max_sum;

            let mut num_candidates = 0usize;
            for (candidate_doc, term_freq) in
                block_docs.iter().copied().zip(block_freqs.iter().copied())
            {
                let fieldnorm_id = self.fieldnorm_reader.fieldnorm_id(candidate_doc);
                let leader_score = self.bm25_weight.score(fieldnorm_id, term_freq);
                self.candidate_doc_ids[num_candidates] = candidate_doc;
                self.candidate_scores[num_candidates] = leader_score;
                num_candidates += (leader_score > score_threshold) as usize;
            }
            self.num_candidates = num_candidates;
            self.candidate_idx = 0;

            // Precompute suffix sums: suffix[i] = sum of block_max for secondaries[i+1..].
            // Used in Phase 2 to prune candidates that can't beat threshold even with
            // remaining secondaries contributing their block_max.
            if self.num_candidates == 0 {
                self.internal_doc = self.window_end + 1;
                continue;
            }

            let mut running = 0.0f32;
            for idx in (0..num_secondaries).rev() {
                self.secondary_suffix_block_max[idx] = running;
                running += self.secondary_block_max_scores[idx];
            }

            if let Some(doc_id) = self.handle_candidates() {
                return doc_id;
            }
            // no candidates left, reset and advance internal doc
            self.num_candidates = 0;
            self.internal_doc = self.window_end + 1;
        }

        self.current = (TERMINATED, Score::MIN);
        TERMINATED
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.current.0
    }

    /// The number of elements yielded by a PruningScorer depends on the threshold and cannot be
    /// computed ahead of time, so just defer to the leader, as it will be the smallest.
    fn size_hint(&self) -> u32 {
        self.leader.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    use proptest::prelude::*;

    use crate::query::term_query::TermScorer;
    use crate::query::{Bm25Weight, Scorer};
    use crate::{DocId, DocSet, Score, TERMINATED};

    struct Float(Score);

    impl Eq for Float {}

    impl PartialEq for Float {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(other) == Ordering::Equal
        }
    }

    impl PartialOrd for Float {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for Float {
        fn cmp(&self, other: &Self) -> Ordering {
            other.0.partial_cmp(&self.0).unwrap_or(Ordering::Equal)
        }
    }

    fn nearly_equals(left: Score, right: Score) -> bool {
        (left - right).abs() < 0.0001 * (left + right).abs()
    }

    /// Run block_wand_intersection and collect (doc, score) pairs above threshold.
    fn compute_checkpoints_block_wand_intersection(
        term_scorers: Vec<TermScorer>,
        top_k: usize,
    ) -> Vec<(DocId, Score)> {
        let mut heap: BinaryHeap<Float> = BinaryHeap::with_capacity(top_k);
        let mut checkpoints: Vec<(DocId, Score)> = Vec::new();
        let mut limit: Score = 0.0;

        let callback = &mut |doc, score| {
            heap.push(Float(score));
            if heap.len() > top_k {
                heap.pop().unwrap();
            }
            if heap.len() == top_k {
                limit = heap.peek().unwrap().0;
            }
            if !nearly_equals(score, limit) {
                checkpoints.push((doc, score));
            }
            limit
        };

        super::block_wand_intersection(term_scorers, Score::MIN, callback);
        checkpoints
    }

    /// Naive baseline: intersect by iterating all docs.
    fn compute_checkpoints_naive_intersection(
        mut term_scorers: Vec<TermScorer>,
        top_k: usize,
    ) -> Vec<(DocId, Score)> {
        let mut heap: BinaryHeap<Float> = BinaryHeap::with_capacity(top_k);
        let mut checkpoints: Vec<(DocId, Score)> = Vec::new();
        let mut limit = Score::MIN;

        // Sort by cost to use the cheapest as driver.
        term_scorers.sort_by_key(|s| s.cost());

        let (leader, secondaries) = term_scorers.split_first_mut().unwrap();

        let mut doc = leader.doc();
        while doc != TERMINATED {
            let mut all_match = true;
            for secondary in secondaries.iter_mut() {
                let secondary_doc = secondary.doc();
                let seek_result = if secondary_doc <= doc {
                    secondary.seek(doc)
                } else {
                    secondary_doc
                };
                if seek_result != doc {
                    all_match = false;
                    break;
                }
            }

            if all_match {
                // Accumulate in the same left-to-right order as the WAND implementation
                // (leader first, then each secondary in turn).  Float addition is not
                // associative, so `leader + secondaries.sum()` gives a different bit
                // pattern and can cause spurious nearly_equals failures.
                let mut score: Score = leader.score();
                for secondary in secondaries.iter_mut() {
                    score += secondary.score();
                }

                if score > limit {
                    heap.push(Float(score));
                    if heap.len() > top_k {
                        heap.pop().unwrap();
                    }
                    if heap.len() == top_k {
                        limit = heap.peek().unwrap().0;
                    }
                    if !nearly_equals(score, limit) {
                        checkpoints.push((doc, score));
                    }
                }
            }
            doc = leader.advance();
        }
        checkpoints
    }

    const MAX_TERM_FREQ: u32 = 100u32;

    fn posting_list(max_doc: u32) -> BoxedStrategy<Vec<(DocId, u32)>> {
        (1..max_doc + 1)
            .prop_flat_map(move |doc_freq| {
                (
                    proptest::bits::bitset::sampled(doc_freq as usize, 0..max_doc as usize),
                    proptest::collection::vec(1u32..MAX_TERM_FREQ, doc_freq as usize),
                )
            })
            .prop_map(|(docset, term_freqs)| {
                docset
                    .iter()
                    .map(|doc| doc as u32)
                    .zip(term_freqs.iter().cloned())
                    .collect::<Vec<_>>()
            })
            .boxed()
    }

    #[expect(clippy::type_complexity)]
    fn gen_term_scorers(num_scorers: usize) -> BoxedStrategy<(Vec<Vec<(DocId, u32)>>, Vec<u32>)> {
        (1u32..100u32)
            .prop_flat_map(move |max_doc: u32| {
                (
                    proptest::collection::vec(posting_list(max_doc), num_scorers),
                    proptest::collection::vec(2u32..10u32 * MAX_TERM_FREQ, max_doc as usize),
                )
            })
            .boxed()
    }

    fn test_block_wand_intersection_aux(posting_lists: &[Vec<(DocId, u32)>], fieldnorms: &[u32]) {
        // Repeat docs 64 times to create multi-block scenarios, matching block_wand.rs test
        // strategy.
        const REPEAT: usize = 64;
        let fieldnorms_expanded: Vec<u32> = fieldnorms
            .iter()
            .cloned()
            .flat_map(|fieldnorm| std::iter::repeat_n(fieldnorm, REPEAT))
            .collect();

        let postings_lists_expanded: Vec<Vec<(DocId, u32)>> = posting_lists
            .iter()
            .map(|posting_list| {
                posting_list
                    .iter()
                    .cloned()
                    .flat_map(|(doc, term_freq)| {
                        (0_u32..REPEAT as u32).map(move |offset| {
                            (
                                doc * (REPEAT as u32) + offset,
                                if offset == 0 { term_freq } else { 1 },
                            )
                        })
                    })
                    .collect::<Vec<(DocId, u32)>>()
            })
            .collect();

        let total_fieldnorms: u64 = fieldnorms_expanded
            .iter()
            .cloned()
            .map(|fieldnorm| fieldnorm as u64)
            .sum();
        let average_fieldnorm = (total_fieldnorms as Score) / (fieldnorms_expanded.len() as Score);
        let max_doc = fieldnorms_expanded.len();

        let make_scorers = || -> Vec<TermScorer> {
            postings_lists_expanded
                .iter()
                .map(|postings| {
                    let bm25_weight = Bm25Weight::for_one_term(
                        postings.len() as u64,
                        max_doc as u64,
                        average_fieldnorm,
                    );
                    TermScorer::create_for_test(postings, &fieldnorms_expanded[..], bm25_weight)
                })
                .collect()
        };

        for top_k in 1..4 {
            let checkpoints_optimized =
                compute_checkpoints_block_wand_intersection(make_scorers(), top_k);
            let checkpoints_naive = compute_checkpoints_naive_intersection(make_scorers(), top_k);
            assert_eq!(
                checkpoints_optimized.len(),
                checkpoints_naive.len(),
                "Mismatch in checkpoint count for top_k={top_k}"
            );
            for (&(left_doc, left_score), &(right_doc, right_score)) in
                checkpoints_optimized.iter().zip(checkpoints_naive.iter())
            {
                assert_eq!(left_doc, right_doc);
                assert!(
                    nearly_equals(left_score, right_score),
                    "Score mismatch for doc {left_doc}: {left_score} vs {right_score}"
                );
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]
        #[test]
        fn test_block_wand_intersection_two_scorers(
            (posting_lists, fieldnorms) in gen_term_scorers(2)
        ) {
            test_block_wand_intersection_aux(&posting_lists[..], &fieldnorms[..]);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]
        #[test]
        fn test_block_wand_intersection_three_scorers(
            (posting_lists, fieldnorms) in gen_term_scorers(3)
        ) {
            test_block_wand_intersection_aux(&posting_lists[..], &fieldnorms[..]);
        }
    }

    #[test]
    fn test_block_wand_intersection_three_scorers_regression() {
        // Minimal failing case found by proptest (CI run 27557430583, job 81460063906).
        // Posting list 0 spans docs 0–63 (all present, doc 8 has tf=80, doc 26 tf=4, rest tf=1).
        // Posting lists 1 and 2 are sparse with varying term freqs, and doc 16/64 appear only
        // in lists 1/2 but not list 0.  The high tf=80 on doc 8 of list 0 makes the WAND
        // upper-bound estimation skip documents that the naive intersection would score.
        let posting_lists: &[&[(DocId, u32)]] = &[
            &[
                (0, 1),
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 1),
                (6, 1),
                (7, 1),
                (8, 80),
                (9, 1),
                (10, 1),
                (11, 1),
                (12, 1),
                (13, 1),
                (14, 1),
                (15, 1),
                (17, 1),
                (18, 1),
                (19, 1),
                (20, 1),
                (21, 1),
                (22, 1),
                (23, 1),
                (24, 1),
                (25, 1),
                (26, 4),
                (27, 1),
                (28, 1),
                (29, 1),
                (30, 1),
                (31, 1),
                (32, 1),
                (33, 1),
                (34, 1),
                (35, 1),
                (36, 1),
                (37, 1),
                (38, 1),
                (39, 1),
                (40, 1),
                (41, 1),
                (42, 1),
                (43, 1),
                (44, 1),
                (45, 1),
                (46, 1),
                (47, 1),
                (48, 1),
                (49, 1),
                (50, 1),
                (51, 1),
                (52, 1),
                (53, 1),
                (54, 1),
                (55, 1),
                (56, 1),
                (57, 1),
                (58, 1),
                (59, 1),
                (60, 1),
                (61, 1),
                (62, 1),
                (63, 1),
            ],
            &[
                (0, 2),
                (3, 98),
                (7, 93),
                (8, 87),
                (9, 39),
                (10, 2),
                (12, 71),
                (14, 47),
                (15, 76),
                (16, 6),
                (17, 38),
                (19, 61),
                (20, 87),
                (21, 1),
                (22, 5),
                (23, 43),
                (25, 48),
                (26, 87),
                (28, 81),
                (29, 69),
                (30, 7),
                (31, 47),
                (32, 32),
                (33, 38),
                (35, 39),
                (38, 65),
                (39, 98),
                (42, 43),
                (43, 52),
                (44, 99),
                (45, 88),
                (48, 24),
                (51, 61),
                (52, 22),
                (53, 58),
                (55, 26),
                (56, 32),
                (58, 57),
                (60, 29),
                (61, 78),
                (62, 9),
                (63, 44),
                (64, 29),
            ],
            &[
                (0, 94),
                (2, 49),
                (3, 63),
                (4, 7),
                (6, 93),
                (7, 17),
                (8, 91),
                (9, 18),
                (10, 85),
                (11, 11),
                (12, 45),
                (13, 42),
                (15, 91),
                (16, 44),
                (17, 36),
                (18, 68),
                (19, 24),
                (20, 17),
                (21, 59),
                (22, 97),
                (24, 20),
                (25, 7),
                (26, 85),
                (27, 69),
                (28, 78),
                (29, 84),
                (30, 35),
                (31, 49),
                (33, 83),
                (34, 97),
                (35, 29),
                (36, 43),
                (37, 59),
                (38, 79),
                (39, 74),
                (40, 21),
                (41, 5),
                (42, 47),
                (43, 27),
                (44, 59),
                (45, 97),
                (46, 91),
                (47, 81),
                (48, 57),
                (49, 47),
                (50, 64),
                (51, 86),
                (52, 60),
                (53, 52),
                (54, 14),
                (55, 23),
                (56, 64),
                (57, 40),
                (58, 5),
                (59, 30),
                (60, 81),
                (61, 62),
                (62, 39),
                (63, 93),
                (64, 82),
            ],
        ];
        let fieldnorms: &[u32] = &[
            624, 668, 725, 670, 851, 169, 537, 627, 200, 757, 51, 272, 835, 89, 750, 63, 272, 406,
            394, 390, 822, 449, 257, 571, 527, 855, 4, 98, 548, 413, 539, 351, 596, 151, 728, 152,
            766, 829, 20, 828, 477, 251, 743, 646, 136, 477, 909, 907, 266, 341, 676, 161, 40, 384,
            347, 707, 42, 397, 482, 814, 801, 528, 465, 410, 171,
        ];
        let posting_lists_owned: Vec<Vec<(DocId, u32)>> =
            posting_lists.iter().map(|pl| pl.to_vec()).collect();
        test_block_wand_intersection_aux(&posting_lists_owned, fieldnorms);
    }

    #[test]
    fn test_block_wand_intersection_disjoint() {
        // Two posting lists with no overlap — intersection is empty.
        let fieldnorms: Vec<u32> = vec![10; 200];
        let average_fieldnorm = 10.0;
        let postings_a: Vec<(DocId, u32)> = (0..100).map(|d| (d, 1)).collect();
        let postings_b: Vec<(DocId, u32)> = (100..200).map(|d| (d, 1)).collect();

        let scorer_a = TermScorer::create_for_test(
            &postings_a,
            &fieldnorms,
            Bm25Weight::for_one_term(100, 200, average_fieldnorm),
        );
        let scorer_b = TermScorer::create_for_test(
            &postings_b,
            &fieldnorms,
            Bm25Weight::for_one_term(100, 200, average_fieldnorm),
        );

        let checkpoints = compute_checkpoints_block_wand_intersection(vec![scorer_a, scorer_b], 10);
        assert!(checkpoints.is_empty());
    }

    #[test]
    fn test_block_wand_intersection_all_overlap() {
        // Two posting lists with full overlap.
        let fieldnorms: Vec<u32> = vec![10; 50];
        let average_fieldnorm = 10.0;
        let postings: Vec<(DocId, u32)> = (0..50).map(|d| (d, 3)).collect();

        let make_scorer = || {
            TermScorer::create_for_test(
                &postings,
                &fieldnorms,
                Bm25Weight::for_one_term(50, 50, average_fieldnorm),
            )
        };

        let checkpoints_opt =
            compute_checkpoints_block_wand_intersection(vec![make_scorer(), make_scorer()], 5);
        let checkpoints_naive =
            compute_checkpoints_naive_intersection(vec![make_scorer(), make_scorer()], 5);
        assert_eq!(checkpoints_opt.len(), checkpoints_naive.len());
    }
}
