use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
use crate::query::term_query::TermScorer;
use crate::query::weight::for_each_pruning_scorer;
use crate::query::{Intersection, Scorer};
use crate::{DocId, DocSet, Score, TERMINATED};

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
/// - `scorers` has less than 16 elements
/// - All scorers read frequencies (`FreqReadingOption::ReadFreq`)
pub(crate) fn block_wand_intersection(
    mut scorers: Vec<TermScorer>,
    mut threshold: Score,
    callback: &mut dyn FnMut(DocId, Score) -> Score,
) {
    assert!(scorers.len() >= 2);
    assert!(scorers.len() <= 16);

    // Sort by cost (ascending). scorers[0] becomes the "leader" (rarest term).
    scorers.sort_by_key(TermScorer::size_hint);

    let (leader, secondaries) = scorers.split_first_mut().unwrap();

    // Precompute global max scores for early termination checks.
    let leader_max_score: Score = leader.max_score();
    let secondaries_global_max_sum: Score = secondaries.iter().map(TermScorer::max_score).sum();

    // Early exit: no document can possibly beat the threshold.
    if leader_max_score + secondaries_global_max_sum <= threshold {
        return;
    }

    // Borrow fieldnorm reader and BM25 weight before the main loop.
    // These are immutable references to disjoint fields from block_cursor,
    // but Rust's borrow checker can't see through method calls, so we
    // extract them once upfront.
    let fieldnorm_reader = leader.fieldnorm_reader().clone();
    let bm25_weight = leader.bm25_weight().clone();

    let mut doc = leader.doc();

    while doc < TERMINATED {
        // --- Phase 1: Block-level pruning ---
        //
        // Position all skip readers on the block containing `doc`.
        // seek_block is cheap: it only advances the skip reader, no block decompression.
        leader.seek_block(doc);
        let leader_block_max: Score = leader.block_max_score();

        // Compute the window end as the minimum last_doc_in_block across all scorers.
        // This ensures the block_max values are valid for all docs in [doc, window_end].
        // Different scorers have independently aligned blocks, so we must use the
        // smallest window where all block_max values hold.
        let mut window_end: DocId = leader.last_doc_in_block();

        let mut secondary_block_max_sum: Score = 0.0;
        let mut secondary_block_max_scores = [0.0f32; 16];
        let num_secondaries = secondaries.len();
        for (idx, secondary) in secondaries.iter_mut().enumerate() {
            if !secondary.block_cursor().seek_block(doc) {
                return;
            }
            window_end = window_end.min(secondary.last_doc_in_block());
            let bms = secondary.block_max_score();
            secondary_block_max_scores[idx] = bms;
            secondary_block_max_sum += bms;
        }

        // Precompute suffix sums: suffix[i] = sum of block_max for secondaries[i+1..].
        // Used in Phase 2 to prune candidates that can't beat threshold even with
        // remaining secondaries contributing their block_max.
        let mut secondary_suffix_block_max = [0.0f32; 16];
        {
            let mut running = 0.0f32;
            for idx in (0..num_secondaries).rev() {
                secondary_suffix_block_max[idx] = running;
                running += secondary_block_max_scores[idx];
            }
        }

        if leader_block_max + secondary_block_max_sum <= threshold {
            // The entire window cannot beat the threshold. Skip past it.
            doc = window_end + 1;
            continue;
        }

        // --- Phase 2: Batch processing within the window ---
        //
        // Score-first approach: decode the leader's block, filter by threshold,
        // then check intersection membership only for survivors. This avoids expensive
        // secondary seeks for docs that can't beat the threshold.
        let block_cursor = leader.block_cursor();
        // seek loads the block and returns the in-block index of the first doc >= `doc`.
        let start_idx = block_cursor.seek(doc);

        // Use the branchless binary search on the doc decoder to find the first
        // index past window_end.
        let end_idx = block_cursor
            .doc_decoder
            .seek_within_block(window_end + 1)
            .min(block_cursor.block_len());

        let block_docs = &block_cursor.doc_decoder.output_array()[start_idx..end_idx];
        let block_freqs = &block_cursor.freq_decoder.output_array()[start_idx..end_idx];

        // Pass 1: Batch-compute leader BM25 scores and branchlessly filter
        // candidates that can't beat the threshold.
        //
        // The trick: always write to the buffer at `num_candidates`, then
        // conditionally advance the count. The compiler can turn this into
        // a cmov instead of a branch, avoiding misprediction costs.
        let score_threshold = threshold - secondary_block_max_sum;
        let mut candidate_doc_ids = [0u32; COMPRESSION_BLOCK_SIZE];
        let mut candidate_scores = [0.0f32; COMPRESSION_BLOCK_SIZE];
        let mut num_candidates = 0usize;

        for (candidate_doc, term_freq) in
            block_docs.iter().copied().zip(block_freqs.iter().copied())
        {
            let fieldnorm_id = fieldnorm_reader.fieldnorm_id(candidate_doc);
            let leader_score = bm25_weight.score(fieldnorm_id, term_freq);
            candidate_doc_ids[num_candidates] = candidate_doc;
            candidate_scores[num_candidates] = leader_score;
            num_candidates += (leader_score > score_threshold) as usize;
        }

        // Pass 2: Check intersection membership only for survivors.
        // score_threshold may be stale (threshold can increase from callbacks),
        // but that's conservative — we may check a few extra candidates, never miss one.
        'next_candidate: for candidate_idx in 0..num_candidates {
            let candidate_doc = candidate_doc_ids[candidate_idx];
            let mut total_score: Score = candidate_scores[candidate_idx];

            for (secondary_idx, secondary) in secondaries.iter_mut().enumerate() {
                // If a previous candidate already advanced this secondary past
                // candidate_doc, the candidate can't be in the intersection.
                if secondary.doc() > candidate_doc {
                    continue 'next_candidate;
                }
                let seek_result = secondary.seek(candidate_doc);
                if seek_result != candidate_doc {
                    continue 'next_candidate;
                }
                total_score += secondary.score();

                // Prune: even if all remaining secondaries score at their block max,
                // can we still beat the threshold?
                if total_score + secondary_suffix_block_max[secondary_idx] <= threshold {
                    continue 'next_candidate;
                }
            }

            // All secondaries matched.
            if total_score > threshold {
                threshold = callback(candidate_doc, total_score);

                if leader_max_score + secondaries_global_max_sum <= threshold {
                    return;
                }
            }
        }

        doc = window_end + 1;
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
                let score: Score =
                    leader.score() + secondaries.iter_mut().map(|s| s.score()).sum::<Score>();

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
