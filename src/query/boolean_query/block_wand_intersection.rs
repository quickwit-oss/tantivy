use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
use crate::query::term_query::TermScorer;
use crate::query::Scorer;
use crate::{DocId, DocSet, Score, TERMINATED};

/// Block-max pruning for intersections, with the score grouping of `Intersection::score`.
/// Stored maxima are corrected conservatively when segment statistics differ.
/// Scorers must read frequencies.
pub(crate) fn block_wand_intersection(
    mut scorers: Vec<TermScorer>,
    mut threshold: Score,
    callback: &mut dyn FnMut(DocId, Score) -> Score,
) {
    assert!(scorers.len() >= 2);
    scorers.sort_by_key(DocSet::cost);

    let bounds = scorers
        .iter()
        .map(TermScorer::pruning_bounds)
        .collect::<Option<Vec<_>>>();
    let Some(mut bounds) = bounds else {
        let mut scorer = crate::query::intersect_scorers(
            scorers
                .into_iter()
                .map(|scorer| Box::new(scorer) as Box<dyn Scorer>)
                .collect(),
            u32::MAX,
        );
        crate::query::weight::for_each_pruning_scorer(scorer.as_mut(), threshold, callback);
        return;
    };
    let global_max_score = (bounds[0].global + bounds[1].global)
        + bounds[2..].iter().map(|bound| bound.global).sum::<Score>();
    if global_max_score <= threshold {
        return;
    }

    let (leader, secondaries) = scorers.split_first_mut().unwrap();
    // Keep scoring data available while the leader block cursor is mutably borrowed.
    let fieldnorm_reader = leader.fieldnorm_reader().clone();
    let bm25_weight = leader.bm25_weight().clone();
    let mut doc = leader.doc();
    let mut secondary_block_max_scores = vec![0.0; secondaries.len()].into_boxed_slice();

    let mut shared_bounds = std::collections::HashMap::new();
    while doc < TERMINATED {
        leader.seek_block(doc);
        let leader_block_max = leader.pruning_block_max_score(&mut bounds[0], &mut shared_bounds);
        // Every bound must remain valid through the end of the shared window.
        let mut window_end = leader.last_doc_in_block();
        for (idx, secondary) in secondaries.iter_mut().enumerate() {
            secondary.block_cursor().seek_block(doc);
            if !secondary.block_cursor().has_remaining_docs() {
                return;
            }
            window_end = window_end.min(secondary.last_doc_in_block());
            secondary_block_max_scores[idx] =
                secondary.pruning_block_max_score(&mut bounds[idx + 1], &mut shared_bounds);
        }
        let secondary_tail_bound: Score = secondary_block_max_scores[1..].iter().copied().sum();
        if (leader_block_max + secondary_block_max_scores[0]) + secondary_tail_bound <= threshold {
            doc = window_end + 1;
            continue;
        }

        let block_cursor = leader.block_cursor();
        let start_idx = block_cursor.seek(doc);
        let end_idx = block_cursor
            .doc_decoder
            .seek_within_block(window_end + 1)
            .min(block_cursor.block_len());
        let block_docs = &block_cursor.doc_decoder.output_array()[start_idx..end_idx];
        let block_freqs = &block_cursor.freq_output_array()[start_idx..end_idx];
        let mut candidate_doc_ids = [0u32; COMPRESSION_BLOCK_SIZE];
        let mut candidate_scores = [0.0f32; COMPRESSION_BLOCK_SIZE];
        let mut num_candidates = 0usize;
        for (candidate_doc, term_freq) in
            block_docs.iter().copied().zip(block_freqs.iter().copied())
        {
            let fieldnorm_id = fieldnorm_reader.fieldnorm_id(candidate_doc);
            let leader_score = bm25_weight.score(fieldnorm_id, term_freq);
            // Unconditional writes allow candidate rejection without a branch.
            candidate_doc_ids[num_candidates] = candidate_doc;
            candidate_scores[num_candidates] = leader_score;
            num_candidates += (((leader_score + secondary_block_max_scores[0])
                + secondary_tail_bound)
                > threshold) as usize;
        }

        'next_candidate: for candidate_idx in 0..num_candidates {
            let candidate_doc = candidate_doc_ids[candidate_idx];
            let (second, tail) = secondaries.split_first_mut().unwrap();
            if second.doc() > candidate_doc || second.seek(candidate_doc) != candidate_doc {
                continue;
            }
            let head_score = candidate_scores[candidate_idx] + second.score();
            if head_score + secondary_tail_bound <= threshold {
                continue;
            }
            for secondary in tail.iter_mut() {
                if secondary.doc() > candidate_doc || secondary.seek(candidate_doc) != candidate_doc
                {
                    continue 'next_candidate;
                }
            }
            let score = head_score + tail.iter_mut().map(Scorer::score).sum::<Score>();
            if score > threshold {
                threshold = callback(candidate_doc, score);
                if global_max_score <= threshold {
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
                let score = leader.score()
                    + secondaries[0].score()
                    + secondaries[1..]
                        .iter_mut()
                        .map(Scorer::score)
                        .sum::<Score>();

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
    #[test]
    fn four_term_pruning_preserves_canonical_scores_and_cutoff_membership() {
        let fieldnorms = (0..192).map(|id| 4 + (id * 37) % 173).collect::<Vec<u32>>();
        let segment_average = fieldnorms.iter().map(|&norm| u64::from(norm)).sum::<u64>() as Score
            / fieldnorms.len() as Score;
        for (average, missing) in [
            (107.0, false),
            (segment_average, false),
            (107.0, true),
            (segment_average, true),
        ] {
            let postings = (0..4)
                .map(|position| {
                    (0..192)
                        .filter(|id| !missing || position == 0 || id % (position + 3) != position)
                        .map(|id| (id, 1 + (id * (7 + position * 4)) % (11 + position * 6)))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let make = || {
                postings
                    .iter()
                    .enumerate()
                    .map(|(position, postings)| {
                        let weight =
                            Bm25Weight::for_one_term(192 + position as u64 * 9, 400, average)
                                .boost_by(0.21);
                        TermScorer::create_for_test(postings, &fieldnorms, weight)
                    })
                    .collect::<Vec<_>>()
            };
            let mut oracle = crate::query::intersect_scorers(
                make()
                    .into_iter()
                    .map(|s| Box::new(s) as Box<dyn Scorer>)
                    .collect(),
                192,
            );
            let mut expected = Vec::new();
            while oracle.doc() != TERMINATED {
                expected.push((oracle.doc(), oracle.score()));
                oracle.advance();
            }
            let mut actual = Vec::new();
            super::block_wand_intersection(make(), Score::MIN, &mut |doc, score| {
                actual.push((doc, score));
                Score::MIN
            });
            let mut membership_difference = None;
            for &(target, score) in &expected {
                for threshold in [score.next_down(), score, score.next_up()] {
                    let wanted = expected
                        .iter()
                        .filter(|(_, s)| *s > threshold)
                        .map(|(d, _)| *d)
                        .collect::<Vec<_>>();
                    let mut found = Vec::new();
                    super::block_wand_intersection(make(), threshold, &mut |doc, _| {
                        found.push(doc);
                        threshold
                    });
                    if found != wanted {
                        membership_difference = Some((target, threshold.to_bits(), wanted, found));
                        break;
                    }
                }
            }
            assert_eq!(
                membership_difference, None,
                "canonical cutoff changed membership"
            );
            assert_eq!(
                actual
                    .iter()
                    .map(|(d, s)| (*d, s.to_bits()))
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|(d, s)| (*d, s.to_bits()))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn intersection_without_fieldnorms_keeps_top_document_across_blocks() -> crate::Result<()> {
        use crate::query::{BooleanQuery, EnableScoring, Occur, Query, TermQuery};
        use crate::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
        use crate::{Index, TantivyDocument, Term};
        let mut schema = Schema::builder();
        let field = schema.add_text_field(
            "text",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqs)
                    .set_fieldnorms(false),
            ),
        );
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer::<TantivyDocument>(20_000_000)?;
        for id in 0..256 {
            let frequency = if id == 255 { 100 } else { 1 };
            let text = std::iter::repeat_n("amber stone", frequency)
                .collect::<Vec<_>>()
                .join(" ");
            writer.add_document(doc!(field => text))?;
        }
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        let query = BooleanQuery::new(
            ["amber", "stone"]
                .iter()
                .map(|term| {
                    (
                        Occur::Must,
                        Box::new(TermQuery::new(
                            Term::from_field_text(field, term),
                            IndexRecordOption::WithFreqs,
                        )) as Box<dyn Query>,
                    )
                })
                .collect(),
        );
        let weight = query.weight(EnableScoring::enabled_from_searcher(&searcher))?;
        let reader = &searcher.segment_readers()[0];
        let mut scorer = weight.scorer(reader, 1.0)?;
        let mut expected = (TERMINATED, Score::MIN);
        while scorer.doc() != TERMINATED {
            let score = scorer.score();
            if score > expected.1 {
                expected = (scorer.doc(), score);
            }
            scorer.advance();
        }
        assert_eq!(expected.0, 255);
        let mut actual = (TERMINATED, Score::MIN);
        weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
            actual = (doc, score);
            score
        })?;
        assert_eq!(
            (actual.0, actual.1.to_bits()),
            (expected.0, expected.1.to_bits())
        );
        Ok(())
    }
    #[test]
    fn unsupported_bounds_delegate_to_canonical_intersection() {
        for (boost, average) in [
            (-0.5, 10.0),
            (Score::INFINITY, 10.0),
            (Score::NAN, 10.0),
            (1.0, 0.0),
        ] {
            let postings = [(0, 1), (1, 3), (2, 9)];
            let make = || {
                (0..4)
                    .map(|_| {
                        TermScorer::create_for_test(
                            &postings,
                            &[4, 8, 16],
                            Bm25Weight::for_one_term(3, 10, average).boost_by(boost),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            let mut oracle = crate::query::intersect_scorers(
                make()
                    .into_iter()
                    .map(|s| Box::new(s) as Box<dyn Scorer>)
                    .collect(),
                3,
            );
            let mut scores = Vec::new();
            while oracle.doc() != TERMINATED {
                scores.push((oracle.doc(), oracle.score()));
                oracle.advance();
            }
            for threshold in [Score::NEG_INFINITY, -1.0, 0.0, Score::INFINITY, Score::NAN] {
                let expected = scores
                    .iter()
                    .filter(|(_, s)| *s > threshold)
                    .map(|(d, s)| (*d, s.to_bits()))
                    .collect::<Vec<_>>();
                let mut actual = Vec::new();
                super::block_wand_intersection(make(), threshold, &mut |d, s| {
                    actual.push((d, s.to_bits()));
                    threshold
                });
                assert_eq!(
                    actual, expected,
                    "boost={boost} average={average} threshold={threshold}"
                );
            }
        }
    }

    #[test]
    fn segment_statistics_and_deletions_preserve_topdocs() -> crate::Result<()> {
        use crate::query::{
            BooleanQuery, BoostQuery, EnableScoring, Occur, Query, TermQuery, Weight,
        };
        use crate::schema::{IndexRecordOption, Schema, FAST, INDEXED, TEXT};
        use crate::{Index, TantivyDocument, Term};
        #[derive(Debug, Clone)]
        struct GenericQuery(BooleanQuery);
        struct GenericWeight(Box<dyn Weight>);
        impl Query for GenericQuery {
            fn weight(&self, scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
                Ok(Box::new(GenericWeight(self.0.weight(scoring)?)))
            }
        }
        impl Weight for GenericWeight {
            fn scorer(
                &self,
                reader: &crate::SegmentReader,
                boost: Score,
            ) -> crate::Result<Box<dyn Scorer>> {
                self.0.scorer(reader, boost)
            }
            fn explain(
                &self,
                reader: &crate::SegmentReader,
                doc: DocId,
            ) -> crate::Result<crate::query::Explanation> {
                self.0.explain(reader, doc)
            }
        }
        let mut schema = Schema::builder();
        let field = schema.add_text_field("text", TEXT);
        let id_field = schema.add_u64_field("id", FAST | INDEXED);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer::<TantivyDocument>(20_000_000)?;
        writer.set_merge_policy(Box::new(crate::merge_policy::NoMergePolicy));
        let terms = ["amber", "stone", "quartz", "ruby"];
        for segment in 0..2 {
            for id in 0..192 {
                let text = terms
                    .iter()
                    .enumerate()
                    .flat_map(|(position, term)| {
                        std::iter::repeat_n(
                            *term,
                            1 + (id * (7 + position * 4)) % (11 + position * 6),
                        )
                    })
                    .chain(std::iter::repeat_n(
                        "filler",
                        1 + (id * 37) % (173 + segment * 300),
                    ))
                    .collect::<Vec<_>>()
                    .join(" ");
                writer
                    .add_document(doc!(field => text, id_field => (segment * 192 + id) as u64))?;
            }
            for (position, term) in terms.iter().enumerate() {
                for _ in 0..(if segment == 0 {
                    position + 1
                } else {
                    4 - position
                }) * 9
                {
                    let text = std::iter::repeat_n(*term, 1 + segment * 200)
                        .collect::<Vec<_>>()
                        .join(" ");
                    writer.add_document(doc!(field => text))?;
                }
            }
            writer.commit()?;
        }
        writer.delete_term(Term::from_field_u64(id_field, 17));
        writer.delete_term(Term::from_field_u64(id_field, 250));
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        assert_eq!(
            searcher
                .segment_readers()
                .iter()
                .map(|r| r.num_deleted_docs())
                .sum::<u32>(),
            2
        );
        let query = BooleanQuery::new(
            terms
                .iter()
                .map(|term| {
                    (
                        Occur::Must,
                        Box::new(BoostQuery::new(
                            Box::new(TermQuery::new(
                                Term::from_field_text(field, term),
                                IndexRecordOption::WithFreqs,
                            )),
                            0.21,
                        )) as Box<dyn Query>,
                    )
                })
                .collect(),
        );
        let generic = GenericQuery(query.clone());
        for (limit, offset) in [(1, 0), (20, 0), (20, 19), (100, 100)] {
            let collector = crate::collector::TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by_score();
            let expected = searcher.search(&generic, &collector)?;
            let actual = searcher.search(&query, &collector)?;
            let weight = query.weight(EnableScoring::enabled_from_searcher(&searcher))?;
            for (score, address) in &actual {
                let reader = &searcher.segment_readers()[address.segment_ord as usize];
                assert_eq!(
                    score.to_bits(),
                    weight.explain(reader, address.doc_id)?.value().to_bits()
                );
            }
            let bits = |hits: Vec<(Score, crate::DocAddress)>| {
                hits.into_iter()
                    .map(|(s, d)| (s.to_bits(), d))
                    .collect::<Vec<_>>()
            };
            assert_eq!(bits(actual), bits(expected));
        }
        Ok(())
    }
    #[test]
    fn saturated_skip_frequency_preserves_cutoff_membership() {
        let frequency = u32::MAX - 255;
        let mut norms = vec![1; 615];
        norms[0] = frequency;
        let average =
            norms.iter().map(|&n| u64::from(n)).sum::<u64>() as Score / norms.len() as Score;
        let postings = (0..128)
            .map(|doc| (doc, if doc == 0 { frequency } else { 1 }))
            .collect::<Vec<_>>();
        let make = || {
            (0..2)
                .map(|_| {
                    TermScorer::create_for_test(
                        &postings,
                        &norms,
                        Bm25Weight::for_one_term(128, 615, average),
                    )
                })
                .collect::<Vec<_>>()
        };
        let mut oracle = crate::query::intersect_scorers(
            make()
                .into_iter()
                .map(|s| Box::new(s) as Box<dyn Scorer>)
                .collect(),
            615,
        );
        assert_eq!(oracle.doc(), 0);
        let top_score = oracle.score();
        let threshold = top_score.next_down();
        let mut expected = Vec::new();
        while oracle.doc() != TERMINATED {
            let score = oracle.score();
            if score > threshold {
                expected.push((oracle.doc(), score.to_bits()));
            }
            oracle.advance();
        }
        assert_eq!(expected, vec![(0, top_score.to_bits())]);
        let mut actual = Vec::new();
        super::block_wand_intersection(make(), threshold, &mut |doc, score| {
            actual.push((doc, score.to_bits()));
            threshold
        });
        assert_eq!(actual, expected);
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn drifting_statistics_preserve_canonical_cutoffs(
            seed in 0u32..10000,
            scale in prop_oneof![Just(0.01f32), Just(0.5), Just(2.0), Just(100.0)],
        ) {
            let norms = (0..385u32)
                .map(|id| 200 + (id.wrapping_mul(193).wrapping_add(seed)) % 2000)
                .collect::<Vec<_>>();
            let average = norms.iter().map(|&n| u64::from(n)).sum::<u64>() as Score / norms.len() as Score;
            let postings = (0..4u32)
                .map(|term| {
                    (0..385u32)
                        .filter(|id| (id + term + seed) % (7 + term) != 0)
                        .map(|id| (id, 1 + (id * 17 + term * 31 + seed) % 40))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let make = || {
                postings
                    .iter()
                    .map(|p| {
                        TermScorer::create_for_test(
                            p,
                            &norms,
                            Bm25Weight::for_one_term(p.len() as u64, 1000, average * scale).boost_by(0.21),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            let mut oracle = crate::query::intersect_scorers(
                make()
                    .into_iter()
                    .map(|s| Box::new(s) as Box<dyn Scorer>)
                    .collect(),
                385,
            );
            let mut expected = Vec::new();
            while oracle.doc() != TERMINATED {
                expected.push((oracle.doc(), oracle.score()));
                oracle.advance();
            }
            let cutoff = expected[20].1;
            for threshold in [Score::MIN, cutoff.next_down(), cutoff, cutoff.next_up()] {
                let wanted = expected
                    .iter()
                    .filter(|(_, s)| *s > threshold)
                    .map(|(d, s)| (*d, s.to_bits()))
                    .collect::<Vec<_>>();
                let mut actual = Vec::new();
                super::block_wand_intersection(make(), threshold, &mut |d, s| {
                    actual.push((d, s.to_bits()));
                    threshold
                });
                prop_assert_eq!(actual, wanted);
            }
        }
    }
    #[test]
    fn drifting_argmax_preserves_canonical_membership() {
        let norms = (0..128)
            .map(|d| if d % 2 == 0 { 2 } else { 1000 })
            .collect::<Vec<_>>();
        let postings = (0..128)
            .map(|d| (d, if d % 2 == 0 { 1 } else { 2 }))
            .collect::<Vec<_>>();
        let make = || {
            (0..2)
                .map(|_| {
                    TermScorer::create_for_test(
                        &postings,
                        &norms,
                        Bm25Weight::for_one_term(128, 1000, 10000.0),
                    )
                })
                .collect::<Vec<_>>()
        };
        let mut oracle = crate::query::intersect_scorers(
            make()
                .into_iter()
                .map(|s| Box::new(s) as Box<dyn Scorer>)
                .collect(),
            128,
        );
        let low = oracle.score();
        oracle.advance();
        let high = oracle.score();
        assert!(high > low);
        let threshold = high.next_down();
        let mut expected = Vec::new();
        while oracle.doc() != TERMINATED {
            let score = oracle.score();
            if score > threshold {
                expected.push((oracle.doc(), score.to_bits()));
            }
            oracle.advance();
        }
        assert_eq!(expected.len(), 64);
        let mut actual = Vec::new();
        super::block_wand_intersection(make(), threshold, &mut |d, s| {
            actual.push((d, s.to_bits()));
            threshold
        });
        assert_eq!(actual, expected);
    }
}
