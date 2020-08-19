use crate::query::term_query::TermScorer;
use crate::query::Scorer;
use crate::{DocId, DocSet, Score, TERMINATED};
use std::ops::Deref;
use std::ops::DerefMut;

fn is_sorted<I: Iterator<Item = DocId>>(mut it: I) -> bool {
    if let Some(first) = it.next() {
        let mut prev = first;
        for doc in it {
            if doc < prev {
                return false;
            }
            prev = doc;
        }
    }
    true
}

/// Takes a term_scorers sorted by their current doc() and a threshold and returns
/// Returns (pivot_len, pivot_ord) defined as follows:
/// - `pivot_doc` lowest document that has a chance of exceeding (>) the threshold score.
/// - `before_pivot_len` number of term_scorers such that term_scorer.doc() < pivot.
/// - `pivot_len` number of term_scorers such that term_scorer.doc() <= pivot.
///
/// We always have `before_pivot_len` < `pivot_len`.
///
/// None is returned if we establish that no document can exceed the threshold.
fn find_pivot_doc(
    term_scorers: &[TermScorerWithMaxScore],
    threshold: Score,
) -> Option<(usize, usize, DocId)> {
    let mut max_score = 0.0;
    let mut before_pivot_len = 0;
    let mut pivot_doc = TERMINATED;
    while before_pivot_len < term_scorers.len() {
        let term_scorer = &term_scorers[before_pivot_len];
        max_score += term_scorer.max_score;
        if max_score > threshold {
            pivot_doc = term_scorer.doc();
            break;
        }
        before_pivot_len += 1;
    }
    if pivot_doc == TERMINATED {
        return None;
    }
    // Right now i is an ordinal, we want a len.
    let mut pivot_len = before_pivot_len + 1;
    // Some other term_scorer may be positioned on the same document.
    pivot_len += term_scorers[pivot_len..]
        .iter()
        .take_while(|term_scorer| term_scorer.doc() == pivot_doc)
        .count();
    Some((before_pivot_len, pivot_len, pivot_doc))
}

struct TermScorerWithMaxScore<'a> {
    scorer: &'a mut TermScorer,
    max_score: Score,
}

impl<'a> From<&'a mut TermScorer> for TermScorerWithMaxScore<'a> {
    fn from(scorer: &'a mut TermScorer) -> Self {
        let max_score = scorer.max_score();
        TermScorerWithMaxScore { scorer, max_score }
    }
}

impl<'a> Deref for TermScorerWithMaxScore<'a> {
    type Target = TermScorer;

    fn deref(&self) -> &Self::Target {
        self.scorer
    }
}

impl<'a> DerefMut for TermScorerWithMaxScore<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scorer
    }
}

// Before and after calling this method, scorers need to be sorted by their `.doc()`.
fn block_max_was_too_low_advance_one_scorer(
    scorers: &mut Vec<TermScorerWithMaxScore>,
    pivot_len: usize,
) {
    let mut scorer_to_seek = pivot_len - 1;
    let mut doc_to_seek_after = scorers[scorer_to_seek].doc();
    for scorer_ord in (0..pivot_len - 1).rev() {
        let scorer = &scorers[scorer_ord];
        if scorer.last_doc_in_block() <= doc_to_seek_after {
            doc_to_seek_after = scorer.last_doc_in_block();
            scorer_to_seek = scorer_ord;
        }
    }
    for scorer in &scorers[pivot_len..] {
        if scorer.doc() <= doc_to_seek_after {
            doc_to_seek_after = scorer.doc();
        }
    }
    scorers[scorer_to_seek].seek(doc_to_seek_after + 1);
    restore_ordering(scorers, scorer_to_seek);
}

// Given a list of term_scorers and a `ord` and assuming that `term_scorers[ord]` is sorted
// except term_scorers[ord] that might be in advance compared to its ranks,
// bubble up term_scorers[ord] in order to restore the ordering.
fn restore_ordering(term_scorers: &mut Vec<TermScorerWithMaxScore>, ord: usize) {
    let doc = term_scorers[ord].doc();
    for i in ord + 1..term_scorers.len() {
        if term_scorers[i].doc() >= doc {
            break;
        }
        term_scorers.swap(i, i - 1);
    }
    debug_assert!(is_sorted(term_scorers.iter().map(|scorer| scorer.doc())));
}

// Attempts to advance all term_scorers between `&term_scorers[0..before_len]` to the pivot.
// If this works, return true.
// If this fails (ie: one of the term_scorer does not contain `pivot_doc` and seek goes past the
// pivot), reorder the term_scorers to ensure the list is still sorted and returns `false`.
// If a term_scorer reach TERMINATED in the process return false remove the term_scorer and return.
fn align_scorers(
    term_scorers: &mut Vec<TermScorerWithMaxScore>,
    pivot_doc: DocId,
    before_pivot_len: usize,
) -> bool {
    debug_assert_ne!(pivot_doc, TERMINATED);
    for i in (0..before_pivot_len).rev() {
        let new_doc = term_scorers[i].seek(pivot_doc);
        if new_doc != pivot_doc {
            if new_doc == TERMINATED {
                term_scorers.swap_remove(i);
            }
            // We went past the pivot.
            // We just go through the outer loop mechanic (Note that pivot is
            // still a possible candidate).
            //
            // Termination is still guaranteed since we can only consider the same
            // pivot at most term_scorers.len() - 1 times.
            restore_ordering(term_scorers, i);
            return false;
        }
    }
    true
}

// Assumes terms_scorers[..pivot_len] are positioned on the same doc (pivot_doc).
// Advance term_scorers[..pivot_len] and out of these removes the terminated scores.
// Restores the ordering of term_scorers.
fn advance_all_scorers_on_pivot(term_scorers: &mut Vec<TermScorerWithMaxScore>, pivot_len: usize) {
    for term_scorer in &mut term_scorers[..pivot_len] {
        term_scorer.advance();
    }
    // TODO use drain_filter when available.
    let mut i = 0;
    while i != term_scorers.len() {
        if term_scorers[i].doc() == TERMINATED {
            term_scorers.swap_remove(i);
        } else {
            i += 1;
        }
    }
    term_scorers.sort_by_key(|scorer| scorer.doc());
}

pub fn block_wand(
    mut scorers: Vec<TermScorer>,
    mut threshold: Score,
    callback: &mut dyn FnMut(u32, Score) -> Score,
) {
    let mut scorers: Vec<TermScorerWithMaxScore> = scorers
        .iter_mut()
        .map(TermScorerWithMaxScore::from)
        .collect();
    scorers.sort_by_key(|scorer| scorer.doc());
    loop {
        // At this point we need to ensure that the scorers are sorted!
        debug_assert!(is_sorted(scorers.iter().map(|scorer| scorer.doc())));
        if let Some((before_pivot_len, pivot_len, pivot_doc)) =
            find_pivot_doc(&scorers[..], threshold)
        {
            debug_assert_ne!(pivot_doc, TERMINATED);
            debug_assert!(before_pivot_len < pivot_len);

            let block_max_score_upperbound: Score = scorers[..pivot_len]
                .iter_mut()
                .map(|scorer| {
                    scorer.shallow_seek(pivot_doc);
                    scorer.block_max_score()
                })
                .sum();

            // Beware after shallow advance, skip readers can be in advance compared to
            // the segment posting lists.
            //
            // `block_segment_postings.load_block()` need to be called separately.
            if block_max_score_upperbound <= threshold {
                // Block max condition was not reached
                // We could get away by simply advancing the scorers to DocId + 1 but it would
                // be inefficient. The optimization requires proper explanation and was
                // isolated in a different function.
                block_max_was_too_low_advance_one_scorer(&mut scorers, pivot_len);
                continue;
            }

            // Block max condition is observed.
            //
            // Let's try and advance all scorers before the pivot to the pivot.
            if !align_scorers(&mut scorers, pivot_doc, before_pivot_len) {
                // At least of the scorer does not contain the pivot.
                //
                // Let's stop scoring this pivot and go through the pivot selection again.
                // Note that the current pivot is not necessarily a bad candidate and it
                // may be picked again.
                continue;
            }

            // At this point, all scorers are positioned on the doc.
            let score = scorers[..pivot_len]
                .iter_mut()
                .map(|scorer| scorer.score())
                .sum();
            if score > threshold {
                threshold = callback(pivot_doc, score);
            }
            // let's advance all of the scorers that are currently positioned on the pivot.
            advance_all_scorers_on_pivot(&mut scorers, pivot_len);
        } else {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::query::score_combiner::SumCombiner;
    use crate::query::term_query::TermScorer;
    use crate::query::Union;
    use crate::query::{BM25Weight, Scorer};
    use crate::{DocId, DocSet, Score, TERMINATED};
    use proptest::prelude::*;
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;
    use std::iter;

    struct Float(Score);
    impl Eq for Float {}
    impl PartialEq for Float {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(&other) == Ordering::Equal
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
        (left - right).abs() < 0.000001 * (left + right).abs()
    }

    fn compute_checkpoints_for_each_pruning(
        term_scorers: Vec<TermScorer>,
        n: usize,
    ) -> Vec<(DocId, Score)> {
        let mut heap: BinaryHeap<Float> = BinaryHeap::with_capacity(n);
        let mut checkpoints: Vec<(DocId, Score)> = Vec::new();
        let mut limit: Score = 0.0;
        super::block_wand(term_scorers, Score::MIN, &mut |doc, score| {
            heap.push(Float(score));
            if heap.len() > n {
                heap.pop().unwrap();
            }
            if heap.len() == n {
                limit = heap.peek().unwrap().0;
            }
            if !nearly_equals(score, limit) {
                checkpoints.push((doc, score));
            }
            return limit;
        });
        checkpoints
    }

    fn compute_checkpoints_manual(term_scorers: Vec<TermScorer>, n: usize) -> Vec<(DocId, Score)> {
        let mut heap: BinaryHeap<Float> = BinaryHeap::with_capacity(n);
        let mut checkpoints: Vec<(DocId, Score)> = Vec::new();
        let mut scorer: Union<TermScorer, SumCombiner> = Union::from(term_scorers);

        let mut limit = Score::MIN;
        loop {
            if scorer.doc() == TERMINATED {
                break;
            }
            let doc = scorer.doc();
            let score = scorer.score();
            if score > limit {
                heap.push(Float(score));
                if heap.len() > n {
                    heap.pop().unwrap();
                }
                if heap.len() == n {
                    limit = heap.peek().unwrap().0;
                }
                if !nearly_equals(score, limit) {
                    checkpoints.push((doc, score));
                }
            }
            scorer.advance();
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

    fn test_block_wand_aux(posting_lists: &[Vec<(DocId, u32)>], fieldnorms: &[u32]) {
        // We virtually repeat all docs 64 times in order to emulate blocks of 2 documents
        // and surface blogs more easily.
        const REPEAT: usize = 64;
        let fieldnorms_expanded = fieldnorms
            .iter()
            .cloned()
            .flat_map(|fieldnorm| iter::repeat(fieldnorm).take(REPEAT))
            .collect::<Vec<u32>>();

        let postings_lists_expanded: Vec<Vec<(DocId, u32)>> = posting_lists
            .iter()
            .map(|posting_list| {
                posting_list
                    .into_iter()
                    .cloned()
                    .flat_map(|(doc, term_freq)| {
                        (0 as u32..REPEAT as u32).map(move |offset| {
                            (
                                doc * (REPEAT as u32) + offset,
                                if offset == 0 { term_freq } else { 1 },
                            )
                        })
                    })
                    .collect::<Vec<(DocId, u32)>>()
            })
            .collect::<Vec<_>>();

        let total_fieldnorms: u64 = fieldnorms_expanded
            .iter()
            .cloned()
            .map(|fieldnorm| fieldnorm as u64)
            .sum();
        let average_fieldnorm = (total_fieldnorms as Score) / (fieldnorms_expanded.len() as Score);
        let max_doc = fieldnorms_expanded.len();

        let term_scorers: Vec<TermScorer> = postings_lists_expanded
            .iter()
            .map(|postings| {
                let bm25_weight = BM25Weight::for_one_term(
                    postings.len() as u64,
                    max_doc as u64,
                    average_fieldnorm,
                );
                TermScorer::create_for_test(postings, &fieldnorms_expanded[..], bm25_weight)
            })
            .collect();
        for top_k in 1..4 {
            let checkpoints_for_each_pruning =
                compute_checkpoints_for_each_pruning(term_scorers.clone(), top_k);
            let checkpoints_manual = compute_checkpoints_manual(term_scorers.clone(), top_k);
            assert_eq!(checkpoints_for_each_pruning.len(), checkpoints_manual.len());
            for (&(left_doc, left_score), &(right_doc, right_score)) in checkpoints_for_each_pruning
                .iter()
                .zip(checkpoints_manual.iter())
            {
                assert_eq!(left_doc, right_doc);
                assert!(nearly_equals(left_score, right_score));
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]
        #[test]
        fn test_block_wand_two_term_scorers((posting_lists, fieldnorms) in gen_term_scorers(2)) {
            test_block_wand_aux(&posting_lists[..], &fieldnorms[..]);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]
        #[test]
        fn test_block_wand_three_term_scorers((posting_lists, fieldnorms) in gen_term_scorers(3)) {
            test_block_wand_aux(&posting_lists[..], &fieldnorms[..]);
        }
    }
}
