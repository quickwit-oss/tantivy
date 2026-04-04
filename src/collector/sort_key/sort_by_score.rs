use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::collector::sort_key::NaturalComparator;
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::{DocAddress, DocId, Score};

/// Sort by similarity score.
#[derive(Clone, Debug, Copy)]
pub struct SortBySimilarityScore;

impl SortKeyComputer for SortBySimilarityScore {
    type SortKey = Score;

    type Child = SortBySimilarityScore;

    type Comparator = NaturalComparator;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn segment_sort_key_computer(
        &self,
        _segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(SortBySimilarityScore)
    }

    // Sorting by score is special in that it allows for the Block-Wand optimization.
    //
    // We use a BinaryHeap (TopNHeap) instead of TopNComputer here so that the
    // threshold is always the exact K-th best score. TopNComputer only updates its
    // threshold every K docs (at truncation), giving Block-WAND a stale bound.
    fn collect_segment_top_k(
        &self,
        k: usize,
        weight: &dyn crate::query::Weight,
        reader: &crate::SegmentReader,
        segment_ord: u32,
    ) -> crate::Result<Vec<(Self::SortKey, DocAddress)>> {
        let mut top_n = TopNHeap::new(k);

        if let Some(alive_bitset) = reader.alive_bitset() {
            let mut threshold = Score::MIN;
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                if alive_bitset.is_deleted(doc) {
                    return threshold;
                }
                top_n.push(score, doc);
                threshold = top_n.threshold.unwrap_or(Score::MIN);
                threshold
            })?;
        } else {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                top_n.push(score, doc);
                top_n.threshold.unwrap_or(Score::MIN)
            })?;
        }

        Ok(top_n
            .into_vec()
            .into_iter()
            .map(|(score, doc)| (score, DocAddress::new(segment_ord, doc)))
            .collect())
    }
}

impl SegmentSortKeyComputer for SortBySimilarityScore {
    type SortKey = Score;
    type SegmentSortKey = Score;
    type SegmentComparator = NaturalComparator;

    #[inline(always)]
    fn segment_sort_key(&mut self, _doc: DocId, score: Score) -> Score {
        score
    }

    fn convert_segment_sort_key(&self, score: Score) -> Score {
        score
    }
}

/// Min-heap entry: higher score = greater, lower doc wins ties.
struct ScoreHeapEntry {
    score: Score,
    doc: DocId,
}

impl Eq for ScoreHeapEntry {}

impl PartialEq for ScoreHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for ScoreHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoreHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.doc.cmp(&self.doc))
    }
}

/// Heap-based top-K for score collection. O(log K) per insert, but the threshold
/// is always tight, so Block-WAND prunes better than with [`TopNComputer`]'s
/// buffer/median approach.
///
/// Like [`TopNComputer`], items must arrive in ascending doc order, and equal
/// scores are rejected (strict `>`) so that lower doc IDs win ties.
///
/// [`TopNComputer`]: crate::collector::TopNComputer
struct TopNHeap {
    heap: BinaryHeap<Reverse<ScoreHeapEntry>>,
    top_n: usize,
    threshold: Option<Score>,
}

impl TopNHeap {
    fn new(top_n: usize) -> Self {
        TopNHeap {
            heap: BinaryHeap::with_capacity(top_n),
            top_n,
            threshold: None,
        }
    }

    #[inline]
    fn push(&mut self, score: Score, doc: DocId) {
        if self.heap.len() < self.top_n {
            self.heap.push(Reverse(ScoreHeapEntry { score, doc }));
            if self.heap.len() == self.top_n {
                self.threshold = self.heap.peek().map(|Reverse(entry)| entry.score);
            }
        } else if let Some(threshold) = self.threshold {
            if score > threshold {
                // peek_mut + assign is a single sift-down, vs pop + push = two sifts.
                if let Some(mut min) = self.heap.peek_mut() {
                    *min = Reverse(ScoreHeapEntry { score, doc });
                }
                self.threshold = self.heap.peek().map(|Reverse(entry)| entry.score);
            }
        }
    }

    fn into_vec(self) -> Vec<(Score, DocId)> {
        self.heap
            .into_vec()
            .into_iter()
            .map(|Reverse(entry)| (entry.score, entry.doc))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::collector::sort_key::NaturalComparator;
    use crate::collector::TopNComputer;

    #[test]
    fn test_top_n_heap_zero_capacity() {
        let mut heap = TopNHeap::new(0);
        heap.push(1.0, 0);
        heap.push(2.0, 1);
        assert!(heap.into_vec().is_empty());
    }

    #[test]
    fn test_top_n_heap_basic() {
        let mut heap = TopNHeap::new(2);
        heap.push(1.0, 0);
        heap.push(3.0, 1);
        heap.push(2.0, 2);

        let mut results = heap.into_vec();
        results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then_with(|| a.1.cmp(&b.1)));
        assert_eq!(results, vec![(3.0, 1), (2.0, 2)]);
    }

    #[test]
    fn test_top_n_heap_threshold_always_accurate() {
        let mut heap = TopNHeap::new(2);
        assert_eq!(heap.threshold, None);

        heap.push(1.0, 0);
        assert_eq!(heap.threshold, None);

        heap.push(3.0, 1);
        assert_eq!(heap.threshold, Some(1.0));

        heap.push(2.0, 2); // evicts 1.0
        assert_eq!(heap.threshold, Some(2.0));

        heap.push(4.0, 3); // evicts 2.0
        assert_eq!(heap.threshold, Some(3.0));
    }

    #[test]
    fn test_top_n_heap_tiebreaking_lower_doc_wins() {
        let mut heap = TopNHeap::new(2);
        heap.push(5.0, 0);
        heap.push(5.0, 1);
        heap.push(5.0, 2); // rejected: not strictly > threshold

        let mut results = heap.into_vec();
        results.sort_by_key(|&(_, doc)| doc);
        assert_eq!(results, vec![(5.0, 0), (5.0, 1)]);
    }

    #[test]
    fn test_top_n_heap_single_element() {
        let mut heap = TopNHeap::new(1);
        heap.push(1.0, 0);
        assert_eq!(heap.threshold, Some(1.0));

        heap.push(0.5, 1); // rejected
        heap.push(2.0, 2); // accepted
        assert_eq!(heap.threshold, Some(2.0));

        let results = heap.into_vec();
        assert_eq!(results, vec![(2.0, 2)]);
    }

    #[test]
    fn test_top_n_heap_under_capacity() {
        let mut heap = TopNHeap::new(5);
        heap.push(3.0, 0);
        heap.push(1.0, 1);
        heap.push(2.0, 2);
        // Only 3 elements, capacity is 5 — all should be kept
        assert_eq!(heap.threshold, None);

        let mut results = heap.into_vec();
        results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then_with(|| a.1.cmp(&b.1)));
        assert_eq!(results, vec![(3.0, 0), (2.0, 2), (1.0, 1)]);
    }

    proptest! {
        #[test]
        fn test_top_n_heap_matches_top_n_computer(
            limit in 0..20_usize,
            mut docs in proptest::collection::vec((0..1000_u32, 0..1000_u32), 0..200_usize),
        ) {
            // Both require ascending doc order.
            docs.sort_by_key(|(_, doc_id)| *doc_id);
            docs.dedup_by_key(|(_, doc_id)| *doc_id);

            let mut heap = TopNHeap::new(limit);
            let mut computer: TopNComputer<Score, DocId, NaturalComparator> =
                TopNComputer::new_with_comparator(limit, NaturalComparator);

            for &(score_u32, doc) in &docs {
                let score = score_u32 as Score;
                heap.push(score, doc);
                computer.push(score, doc);
            }

            let mut heap_results = heap.into_vec();
            heap_results.sort_by(|a, b| {
                b.0.partial_cmp(&a.0).unwrap().then_with(|| a.1.cmp(&b.1))
            });

            let computer_results: Vec<(Score, DocId)> = computer
                .into_sorted_vec()
                .into_iter()
                .map(|cd| (cd.sort_key, cd.doc))
                .collect();

            prop_assert_eq!(heap_results, computer_results);
        }
    }
}
