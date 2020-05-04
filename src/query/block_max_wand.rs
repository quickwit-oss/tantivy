use crate::docset::{DocSet, SkipResult};
use crate::query::score_combiner::ScoreCombiner;
use crate::query::{BlockMaxScorer, Scorer};
use crate::DocId;
use crate::Score;
use crate::query::scorer::ScorerWithPruning;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct Pivot {
    position: usize,
    first_occurrence: usize,
    doc: DocId,
}


/// Find the position in the sorted list of posting lists of the **pivot**.
///
/// docsets need to be advanced, and are required to be sorted by the doc they point to.
///
/// The pivot is then defined as the lowest DocId that has a chance of matching our condition.
fn find_pivot_position<'a, TScorer>(
    mut docsets: impl Iterator<Item = &'a TScorer>,
    lower_bound_score: Score,
) -> Option<Pivot>
    where
        TScorer: BlockMaxScorer + Scorer,
{
    let mut position = 0;
    let mut upper_bound = Score::default();
    while let Some(docset) = docsets.next() {
        upper_bound += docset.max_score();
        if lower_bound_score < upper_bound {
            let pivot_doc = docset.doc();
            let first_occurrence = position;
            while let Some(docset) = docsets.next() {
                if docset.doc() != pivot_doc {
                    break;
                } else {
                    position += 1;
                }
            }
            return Some(Pivot {
                position,
                doc: pivot_doc,
                first_occurrence,
            });
        }
        position += 1;
    }
    None
}

/// Given an iterator over all ordered lists up to the pivot (inclusive) and the following list (if
/// exists), it returns the next document ID that can be possibly relevant, based on the block max
/// scores.
fn find_next_relevant_doc<T, TScorer>(
    docsets_up_to_pivot: &mut [T],
    pivot_docset: &mut T,
    docset_after_pivot: Option<&mut T>,
) -> DocId
    where
        T: AsMut<TScorer>,
        TScorer: BlockMaxScorer + Scorer,
{
    let pivot_docset = pivot_docset.as_mut();
    let mut next_doc = 1 + docsets_up_to_pivot
        .iter_mut()
        .map(|docset| docset.as_mut().block_max_doc())
        .chain(std::iter::once(pivot_docset.block_max_doc()))
        .min()
        .unwrap();
    if let Some(docset) = docset_after_pivot {
        let doc = docset.as_mut().doc();
        if doc < next_doc {
            next_doc = doc;
        }
    }
    if next_doc <= pivot_docset.doc() {
        pivot_docset.doc() + 1
    } else {
        next_doc
    }
}

/// Sifts down the first element of the slice.
fn sift_down<T, TScorer>(docsets: &mut [T])
    where
        T: AsRef<TScorer>,
        TScorer: BlockMaxScorer + Scorer,
{
    for idx in 1..docsets.len() {
        if docsets[idx].as_ref().doc() < docsets[idx - 1].as_ref().doc() {
            docsets.swap(idx, idx - 1);
        } else {
            break;
        }
    }
}

/// Creates a `DocSet` that iterates through the union of two or more `DocSet`s,
/// applying [BlockMaxWand] dynamic pruning.
///
/// [BlockMaxWand]: https://dl.acm.org/doi/10.1145/2009916.2010048
pub struct BlockMaxWand<TScorer, TScoreCombiner> {
    docsets: Vec<Box<TScorer>>,
    doc: DocId,
    score: Score,
    combiner: TScoreCombiner,
}

impl<TScorer, TScoreCombiner> BlockMaxWand<TScorer, TScoreCombiner>
    where
        TScoreCombiner: ScoreCombiner,
        TScorer: BlockMaxScorer + Scorer,
{
    fn new(
        docsets: Vec<TScorer>,
        combiner: TScoreCombiner,
    ) -> BlockMaxWand<TScorer, TScoreCombiner> {
        let mut non_empty_docsets: Vec<_> = docsets
            .into_iter()
            .flat_map(|mut docset| {
                if docset.advance() {
                    Some(Box::new(docset))
                } else {
                    None
                }
            })
            .collect();
        non_empty_docsets.sort_by_key(Box::<TScorer>::doc);
        BlockMaxWand {
            docsets: non_empty_docsets,
            combiner,
            doc: 0u32,
            score: 0f32
        }
    }

    /// Find the position in the sorted list of posting lists of the **pivot**.
    fn find_pivot_position(&self, lower_bound_score: Score) -> Option<Pivot> {
        find_pivot_position(
            self.docsets.iter().map(|docset| docset.as_ref()),
            lower_bound_score)
    }

    fn advance_with_pivot(&mut self, pivot: Pivot, lower_bound_score: Score) -> SkipResult {
        let block_upper_bound: Score = self.docsets[..=pivot.position]
            .iter_mut()
            .map(|docset| docset.block_max_score())
            .sum();
        if block_upper_bound > lower_bound_score {
            if pivot.doc == self.docsets[0].doc() {
                // Since self.docsets is sorted by their current doc, in this branch, all
                // docsets in [0..=pivot] are positioned on pivot.doc.
                //
                // Lets compute the actual score for this doc.
                //
                // NOTE(elshize): One additional check needs to be done to improve performance:
                // update block-wise bound while accumulating score with the actual score,
                // and check each time if still above threshold.
                self.combiner.clear();
                for idx in (0..=pivot.position).rev() {
                    self.combiner.update(self.docsets[idx].as_mut());
                    if !self.docsets[idx].advance() {
                        self.docsets.swap_remove(idx);
                    }
                }
                self.score = self.combiner.score();
                self.doc = pivot.doc;
                self.docsets.sort_by_key(Box::<TScorer>::doc);
                SkipResult::Reached
            } else {
                // The substraction is correct because otherwise we would go to the other branch.
                let advanced_idx = pivot.first_occurrence - 1;
                if !self.docsets[advanced_idx].advance() {
                    self.docsets.swap_remove(advanced_idx);
                }
                if self.docsets.is_empty() {
                    SkipResult::End
                } else {
                    sift_down(&mut self.docsets[advanced_idx..]);
                    SkipResult::OverStep
                }
            }
        } else {
            let (up_to_pivot, pivot_and_rest) = self.docsets.split_at_mut(pivot.position as usize);
            let (pivot, after_pivot) = pivot_and_rest.split_first_mut().unwrap();
            let next_doc = find_next_relevant_doc(up_to_pivot, pivot, after_pivot.first_mut());
            // NOTE(elshize): It might be more efficient to advance the list with the higher
            // max score, but let's advance the first one for now for simplicity.
            if self.docsets[0].skip_next(next_doc) == SkipResult::End {
                self.docsets.swap_remove(0);
            }
            if self.docsets.is_empty() {
                SkipResult::End
            } else {
                sift_down(&mut self.docsets[..]);
                SkipResult::OverStep
            }
        }
    }
}

impl<TScorer, TScoreCombiner> DocSet
for BlockMaxWand<TScorer, TScoreCombiner>
    where
        TScorer: BlockMaxScorer + Scorer,
        TScoreCombiner: ScoreCombiner,
{
    fn advance(&mut self) -> bool {
       unimplemented!();
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        while self.doc() < target {
            if !self.advance() {
                return SkipResult::End;
            }
        }
        if self.doc() == target {
            SkipResult::Reached
        } else {
            SkipResult::OverStep
        }
    }

    // TODO implement `count` efficiently.

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        0u32
    }

    fn count_including_deleted(&mut self) -> u32 {
        unimplemented!();
    }
}

impl<TScorer, TScoreCombiner> Scorer
for BlockMaxWand<TScorer, TScoreCombiner>
    where
        TScoreCombiner: ScoreCombiner,
        TScorer: Scorer + BlockMaxScorer,
{
    fn score(&mut self) -> Score {
        self.score
    }

    /// Returns `Some(&mut self)` if pruning is supported by the current scorer.
    /// None, if pruning is score is not supported.
    fn get_pruning_scorer(&mut self) -> Option<&mut dyn ScorerWithPruning> {
        Some(self)
    }
}

impl<TScorer, TScoreCombiner> ScorerWithPruning
for BlockMaxWand<TScorer, TScoreCombiner>
    where
        TScoreCombiner: ScoreCombiner,
        TScorer: Scorer + BlockMaxScorer {
    fn advance_with_pruning(&mut self, lower_bound_score: f32) -> bool {
        while let Some(pivot) = self.find_pivot_position(lower_bound_score) {
            match self.advance_with_pivot(pivot, lower_bound_score) {
                SkipResult::End => { return false },
                SkipResult::Reached=> { return true; }
                SkipResult::OverStep => {}
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::common::HasLen;
    use crate::docset::DocSet;
    use crate::query::score_combiner::SumCombiner;
    use crate::query::Union;
    use crate::query::{BlockMaxScorer, Scorer};
    use crate::{DocId, Score};
    use float_cmp::approx_eq;
    use proptest::strategy::Strategy;
    use std::cmp::Ordering;
    use std::num::Wrapping;
    use crate::collector::{SegmentCollector, TopScoreSegmentCollector};

    #[derive(Debug, Clone)]
    pub struct VecDocSet {
        postings: Vec<(DocId, Score)>,
        cursor: Wrapping<usize>,
        block_max_scores: Vec<(DocId, Score)>,
        max_score: Score,
        block_size: usize,
    }

    impl VecDocSet {
        fn new(postings: Vec<(DocId, Score)>, block_size: usize) -> VecDocSet {
            let block_max_scores: Vec<(DocId, f32)> = postings
                .chunks(block_size)
                .into_iter()
                .map(|block| {
                    (
                        block.iter().last().unwrap().0,
                        block
                            .iter()
                            .map(|(_, s)| *s)
                            .fold(-f32::INFINITY, |left, right| left.max(right))
                    )
                })
                .collect();
            let max_score = block_max_scores
                .iter()
                .copied()
                .map(|(_, s)| s)
                .fold(-f32::INFINITY, |left, right| left.max(right));
            VecDocSet {
                postings,
                cursor: Wrapping(0_usize) - Wrapping(1_usize),
                block_max_scores,
                max_score,
                block_size,
            }
        }
        /// Constructs a new set and advances it.
        fn started(postings: Vec<(DocId, Score)>, block_size: usize) -> VecDocSet {
            let mut docset = VecDocSet::new(postings, block_size);
            docset.advance();
            docset
        }
    }

    impl DocSet for VecDocSet {
        fn advance(&mut self) -> bool {
            self.cursor += Wrapping(1);
            self.postings.len() > self.cursor.0
        }

        fn doc(&self) -> DocId {
            self.postings[self.cursor.0].0
        }

        fn size_hint(&self) -> u32 {
            self.len() as u32
        }
    }

    impl HasLen for VecDocSet {
        fn len(&self) -> usize {
            self.postings.len()
        }
    }

    impl BlockMaxScorer for VecDocSet {
        fn max_score(&self) -> Score {
            self.max_score
        }
        fn block_max_score(&mut self) -> Score {
            self.block_max_scores[self.cursor.0 / self.block_size].1
        }
        fn block_max_doc(&mut self) -> DocId {
            self.block_max_scores[self.cursor.0 / self.block_size].0
        }
    }

    impl Scorer for VecDocSet {
        fn score(&mut self) -> Score {
            self.postings[self.cursor.0].1
        }
    }

    #[derive(Debug)]
    struct ComparableDoc<T, D> {
        feature: T,
        doc: D,
    }

    impl<T: PartialOrd, D: PartialOrd> PartialOrd for ComparableDoc<T, D> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl<T: PartialOrd, D: PartialOrd> Ord for ComparableDoc<T, D> {
        #[inline]
        fn cmp(&self, other: &Self) -> Ordering {
            // Reversed to make BinaryHeap work as a min-heap
            let by_feature = other
                .feature
                .partial_cmp(&self.feature)
                .unwrap_or(Ordering::Equal);

            let lazy_by_doc_address =
                || self.doc.partial_cmp(&other.doc).unwrap_or(Ordering::Equal);

            // In case of a tie on the feature, we sort by ascending
            // `DocAddress` in order to ensure a stable sorting of the
            // documents.
            by_feature.then_with(lazy_by_doc_address)
        }
    }

    impl<T: PartialOrd, D: PartialOrd> PartialEq for ComparableDoc<T, D> {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(other) == Ordering::Equal
        }
    }

    impl<T: PartialOrd, D: PartialOrd> Eq for ComparableDoc<T, D> {}

    fn union_vs_bmw(posting_lists: Vec<VecDocSet>) {
        let mut union = Union::<VecDocSet, SumCombiner>::from(posting_lists.clone());
        let mut top_union = TopScoreSegmentCollector::new(0, 10);
        while union.advance() {
            top_union.collect(union.doc(), union.score());
        }
        let top_bmw = TopScoreSegmentCollector::new(0, 10 );
        let mut bmw = BlockMaxWand::new(posting_lists, SumCombiner::default());
        let top_docs_bnw = top_bmw.collect_scorer(&mut bmw, None);
        for ((expected_score, expected_doc), (actual_score, actual_doc)) in
        top_union.harvest().into_iter().zip( top_docs_bnw )
        {
            assert!(approx_eq!(
                f32,
                expected_score,
                actual_score,
                epsilon = 0.0001
            ));
            assert_eq!(expected_doc, actual_doc);
        }
    }

    #[test]
    fn test_bmw_0() {
        union_vs_bmw(vec![
            VecDocSet {
                postings: vec![
                    (0, 1.0),
                    (23, 1.0),
                    (28, 1.0),
                    (56, 1.0),
                    (59, 1.0),
                    (66, 1.0),
                    (93, 1.0),
                ],
                cursor: Wrapping(0_usize) - Wrapping(1_usize),
                block_max_scores: vec![(93, 1.0)],
                max_score: 1.0,
                block_size: 16,
            },
            VecDocSet {
                postings: vec![
                    (2, 1.6549665),
                    (43, 2.6958032),
                    (53, 3.5309567),
                    (71, 2.7688136),
                    (87, 3.4279852),
                    (96, 3.9028034),
                ],
                cursor: Wrapping(0_usize) - Wrapping(1_usize),
                block_max_scores: vec![(96, 3.9028034)],
                max_score: 3.9028034,
                block_size: 16,
            },
        ])
    }

    #[test]
    fn test_bmw_1() {
        union_vs_bmw(vec![
            VecDocSet {
                postings: vec![(73, 1.0), (82, 1.0)],
                cursor: Wrapping(0_usize) - Wrapping(1_usize),
                block_max_scores: vec![(82, 1.0)],
                max_score: 1.0,
                block_size: 16,
            },
            VecDocSet {
                postings: vec![
                    (21, 3.582513),
                    (23, 1.6928024),
                    (27, 3.887647),
                    (42, 1.5469292),
                    (61, 1.7317574),
                    (62, 1.2968783),
                    (82, 2.4040694),
                    (85, 3.1487892),
                ],
                cursor: Wrapping(0_usize) - Wrapping(1_usize),
                block_max_scores: vec![(85, 3.887647)],
                max_score: 3.887647,
                block_size: 16,
            },
        ])
    }

    proptest::proptest! {
        #[test]
        fn test_union_vs_bmw(postings in proptest::collection::vec(
            proptest::collection::vec(0_u32..100, 1..10)
                .prop_flat_map(|v| {
                    let scores = proptest::collection::vec(1_f32..4_f32, v.len()..=v.len());
                    scores.prop_map(move |s| {
                        let mut postings: Vec<_> = v.iter().copied().zip(s.iter().copied()).collect();
                        postings.sort_by_key(|p| p.0);
                        postings.dedup_by_key(|p| p.0);
                        VecDocSet::new(postings, 16)
                    })
                }),
            2..5)
        ) {
            union_vs_bmw(postings);
        }
    }

    #[test]
    fn test_find_pivot_position() {
        let postings = vec![
            VecDocSet::started(vec![(0, 2.0)], 1),
            VecDocSet::started(vec![(1, 3.0)], 1),
            VecDocSet::started(vec![(2, 4.0)], 1),
            VecDocSet::started(vec![(3, 5.0)], 1),
            VecDocSet::started(vec![(3, 6.0)], 1),
        ];
        assert_eq!(
            find_pivot_position(postings.iter(), 2.0f32),
            Some(Pivot {
                position: 1,
                doc: 1,
                first_occurrence: 1,
            })
        );
        assert_eq!(
            find_pivot_position(postings.iter(), 5.0f32),
            Some(Pivot {
                position: 2,
                doc: 2,
                first_occurrence: 2,
            })
        );
        assert_eq!(
            find_pivot_position(postings.iter(), 9.0f32),
            Some(Pivot {
                position: 4,
                doc: 3,
                first_occurrence: 3,
            })
        );
        assert_eq!(
            find_pivot_position(postings.iter(), 20.0f32),
            None
        );
    }

    #[test]
    fn test_find_next_relevant_doc_before_pivot() {
        let mut postings = vec![
            Box::new(VecDocSet::started(vec![(0, 0.0), (3, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(1, 0.0), (4, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(2, 0.0), (6, 0.0)], 2)), // pivot
            Box::new(VecDocSet::started(vec![(6, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(6, 0.0), (8, 0.0)], 2)),
        ];
        let (up_to_pivot, rest) = postings.split_at_mut(2);
        let (pivot, after_pivot) = rest.split_first_mut().unwrap();
        let next_doc = find_next_relevant_doc(up_to_pivot, pivot, Some(&mut after_pivot[0]));
        assert_eq!(next_doc, 4);
    }

    #[test]
    fn test_find_next_relevant_doc_prefix_smaller_than_pivot() {
        let mut postings = vec![
            Box::new(VecDocSet::started(vec![(0, 0.0), (3, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(1, 0.0), (4, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(5, 0.0), (8, 0.0)], 2)), // pivot
            Box::new(VecDocSet::started(vec![(6, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(6, 0.0), (8, 0.0)], 2)),
        ];
        let (up_to_pivot, rest) = postings.split_at_mut(2);
        let (pivot, after_pivot) = rest.split_first_mut().unwrap();
        let next_doc = find_next_relevant_doc(up_to_pivot, pivot, Some(&mut after_pivot[0]));
        assert_eq!(next_doc, 6);
    }

    #[test]
    fn test_find_next_relevant_doc_after_pivot() {
        let mut postings = vec![
            Box::new(VecDocSet::started(vec![(0, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(1, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(2, 0.0), (8, 0.0)], 2)), // pivot
            Box::new(VecDocSet::started(vec![(5, 0.0), (7, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(6, 0.0), (7, 0.0)], 2)),
        ];
        let (up_to_pivot, rest) = postings.split_at_mut(2);
        let (pivot, after_pivot) = rest.split_first_mut().unwrap();
        let next_doc = find_next_relevant_doc(up_to_pivot, pivot, Some(&mut after_pivot[0]));
        assert_eq!(next_doc, 5);
    }

    #[test]
    fn test_sift_down_already_sifted() {
        let mut postings = vec![
            Box::new(VecDocSet::started(vec![(0, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(1, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(2, 0.0), (8, 0.0)], 2)), // pivot
            Box::new(VecDocSet::started(vec![(5, 0.0), (7, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(6, 0.0), (7, 0.0)], 2)),
        ];
        sift_down(&mut postings[2..]);
        assert_eq!(
            postings.into_iter().map(|p| p.doc()).collect::<Vec<_>>(),
            vec![0, 1, 2, 5, 6]
        );
    }

    #[test]
    fn test_sift_down_sift_one_down() {
        let mut postings = vec![
            Box::new(VecDocSet::started(vec![(0, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(1, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(6, 0.0), (8, 0.0)], 2)), // pivot
            Box::new(VecDocSet::started(vec![(5, 0.0), (7, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(7, 0.0), (7, 0.0)], 2)),
        ];
        sift_down(&mut postings[2..]);
        assert_eq!(
            postings.into_iter().map(|p| p.doc()).collect::<Vec<_>>(),
            vec![0, 1, 5, 6, 7]
        );
    }

    #[test]
    fn test_sift_down_to_bottom() {
        let mut postings = vec![
            Box::new(VecDocSet::started(vec![(0, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(1, 0.0), (8, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(7, 0.0), (8, 0.0)], 2)), // pivot
            Box::new(VecDocSet::started(vec![(5, 0.0), (7, 0.0)], 2)),
            Box::new(VecDocSet::started(vec![(6, 0.0), (7, 0.0)], 2)),
        ];
        sift_down(&mut postings[2..]);
        assert_eq!(
            postings.into_iter().map(|p| p.doc()).collect::<Vec<_>>(),
            vec![0, 1, 5, 6, 7]
        );
    }
}
