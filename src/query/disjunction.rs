use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::query::score_combiner::DoNothingCombiner;
use crate::query::{ScoreCombiner, Scorer};
use crate::{DocId, DocSet, Score, TERMINATED};

pub struct DisjunctionScorer<TScorer, TScoreCombiner = DoNothingCombiner> {
    chains: MinHeap<TScorer>,
    minimum_matches_required: usize,
    score_combiner: TScoreCombiner,

    doc: DocId,
    score: Score,
    is_end: bool,
}

// TODO: Try reconstruct, it's ugly.
type MinHeap<T> = BinaryHeap<Reverse<ScorerWrapper<T>>>;

#[repr(transparent)]
struct ScorerWrapper<T>(T);

impl<T: Scorer> PartialEq for ScorerWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.doc() == other.0.doc()
    }
}

impl<T: Scorer> Eq for ScorerWrapper<T> {}

impl<T: Scorer> PartialOrd for ScorerWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Scorer> Ord for ScorerWrapper<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.doc().cmp(&other.0.doc())
    }
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> DisjunctionScorer<TScorer, TScoreCombiner> {
    pub fn new<T: IntoIterator<Item = TScorer>>(
        docsets: T,
        score_combiner: TScoreCombiner,
        minimum_matches_required: usize,
    ) -> Self {
        debug_assert!(
            minimum_matches_required > 1,
            "union scorer works better if just one matches required"
        );
        let chains: MinHeap<_> = docsets
            .into_iter()
            .map(|doc| Reverse(ScorerWrapper(doc)))
            .collect();
        let mut disjunction = Self {
            chains,
            score_combiner,
            doc: TERMINATED,
            minimum_matches_required,
            score: 0.0,
            is_end: false,
        };
        if minimum_matches_required > disjunction.chains.len() {
            // Mark it as empty.
            disjunction.is_end = true;
            return disjunction;
        }
        disjunction.advance();
        disjunction
    }
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> DocSet
    for DisjunctionScorer<TScorer, TScoreCombiner>
{
    fn advance(&mut self) -> DocId {
        let mut votes = 0;
        while let Some(mut candidate) = self.chains.pop() {
            let next = candidate.0 .0.doc();
            if next != TERMINATED {
                // Peek next doc.
                if self.doc != next {
                    if votes >= self.minimum_matches_required {
                        self.chains.push(candidate);
                        self.score = self.score_combiner.score();
                        return self.doc;
                    }
                    // Reset votes and scores.
                    votes = 0;
                    self.doc = next;
                    self.score_combiner.clear();
                }
                votes += 1;
                self.score_combiner.update(&mut candidate.0 .0);
                candidate.0 .0.advance();
                self.chains.push(candidate);
            }
        }
        if votes < self.minimum_matches_required {
            self.doc = TERMINATED;
            self.is_end = true;
        }
        return self.doc;
    }

    fn doc(&self) -> DocId {
        if self.is_end {
            return TERMINATED;
        }
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.chains
            .iter()
            .map(|docset| docset.0 .0.size_hint())
            .max()
            .unwrap_or(0u32)
    }
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> Scorer
    for DisjunctionScorer<TScorer, TScoreCombiner>
{
    fn score(&mut self) -> Score {
        self.score
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::query::score_combiner::DoNothingCombiner;
    use crate::query::{ConstScorer, Scorer, SumCombiner, VecDocSet};
    use crate::{DocId, DocSet, Score, TERMINATED};

    use super::DisjunctionScorer;

    fn conjunct<T: Ord + Copy>(arrays: &[Vec<T>], pass_line: usize) -> Vec<T> {
        let mut counts = BTreeMap::new();
        for array in arrays {
            for &element in array {
                *counts.entry(element).or_insert(0) += 1;
            }
        }
        counts
            .iter()
            .filter_map(|(&element, &count)| {
                if count >= pass_line {
                    Some(element)
                } else {
                    None
                }
            })
            .collect()
    }

    fn aux_test_conjunction(vals: Vec<Vec<u32>>, min_match: usize) {
        let mut union_expected = VecDocSet::from(conjunct(&vals, min_match));
        let make_scorer = || {
            DisjunctionScorer::new(
                vals.iter()
                    .cloned()
                    .map(VecDocSet::from)
                    .map(|d| ConstScorer::new(d, 1.0)),
                DoNothingCombiner::default(),
                min_match,
            )
        };
        let mut scorer: DisjunctionScorer<_, DoNothingCombiner> = make_scorer();
        let mut count = 0;
        while scorer.doc() != TERMINATED {
            assert_eq!(union_expected.doc(), scorer.doc());
            assert_eq!(union_expected.advance(), scorer.advance());
            count += 1;
        }
        assert_eq!(union_expected.advance(), TERMINATED);
        assert_eq!(count, make_scorer().count_including_deleted());
    }

    #[should_panic]
    #[test]
    fn test_arg_check1() {
        aux_test_conjunction(vec![], 0);
    }

    #[should_panic]
    #[test]
    fn test_arg_check2() {
        aux_test_conjunction(vec![], 1);
    }

    #[test]
    fn test_corner_case() {
        aux_test_conjunction(vec![], 2);
        aux_test_conjunction(vec![vec![]; 1000], 2);
        aux_test_conjunction(vec![vec![]; 100], usize::MAX);
        aux_test_conjunction(vec![vec![0xC0FFEE]; 10000], usize::MAX);
        aux_test_conjunction((1..10000u32).map(|i| vec![i]).collect::<Vec<_>>(), 2);
    }

    #[test]
    fn test_conjunction() {
        aux_test_conjunction(
            vec![
                vec![1, 3333, 100000000u32],
                vec![1, 2, 100000000u32],
                vec![1, 2, 100000000u32],
            ],
            2,
        );
        aux_test_conjunction(
            vec![vec![8], vec![3, 4, 0xC0FFEEu32], vec![1, 2, 100000000u32]],
            2,
        );
        aux_test_conjunction(
            vec![
                vec![1, 3333, 100000000u32],
                vec![1, 2, 100000000u32],
                vec![1, 2, 100000000u32],
            ],
            3,
        )
    }

    // This dummy scorer does nothing but yield doc id increasingly.
    // with constant score 1.0
    #[derive(Clone)]
    struct DummyScorer {
        cursor: usize,
        foo: Vec<(DocId, f32)>,
    }

    impl DummyScorer {
        fn new(doc_score: Vec<(DocId, f32)>) -> Self {
            Self {
                cursor: 0,
                foo: doc_score,
            }
        }
    }

    impl DocSet for DummyScorer {
        fn advance(&mut self) -> DocId {
            self.cursor += 1;
            self.doc()
        }

        fn doc(&self) -> DocId {
            self.foo.get(self.cursor).map(|x| x.0).unwrap_or(TERMINATED)
        }

        fn size_hint(&self) -> u32 {
            self.foo.len() as u32
        }
    }

    impl Scorer for DummyScorer {
        fn score(&mut self) -> Score {
            self.foo.get(self.cursor).map(|x| x.1).unwrap_or(0.0)
        }
    }

    #[test]
    fn test_score_calculate() {
        let mut scorer = DisjunctionScorer::new(
            vec![
                DummyScorer::new(vec![(1, 1f32), (2, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (3, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (4, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (2, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (2, 1f32)]),
            ],
            SumCombiner::default(),
            3,
        );
        assert_eq!(scorer.score(), 5.0);
        assert_eq!(scorer.advance(), 2);
        assert_eq!(scorer.score(), 3.0);
    }
}
