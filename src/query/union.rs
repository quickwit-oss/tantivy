use crate::common::TinySet;
use crate::docset::{DocSet, TERMINATED};
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner};
use crate::query::Scorer;
use crate::DocId;
use crate::Score;

const HORIZON_NUM_TINYBITSETS: usize = 64;
const HORIZON: u32 = 64u32 * HORIZON_NUM_TINYBITSETS as u32;

// `drain_filter` is not stable yet.
// This function is similar except that it does is not unstable, and
// it does not keep the original vector ordering.
//
// Also, it does not "yield" any elements.
fn unordered_drain_filter<T, P>(v: &mut Vec<T>, mut predicate: P)
where
    P: FnMut(&mut T) -> bool,
{
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
pub struct Union<TScorer, TScoreCombiner = DoNothingCombiner> {
    docsets: Vec<TScorer>,
    bitsets: Box<[TinySet; HORIZON_NUM_TINYBITSETS]>,
    scores: Box<[TScoreCombiner; HORIZON as usize]>,
    cursor: usize,
    offset: DocId,
    doc: DocId,
    score: Score,
}

impl<TScorer, TScoreCombiner> From<Vec<TScorer>> for Union<TScorer, TScoreCombiner>
where
    TScoreCombiner: ScoreCombiner,
    TScorer: Scorer,
{
    fn from(docsets: Vec<TScorer>) -> Union<TScorer, TScoreCombiner> {
        let non_empty_docsets: Vec<TScorer> = docsets
            .into_iter()
            .filter(|docset| docset.doc() != TERMINATED)
            .collect();
        let mut union = Union {
            docsets: non_empty_docsets,
            bitsets: Box::new([TinySet::empty(); HORIZON_NUM_TINYBITSETS]),
            scores: Box::new([TScoreCombiner::default(); HORIZON as usize]),
            cursor: HORIZON_NUM_TINYBITSETS,
            offset: 0,
            doc: 0,
            score: 0.0,
        };
        if union.refill() {
            union.advance();
        } else {
            union.doc = TERMINATED;
        }
        union
    }
}

fn refill<TScorer: Scorer, TScoreCombiner: ScoreCombiner>(
    scorers: &mut Vec<TScorer>,
    bitsets: &mut [TinySet; HORIZON_NUM_TINYBITSETS],
    score_combiner: &mut [TScoreCombiner; HORIZON as usize],
    min_doc: DocId,
) {
    unordered_drain_filter(scorers, |scorer| {
        let horizon = min_doc + HORIZON as u32;
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

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> Union<TScorer, TScoreCombiner> {
    fn refill(&mut self) -> bool {
        if let Some(min_doc) = self.docsets.iter().map(DocSet::doc).min() {
            self.offset = min_doc;
            self.cursor = 0;
            self.doc = min_doc;
            refill(
                &mut self.docsets,
                &mut *self.bitsets,
                &mut *self.scores,
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
}

impl<TScorer, TScoreCombiner> DocSet for Union<TScorer, TScoreCombiner>
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
        let gap = target - self.offset;
        if gap < HORIZON {
            // Our value is within the buffered horizon.

            // Skipping to  corresponding bucket.
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
            #[cfg_attr(feature = "cargo-clippy", allow(clippy::clippy::collapsible_if))]
            unordered_drain_filter(&mut self.docsets, |docset| {
                if docset.doc() < target {
                    docset.seek(target);
                }
                docset.doc() == TERMINATED
            });

            // at this point all of the docsets
            // are positionned on a doc >= to the target.
            if !self.refill() {
                self.doc = TERMINATED;
                return TERMINATED;
            }
            self.advance()
        }
    }

    // TODO Also implement `count` with deletes efficiently.

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.docsets
            .iter()
            .map(|docset| docset.size_hint())
            .max()
            .unwrap_or(0u32)
    }

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

impl<TScorer, TScoreCombiner> Scorer for Union<TScorer, TScoreCombiner>
where
    TScoreCombiner: ScoreCombiner,
    TScorer: Scorer,
{
    fn score(&mut self) -> Score {
        self.score
    }
}

#[cfg(test)]
mod tests {

    use super::Union;
    use super::HORIZON;
    use crate::docset::{DocSet, TERMINATED};
    use crate::postings::tests::test_skip_against_unoptimized;
    use crate::query::score_combiner::DoNothingCombiner;
    use crate::query::ConstScorer;
    use crate::query::VecDocSet;
    use crate::tests;
    use crate::DocId;
    use std::collections::BTreeSet;

    fn aux_test_union(vals: Vec<Vec<u32>>) {
        let mut val_set: BTreeSet<u32> = BTreeSet::new();
        for vs in &vals {
            for &v in vs {
                val_set.insert(v);
            }
        }
        let union_vals: Vec<u32> = val_set.into_iter().collect();
        let mut union_expected = VecDocSet::from(union_vals);
        let make_union = || {
            Union::from(
                vals.iter()
                    .cloned()
                    .map(VecDocSet::from)
                    .map(|docset| ConstScorer::new(docset, 1.0))
                    .collect::<Vec<ConstScorer<VecDocSet>>>(),
            )
        };
        let mut union: Union<_, DoNothingCombiner> = make_union();
        let mut count = 0;
        while union.doc() != TERMINATED {
            assert_eq!(union_expected.doc(), union.doc());
            assert_eq!(union_expected.advance(), union.advance());
            count += 1;
        }
        assert_eq!(union_expected.advance(), TERMINATED);
        assert_eq!(count, make_union().count_including_deleted());
    }

    #[test]
    fn test_union() {
        aux_test_union(vec![
            vec![1, 3333, 100000000u32],
            vec![1, 2, 100000000u32],
            vec![1, 2, 100000000u32],
            vec![],
        ]);
        aux_test_union(vec![
            vec![1, 3333, 100000000u32],
            vec![1, 2, 100000000u32],
            vec![1, 2, 100000000u32],
            vec![],
        ]);
        aux_test_union(vec![
            tests::sample_with_seed(100_000, 0.01, 1),
            tests::sample_with_seed(100_000, 0.05, 2),
            tests::sample_with_seed(100_000, 0.001, 3),
        ]);
    }

    fn test_aux_union_skip(docs_list: &[Vec<DocId>], skip_targets: Vec<DocId>) {
        let mut btree_set = BTreeSet::new();
        for docs in docs_list {
            btree_set.extend(docs.iter().cloned());
        }
        let docset_factory = || {
            let res: Box<dyn DocSet> = Box::new(Union::<_, DoNothingCombiner>::from(
                docs_list
                    .iter()
                    .map(|docs| docs.clone())
                    .map(VecDocSet::from)
                    .map(|docset| ConstScorer::new(docset, 1.0))
                    .collect::<Vec<_>>(),
            ));
            res
        };
        let mut docset = docset_factory();
        for el in btree_set {
            assert_eq!(el, docset.doc());
            docset.advance();
        }
        assert_eq!(docset.doc(), TERMINATED);
        test_skip_against_unoptimized(docset_factory, skip_targets);
    }

    #[test]
    fn test_union_skip_corner_case() {
        test_aux_union_skip(&[vec![165132, 167382], vec![25029, 25091]], vec![25029]);
    }

    #[test]
    fn test_union_skip_corner_case2() {
        test_aux_union_skip(
            &[vec![1u32, 1u32 + HORIZON], vec![2u32, 1000u32, 10_000u32]],
            vec![0u32, 1u32, 2u32, 3u32, 1u32 + HORIZON, 2u32 + HORIZON],
        );
    }

    #[test]
    fn test_union_skip_corner_case3() {
        let mut docset = Union::<_, DoNothingCombiner>::from(vec![
            ConstScorer::from(VecDocSet::from(vec![0u32, 5u32])),
            ConstScorer::from(VecDocSet::from(vec![1u32, 4u32])),
        ]);
        assert_eq!(docset.doc(), 0u32);
        assert_eq!(docset.seek(0u32), 0u32);
        assert_eq!(docset.seek(0u32), 0u32);
        assert_eq!(docset.doc(), 0u32)
    }

    #[test]
    fn test_union_skip_random() {
        test_aux_union_skip(
            &[
                vec![1, 2, 3, 7],
                vec![1, 3, 9, 10000],
                vec![1, 3, 8, 9, 100],
            ],
            vec![1, 2, 3, 5, 6, 7, 8, 100],
        );
        test_aux_union_skip(
            &[
                tests::sample_with_seed(100_000, 0.001, 1),
                tests::sample_with_seed(100_000, 0.002, 2),
                tests::sample_with_seed(100_000, 0.005, 3),
            ],
            tests::sample_with_seed(100_000, 0.01, 4),
        );
    }

    #[test]
    fn test_union_skip_specific() {
        test_aux_union_skip(
            &[
                vec![1, 2, 3, 7],
                vec![1, 3, 9, 10000],
                vec![1, 3, 8, 9, 100],
            ],
            vec![1, 2, 3, 7, 8, 9, 99, 100, 101, 500, 20000],
        );
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use crate::query::score_combiner::DoNothingCombiner;
    use crate::query::{ConstScorer, Union, VecDocSet};
    use crate::DocId;
    use crate::DocSet;
    use crate::{tests, TERMINATED};
    use test::Bencher;

    #[bench]
    fn bench_union_3_high(bench: &mut Bencher) {
        let union_docset: Vec<Vec<DocId>> = vec![
            tests::sample_with_seed(100_000, 0.1, 0),
            tests::sample_with_seed(100_000, 0.2, 1),
        ];
        bench.iter(|| {
            let mut v = Union::<_, DoNothingCombiner>::from(
                union_docset
                    .iter()
                    .map(|doc_ids| VecDocSet::from(doc_ids.clone()))
                    .map(|docset| ConstScorer::new(docset, 1.0))
                    .collect::<Vec<_>>(),
            );
            while v.doc() != TERMINATED {
                v.advance();
            }
        });
    }
    #[bench]
    fn bench_union_3_low(bench: &mut Bencher) {
        let union_docset: Vec<Vec<DocId>> = vec![
            tests::sample_with_seed(100_000, 0.01, 0),
            tests::sample_with_seed(100_000, 0.05, 1),
            tests::sample_with_seed(100_000, 0.001, 2),
        ];
        bench.iter(|| {
            let mut v = Union::<_, DoNothingCombiner>::from(
                union_docset
                    .iter()
                    .map(|doc_ids| VecDocSet::from(doc_ids.clone()))
                    .map(|docset| ConstScorer::new(docset, 1.0))
                    .collect::<Vec<_>>(),
            );
            while v.doc() != TERMINATED {
                v.advance();
            }
        });
    }
}
