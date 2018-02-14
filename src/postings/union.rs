use postings::DocSet;
use postings::SkipResult;
use common::TinySet;
use std::cmp::Ordering;
use DocId;


const HORIZON_NUM_TINYBITSETS: usize = 2048;
const HORIZON: u32 = 64u32 * HORIZON_NUM_TINYBITSETS as u32;

/// Creates a `DocSet` that iterator through the intersection of two `DocSet`s.
pub struct UnionDocSet<TDocSet: DocSet> {
    docsets: Vec<TDocSet>,
    bitsets: Box<[TinySet; HORIZON_NUM_TINYBITSETS]>,
    cursor: usize,
    offset: DocId,
    doc: DocId,
}

impl<TDocSet: DocSet> From<Vec<TDocSet>> for UnionDocSet<TDocSet> {
    fn from(docsets: Vec<TDocSet>) -> UnionDocSet<TDocSet> {
        let non_empty_docsets: Vec<TDocSet> =
            docsets
                .into_iter()
                .flat_map(|mut docset| {
                    if docset.advance() {
                        Some(docset)
                    } else {
                        None
                    }
                })
                .collect();
        UnionDocSet {
            docsets: non_empty_docsets,
            bitsets: Box::new([TinySet::empty(); HORIZON_NUM_TINYBITSETS]),
            cursor: HORIZON_NUM_TINYBITSETS,
            offset: 0,
            doc: 0
        }
    }
}


fn refill<TDocSet: DocSet>(docsets: &mut Vec<TDocSet>, bitsets: &mut [TinySet; HORIZON_NUM_TINYBITSETS], min_doc: DocId) {
    docsets
        .drain_filter(|docset| {
            let horizon = min_doc + HORIZON as u32;
            loop {
                let doc = docset.doc();
                if doc >= horizon {
                    return false;
                }
                // add this document
                let delta = doc - min_doc;
                bitsets[(delta / 64) as usize].insert_mut(delta % 64u32);
                if !docset.advance() {
                    // remove the docset, it has been entirely consumed.
                    return true;
                }
            }
        });
}

impl<TDocSet: DocSet> UnionDocSet<TDocSet> {
    fn refill(&mut self) -> bool {
        if let Some(min_doc) = self.docsets
            .iter_mut()
            .map(|docset| docset.doc())
            .min() {
            self.offset = min_doc;
            self.cursor = 0;
            refill(&mut self.docsets, &mut *self.bitsets, min_doc);
            self.advance();
            true
        } else {
            false
        }
    }

    fn advance_buffered(&mut self) -> bool {
        while self.cursor < HORIZON_NUM_TINYBITSETS {
            if let Some(val) = self.bitsets[self.cursor].pop_lowest() {
                self.doc = self.offset + val + (self.cursor as u32) * 64;
                return true;
            } else {
                self.cursor += 1;
            }
        }
        false
    }
}

impl<TDocSet: DocSet> DocSet for UnionDocSet<TDocSet> {

    fn advance(&mut self) -> bool {
        if self.advance_buffered() {
            return true;
        }
        self.refill()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        if !self.advance() {
            return SkipResult::End;
        }
        match self.doc.cmp(&target) {
            Ordering::Equal => {
                return SkipResult::Reached;
            }
            Ordering::Greater => {
                return SkipResult::OverStep;
            }
            Ordering::Less => {}
        }
        let gap = target - self.offset;
        if gap < HORIZON {
            // Our value is within the buffered horizon.

            // Skipping to  corresponding bucket.
            let new_cursor = gap as usize / 64;
            for obsolete_tinyset in &mut self.bitsets[self.cursor..new_cursor] {
                *obsolete_tinyset = TinySet::empty();
            }
            self.cursor = new_cursor;

            // Advancing until we reach the end of the bucket
            // or we reach a doc greater or equal to the target.
            while self.advance() {
                match self.doc().cmp(&target) {
                    Ordering::Equal => {
                        return SkipResult::Reached;
                    }
                    Ordering::Greater => {
                        return SkipResult::OverStep;
                    }
                    Ordering::Less => {}
                }
            }
            SkipResult::End
        } else {
            // clear the buffered info.
            for obsolete_tinyset in self.bitsets.iter_mut() {
                *obsolete_tinyset = TinySet::empty();
            }

            // The target is outside of the buffered horizon.
            // advance all docsets to a doc >= to the target.
            self.docsets
                .drain_filter(|docset| {
                    match docset.doc().cmp(&target) {
                        Ordering::Less => {
                            match docset.skip_next(target) {
                                SkipResult::End => true,
                                SkipResult::Reached | SkipResult::OverStep => false
                            }
                        }
                        Ordering::Equal | Ordering::Greater => {
                            false
                        }
                    }
                });

            // at this point all of the docsets
            // are positionned on a doc >= to the target.
            if self.refill() {
                if self.doc() == target {
                    SkipResult::Reached
                } else {
                    debug_assert!(self.doc() > target);
                    SkipResult::OverStep
                }
            } else {
                SkipResult::End
            }
        }

    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        0u32
    }
}


#[cfg(test)]
mod tests {

    use super::UnionDocSet;
    use postings::{VecPostings, DocSet};
    use tests;
    use test::Bencher;
    use DocId;
    use std::collections::BTreeSet;
    use super::HORIZON;
    use postings::SkipResult;
    use postings::tests::test_skip_against_unoptimized;


    fn aux_test_union(vals: Vec<Vec<u32>>) {
        use std::collections::BTreeSet;
        let mut val_set: BTreeSet<u32> = BTreeSet::new();
        for vs in &vals {
            for &v in vs {
                val_set.insert(v);
            }
        }
        let union_vals: Vec<u32> = val_set
            .into_iter()
            .collect();
        let mut union_expected = VecPostings::from(union_vals);

        let mut union = UnionDocSet::from(
            vals.into_iter()
                .map(VecPostings::from)
                .collect::<Vec<VecPostings>>()
        );
        while union.advance() {
            assert!(union_expected.advance());
            assert_eq!(union_expected.doc(), union.doc());
        }
        assert!(!union_expected.advance());
    }

    #[test]
    fn test_union() {
        aux_test_union(
            vec![
                vec![1, 3333, 100000000u32],
                vec![1,2, 100000000u32],
                vec![1,2, 100000000u32],
                vec![]
            ]
        );
        aux_test_union(
            vec![
                vec![1, 3333, 100000000u32],
                vec![1,2, 100000000u32],
                vec![1,2, 100000000u32],
                vec![]
            ]
        );
        aux_test_union(vec![
            tests::sample_with_seed(100_000, 0.01, 1),
            tests::sample_with_seed(100_000, 0.05, 2),
            tests::sample_with_seed(100_000, 0.001, 3)
        ]);
    }


    fn test_aux_union_skip(docs_list: &[Vec<DocId>], skip_targets: Vec<DocId>) {
        let mut btree_set = BTreeSet::new();
        for docs in docs_list {
            for &doc in docs.iter() {
                btree_set.insert(doc);
            }
        }
        let docset_factory = || {
            let res: Box<DocSet> = box UnionDocSet::from(
                docs_list
                    .iter()
                    .map(|docs| docs.clone())
                    .map(VecPostings::from)
                    .collect::<Vec<VecPostings>>()
            );
            res
        };
        let mut docset = docset_factory();
        for el in btree_set {
            assert!(docset.advance());
            assert_eq!(el, docset.doc());
        }
        assert!(!docset.advance());
        test_skip_against_unoptimized(docset_factory, skip_targets);
    }


    #[test]
    fn test_union_skip_corner_case() {
        test_aux_union_skip(
            &[vec![165132, 167382], vec![25029, 25091]],
            vec![25029],
        );
    }

    #[test]
    fn test_union_skip_corner_case2() {
        test_aux_union_skip(
            &[
                vec![1u32, 1u32 + HORIZON],
                vec![2u32, 1000u32, 10_000u32]
            ], vec![0u32, 1u32, 2u32, 3u32, 1u32 + HORIZON, 2u32 + HORIZON]);
    }

    #[test]
    fn test_union_skip_corner_case3() {
        let mut docset = UnionDocSet::from(vec![
                VecPostings::from(vec![0u32, 5u32]),
                VecPostings::from(vec![1u32, 4u32]),
        ]);
        assert!(docset.advance());
        assert_eq!(docset.doc(), 0u32);
        assert_eq!(docset.skip_next(0u32), SkipResult::OverStep);
        assert_eq!(docset.doc(), 1u32)
    }

    #[test]
    fn test_union_skip_random() {
        test_aux_union_skip(&[
            vec![1,2,3,7],
            vec![1,3,9,10000],
            vec![1,3,8,9,100]
        ], vec![1,2,3,5,6,7,8,100]);
        test_aux_union_skip(&[
            tests::sample_with_seed(100_000, 0.001, 1),
            tests::sample_with_seed(100_000, 0.002, 2),
            tests::sample_with_seed(100_000, 0.005, 3)
        ],  tests::sample_with_seed(100_000, 0.01, 4));
    }

    #[test]
    fn test_union_skip_specific() {
        test_aux_union_skip(&[
            vec![1,2,3,7],
            vec![1,3,9,10000],
            vec![1,3,8,9,100]
        ], vec![1,2,3,7,8,9,99,100,101,500,20000]);
    }

    #[bench]
    fn bench_union_3_high(bench: &mut Bencher) {
        let union_docset: Vec<Vec<DocId>>  = vec![
            tests::sample_with_seed(100_000, 0.1, 0),
            tests::sample_with_seed(100_000, 0.2, 1),
        ];
        bench.iter(|| {
            let mut v = UnionDocSet::from(union_docset.iter()
                .map(|doc_ids| VecPostings::from(doc_ids.clone()))
                .collect::<Vec<VecPostings>>());
            while v.advance() {};
        });
    }
    #[bench]
    fn bench_union_3_low(bench: &mut Bencher) {
        let union_docset: Vec<Vec<DocId>>  = vec![
            tests::sample_with_seed(100_000, 0.01, 0),
            tests::sample_with_seed(100_000, 0.05, 1),
            tests::sample_with_seed(100_000, 0.001, 2)
        ];
        bench.iter(|| {
            let mut v = UnionDocSet::from(union_docset.iter()
                .map(|doc_ids| VecPostings::from(doc_ids.clone()))
                .collect::<Vec<VecPostings>>());
            while v.advance() {};
        });
    }

}