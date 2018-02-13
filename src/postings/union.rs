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
    use postings::{VecPostings, SkipResult, DocSet};
    use tests;
    use test::Bencher;
    use DocId;
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
            tests::generate_array(100_000, 0.01),
            tests::generate_array(100_000, 0.05),
            tests::generate_array(100_000, 0.001)
        ]);
    }


    #[test]
    fn test_union_skip_corner_case() {
        let mut union_docset = UnionDocSet::from(vec![
            VecPostings::from(vec![165132, 167382]),
            VecPostings::from(vec![25029, 25091]),
        ]);
        assert_eq!(union_docset.skip_next(25802), SkipResult::OverStep);
        assert_eq!(union_docset.doc(), 165132);
        assert!(union_docset.advance());
        assert_eq!(union_docset.doc(), 167382);
        assert!(!union_docset.advance());
    }

    #[test]
    fn test_union_skip() {
        test_skip_against_unoptimized(|| {
            box UnionDocSet::from(vec![
                VecPostings::from(vec![1,2,3,7]),
                VecPostings::from(vec![1,3,9,10000]),
                VecPostings::from(vec![1,3,8,9,100])
            ])
        }, vec![1,2,3,5,6,7,8,100]);
        test_skip_against_unoptimized(|| {
            box UnionDocSet::from(vec![
                VecPostings::from(tests::generate_array(1_000, 0.001)[160..165].to_owned()),
                VecPostings::from(tests::generate_array(1_000, 0.005)[100..105].to_owned()),
                VecPostings::from(tests::generate_array(10_000, 0.001))
            ])
        },  tests::generate_array(100, 0.001));
    }

    #[test]
    fn test_union_skip_specific() {
        let mut docset = UnionDocSet::from(vec![
            VecPostings::from(vec![1,2,3,7]),
            VecPostings::from(vec![1,3,9,10000]),
            VecPostings::from(vec![1,3,8,9,100])
        ]);
        assert_eq!(docset.skip_next(1), SkipResult::Reached);
    }

    #[bench]
    fn bench_union_3_high(bench: &mut Bencher) {
        let union_docset: Vec<Vec<DocId>>  = vec![
            tests::generate_array(100_000, 0.1),
            tests::generate_array(100_000, 0.2),
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
            tests::generate_array(100_000, 0.01),
            tests::generate_array(100_000, 0.05),
            tests::generate_array(100_000, 0.001)
        ];
        bench.iter(|| {
            let mut v = UnionDocSet::from(union_docset.iter()
                .map(|doc_ids| VecPostings::from(doc_ids.clone()))
                .collect::<Vec<VecPostings>>());
            while v.advance() {};
        });
    }

}