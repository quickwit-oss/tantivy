use postings::DocSet;
use postings::SkipResult;
use common::TinySet;
use DocId;


const HORIZON_NUM_TINYBITSETS: usize = 1_024;
const HORIZON: usize = 64 * HORIZON_NUM_TINYBITSETS;

/// Creates a `DocSet` that iterator through the intersection of two `DocSet`s.
pub struct UnionDocSet<TDocSet: DocSet> {
    docsets: Vec<TDocSet>,
    bitsets: Box<[u64; HORIZON_NUM_TINYBITSETS]>,
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
            bitsets: Box::new([0u64; HORIZON_NUM_TINYBITSETS]),
            cursor: HORIZON_NUM_TINYBITSETS,
            offset: 0,
            doc: 0
        }
    }
}


fn refill<TDocSet: DocSet>(docsets: &mut Vec<TDocSet>, bitsets: &mut [u64; HORIZON_NUM_TINYBITSETS], min_doc: DocId) {
    docsets
        .drain_filter(|docset| {
            let horizon = min_doc + HORIZON_NUM_TINYBITSETS as u32;
            loop {
                let doc = docset.doc();
                if doc >= horizon {
                    return false;
                }
                // add this document
                let delta = doc - min_doc;
                bitsets[(delta / 64) as usize] |= 1 << (delta % 64);
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
}

impl<TDocSet: DocSet> DocSet for UnionDocSet<TDocSet> {

    fn advance(&mut self) -> bool {
        while self.cursor < HORIZON_NUM_TINYBITSETS {
            if let Some(val) = self.bitsets[self.cursor].pop_lowest() {
                self.doc = self.offset + val + (self.cursor as u32) * 64;
                return true;
            } else {
                self.cursor += 1;
            }
        }
        self.refill()
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        0u32
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let mut reached = false;
        self.docsets
            .drain_filter(|docset| {
                match docset.skip_next(target) {
                    SkipResult::End => true,
                    SkipResult::Reached => {
                        reached = true;
                        false
                    },
                    SkipResult::OverStep => false
                }
            });
        if self.docsets.is_empty() {
            SkipResult::End
        } else {
            if reached {
                SkipResult::Reached
            } else {
                SkipResult::OverStep
            }
        }
    }
}


#[cfg(test)]
mod tests {

    use super::UnionDocSet;
    use postings::VecPostings;
    use postings::DocSet;

    #[test]
    fn test_union() {
        let mut union = UnionDocSet::from(
            vec!(
                VecPostings::from(vec![1, 3333, 100000000u32]),
                VecPostings::from(vec![1,2, 100000000u32]),
                VecPostings::from(vec![1,2, 100000000u32]),
                VecPostings::from(vec![])
            )
        );
        let mut docsets = vec![];
        while union.advance() {
            docsets.push(union.doc());
        }
        assert_eq!(&docsets, &[1u32, 2u32, 3333u32, 100000000u32]);
    }

}