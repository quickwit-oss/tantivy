use postings::DocSet;
use postings::SkipResult;
use DocId;


/// Creates a `DocSet` that iterator through the intersection of two `DocSet`s.
pub struct IntersectionDocSet<TDocSet: DocSet> {
    docsets: Vec<TDocSet>,
    finished: bool,
    doc: DocId,
}

impl<TDocSet: DocSet> From<Vec<TDocSet>> for IntersectionDocSet<TDocSet> {
    fn from(mut docsets: Vec<TDocSet>) -> IntersectionDocSet<TDocSet> {
        assert!(docsets.len() >= 2);
        docsets.sort_by_key(|docset| docset.size_hint());
        IntersectionDocSet {
            docsets,
            finished: false,
            doc: 0u32,
        }
    }
}

impl<TDocSet: DocSet> IntersectionDocSet<TDocSet> {
    /// Returns an array to the underlying `DocSet`s of the intersection.
    /// These `DocSet` are in the same position as the `IntersectionDocSet`,
    /// so that user can access their `docfreq` and `positions`.
    pub fn docsets(&self) -> &[TDocSet] {
        &self.docsets[..]
    }
}

impl<TDocSet: DocSet> DocSet for IntersectionDocSet<TDocSet> {
    /// Returns the minimum `.size_hint()` of the intersected docsets.
    fn size_hint(&self) -> u32 {
        self.docsets
            .iter()
            .map(|docset| docset.size_hint())
            .min()
            .unwrap() // safe as docsets cannot be empty.
    }

    fn skip_next(&mut self, mut target: DocId) -> SkipResult {
        let mut overstep = false;
        for docset in &mut self.docsets {
            match docset.skip_next(target) {
                SkipResult::End => {
                    return SkipResult::End;
                }
                SkipResult::OverStep => {
                    overstep = true;
                    target = docset.doc();
                }
                SkipResult::Reached => {}
            }
        }
        if overstep {
            SkipResult::OverStep
        } else {
            SkipResult::Reached
        }
    }


    #[allow(never_loop)]
    fn advance(&mut self) -> bool {
        if self.finished {
            return false;
        }

        let mut candidate_doc = self.doc;
        let mut candidate_ord = self.docsets.len();

        'outer: loop {
            for (ord, docset) in self.docsets.iter_mut().enumerate() {
                if ord != candidate_ord {
                    // `candidate_ord` is already at the
                    // right position.
                    //
                    // Calling `skip_next` would advance this docset
                    // and miss it.
                    match docset.skip_next(candidate_doc) {
                        SkipResult::Reached => {}
                        SkipResult::OverStep => {
                            // this is not in the intersection,
                            // let's update our candidate.
                            candidate_doc = docset.doc();
                            candidate_ord = ord;
                            continue 'outer;
                        }
                        SkipResult::End => {
                            self.finished = true;
                            return false;
                        }
                    }
                }
            }

            self.doc = candidate_doc;
            return true;
        }
    }

    fn doc(&self) -> DocId {
        self.doc
    }
}

#[cfg(test)]
mod tests {

    use postings::{DocSet, IntersectionDocSet, VecPostings};

    #[test]
    fn test_intersection() {
        {
            let left = VecPostings::from(vec![1, 3, 9]);
            let right = VecPostings::from(vec![3, 4, 9, 18]);
            let mut intersection = IntersectionDocSet::from(vec![left, right]);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 3);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
        {
            let a = VecPostings::from(vec![1, 3, 9]);
            let b = VecPostings::from(vec![3, 4, 9, 18]);
            let c = VecPostings::from(vec![1, 5, 9, 111]);
            let mut intersection = IntersectionDocSet::from(vec![a, b, c]);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
    }

    #[test]
    fn test_intersection_zero() {
        let left = VecPostings::from(vec![0]);
        let right = VecPostings::from(vec![0]);
        let mut intersection = IntersectionDocSet::from(vec![left, right]);
        assert!(intersection.advance());
        assert_eq!(intersection.doc(), 0);
    }

    #[test]
    fn test_intersection_empty() {
        let a = VecPostings::from(vec![1, 3]);
        let b = VecPostings::from(vec![1, 4]);
        let c = VecPostings::from(vec![3, 9]);
        let mut intersection = IntersectionDocSet::from(vec![a, b, c]);
        assert!(!intersection.advance());
    }
}
