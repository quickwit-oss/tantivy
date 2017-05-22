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
            docsets: docsets,
            finished: false,
            doc: DocId::max_value(),
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
    fn size_hint(&self) -> usize {
        self.docsets
            .iter()
            .map(|docset| docset.size_hint())
            .min()
            .unwrap() // safe as docsets cannot be empty.
    }

    #[allow(never_loop)]
    fn advance(&mut self) -> bool {
        if self.finished {
            return false;
        }
        let (head_arr, tail) = self.docsets.split_at_mut(1);
        let head: &mut TDocSet = &mut head_arr[0];
        if !head.advance() {
            self.finished = true;
            return false;
        }
        let mut doc_candidate = head.doc();

        'outer: loop {

            for docset in tail.iter_mut() {
                match docset.skip_next(doc_candidate) {
                    SkipResult::Reached => {}
                    SkipResult::OverStep => {
                        doc_candidate = docset.doc();
                        match head.skip_next(doc_candidate) {
                            SkipResult::Reached => {}
                            SkipResult::End => {
                                self.finished = true;
                                return false;
                            }
                            SkipResult::OverStep => {
                                doc_candidate = head.doc();
                                continue 'outer;
                            }
                        }
                    }
                    SkipResult::End => {
                        self.finished = true;
                        return false;
                    }
                }
            }

            self.doc = doc_candidate;
            return true;
        }
    }

    fn doc(&self) -> DocId {
        self.doc
    }
}
