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
    fn from(docsets: Vec<TDocSet>) -> IntersectionDocSet<TDocSet> {
        assert!(docsets.len() >= 2);
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
    fn advance(&mut self) -> bool {
        if self.finished {
            return false;
        }
        let num_docsets = self.docsets.len();
        let mut count_matching = 0;
        let mut doc_candidate = 0;
        let mut ord = 0;
        loop {
            let mut doc_set = &mut self.docsets[ord];
            match doc_set.skip_next(doc_candidate) {
                SkipResult::Reached => {
                    count_matching += 1;
                    if count_matching == num_docsets {
                        self.doc = doc_candidate;
                        return true;
                    }
                }
                SkipResult::End => {
                    self.finished = true;
                    return false;
                }
                SkipResult::OverStep => {
                    count_matching = 1;
                    doc_candidate = doc_set.doc();
                }
            }
            ord += 1;
            if ord == num_docsets {
                ord = 0;
            }
        }
    }

    fn doc(&self) -> DocId {
        self.doc
    }
}
