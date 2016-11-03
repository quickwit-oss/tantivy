use postings::DocSet;
use postings::SkipResult;
use std::cmp::Ordering;
use DocId;

// TODO Find a way to specialize `IntersectionDocSet`

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


impl<TDocSet: DocSet> DocSet for IntersectionDocSet<TDocSet> {
    
    fn advance(&mut self,) -> bool {
        if self.finished {
            return false;
        }

        'outter: loop {
            let doc_candidate = {
                let mut first_docset = &mut self.docsets[0];
                if !first_docset.advance() {
                    self.finished = true;
                    return false;
                }
                first_docset.doc()
            };
            for docset_ord in 1..self.docsets.len() {
                let docset: &mut TDocSet = &mut self.docsets[docset_ord];
                match docset.skip_next(doc_candidate) {
                    SkipResult::End => {
                        self.finished = true;
                        return false;
                    }
                    SkipResult::OverStep => {
                        continue 'outter;
                    },
                    SkipResult::Reached => {}
                }
            }
            self.doc = doc_candidate;
            return true;
        }
    }
    
    fn doc(&self,) -> DocId {
        self.doc
    }
}
