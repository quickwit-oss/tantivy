use Term;
use Result;
use super::term_weight::TermWeight;
use query::Query;
use query::Weight;
use Searcher;
use std::any::Any;

#[derive(Debug)]
pub struct TermQuery {
    term: Term,
}

impl TermQuery {
    pub fn specialized_weight(&self, searcher: &Searcher) -> TermWeight {
        let doc_freq = searcher.doc_freq(&self.term);
        TermWeight {
            doc_freq: doc_freq,
            term: self.term.clone()
        }
    }
}

impl From<Term> for TermQuery {
    fn from(term: Term) -> TermQuery {
        TermQuery {
            term: term
        }
    }
}

impl Query for TermQuery {
    fn as_any(&self) -> &Any {
        self
    }

    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
        Ok(box self.specialized_weight(searcher))
    }
    
}
