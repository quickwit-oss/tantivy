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
        let doc_freq = searcher.doc_freq(&self.term);
        Ok(box TermWeight {
            doc_freq: doc_freq,
            term: self.term.clone()
        })
    }
}
