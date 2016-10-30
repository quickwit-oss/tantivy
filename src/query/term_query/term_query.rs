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

    fn weight(&self, _searcher: &Searcher) -> Result<Box<Weight>> {
        Ok(box TermWeight {
            term: self.term.clone()
        })
    }
}
