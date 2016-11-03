use schema::Term;
use query::Query;
use core::searcher::Searcher;
use super::PhraseWeight;
use std::any::Any;
use query::Weight;
use Result;


#[derive(Debug)]
pub struct PhraseQuery {
    phrase_terms: Vec<Term>,    
}

impl Query for PhraseQuery {
     

    /// Used to make it possible to cast Box<Query>
    /// into a specific type. This is mostly useful for unit tests.
    fn as_any(&self) -> &Any {
        self
    }

    /// Create the weight associated to a query.
    ///
    /// See [Weight](./trait.Weight.html).
    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
        Ok(box PhraseWeight::from(self.phrase_terms.clone()))
    }

}


impl From<Vec<Term>> for PhraseQuery {
    fn from(phrase_terms: Vec<Term>) -> PhraseQuery {
        assert!(phrase_terms.len() > 1);
        PhraseQuery {
            phrase_terms: phrase_terms,
        }
    }
}
