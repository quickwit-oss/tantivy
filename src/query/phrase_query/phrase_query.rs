use schema::Term;
use query::Query;
use core::searcher::Searcher;
use query::Occur;
use super::PhraseWeight;
use query::MultiTermQuery;
use std::any::Any;
use query::Weight;
use Result;


#[derive(Debug)]
pub struct PhraseQuery {
    all_terms_query: MultiTermQuery,    
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
        let multi_term_weight = self.all_terms_query.specialized_weight(searcher);
        Ok(box PhraseWeight::from(multi_term_weight))
    }

}

impl PhraseQuery {
    pub fn new(terms: Vec<Term>) -> PhraseQuery {
        assert!(terms.len() > 1);
        let occur_terms: Vec<(Occur, Term)> = terms.into_iter()
            .map(|term| (Occur::Must, term))
            .collect();
        PhraseQuery {
            all_terms_query: MultiTermQuery::from(occur_terms),
        }
    }
}
