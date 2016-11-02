use Result;
use query::Weight;
use std::any::Any;
use schema::Term;
use query::MultiTermWeight;
use query::Query;
use core::searcher::Searcher;
use query::occur::Occur;
use query::occur_filter::OccurFilter;
use query::term_query::TermQuery;


/// Query involving one or more terms.

#[derive(Eq, Clone, PartialEq, Debug)]
pub struct MultiTermQuery {  
    // TODO need a better Debug   
    occur_terms: Vec<(Occur, Term)>
}

impl MultiTermQuery {
    
    /// Accessor for the number of terms
    pub fn num_terms(&self,) -> usize {
        self.occur_terms.len()
    }

    pub fn specialized_weight(&self, searcher: &Searcher) -> MultiTermWeight {
        let term_queries: Vec<TermQuery> = self.occur_terms
            .iter()
            .map(|&(_, ref term)| TermQuery::from(term.clone()))
            .collect();
        let occurs: Vec<Occur> = self.occur_terms
            .iter()
            .map(|&(occur, _) | occur.clone())
            .collect();
        let occur_filter = OccurFilter::new(&occurs);
        let weights = term_queries.iter()
            .map(|term_query| term_query.specialized_weight(searcher))
            .collect();
        MultiTermWeight {
            weights: weights,
            occur_filter: occur_filter,
        }
    }
}



impl Query for MultiTermQuery {
    
    fn as_any(&self) -> &Any {
        self
    }
    
    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
        Ok(box self.specialized_weight(searcher))
    }
}


impl From<Vec<(Occur, Term)>> for MultiTermQuery {
    fn from(occur_terms: Vec<(Occur, Term)>) -> MultiTermQuery {
        MultiTermQuery {
            occur_terms: occur_terms
        }
    }
}

impl From<Vec<Term>> for MultiTermQuery {
    fn from(terms: Vec<Term>) -> MultiTermQuery {
        let should_terms: Vec<(Occur, Term)> = terms
            .into_iter()
            .map(|term| (Occur::Should, term))
            .collect();
        MultiTermQuery::from(should_terms)
    }
}