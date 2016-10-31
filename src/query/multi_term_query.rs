use Result;
use super::Weight;
use std::any::Any;
use schema::Term;
use query::Query;
use core::searcher::Searcher;
use core::SegmentReader;
use query::Scorer;
use query::occur::Occur;
use query::occur_filter::OccurFilter;
use query::term_query::{TermQuery, TermWeight, TermScorer};
use query::boolean_query::BooleanScorer;


struct MultiTermWeight {
    weights: Vec<TermWeight>,
    occur_filter: OccurFilter,
}


impl Weight for MultiTermWeight {
    
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let mut term_scorers: Vec<TermScorer<'a>> = Vec::new();
        for term_weight in &self.weights {
            let term_scorer_option = try!(term_weight.specialized_scorer(reader));
            if let Some(term_scorer) = term_scorer_option {
                term_scorers.push(term_scorer);
            }
        }
        Ok(box BooleanScorer::new(term_scorers, self.occur_filter.clone())) 
    }
}

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

}



impl Query for MultiTermQuery {
    
    fn as_any(&self) -> &Any {
        self
    }
    
    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
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
        Ok(
            Box::new(MultiTermWeight {
                weights: weights,
                occur_filter: occur_filter,
            })
        )
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