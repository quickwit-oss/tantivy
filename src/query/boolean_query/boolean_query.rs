use Result;
use std::any::Any;
use super::boolean_weight::BooleanWeight;
use super::BooleanClause;
use query::Weight;
use Searcher;
use query::Query;
use query::Occur;
use query::OccurFilter;

/// The boolean query combines a set of queries
///
/// The documents matched by the boolean query are
/// those which
/// * match all of the sub queries associated with the 
/// `Must` occurence
/// * match none of the sub queries associated with the 
/// `MustNot` occurence.
/// * match at least one of the subqueries that is not 
/// a `MustNot` occurence.
#[derive(Debug)]
pub struct BooleanQuery {
    clauses: Vec<BooleanClause>,
}

impl From<Vec<BooleanClause>> for BooleanQuery {
    fn from(clauses: Vec<BooleanClause>) -> BooleanQuery {
        BooleanQuery {
            clauses: clauses,
        }
    } 
}

impl Query for BooleanQuery {
    
    fn as_any(&self) -> &Any {
        self
    }

    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
        let sub_weights = try!(self.clauses
            .iter()
            .map(|clause| clause.query.weight(searcher))
            .collect()
        );
        let occurs: Vec<Occur> = self.clauses
            .iter()
            .map(|clause| clause.occur)
            .collect();
        let filter = OccurFilter::new(&occurs);
        Ok(box BooleanWeight::new(sub_weights, filter))
    }
    
}