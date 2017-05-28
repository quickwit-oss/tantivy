use Result;
use std::any::Any;
use super::boolean_weight::BooleanWeight;
use query::Weight;
use Searcher;
use query::Query;
use schema::Term;
use query::TermQuery;
use postings::SegmentPostingsOption;
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
    subqueries: Vec<(Occur, Box<Query>)>,
}

impl From<Vec<(Occur, Box<Query>)>> for BooleanQuery {
    fn from(subqueries: Vec<(Occur, Box<Query>)>) -> BooleanQuery {
        BooleanQuery { subqueries: subqueries }
    }
}

impl Query for BooleanQuery {
    fn as_any(&self) -> &Any {
        self
    }

    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
        let sub_weights = try!(self.subqueries
                     .iter()
                     .map(|&(ref _occur, ref subquery)| subquery.weight(searcher))
                     .collect());
        let occurs: Vec<Occur> = self.subqueries
            .iter()
            .map(|&(ref occur, ref _subquery)| *occur)
            .collect();
        let filter = OccurFilter::new(&occurs);
        Ok(box BooleanWeight::new(sub_weights, filter))
    }
}

impl BooleanQuery {
    /// Helper method to create a boolean query matching a given list of terms.
    /// The resulting query is a disjunction of the terms.
    pub fn new_multiterms_query(terms: Vec<Term>) -> BooleanQuery {
        let occur_term_queries: Vec<(Occur, Box<Query>)> = terms
            .into_iter()
            .map(|term| {
                     let term_query: Box<Query> = box TermQuery::new(term,
                                                                     SegmentPostingsOption::Freq);
                     (Occur::Should, term_query)
                 })
            .collect();
        BooleanQuery::from(occur_term_queries)
    }
}
