use Result;
use std::any::Any;
use super::boolean_weight::BooleanWeight;
use query::Weight;
use Searcher;
use query::Query;
use schema::Term;
use query::TermQuery;
use schema::IndexRecordOption;
use query::Occur;

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
    scoring_disabled: bool
}

impl From<Vec<(Occur, Box<Query>)>> for BooleanQuery {
    fn from(subqueries: Vec<(Occur, Box<Query>)>) -> BooleanQuery {
        BooleanQuery {
            subqueries,
            scoring_disabled: false
        }
    }
}

impl Query for BooleanQuery {
    fn as_any(&self) -> &Any {
        self
    }

    fn disable_scoring(&mut self) {
        self.scoring_disabled = true;
        for &mut (_, ref mut subquery) in &mut self.subqueries {
            subquery.disable_scoring();
        }
    }

    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
        let sub_weights = self.subqueries
            .iter()
            .map(|&(ref occur, ref subquery)| {
                Ok((*occur, subquery.weight(searcher)?))
            })
            .collect::<Result<_>>()?;
        Ok(box BooleanWeight::new(sub_weights, self.scoring_disabled))
    }
}

impl BooleanQuery {
    /// Helper method to create a boolean query matching a given list of terms.
    /// The resulting query is a disjunction of the terms.
    pub fn new_multiterms_query(terms: Vec<Term>) -> BooleanQuery {
        let occur_term_queries: Vec<(Occur, Box<Query>)> = terms
            .into_iter()
            .map(|term| {
                let term_query: Box<Query> = box TermQuery::new(term, IndexRecordOption::WithFreqs);
                (Occur::Should, term_query)
            })
            .collect();
        BooleanQuery::from(occur_term_queries)
    }
}
