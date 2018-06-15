use super::boolean_weight::BooleanWeight;
use query::Occur;
use query::Query;
use query::TermQuery;
use query::Weight;
use schema::IndexRecordOption;
use schema::Term;
use Result;
use Searcher;

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

impl Clone for BooleanQuery {
    fn clone(&self) -> Self {
        self.subqueries
            .iter()
            .map(|(x, y)| (x.clone(), y.box_clone()))
            .collect::<Vec<_>>()
            .into()
    }
}

impl From<Vec<(Occur, Box<Query>)>> for BooleanQuery {
    fn from(subqueries: Vec<(Occur, Box<Query>)>) -> BooleanQuery {
        BooleanQuery { subqueries }
    }
}

impl Query for BooleanQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<Weight>> {
        let sub_weights = self.subqueries
            .iter()
            .map(|&(ref occur, ref subquery)| {
                Ok((*occur, subquery.weight(searcher, scoring_enabled)?))
            })
            .collect::<Result<_>>()?;
        Ok(Box::new(BooleanWeight::new(sub_weights, scoring_enabled)))
    }
}

impl BooleanQuery {
    /// Helper method to create a boolean query matching a given list of terms.
    /// The resulting query is a disjunction of the terms.
    pub fn new_multiterms_query(terms: Vec<Term>) -> BooleanQuery {
        let occur_term_queries: Vec<(Occur, Box<Query>)> = terms
            .into_iter()
            .map(|term| {
                let term_query: Box<Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Should, term_query)
            })
            .collect();
        BooleanQuery::from(occur_term_queries)
    }

    /// Deconstructed view of the clauses making up this query.
    pub fn clauses(&self) -> &[(Occur, Box<Query>)] {
        &self.subqueries[..]
    }
}
