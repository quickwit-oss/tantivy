use super::boolean_weight::BooleanWeight;
use crate::query::Occur;
use crate::query::Query;
use crate::query::TermQuery;
use crate::query::Weight;
use crate::schema::IndexRecordOption;
use crate::schema::Term;
use crate::Result;
use crate::Searcher;
use std::collections::BTreeSet;

/// The boolean query returns a set of documents
/// that matches the Boolean combination of constituent subqueries.
///
/// The documents matched by the boolean query are
/// those which
/// * match all of the sub queries associated with the
/// `Must` occurence
/// * match none of the sub queries associated with the
/// `MustNot` occurence.
/// * match at least one of the subqueries that is not
/// a `MustNot` occurence.
///
/// ```rust
///#[macro_use]
///extern crate tantivy;
///use tantivy::collector::Count;
///use tantivy::query::QueryParser;
///use tantivy::schema::{Schema, TEXT};
///use tantivy::{Index, Result};
///
///fn main() -> Result<()> {
///    let mut schema_builder = Schema::builder();
///    let title = schema_builder.add_text_field("title", TEXT);
///    let schema = schema_builder.build();
///    let index = Index::create_in_ram(schema);
///    {
///        let mut index_writer = index.writer(3_000_000)?;
///        index_writer.add_document(doc!(
///            title => "The Name of the Wind",
///        ));
///        index_writer.add_document(doc!(
///            title => "The Diary of Muadib",
///        ));
///        index_writer.add_document(doc!(
///            title => "A Dairy Cow",
///        ));
///        index_writer.add_document(doc!(
///            title => "The Diary of a Young Girl",
///        ));
///        index_writer.commit().unwrap();
///    }
///
///    let reader = index.reader()?;
///    let searcher = reader.searcher();
///
///    let query_parser = QueryParser::for_index(&index, vec![title]);
///
///    // TermQuery "diary" must and "girl" mustnot be present
///    let query1 = query_parser.parse_query("+diary -girl")?;
///    let count1 = searcher.search(&query1, &Count)?;
///    assert_eq!(count1, 1);
///
///    // "diary" must occur
///    let query2 = query_parser.parse_query("+diary")?;
///    let count2 = searcher.search(&query2, &Count)?;
///    assert_eq!(count2, 2);
///
///    // BooleanQuery with 2 `TermQuery`s
///    let query3 = query_parser.parse_query("title:diary OR title:cow")?;
///    let count3 = searcher.search(&query3, &Count)?;
///    assert_eq!(count3, 3);
///
///    // BooleanQuery comprising of subqueries of different types:
///    // `TermQuery` and `PhraseQuery`
///    let query4 = query_parser.parse_query("title:diary OR \"dairy cow\"")?;
///    let count4 = searcher.search(&query4, &Count)?;
///    assert_eq!(count4, 3);
///    Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct BooleanQuery {
    subqueries: Vec<(Occur, Box<dyn Query>)>,
}

impl Clone for BooleanQuery {
    fn clone(&self) -> Self {
        self.subqueries
            .iter()
            .map(|(occur, subquery)| (*occur, subquery.box_clone()))
            .collect::<Vec<_>>()
            .into()
    }
}

impl From<Vec<(Occur, Box<dyn Query>)>> for BooleanQuery {
    fn from(subqueries: Vec<(Occur, Box<dyn Query>)>) -> BooleanQuery {
        BooleanQuery { subqueries }
    }
}

impl Query for BooleanQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        let sub_weights = self
            .subqueries
            .iter()
            .map(|&(ref occur, ref subquery)| {
                Ok((*occur, subquery.weight(searcher, scoring_enabled)?))
            })
            .collect::<Result<_>>()?;
        Ok(Box::new(BooleanWeight::new(sub_weights, scoring_enabled)))
    }

    fn query_terms(&self, term_set: &mut BTreeSet<Term>) {
        for (_occur, subquery) in &self.subqueries {
            subquery.query_terms(term_set);
        }
    }
}

impl BooleanQuery {
    /// Helper method to create a boolean query matching a given list of terms.
    /// The resulting query is a disjunction of the terms.
    #[cfg(test)]
    pub fn new_multiterms_query(terms: Vec<Term>) -> BooleanQuery {
        let occur_term_queries: Vec<(Occur, Box<dyn Query>)> = terms
            .into_iter()
            .map(|term| {
                let term_query: Box<dyn Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Should, term_query)
            })
            .collect();
        BooleanQuery::from(occur_term_queries)
    }

    /// Deconstructed view of the clauses making up this query.
    pub fn clauses(&self) -> &[(Occur, Box<dyn Query>)] {
        &self.subqueries[..]
    }
}
