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
///
/// You combine other query types and their `Occur`ances into one `BooleanQuery`
///
/// ```rust
///#[macro_use]
///extern crate tantivy;
///use tantivy::collector::Count;
///use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
///use tantivy::schema::{IndexRecordOption, Schema, TEXT};
///use tantivy::Term;
///use tantivy::{Index, Result};
///
///fn main() -> Result<()> {
///    let mut schema_builder = Schema::builder();
///    let title = schema_builder.add_text_field("title", TEXT);
///    let body = schema_builder.add_text_field("body", TEXT);
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
///            body => "find me",
///        ));
///        index_writer.add_document(doc!(
///            title => "A Dairy Cow",
///            body => "i won't be found",
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
///    // TermQuery "diary" must and "girl" must not be present
///    let girl_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "girl"),
///        IndexRecordOption::Basic,
///    ));
///    let diary_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "diary"),
///        IndexRecordOption::Basic,
///    ));
///    let queries_with_occurs1 = vec![
///        (Occur::Must, diary_term_query.box_clone()),
///        (Occur::MustNot, girl_term_query),
///    ];
///    let query1 = BooleanQuery::from(queries_with_occurs1);
///    let count1 = searcher.search(&query1, &Count)?;
///    assert_eq!(count1, 1);
///
///    // BooleanQuery with 2 `TermQuery`s
///    // "title:diary OR title:cow"
///    let cow_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "cow"),
///        IndexRecordOption::Basic,
///    ));
///    let query2 = BooleanQuery::from(vec![
///        (Occur::Should, diary_term_query.box_clone()),
///        (Occur::Should, cow_term_query),
///    ]);
///    let count3 = searcher.search(&query2, &Count)?;
///    assert_eq!(count3, 4);
///
///    // BooleanQuery comprising of subqueries of different types:
///    // `TermQuery` and `PhraseQuery`
///    // "title:diary OR \"dairy cow\""
///    let phrase_query: Box<dyn Query> = Box::new(PhraseQuery::new(vec![
///        Term::from_field_text(title, "dairy"),
///        Term::from_field_text(title, "cow"),
///    ]));
///    let query4 = BooleanQuery::from(vec![
///        (Occur::Should, diary_term_query.box_clone()),
///        (Occur::Should, phrase_query),
///    ]);
///    let count4 = searcher.search(&query4, &Count)?;
///    assert_eq!(count4, 4);
///
///    // ("title:diary OR \"dairy cow\"") AND body:found
///    let body_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(body, "found"),
///        IndexRecordOption::Basic,
///    ));
///    let query5 = BooleanQuery::from(vec![(Occur::Must, body_query),
///                                         (Occur::Must, query4)]);
///    let count5 = searcher.search(&query5, &Count)?;
///    assert_eq!(count5, 1);
///    Ok(())
///}
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
