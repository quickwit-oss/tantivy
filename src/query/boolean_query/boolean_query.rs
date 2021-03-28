use super::boolean_weight::BooleanWeight;
use crate::query::Occur;
use crate::query::Query;
use crate::query::TermQuery;
use crate::query::Weight;
use crate::schema::IndexRecordOption;
use crate::schema::Term;
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
/// You can combine other query types and their `Occur`ances into one `BooleanQuery`
///
/// ```rust
///use tantivy::collector::Count;
///use tantivy::doc;
///use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
///use tantivy::schema::{IndexRecordOption, Schema, TEXT};
///use tantivy::Term;
///use tantivy::Index;
///
///fn main() -> tantivy::Result<()> {
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
///            body => "hidden",
///        ));
///        index_writer.add_document(doc!(
///            title => "A Dairy Cow",
///            body => "found",
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
///    // Make TermQuery's for "girl" and "diary" in the title
///    let girl_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "girl"),
///        IndexRecordOption::Basic,
///    ));
///    let diary_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "diary"),
///        IndexRecordOption::Basic,
///    ));
///    // A TermQuery with "found" in the body
///    let body_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(body, "found"),
///        IndexRecordOption::Basic,
///    ));
///    // TermQuery "diary" must and "girl" must not be present
///    let queries_with_occurs1 = vec![
///        (Occur::Must, diary_term_query.box_clone()),
///        (Occur::MustNot, girl_term_query),
///    ];
///    // Make a BooleanQuery equivalent to
///    // title:+diary title:-girl
///    let diary_must_and_girl_mustnot = BooleanQuery::new(queries_with_occurs1);
///    let count1 = searcher.search(&diary_must_and_girl_mustnot, &Count)?;
///    assert_eq!(count1, 1);
///
///    // TermQuery for "cow" in the title
///    let cow_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "cow"),
///        IndexRecordOption::Basic,
///    ));
///    // "title:diary OR title:cow"
///    let title_diary_or_cow = BooleanQuery::new(vec![
///        (Occur::Should, diary_term_query.box_clone()),
///        (Occur::Should, cow_term_query),
///    ]);
///    let count2 = searcher.search(&title_diary_or_cow, &Count)?;
///    assert_eq!(count2, 4);
///
///    // Make a `PhraseQuery` from a vector of `Term`s
///    let phrase_query: Box<dyn Query> = Box::new(PhraseQuery::new(vec![
///        Term::from_field_text(title, "dairy"),
///        Term::from_field_text(title, "cow"),
///    ]));
///    // You can combine subqueries of different types into 1 BooleanQuery:
///    // `TermQuery` and `PhraseQuery`
///    // "title:diary OR "dairy cow"
///    let term_of_phrase_query = BooleanQuery::new(vec![
///        (Occur::Should, diary_term_query.box_clone()),
///        (Occur::Should, phrase_query.box_clone()),
///    ]);
///    let count3 = searcher.search(&term_of_phrase_query, &Count)?;
///    assert_eq!(count3, 4);
///
///    // You can nest one BooleanQuery inside another
///    // body:found AND ("title:diary OR "dairy cow")
///    let nested_query = BooleanQuery::new(vec![
///        (Occur::Must, body_term_query),
///        (Occur::Must, Box::new(term_of_phrase_query))
///    ]);
///    let count4 = searcher.search(&nested_query, &Count)?;
///    assert_eq!(count4, 1);
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
        BooleanQuery::new(subqueries)
    }
}

impl Query for BooleanQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> crate::Result<Box<dyn Weight>> {
        let sub_weights = self
            .subqueries
            .iter()
            .map(|&(ref occur, ref subquery)| {
                Ok((*occur, subquery.weight(searcher, scoring_enabled)?))
            })
            .collect::<crate::Result<_>>()?;
        Ok(Box::new(BooleanWeight::new(sub_weights, scoring_enabled)))
    }

    fn query_terms(&self, term_set: &mut BTreeSet<Term>) {
        for (_occur, subquery) in &self.subqueries {
            subquery.query_terms(term_set);
        }
    }
}

impl BooleanQuery {
    /// Creates a new boolean query.
    pub fn new(subqueries: Vec<(Occur, Box<dyn Query>)>) -> BooleanQuery {
        BooleanQuery { subqueries }
    }

    /// Returns the intersection of the queries.
    pub fn intersection(queries: Vec<Box<dyn Query>>) -> BooleanQuery {
        let subqueries = queries.into_iter().map(|s| (Occur::Must, s)).collect();
        BooleanQuery::new(subqueries)
    }

    /// Returns the union of the queries.
    pub fn union(queries: Vec<Box<dyn Query>>) -> BooleanQuery {
        let subqueries = queries.into_iter().map(|s| (Occur::Should, s)).collect();
        BooleanQuery::new(subqueries)
    }

    /// Helper method to create a boolean query matching a given list of terms.
    /// The resulting query is a disjunction of the terms.
    pub fn new_multiterms_query(terms: Vec<Term>) -> BooleanQuery {
        let occur_term_queries: Vec<(Occur, Box<dyn Query>)> = terms
            .into_iter()
            .map(|term| {
                let term_query: Box<dyn Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Should, term_query)
            })
            .collect();
        BooleanQuery::new(occur_term_queries)
    }

    /// Deconstructed view of the clauses making up this query.
    pub fn clauses(&self) -> &[(Occur, Box<dyn Query>)] {
        &self.subqueries[..]
    }
}

#[cfg(test)]
mod tests {
    use super::BooleanQuery;
    use crate::collector::DocSetCollector;
    use crate::query::{QueryClone, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, TEXT};
    use crate::{DocAddress, Index, Term};

    fn create_test_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc!(text=>"b c"));
        writer.add_document(doc!(text=>"a c"));
        writer.add_document(doc!(text=>"a b"));
        writer.add_document(doc!(text=>"a d"));
        writer.commit()?;
        Ok(index)
    }

    #[test]
    fn test_union() -> crate::Result<()> {
        let index = create_test_index()?;
        let searcher = index.reader()?.searcher();
        let text = index.schema().get_field("text").unwrap();
        let term_a = TermQuery::new(Term::from_field_text(text, "a"), IndexRecordOption::Basic);
        let term_d = TermQuery::new(Term::from_field_text(text, "d"), IndexRecordOption::Basic);
        let union_ad = BooleanQuery::union(vec![term_a.box_clone(), term_d.box_clone()]);
        let docs = searcher.search(&union_ad, &DocSetCollector)?;
        assert_eq!(
            docs,
            vec![
                DocAddress::new(0u32, 1u32),
                DocAddress::new(0u32, 2u32),
                DocAddress::new(0u32, 3u32)
            ]
            .into_iter()
            .collect()
        );
        Ok(())
    }

    #[test]
    fn test_intersection() -> crate::Result<()> {
        let index = create_test_index()?;
        let searcher = index.reader()?.searcher();
        let text = index.schema().get_field("text").unwrap();
        let term_a = TermQuery::new(Term::from_field_text(text, "a"), IndexRecordOption::Basic);
        let term_b = TermQuery::new(Term::from_field_text(text, "b"), IndexRecordOption::Basic);
        let term_c = TermQuery::new(Term::from_field_text(text, "c"), IndexRecordOption::Basic);
        let intersection_ab =
            BooleanQuery::intersection(vec![term_a.box_clone(), term_b.box_clone()]);
        let intersection_ac =
            BooleanQuery::intersection(vec![term_a.box_clone(), term_c.box_clone()]);
        let intersection_bc =
            BooleanQuery::intersection(vec![term_b.box_clone(), term_c.box_clone()]);
        {
            let docs = searcher.search(&intersection_ab, &DocSetCollector)?;
            assert_eq!(
                docs,
                vec![DocAddress::new(0u32, 2u32)].into_iter().collect()
            );
        }
        {
            let docs = searcher.search(&intersection_ac, &DocSetCollector)?;
            assert_eq!(
                docs,
                vec![DocAddress::new(0u32, 1u32)].into_iter().collect()
            );
        }
        {
            let docs = searcher.search(&intersection_bc, &DocSetCollector)?;
            assert_eq!(
                docs,
                vec![DocAddress::new(0u32, 0u32)].into_iter().collect()
            );
        }
        Ok(())
    }
}
