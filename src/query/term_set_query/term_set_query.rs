use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use tantivy_fst::Map;

use super::term_set_strategy::TermSetStrategyConfig;
use super::term_set_weight::{SetDfaWrapper, TermSetWeight};
use crate::query::score_combiner::DoNothingCombiner;
use crate::query::{AutomatonWeight, BooleanWeight, EnableScoring, Occur, Query, Weight};
use crate::schema::{Field, Schema};
use crate::{SegmentReader, Term};

/// A Term Set Query matches all of the documents containing any of the Term provided
#[derive(Debug, Clone)]
pub struct TermSetQuery {
    terms_map: HashMap<Field, Vec<Term>>,
    strategy_config: TermSetStrategyConfig,
}

impl TermSetQuery {
    /// Create a Term Set Query
    pub fn new<T: IntoIterator<Item = Term>>(terms: T) -> Self {
        let mut terms_map: HashMap<_, Vec<_>> = HashMap::new();
        for term in terms {
            terms_map.entry(term.field()).or_default().push(term);
        }

        TermSetQuery {
            terms_map,
            strategy_config: TermSetStrategyConfig::default(),
        }
    }

    /// Override the fast-field strategy thresholds. Consumers (paradedb) build
    /// the config from GUCs; the inner `FastFieldTermSetWeight` carries the
    /// same config it would if constructed directly via
    /// `FastFieldTermSetQuery::with_strategy_config`.
    pub fn with_strategy_config(mut self, cfg: TermSetStrategyConfig) -> Self {
        self.strategy_config = cfg;
        self
    }

    /// Build the per-field weights and wrap them in a `BooleanWeight`.
    /// Every field routes through `TermSetWeight`, which dispatches per
    /// segment based on what the field actually supports — fast
    /// strategies (Gallop / Linear / Bitset on the fast column) when
    /// available, `BitsetFromPostings` / `Automaton` on the inverted
    /// index otherwise.
    fn build_weight(&self, schema: &Schema) -> crate::Result<BooleanWeight<DoNothingCombiner>> {
        let mut sub_queries: Vec<(_, Box<dyn Weight>)> = Vec::with_capacity(self.terms_map.len());

        for (&field, terms) in self.terms_map.iter() {
            let field_entry = schema.get_field_entry(field);
            let field_type = field_entry.field_type();
            if !field_type.is_indexed() {
                // Top-level schema gate: `TermSetQuery` requires an
                // inverted index on every field. `TermSetWeight::new`
                // would also reject (neither-fast-nor-indexed → schema
                // error), but the early return here short-circuits
                // before per-term decoding and yields a more specific
                // error message.
                let error_msg = format!("Field {:?} is not indexed.", field_entry.name());
                return Err(crate::TantivyError::SchemaError(error_msg));
            }

            sub_queries.push((
                Occur::Should,
                Box::new(TermSetWeight::new(
                    field,
                    terms,
                    field_type,
                    self.strategy_config.clone(),
                )?),
            ));
        }

        Ok(BooleanWeight::new(
            sub_queries,
            false,
            Box::new(DoNothingCombiner::default),
        ))
    }
}

impl Query for TermSetQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.build_weight(enable_scoring.schema())?))
    }

    fn query_terms(
        &self,
        _field: Field,
        _segment_reader: &SegmentReader,
        visitor: &mut dyn FnMut(&Term, bool),
    ) {
        for terms in self.terms_map.values() {
            for term in terms {
                visitor(term, false);
            }
        }
    }
}

/// `InvertedIndexTermSetQuery` is the same as [TermSetQuery] but only uses the inverted index.
#[derive(Debug, Clone)]
pub struct InvertedIndexTermSetQuery {
    terms_map: HashMap<Field, Vec<Term>>,
}

impl InvertedIndexTermSetQuery {
    /// Create a new `InvertedIndexTermSetQuery`.
    pub fn new<T: IntoIterator<Item = Term>>(terms: T) -> Self {
        let mut terms_map: HashMap<_, Vec<_>> = HashMap::new();
        for term in terms {
            terms_map.entry(term.field()).or_default().push(term);
        }

        for terms in terms_map.values_mut() {
            terms.sort_unstable();
            terms.dedup();
        }

        InvertedIndexTermSetQuery { terms_map }
    }
}

impl Query for InvertedIndexTermSetQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let mut sub_queries: Vec<(_, Box<dyn Weight>)> = Vec::with_capacity(self.terms_map.len());
        for (&field, sorted_terms) in &self.terms_map {
            let schema = enable_scoring.schema();
            let field_entry = schema.get_field_entry(field);
            if !field_entry.field_type().is_indexed() {
                let error_msg = format!("Field {:?} is not indexed.", field_entry.name());
                return Err(crate::TantivyError::SchemaError(error_msg));
            }
            let map = Map::from_iter(
                sorted_terms
                    .iter()
                    .map(|key| (key.serialized_value_bytes(), 0u64)),
            )
            .map_err(io::Error::other)?;

            sub_queries.push((
                Occur::Should,
                Box::new(AutomatonWeight::new(field, SetDfaWrapper(Arc::new(map)))),
            ));
        }
        Ok(Box::new(BooleanWeight::new(
            sub_queries,
            false,
            Box::new(DoNothingCombiner::default),
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::collector::TopDocs;
    use crate::query::{QueryParser, TermSetQuery};
    use crate::schema::{Schema, TEXT};
    use crate::{assert_nearly_equals, Index, IndexWriter, Term};

    #[test]
    pub fn test_term_set_query() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field1 = schema_builder.add_text_field("field1", TEXT);
        let field2 = schema_builder.add_text_field("field2", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(
                field1 => "doc1",
                field2 => "val1",
            ))?;
            index_writer.add_document(doc!(
                field1 => "doc2",
                field2 => "val2",
            ))?;
            index_writer.add_document(doc!(
                field1 => "doc3",
                field2 => "val3",
            ))?;
            index_writer.add_document(doc!(
                field1 => "val3",
                field2 => "doc3",
            ))?;
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        {
            // single element
            let terms = vec![Term::from_field_text(field1, "doc1")];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);
        }

        {
            // single element, absent
            let terms = vec![Term::from_field_text(field1, "doc4")];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(1).order_by_score())?;
            assert!(top_docs.is_empty(), "Expected 0 document");
        }

        {
            // multiple elements
            let terms = vec![
                Term::from_field_text(field1, "doc1"),
                Term::from_field_text(field1, "doc2"),
            ];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 2, "Expected 2 documents");
            for (score, _) in top_docs {
                assert_nearly_equals!(1.0, score);
            }
        }

        {
            // multiple elements, mixed fields
            let terms = vec![
                Term::from_field_text(field1, "doc1"),
                Term::from_field_text(field1, "doc1"),
                Term::from_field_text(field2, "val2"),
            ];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(3).order_by_score())?;

            assert_eq!(top_docs.len(), 2, "Expected 2 document");
            for (score, _) in top_docs {
                assert_nearly_equals!(1.0, score);
            }
        }

        {
            // no field crosstalk
            let terms = vec![Term::from_field_text(field1, "doc3")];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(3).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected 1 document");

            let terms = vec![Term::from_field_text(field2, "doc3")];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(3).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected 1 document");

            let terms = vec![
                Term::from_field_text(field1, "doc3"),
                Term::from_field_text(field2, "doc3"),
            ];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(3).order_by_score())?;
            assert_eq!(top_docs.len(), 2, "Expected 2 document");
        }

        Ok(())
    }

    #[test]
    fn test_term_set_query_parser() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("field", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        let field = schema.get_field("field").unwrap();
        index_writer.add_document(doc!(
          field => "val1",
        ))?;
        index_writer.add_document(doc!(
          field => "val2",
        ))?;
        index_writer.add_document(doc!(
          field => "val3",
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![]);
        let query = query_parser.parse_query("field: IN [val1 val2]")?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(3).order_by_score())?;
        assert_eq!(top_docs.len(), 2);
        Ok(())
    }
}
