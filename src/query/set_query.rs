use tantivy_fst::raw::CompiledAddr;
use tantivy_fst::{Automaton, Map};

use crate::query::{AutomatonWeight, Query, Weight};
use crate::schema::Field;
use crate::{Searcher, Term};

/// A Term Set Query matches all of the documents containing any of the Term provided
///
/// Terms not using the right Field are discared.
#[derive(Debug, Clone)]
pub struct TermSetQuery {
    field: Field,
    terms: Vec<Term>,
}

impl TermSetQuery {
    /// Create a Term Set Query
    pub fn new<T: IntoIterator<Item = Term>>(field: Field, terms: T) -> Self {
        let mut terms: Vec<_> = terms
            .into_iter()
            .filter(|key| key.field() == field)
            .collect();
        terms.sort_unstable();
        terms.dedup();
        TermSetQuery { field, terms }
    }

    fn specialized_weight(
        &self,
        searcher: &Searcher,
    ) -> crate::Result<AutomatonWeight<SetDfaWrapper>> {
        let field_entry = searcher.schema().get_field_entry(self.field);
        let field_type = field_entry.field_type();
        if !field_type.is_indexed() {
            let error_msg = format!("Field {:?} is not indexed.", field_entry.name());
            return Err(crate::TantivyError::SchemaError(error_msg));
        }
        let field_type = field_type.value_type();

        // In practice this won't fail because:
        // - we are writing to memory, so no IoError
        // - BTreeSet are ordered, and we limit ourselves to values with a fixed prefix (which we
        // strip), so Map::from_iter get values in order
        let map = Map::from_iter(
            self.terms
                .iter()
                .filter(|key| key.typ() == field_type)
                .map(|key| (key.value_bytes(), 0)),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(AutomatonWeight::new(self.field, SetDfaWrapper(map)))
    }
}

impl Query for TermSetQuery {
    fn weight(
        &self,
        searcher: &Searcher,
        _scoring_enabled: bool,
    ) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight(searcher)?))
    }
}

struct SetDfaWrapper(Map<Vec<u8>>);

impl Automaton for SetDfaWrapper {
    type State = Option<CompiledAddr>;

    fn start(&self) -> Self::State {
        Some(self.0.as_ref().root().addr())
    }
    fn is_match(&self, state: &Self::State) -> bool {
        state
            .map(|s| self.0.as_ref().node(s).is_final())
            .unwrap_or(false)
    }
    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        state.and_then(|state| {
            let state = self.0.as_ref().node(state);
            let transition = state.find_input(byte)?;
            Some(state.transition_addr(transition))
        })
    }

    fn can_match(&self, state: &Self::State) -> bool {
        state.is_some()
    }
}

#[cfg(test)]
mod tests {

    use crate::collector::TopDocs;
    use crate::query::TermSetQuery;
    use crate::schema::{Schema, TEXT};
    use crate::{assert_nearly_equals, Index, Term};

    #[test]
    pub fn test_term_set_query() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field1 = schema_builder.add_text_field("field1", TEXT);
        let field2 = schema_builder.add_text_field("field1", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
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
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        {
            // single element
            let mut terms = vec![Term::from_field_text(field1, "doc1")];

            let term_set_query = TermSetQuery::new(field1, terms);
            let top_docs = searcher.search(&term_set_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 1, "Expected 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);
        }

        {
            // single element, absent
            let mut terms = vec![Term::from_field_text(field1, "doc4")];

            let term_set_query = TermSetQuery::new(field1, terms);
            let top_docs = searcher.search(&term_set_query, &TopDocs::with_limit(1))?;
            assert!(top_docs.is_empty(), "Expected 0 document");
        }

        {
            // multiple elements
            let mut terms = vec![
                Term::from_field_text(field1, "doc1"),
                Term::from_field_text(field1, "doc2"),
            ];

            let term_set_query = TermSetQuery::new(field1, terms);
            let top_docs = searcher.search(&term_set_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 2, "Expected 2 documents");
            for (score, _) in top_docs {
                assert_nearly_equals!(1.0, score);
            }
        }

        {
            // single element, wrong field
            let mut terms = vec![Term::from_field_text(field1, "doc1")];

            let term_set_query = TermSetQuery::new(field2, terms);
            let top_docs = searcher.search(&term_set_query, &TopDocs::with_limit(1))?;
            assert!(top_docs.is_empty(), "Expected 0 document");
        }
        {
            // multiple elements, mixed fields
            let mut terms = vec![
                Term::from_field_text(field1, "doc1"),
                Term::from_field_text(field1, "doc1"),
                Term::from_field_text(field2, "val2"),
            ];

            let term_set_query = TermSetQuery::new(field1, terms.clone());
            let top_docs = searcher.search(&term_set_query, &TopDocs::with_limit(2))?;

            assert_eq!(top_docs.len(), 1, "Expected 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);

            let term_set_query = TermSetQuery::new(field2, terms.clone());
            let top_docs = searcher.search(&term_set_query, &TopDocs::with_limit(2))?;

            assert_eq!(top_docs.len(), 1, "Expected 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);
            let term_set_query = TermSetQuery::new(field1, terms.clone());
            let top_docs = searcher.search(&term_set_query, &TopDocs::with_limit(2))?;

            assert_eq!(top_docs.len(), 1, "Expected 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);
        }

        Ok(())
    }
}
