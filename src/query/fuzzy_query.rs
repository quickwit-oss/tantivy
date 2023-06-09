use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA};
use once_cell::sync::OnceCell;
use tantivy_fst::Automaton;

use crate::query::{AutomatonWeight, EnableScoring, Query, Weight};
use crate::schema::Term;
use crate::TantivyError::InvalidArgument;

pub(crate) struct DfaWrapper(pub DFA);

impl Automaton for DfaWrapper {
    type State = u32;

    fn start(&self) -> Self::State {
        self.0.initial_state()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        match self.0.distance(*state) {
            Distance::Exact(_) => true,
            Distance::AtLeast(_) => false,
        }
    }

    fn can_match(&self, state: &u32) -> bool {
        *state != levenshtein_automata::SINK_STATE
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0.transition(*state, byte)
    }
}

/// A Fuzzy Query matches all of the documents
/// containing a specific term that is within
/// Levenshtein distance
/// ```rust
/// use tantivy::collector::{Count, TopDocs};
/// use tantivy::query::FuzzyTermQuery;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index, Term};
///
/// fn example() -> tantivy::Result<()> {
///     let mut schema_builder = Schema::builder();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer: IndexWriter = index.writer(3_000_000)?;
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///         ))?;
///         index_writer.add_document(doc!(
///             title => "The Diary of Muadib",
///         ))?;
///         index_writer.add_document(doc!(
///             title => "A Dairy Cow",
///         ))?;
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///         ))?;
///         index_writer.commit()?;
///     }
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///
///     {
///         let term = Term::from_field_text(title, "Diary");
///         let query = FuzzyTermQuery::new(term, 1, true);
///         let (top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count)).unwrap();
///         assert_eq!(count, 2);
///         assert_eq!(top_docs.len(), 2);
///     }
///
///     Ok(())
/// }
/// # assert!(example().is_ok());
/// ```
#[derive(Debug, Clone)]
pub struct FuzzyTermQuery {
    /// What term are we searching
    term: Term,
    /// How many changes are we going to allow
    distance: u8,
    /// Should a transposition cost 1 or 2?
    transposition_cost_one: bool,
    ///
    prefix: bool,
}

impl FuzzyTermQuery {
    /// Creates a new Fuzzy Query
    pub fn new(term: Term, distance: u8, transposition_cost_one: bool) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            distance,
            transposition_cost_one,
            prefix: false,
        }
    }

    /// Creates a new Fuzzy Query of the Term prefix
    pub fn new_prefix(term: Term, distance: u8, transposition_cost_one: bool) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            distance,
            transposition_cost_one,
            prefix: true,
        }
    }

    fn specialized_weight(&self) -> crate::Result<AutomatonWeight<DfaWrapper>> {
        static AUTOMATON_BUILDER: [[OnceCell<LevenshteinAutomatonBuilder>; 2]; 3] = [
            [OnceCell::new(), OnceCell::new()],
            [OnceCell::new(), OnceCell::new()],
            [OnceCell::new(), OnceCell::new()],
        ];

        let automaton_builder = AUTOMATON_BUILDER
            .get(self.distance as usize)
            .ok_or_else(|| {
                InvalidArgument(format!(
                    "Levenshtein distance of {} is not allowed. Choose a value less than {}",
                    self.distance,
                    AUTOMATON_BUILDER.len()
                ))
            })?
            .get(self.transposition_cost_one as usize)
            .unwrap()
            .get_or_init(|| {
                LevenshteinAutomatonBuilder::new(self.distance, self.transposition_cost_one)
            });

        let term_value = self.term.value();
        let term_text = term_value.as_str().ok_or_else(|| {
            InvalidArgument("The fuzzy term query requires a string term.".to_string())
        })?;
        let automaton = if self.prefix {
            automaton_builder.build_prefix_dfa(term_text)
        } else {
            automaton_builder.build_dfa(term_text)
        };
        Ok(AutomatonWeight::new(
            self.term.field(),
            DfaWrapper(automaton),
        ))
    }
}

impl Query for FuzzyTermQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight()?))
    }
}

#[cfg(test)]
mod test {
    use super::FuzzyTermQuery;
    use crate::collector::{Count, TopDocs};
    use crate::schema::{Schema, TEXT};
    use crate::{assert_nearly_equals, Index, IndexWriter, Term};

    #[test]
    pub fn test_fuzzy_term() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let country_field = schema_builder.add_text_field("country", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(
                country_field => "japan",
            ))?;
            index_writer.add_document(doc!(
                country_field => "korea",
            ))?;
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // passes because Levenshtein distance is 1 (substitute 'o' with 'a')
        {
            let term = Term::from_field_text(country_field, "japon");
            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs = searcher.search(&fuzzy_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);
        }

        // fails because non-prefix Levenshtein distance is more than 1 (add 'a' and 'n')
        {
            let term = Term::from_field_text(country_field, "jap");

            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs = searcher.search(&fuzzy_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 0, "Expected no document");
        }

        // passes because prefix Levenshtein distance is 0
        {
            let term = Term::from_field_text(country_field, "jap");
            let fuzzy_query = FuzzyTermQuery::new_prefix(term, 1, true);
            let top_docs = searcher.search(&fuzzy_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);
        }
        Ok(())
    }

    #[test]
    pub fn test_fuzzy_term_transposition_cost_one() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let country_field = schema_builder.add_text_field("country", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(country_field => "japan"))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let term_jaapn = Term::from_field_text(country_field, "jaapn");
        {
            let fuzzy_query_transposition = FuzzyTermQuery::new(term_jaapn.clone(), 1, true);
            let count = searcher.search(&fuzzy_query_transposition, &Count)?;
            assert_eq!(count, 1);
        }
        {
            let fuzzy_query_transposition = FuzzyTermQuery::new(term_jaapn, 1, false);
            let count = searcher.search(&fuzzy_query_transposition, &Count)?;
            assert_eq!(count, 0);
        }
        Ok(())
    }
}
