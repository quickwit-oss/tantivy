use crate::error::TantivyError::InvalidArgument;
use crate::query::{AutomatonWeight, Query, Weight};
use crate::schema::Term;
use crate::Result;
use crate::Searcher;
use levenshtein_automata::{LevenshteinAutomatonBuilder, DFA};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::ops::Range;

/// A range of Levenshtein distances that we will build DFAs for our terms
/// The computation is exponential, so best keep it to low single digits
const VALID_LEVENSHTEIN_DISTANCE_RANGE: Range<u8> = (0..3);

static LEV_BUILDER: Lazy<HashMap<(u8, bool), LevenshteinAutomatonBuilder>> = Lazy::new(|| {
    let mut lev_builder_cache = HashMap::new();
    // TODO make population lazy on a `(distance, val)` basis
    for distance in VALID_LEVENSHTEIN_DISTANCE_RANGE {
        for &transposition in &[false, true] {
            let lev_automaton_builder = LevenshteinAutomatonBuilder::new(distance, transposition);
            lev_builder_cache.insert((distance, transposition), lev_automaton_builder);
        }
    }
    lev_builder_cache
});

/// A Fuzzy Query matches all of the documents
/// containing a specific term that is within
/// Levenshtein distance
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{Index, Result, Term};
/// use tantivy::collector::{Count, TopDocs};
/// use tantivy::query::FuzzyTermQuery;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = Schema::builder();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer = index.writer(3_000_000)?;
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of Muadib",
///         ));
///         index_writer.add_document(doc!(
///             title => "A Dairy Cow",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///         ));
///         index_writer.commit().unwrap();
///     }
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///
///     {
///
///         let term = Term::from_field_text(title, "Diary");
///         let query = FuzzyTermQuery::new(term, 1, true);
///         let (top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count)).unwrap();
///         assert_eq!(count, 2);
///         assert_eq!(top_docs.len(), 2);
///     }
///
///     Ok(())
/// }
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

    /// Creates a new Fuzzy Query that treats transpositions as cost one rather than two
    pub fn new_prefix(term: Term, distance: u8, transposition_cost_one: bool) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            distance,
            transposition_cost_one,
            prefix: true,
        }
    }

    fn specialized_weight(&self) -> Result<AutomatonWeight<DFA>> {
        // LEV_BUILDER is a HashMap, whose `get` method returns an Option
        match LEV_BUILDER.get(&(self.distance, false)) {
            // Unwrap the option and build the Ok(AutomatonWeight)
            Some(automaton_builder) => {
                let automaton = automaton_builder.build_dfa(self.term.text());
                Ok(AutomatonWeight::new(self.term.field(), automaton))
            }
            None => Err(InvalidArgument(format!(
                "Levenshtein distance of {} is not allowed. Choose a value in the {:?} range",
                self.distance, VALID_LEVENSHTEIN_DISTANCE_RANGE
            ))),
        }
    }
}

impl Query for FuzzyTermQuery {
    fn weight(&self, _searcher: &Searcher, _scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight()?))
    }
}

#[cfg(test)]
mod test {
    use super::FuzzyTermQuery;
    use crate::collector::TopDocs;
    use crate::schema::Schema;
    use crate::schema::TEXT;
    use crate::tests::assert_nearly_equals;
    use crate::Index;
    use crate::Term;

    #[test]
    pub fn test_fuzzy_term() {
        let mut schema_builder = Schema::builder();
        let country_field = schema_builder.add_text_field("country", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
            index_writer.add_document(doc!(
                country_field => "japan",
            ));
            index_writer.add_document(doc!(
                country_field => "korea",
            ));
            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        {
            let term = Term::from_field_text(country_field, "japon");

            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs = searcher
                .search(&fuzzy_query, &TopDocs::with_limit(2))
                .unwrap();
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals(1f32, score);
        }
    }
}
