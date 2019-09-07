use crate::error::TantivyError::InvalidArgument;
use crate::query::{AutomatonWeight, Query, Weight};
use crate::schema::Term;
use crate::termdict::WrappedDFA;
use crate::Result;
use crate::Searcher;
use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::ops::Range;
use derive_builder::Builder;

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


#[derive(Builder, Default, Clone, Debug)]
pub struct FuzzyConfiguration {
    /// How many changes are we going to allow
    pub distance: u8,
    /// Should a transposition cost 1 or 2?
    #[builder(default)]
    pub transposition_cost_one: bool,
    #[builder(default)]
    pub prefix: bool,
    /// If true, only the term with a levenshtein of exactly `distance` will match.
    /// If false, terms at a distance `<=` to `distance` will match.
    #[builder(default)]
    pub exact_distance: bool,
}

fn build_dfa(fuzzy_configuration: &FuzzyConfiguration, term_text: &str) -> Result<DFA> {
    let automaton_builder = LEV_BUILDER
        .get(&(fuzzy_configuration.distance, fuzzy_configuration.transposition_cost_one))
        .ok_or_else(|| {
            InvalidArgument(format!(
                "Levenshtein distance of {} is not allowed. Choose a value in the {:?} range",
                fuzzy_configuration.distance, VALID_LEVENSHTEIN_DISTANCE_RANGE
            ))
        })?;
    if fuzzy_configuration.prefix {
        Ok(automaton_builder.build_prefix_dfa(term_text))
    } else {
        Ok(automaton_builder.build_dfa(term_text))
    }
}

/// A Fuzzy Query matches all of the documents
/// containing a specific term that is within
/// Levenshtein distance
/// ```rust
/// use tantivy::collector::{Count, TopDocs};
/// use tantivy::query::FuzzyTermQuery;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index, Result, Term};
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = Schema::builder();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer = index.writer(3_000_000)?;
///         index_writer.add_document(doc!(title => "The Name of the Wind"));
///         index_writer.add_document(doc!(title => "The Diary of Muadib"));
///         index_writer.add_document(doc!(title => "A Dairy Cow"));
///         index_writer.add_document(doc!(title => "The Diary of a Young Girl"));
///         index_writer.commit().unwrap();
///     }
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///     let term = Term::from_field_text(title, "Diary");
///     let query = FuzzyTermQuery::new(term, 1, true);
///     let (top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count)).unwrap();
///     assert_eq!(count, 2);
///     assert_eq!(top_docs.len(), 2);
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct FuzzyTermQuery {
    /// What term are we searching
    term: Term,
    configuration: FuzzyConfiguration
}

impl FuzzyTermQuery {
    pub fn new_from_configuration(term: Term, configuration: FuzzyConfiguration) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            configuration
        }
    }

    /// Creates a new Fuzzy Query
    pub fn new(term: Term, distance: u8, transposition_cost_one: bool) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            configuration: FuzzyConfiguration {
                distance,
                transposition_cost_one,
                prefix: false,
                exact_distance: false
            }
        }
    }
}

impl Query for FuzzyTermQuery {
    fn weight(&self, _searcher: &Searcher, _scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        let dfa = build_dfa(&self.configuration, self.term.text())?;
        // TODO optimize for distance = 0 and possibly prefix
        if self.configuration.exact_distance {
            let target_distance = self.configuration.distance;
            let wrapped_dfa = WrappedDFA {
                dfa,
                condition: move |distance: Distance| distance == Distance::Exact(target_distance),
            };
            Ok(Box::new(AutomatonWeight::new(
                self.term.field(),
                wrapped_dfa,
            )))
        } else {
            let wrapped_dfa = WrappedDFA {
                dfa,
                condition: move |distance: Distance| match distance {
                    Distance::Exact(_) => true,
                    Distance::AtLeast(_) => false,
                },
            };
            Ok(Box::new(AutomatonWeight::new(
                self.term.field(),
                wrapped_dfa,
            )))
        }
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
    use super::FuzzyConfigurationBuilder;

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
        {
            let term = Term::from_field_text(country_field, "japon");
            let fuzzy_conf = FuzzyConfigurationBuilder::default()
                .distance(2)
                .exact_distance(true)
                .build()
                .unwrap();
            let fuzzy_query = FuzzyTermQuery::new_from_configuration(term, fuzzy_conf);
            let top_docs = searcher
                .search(&fuzzy_query, &TopDocs::with_limit(2))
                .unwrap();
            assert!(top_docs.is_empty());
        }
        {
            let term = Term::from_field_text(country_field, "japon");
            let fuzzy_conf = FuzzyConfigurationBuilder::default()
                .distance(1)
                .exact_distance(true)
                .build()
                .unwrap();
            let fuzzy_query = FuzzyTermQuery::new_from_configuration(term, fuzzy_conf);
            let top_docs = searcher
                .search(&fuzzy_query, &TopDocs::with_limit(2))
                .unwrap();
            assert_eq!(top_docs.len(), 1);
        }
        {
            let term = Term::from_field_text(country_field, "jpp");
            let fuzzy_conf = FuzzyConfigurationBuilder::default()
                .distance(1)
                .prefix(true)
                .build()
                .unwrap();
            let fuzzy_query = FuzzyTermQuery::new_from_configuration(term, fuzzy_conf);
            let top_docs = searcher
                .search(&fuzzy_query, &TopDocs::with_limit(2))
                .unwrap();
            assert_eq!(top_docs.len(), 1);
        }
        {
            let term = Term::from_field_text(country_field, "jpaan");
            let fuzzy_conf = FuzzyConfigurationBuilder::default()
                .distance(1)
                .exact_distance(true)
                .transposition_cost_one(true)
                .build()
                .unwrap();
            let fuzzy_query = FuzzyTermQuery::new_from_configuration(term, fuzzy_conf);
            let top_docs = searcher
                .search(&fuzzy_query, &TopDocs::with_limit(2))
                .unwrap();
            assert_eq!(top_docs.len(), 1);
        }
        {
            let term = Term::from_field_text(country_field, "jpaan");
            let fuzzy_conf = FuzzyConfigurationBuilder::default()
                .distance(2)
                .exact_distance(true)
                .transposition_cost_one(false)
                .build()
                .unwrap();
            let fuzzy_query = FuzzyTermQuery::new_from_configuration(term, fuzzy_conf);
            let top_docs = searcher
                .search(&fuzzy_query, &TopDocs::with_limit(2))
                .unwrap();
            assert_eq!(top_docs.len(), 1);
        }
    }

}
