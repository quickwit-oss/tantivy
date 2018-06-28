use levenshtein_automata::{LevenshteinAutomatonBuilder, DFA};
use query::{AutomatonWeight, Query, Weight};
use schema::Term;
use std::collections::HashMap;
use Result;
use Searcher;

lazy_static! {
    static ref LEV_BUILDER: HashMap<(u8, bool), LevenshteinAutomatonBuilder> = {
        let mut lev_builder_cache = HashMap::new();
        // TODO make population lazy on a `(distance, val)` basis
        for distance in 0..3 {
            for &transposition in [false, true].iter() {
                let lev_automaton_builder = LevenshteinAutomatonBuilder::new(distance, transposition);
                lev_builder_cache.insert((distance, transposition), lev_automaton_builder);
            }
        }
        lev_builder_cache
    };
}

/// A Fuzzy Query matches all of the documents
/// containing a specific term that is within
/// Levenshtein distance
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::schema::{SchemaBuilder, TEXT};
/// use tantivy::{Index, Result, Term};
/// use tantivy::collector::{CountCollector, TopCollector, chain};
/// use tantivy::query::FuzzyTermQuery;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = SchemaBuilder::new();
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
///
///     index.load_searchers()?;
///     let searcher = index.searcher();
///
///     {
///         let mut top_collector = TopCollector::with_limit(2);
///         let mut count_collector = CountCollector::default();
///         {
///             let mut collectors = chain().push(&mut top_collector).push(&mut count_collector);
///             let term = Term::from_field_text(title, "Diary");
///             let query = FuzzyTermQuery::new(term, 1, true);
///             searcher.search(&query, &mut collectors).unwrap();
///         }
///         assert_eq!(count_collector.count(), 2);
///         assert!(top_collector.at_capacity());
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
        let automaton = LEV_BUILDER.get(&(self.distance, false))
            .unwrap() // TODO return an error
            .build_dfa(self.term.text());
        Ok(AutomatonWeight::new(self.term.field(), automaton))
    }
}

impl Query for FuzzyTermQuery {
    fn weight(&self, _searcher: &Searcher, _scoring_enabled: bool) -> Result<Box<Weight>> {
        Ok(Box::new(self.specialized_weight()?))
    }
}

#[cfg(test)]
mod test {
    use super::FuzzyTermQuery;
    use collector::TopCollector;
    use schema::SchemaBuilder;
    use schema::TEXT;
    use tests::assert_nearly_equals;
    use Index;
    use Term;

    #[test]
    pub fn test_fuzzy_term() {
        let mut schema_builder = SchemaBuilder::new();
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
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        {
            let mut collector = TopCollector::with_limit(2);
            let term = Term::from_field_text(country_field, "japon");

            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            searcher.search(&fuzzy_query, &mut collector).unwrap();
            let scored_docs = collector.score_docs();
            assert_eq!(scored_docs.len(), 1, "Expected only 1 document");
            let (score, _) = scored_docs[0];
            assert_nearly_equals(1f32, score);
        }
    }
}
