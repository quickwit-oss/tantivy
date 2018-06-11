use common::BitSet;
use core::SegmentReader;
use fst::Automaton;
use levenshtein_automata::{LevenshteinAutomatonBuilder, DFA};
use query::BitSetDocSet;
use query::ConstScorer;
use query::{Query, Scorer, Weight};
use schema::{Field, IndexRecordOption, Term};
use std::collections::HashMap;
use termdict::{TermDictionary, TermStreamer};
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

    pub fn new_prefix(term: Term, distance: u8, transposition_cost_one: bool) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            distance,
            transposition_cost_one,
            prefix: true,
        }
    }

    pub fn specialized_weight(&self) -> Result<AutomatonWeight<DFA>> {
        let automaton = LEV_BUILDER.get(&(self.distance, false))
            .unwrap() // TODO return an error
            .build_dfa(self.term.text());
        Ok(AutomatonWeight {
            term: self.term.clone(),
            field: self.term.field(),
            automaton,
        })
    }
}

impl Query for FuzzyTermQuery {
    fn weight(&self, _searcher: &Searcher, _scoring_enabled: bool) -> Result<Box<Weight>> {
        Ok(Box::new(self.specialized_weight()?))
    }
}

pub struct AutomatonWeight<A>
where
    A: Automaton,
{
    term: Term,
    field: Field,
    automaton: A,
}

impl<A> AutomatonWeight<A>
where
    A: Automaton,
{
    fn automaton_stream<'a>(&'a self, term_dict: &'a TermDictionary) -> TermStreamer<'a, &'a A> {
        let term_stream_builder = term_dict.search(&self.automaton);
        term_stream_builder.into_stream()
    }
}

impl<A> Weight for AutomatonWeight<A>
where
    A: Automaton,
{
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(self.field);
        let term_dict = inverted_index.terms();
        let mut term_stream = self.automaton_stream(term_dict);
        while term_stream.advance() {
            let term_info = term_stream.value();
            let mut block_segment_postings = inverted_index
                .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic);
            while block_segment_postings.advance() {
                for &doc in block_segment_postings.docs() {
                    doc_bitset.insert(doc);
                }
            }
        }
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_bitset)))
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
    pub fn test_automaton_weight() {
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
