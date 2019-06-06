use common::BitSet;
use core::SegmentReader;
use query::ConstScorer;
use query::{BitSetDocSet, Explanation};
use query::{Scorer, Weight};
use schema::{Field, IndexRecordOption};
use tantivy_fst::Automaton;
use termdict::{TermDictionary, TermStreamer};
use DocId;
use TantivyError;
use {Result, SkipResult};

/// A weight struct for Fuzzy Term and Regex Queries
pub struct AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
{
    field: Field,
    automaton: A,
}

impl<A> AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
{
    /// Create a new AutomationWeight
    pub fn new(field: Field, automaton: A) -> AutomatonWeight<A> {
        AutomatonWeight { field, automaton }
    }

    fn automaton_stream<'a>(&'a self, term_dict: &'a TermDictionary) -> TermStreamer<'a, &'a A> {
        let term_stream_builder = term_dict.search(&self.automaton);
        term_stream_builder.into_stream()
    }
}

impl<A> Weight for AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
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

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> Result<Explanation> {
        let mut scorer = self.scorer(reader)?;
        if scorer.skip_next(doc) == SkipResult::Reached {
            Ok(Explanation::new("AutomatonScorer", 1.0f32))
        } else {
            Err(TantivyError::InvalidArgument(
                "Document does not exist".to_string(),
            ))
        }
    }
}
