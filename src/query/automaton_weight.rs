use common::BitSet;
use core::SegmentReader;
use fst::Automaton;
use query::BitSetDocSet;
use query::ConstScorer;
use query::{Scorer, Weight};
use schema::{Field, IndexRecordOption};
use termdict::{TermDictionary, TermStreamer};
use Result;
use query::weight::MatchingTerms;
use SkipResult;
use Term;
use DocId;
use DocSet;

/// A weight struct for Fuzzy Term and Regex Queries
pub struct AutomatonWeight<A>
where
    A: Automaton,
{
    field: Field,
    automaton: A,
}

impl<A> AutomatonWeight<A>
where
    A: Automaton,
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
    A: Automaton,
{
    fn matching_terms(&self,
                      reader: &SegmentReader,
                      matching_terms: &mut MatchingTerms) -> Result<()> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let inverted_index = reader.inverted_index(self.field);
        let term_dict = inverted_index.terms();
        let mut term_stream = self.automaton_stream(term_dict);

        let doc_ids = matching_terms.sorted_doc_ids();
        let mut docs_matching_current_term: Vec<DocId> = vec![];

        let mut term_buffer: Vec<u8> = vec![];

        while term_stream.advance() {
            docs_matching_current_term.clear();
            let term_info = term_stream.value();
            let mut segment_postings = inverted_index.read_postings_from_terminfo(term_info, IndexRecordOption::Basic);
            for &doc_id in &doc_ids {
                match segment_postings.skip_next(doc_id) {
                    SkipResult::Reached => {
                        docs_matching_current_term.push(doc_id);
                    }
                    SkipResult::OverStep => {}
                    SkipResult::End => {}
                }
            }
            if !docs_matching_current_term.is_empty() {
                term_buffer.clear();
                let term_ord = term_stream.term_ord();
                inverted_index.terms().ord_to_term(term_ord, &mut term_buffer);
                let term = Term::from_field_bytes(self.field, &term_buffer[..]);
                for &doc_id in &docs_matching_current_term {
                    matching_terms.add_term(doc_id, term.clone());
                }
            }
        }
        Ok(())
    }

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
