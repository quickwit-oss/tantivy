use common::BitSet;
use core::SegmentReader;
use fst::Automaton;
use query::BitSetDocSet;
use query::ConstScorer;
use query::{Query, Scorer, Weight};
use schema::{Field, IndexRecordOption, Term};
use termdict::{TermDictionary, TermStreamer};
use Result;

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
  fn automaton_stream<'a>(&self, term_dict: &'a TermDictionary) -> TermStreamer<'a, A> {
    // TODO: how do we handle ownership here?
    // No .clone()
    // Search needs to be owned because fst::Map.search<A: Automaton>(&self, aut: A)
    let term_stream_builder = term_dict.search(self.automaton);

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
    let mut term_range = self.automaton_stream(term_dict);
    while term_range.advance() {
      let term_info = term_range.value();
      let mut block_segment_postings =
        inverted_index.read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic);
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
