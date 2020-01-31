use crate::common::BitSet;
use crate::core::SegmentReader;
use crate::query::ConstScorer;
use crate::query::{BitSetDocSet, Explanation};
use crate::query::{Scorer, Weight};
use crate::schema::{Field, IndexRecordOption};
use crate::termdict::{TermDictionary, TermStreamer};
use crate::DocId;
use crate::TantivyError;
use crate::{Result, SkipResult};
use std::sync::Arc;
use tantivy_fst::Automaton;

/// A weight struct for Fuzzy Term and Regex Queries
pub struct AutomatonWeight<A> {
    field: Field,
    automaton: Arc<A>,
    boost: f32,
}

impl<A> AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
{
    /// Create a new AutomationWeight
    pub fn new<IntoArcA: Into<Arc<A>>>(field: Field, automaton: IntoArcA) -> AutomatonWeight<A> {
        AutomatonWeight {
            field,
            automaton: automaton.into(),
            boost: 1.0,
        }
    }

    /// Boost the scorer by the given factor.
    pub fn boost_by(self, boost: f32) -> Self {
        Self { boost, ..self }
    }

    fn automaton_stream<'a>(&'a self, term_dict: &'a TermDictionary) -> TermStreamer<'a, &'a A> {
        let automaton: &A = &*self.automaton;
        let term_stream_builder = term_dict.search(automaton);
        term_stream_builder.into_stream()
    }
}

impl<A> Weight for AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
{
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<dyn Scorer>> {
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
        Ok(Box::new(ConstScorer::with_score(doc_bitset, self.boost)))
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
