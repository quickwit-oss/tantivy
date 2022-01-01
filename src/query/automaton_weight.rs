use crate::core::SegmentReader;
use crate::query::fuzzy_query::DfaWrapper;
use crate::query::score_combiner::SumCombiner;
use crate::query::Explanation;
use crate::query::{ConstScorer, Union};
use crate::query::{Scorer, Weight};
use crate::schema::{Field, IndexRecordOption};
use crate::termdict::{TermDictionary, TermWithStateStreamer};
use crate::TantivyError;
use crate::{DocId, Score};
use std::any::{Any, TypeId};
use std::io;
use std::sync::Arc;
use tantivy_fst::Automaton;

/// A weight struct for Fuzzy Term and Regex Queries
pub struct AutomatonWeight<A, F> {
    field: Field,
    automaton: Arc<A>,
    score_fn: F,
}

impl<A, F> AutomatonWeight<A, F>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
    F: Fn(u8) -> f32,
{
    /// Create a new AutomationWeight
    pub fn new<IntoArcA: Into<Arc<A>>>(field: Field, automaton: IntoArcA, score_fn: F) -> AutomatonWeight<A, F> {
        AutomatonWeight {
            field,
            automaton: automaton.into(),
            score_fn,
        }
    }

    fn automaton_stream<'a>(
        &'a self,
        term_dict: &'a TermDictionary,
    ) -> io::Result<TermWithStateStreamer<'a, &'a A>> {
        let automaton: &A = &*self.automaton;
        let term_stream_builder = term_dict.search_with_state(automaton);
        term_stream_builder.into_stream()
    }
}

impl<A, F> Weight for AutomatonWeight<A, F>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
    F: Fn(u8) -> f32 + Send + Sync + 'static,
{
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        let mut term_stream = self.automaton_stream(term_dict)?;

        let mut scorers = vec![];
        while let Some((_term, term_info, state)) = term_stream.next() {
            let score = automaton_score(self.automaton.as_ref(), state, &self.score_fn);
            let segment_postings =
                inverted_index.read_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            let scorer = ConstScorer::new(segment_postings, boost * score);
            scorers.push(scorer);
        }

        let scorer = Union::<_, SumCombiner>::from(scorers);
        Ok(Box::new(scorer))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) == doc {
            Ok(Explanation::new("AutomatonScorer", scorer.score()))
        } else {
            Err(TantivyError::InvalidArgument(
                "Document does not exist".to_string(),
            ))
        }
    }
}

fn automaton_score<A, F>(automaton: &A, state: A::State, score_fn: F) -> f32
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
    F: Fn(u8) -> f32,
{
    if TypeId::of::<DfaWrapper>() == automaton.type_id() && TypeId::of::<u32>() == state.type_id() {
        let dfa = automaton as *const A as *const DfaWrapper;
        let dfa = unsafe { &*dfa };

        let id = &state as *const A::State as *const u32;
        let id = unsafe { *id };

        let dist = dfa.0.distance(id).to_u8();
        score_fn(dist)
    } else {
        1.0
    }
}

#[cfg(test)]
mod tests {
    use super::AutomatonWeight;
    use crate::docset::TERMINATED;
    use crate::query::Weight;
    use crate::schema::{Schema, STRING};
    use crate::Index;
    use tantivy_fst::Automaton;

    fn create_index() -> crate::Result<Index> {
        let mut schema = Schema::builder();
        let title = schema.add_text_field("title", STRING);
        let index = Index::create_in_ram(schema.build());
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(title=>"abc"))?;
        index_writer.add_document(doc!(title=>"bcd"))?;
        index_writer.add_document(doc!(title=>"abcd"))?;
        index_writer.commit()?;
        Ok(index)
    }

    #[derive(Clone, Copy)]
    enum State {
        Start,
        NotMatching,
        AfterA,
    }

    struct PrefixedByA;

    impl Automaton for PrefixedByA {
        type State = State;

        fn start(&self) -> Self::State {
            State::Start
        }

        fn is_match(&self, state: &Self::State) -> bool {
            matches!(*state, State::AfterA)
        }

        fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
            match *state {
                State::Start => {
                    if byte == b'a' {
                        State::AfterA
                    } else {
                        State::NotMatching
                    }
                }
                State::AfterA => State::AfterA,
                State::NotMatching => State::NotMatching,
            }
        }
    }

    #[test]
    fn test_automaton_weight() -> crate::Result<()> {
        let index = create_index()?;
        let field = index.schema().get_field("title").unwrap();
        let automaton_weight = AutomatonWeight::new(field, PrefixedByA, |_| 1.0);
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut scorer = automaton_weight.scorer(searcher.segment_reader(0u32), 1.0)?;
        assert_eq!(scorer.doc(), 0u32);
        assert_eq!(scorer.score(), 1.0);
        assert_eq!(scorer.advance(), 2u32);
        assert_eq!(scorer.doc(), 2u32);
        assert_eq!(scorer.score(), 1.0);
        assert_eq!(scorer.advance(), TERMINATED);
        Ok(())
    }

    #[test]
    fn test_automaton_weight_boost() -> crate::Result<()> {
        let index = create_index()?;
        let field = index.schema().get_field("title").unwrap();
        let automaton_weight = AutomatonWeight::new(field, PrefixedByA, |_| 1.0);
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut scorer = automaton_weight.scorer(searcher.segment_reader(0u32), 1.32)?;
        assert_eq!(scorer.doc(), 0u32);
        assert_eq!(scorer.score(), 1.32);
        Ok(())
    }
}
