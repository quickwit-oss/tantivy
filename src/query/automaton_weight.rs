use crate::postings::TermInfo;
use crate::query::fuzzy_query::{DfaWrapper, IntersectionState, StartsWithAutomatonState};
use crate::query::score_combiner::SumCombiner;
use std::any::{Any, TypeId};
use std::io;
use std::sync::Arc;

use common::BitSet;
use tantivy_fst::Automaton;

use super::fuzzy_query::StartsWithAutomaton;
use super::phrase_prefix_query::prefix_end;
use super::BitSetDocSet;
use crate::index::SegmentReader;
use crate::query::{ConstScorer, Explanation, Scorer, Weight};
use crate::schema::{Field, IndexRecordOption};
use crate::termdict::{TermDictionary, TermWithStateStreamer};
use crate::{DocId, Score, TantivyError};

/// A weight struct for Fuzzy Term and Regex Queries
pub struct AutomatonWeight<A> {
    field: Field,
    automaton: Arc<A>,
    // For JSON fields, the term dictionary include terms from all paths.
    // We apply additional filtering based on the given JSON path, when searching within the term
    // dictionary. This prevents terms from unrelated paths from matching the search criteria.
    json_path_bytes: Option<Box<[u8]>>,
    max_expansions: Option<u32>,
    fuzzy_scoring: bool,
}

impl<A> AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    /// Create a new AutomationWeight
    pub fn new<IntoArcA: Into<Arc<A>>>(
        field: Field,
        automaton: IntoArcA,
        max_expansions: Option<u32>,
        fuzzy_scoring: bool,
    ) -> AutomatonWeight<A> {
        AutomatonWeight {
            field,
            automaton: automaton.into(),
            json_path_bytes: None,
            max_expansions: max_expansions,
            fuzzy_scoring,
        }
    }

    /// Create a new AutomationWeight for a json path
    pub fn new_for_json_path<IntoArcA: Into<Arc<A>>>(
        field: Field,
        automaton: IntoArcA,
        json_path_bytes: &[u8],
        max_expansions: Option<u32>,
        fuzzy_scoring: bool,
    ) -> AutomatonWeight<A> {
        AutomatonWeight {
            field,
            automaton: automaton.into(),
            json_path_bytes: Some(json_path_bytes.to_vec().into_boxed_slice()),
            max_expansions: max_expansions,
            fuzzy_scoring,
        }
    }

    fn automaton_stream<'a>(
        &'a self,
        term_dict: &'a TermDictionary,
    ) -> io::Result<TermWithStateStreamer<'a, &'a A>> {
        let automaton: &A = &self.automaton;
        let mut term_stream_builder = term_dict.search_with_state(automaton);

        if let Some(json_path_bytes) = &self.json_path_bytes {
            term_stream_builder = term_stream_builder.ge(json_path_bytes);
            if let Some(end) = prefix_end(json_path_bytes) {
                term_stream_builder = term_stream_builder.lt(&end);
            }
        }

        term_stream_builder.into_stream()
    }

    /// Returns the term infos that match the automaton
    pub fn get_match_term_infos(&self, reader: &SegmentReader) -> crate::Result<Vec<TermInfo>> {
        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        let mut term_stream = self.automaton_stream(term_dict)?;
        let mut term_infos = Vec::new();
        while term_stream.advance() {
            term_infos.push(term_stream.value().clone());
        }
        Ok(term_infos)
    }

    fn process_term(
        &self,
        term_stream: &mut TermWithStateStreamer<'_, &A>,
        inverted_index: &Arc<crate::InvertedIndexReader>,
        doc_bitset: &mut BitSet,
    ) -> Result<(), TantivyError> {
        let mut block_segment_postings = inverted_index
            .read_block_postings_from_terminfo(term_stream.value(), IndexRecordOption::Basic)?;

        loop {
            let docs = block_segment_postings.docs();
            if docs.is_empty() {
                break;
            }
            for &doc in docs {
                doc_bitset.insert(doc);
            }
            block_segment_postings.advance();
        }
        Ok(())
    }

    fn process_term_fuzzy_scoring(
        &self,
        term_stream: &mut TermWithStateStreamer<'_, &A>,
        inverted_index: &Arc<crate::InvertedIndexReader>,
        boost: f32,
        scorers: &mut Vec<ConstScorer<crate::postings::SegmentPostings>>,
    ) -> Result<(), TantivyError> {
        if let Some(state) = term_stream.state() {
            let score = automaton_score(self.automaton.as_ref(), state);
            let segment_postings =
                inverted_index.read_postings_from_terminfo(term_stream.value(), IndexRecordOption::Basic)?;
            let scorer = ConstScorer::new(segment_postings, boost * score);
            scorers.push(scorer);
        }
        Ok(())
    }
}

impl<A> Weight for AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        let mut term_stream = self.automaton_stream(term_dict)?;

        if self.fuzzy_scoring {
            let mut scorers = vec![];
            if let Some(max_expansion) = self.max_expansions {
                let mut counter: u32 = 0;
                while counter < max_expansion && term_stream.advance() {
                    self.process_term_fuzzy_scoring(
                        &mut term_stream,
                        &inverted_index,
                        boost,
                        &mut scorers,
                    )?;
                    counter += 1;
                }
            } else {
                while term_stream.advance() {
                    self.process_term_fuzzy_scoring(
                        &mut term_stream,
                        &inverted_index,
                        boost,
                        &mut scorers,
                    )?;
                }
            }

            let scorer = super::BufferedUnionScorer::build(scorers, SumCombiner::default);
            Ok(Box::new(scorer))
        } else {
            let max_doc = reader.max_doc();
            let mut doc_bitset = BitSet::with_max_value(max_doc);
            if let Some(max_expansion) = self.max_expansions {
                let mut counter: u32 = 0;
                while counter < max_expansion && term_stream.advance() {
                    self.process_term(&mut term_stream, &inverted_index, &mut doc_bitset)?;
                    counter += 1;
                }
            } else {
                while term_stream.advance() {
                    self.process_term(&mut term_stream, &inverted_index, &mut doc_bitset)?;
                }
            }
            let doc_bitset = BitSetDocSet::from(doc_bitset);
            let const_scorer = ConstScorer::new(doc_bitset, boost);
            Ok(Box::new(const_scorer))
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) == doc {
            if self.fuzzy_scoring {
                Ok(Explanation::new("AutomatonScorer", scorer.score()))
            } else {
                Ok(Explanation::new("AutomatonScorer", 1.0))
            }
        } else {
            Err(TantivyError::InvalidArgument(
                "Document does not exist".to_string(),
            ))
        }
    }
}

fn automaton_score<A>(automaton: &A, state: &A::State) -> f32
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    if TypeId::of::<DfaWrapper>() == automaton.type_id() && TypeId::of::<u32>() == state.type_id() {
        let dfa = automaton as *const A as *const DfaWrapper;
        let dfa = unsafe { &*dfa };
        let id = state as *const A::State as *const u32;
        let id = unsafe { *id };
        let dist = dfa.0.distance(id).to_u8() as f32;
        1.0 / (1.0 + dist)
    } else if TypeId::of::<
        super::fuzzy_query::Intersection<
            DfaWrapper,
            StartsWithAutomaton<super::fuzzy_query::Str, Option<usize>>,
            <DfaWrapper as tantivy_fst::Automaton>::State,
            <StartsWithAutomaton<super::fuzzy_query::Str, Option<usize>> as tantivy_fst::Automaton>::State,
        >,
    >() == automaton.type_id()
        && TypeId::of::<IntersectionState<u32, StartsWithAutomatonState<Option<usize>>>>() == state.type_id()
    {
        let dfa = automaton as *const A
            as *const super::fuzzy_query::Intersection<
            DfaWrapper,
            StartsWithAutomaton<super::fuzzy_query::Str, Option<usize>>,
            <DfaWrapper as tantivy_fst::Automaton>::State,
            <StartsWithAutomaton<super::fuzzy_query::Str, Option<usize>> as tantivy_fst::Automaton>::State,
        >;
        let dfa = unsafe { &*dfa };
        let id = state as *const A::State as *const IntersectionState<u32, StartsWithAutomatonState<Option<usize>>>;
        let id = unsafe { &*id };
        let dist = dfa.automaton_a.0.distance(id.0).to_u8() as f32;
        1.0 / (1.0 + dist)
    } else {
        1.0
    }
}
#[cfg(test)]
mod tests {
    use tantivy_fst::Automaton;

    use super::AutomatonWeight;
    use crate::docset::TERMINATED;
    use crate::query::Weight;
    use crate::schema::{Schema, STRING};
    use crate::{Index, IndexWriter};

    fn create_index() -> crate::Result<Index> {
        let mut schema = Schema::builder();
        let title = schema.add_text_field("title", STRING);
        let index = Index::create_in_ram(schema.build());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
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
        let automaton_weight = AutomatonWeight::new(field, PrefixedByA, None, false);
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
        let automaton_weight = AutomatonWeight::new(field, PrefixedByA, None, false);
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut scorer = automaton_weight.scorer(searcher.segment_reader(0u32), 1.32)?;
        assert_eq!(scorer.doc(), 0u32);
        assert_eq!(scorer.score(), 1.32);
        Ok(())
    }
}
