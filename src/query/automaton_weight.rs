use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};
use std::{fmt, io};

use common::BitSet;
use tantivy_fst::Automaton;

use super::phrase_prefix_query::prefix_end;
use crate::index::SegmentReader;
use crate::postings::TermInfo;
use crate::query::{BitSetDocSet, ConstScorer, Explanation, Scorer, Weight};
use crate::schema::{Field, IndexRecordOption};
use crate::termdict::{TermDictionary, TermStreamer, TermStreamerBuilder, WithoutKeys};
use crate::{DocId, Score, TantivyError};

/// Collects the terms an [`AutomatonWeight`] matched in the term dictionary.
///
/// By default, automaton queries only read the [`TermInfo`] of the matching terms and never copy
/// their bytes. Attaching a sink (e.g. with
/// [`FuzzyTermQuery::set_matched_terms_sink`](crate::query::FuzzyTermQuery::set_matched_terms_sink))
/// makes the same dictionary walk also record the matching terms, at the cost of one copy per
/// matched term. Queries without a sink run exactly as before.
///
/// Terms are recorded each time a scorer is built (once per segment, and again on `explain`) and
/// are deduplicated. Clones share the same store, so a sink can be attached to a query and read
/// after the search.
///
/// For a weight restricted to a JSON path (e.g. a [`FuzzyTermQuery`](crate::query::FuzzyTermQuery)
/// on a JSON field), the recorded terms are the values only, without the JSON path and the type
/// code: for a string value, its text.
#[derive(Clone, Default)]
pub struct MatchedTermsSink {
    terms: Arc<Mutex<BTreeSet<Vec<u8>>>>,
}

impl MatchedTermsSink {
    /// Creates an empty sink.
    pub fn new() -> MatchedTermsSink {
        MatchedTermsSink::default()
    }

    /// Drains the collected terms as raw term dictionary bytes, sorted and deduplicated.
    ///
    /// For `str` fields, these are the UTF-8 bytes of the term's text.
    pub fn take_bytes(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.lock()).into_iter().collect()
    }

    /// Drains the collected terms as strings, sorted and deduplicated.
    ///
    /// Terms that are not valid UTF-8 are dropped; use [`take_bytes`](Self::take_bytes) to keep
    /// them.
    pub fn take_strings(&self) -> Vec<String> {
        std::mem::take(&mut *self.lock())
            .into_iter()
            .filter_map(|bytes| String::from_utf8(bytes).ok())
            .collect()
    }

    /// Returns the number of distinct terms collected since the last drain.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Returns true if no term was collected since the last drain.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn extend(&self, terms: Vec<Vec<u8>>) {
        if !terms.is_empty() {
            self.lock().extend(terms);
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeSet<Vec<u8>>> {
        // Poisoning is ignored: a panic while the lock is held can at worst leave out some terms
        // of one segment, and the set itself stays valid.
        self.terms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl fmt::Debug for MatchedTermsSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MatchedTermsSink")
            .field("len", &self.len())
            .finish()
    }
}

/// A weight struct for Fuzzy Term and Regex Queries
pub struct AutomatonWeight<A> {
    field: Field,
    automaton: Arc<A>,
    // For JSON fields, the term dictionary include terms from all paths.
    // We apply additional filtering based on the given JSON path, when searching within the term
    // dictionary. This prevents terms from unrelated paths from matching the search criteria.
    json_path_bytes: Option<Box<[u8]>>,
    matched_terms_sink: Option<MatchedTermsSink>,
}

impl<A> AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    /// Create a new AutomationWeight
    pub fn new<IntoArcA: Into<Arc<A>>>(field: Field, automaton: IntoArcA) -> AutomatonWeight<A> {
        AutomatonWeight {
            field,
            automaton: automaton.into(),
            json_path_bytes: None,
            matched_terms_sink: None,
        }
    }

    /// Create a new AutomationWeight for a json path
    ///
    /// `json_path_bytes` is a JSON path ending with its end-of-path byte, as it starts the value
    /// of a JSON [`Term`](crate::Term).
    pub fn new_for_json_path<IntoArcA: Into<Arc<A>>>(
        field: Field,
        automaton: IntoArcA,
        json_path_bytes: &[u8],
    ) -> AutomatonWeight<A> {
        AutomatonWeight {
            field,
            automaton: automaton.into(),
            json_path_bytes: Some(json_path_bytes.to_vec().into_boxed_slice()),
            matched_terms_sink: None,
        }
    }

    /// Records the terms this weight matches into `sink`. See [`MatchedTermsSink`].
    pub fn set_matched_terms_sink(&mut self, sink: MatchedTermsSink) {
        self.matched_terms_sink = Some(sink);
    }

    fn automaton_stream_builder<'a>(
        &'a self,
        term_dict: &'a TermDictionary,
    ) -> TermStreamerBuilder<'a, &'a A> {
        let automaton: &A = &self.automaton;
        let mut term_stream_builder = term_dict.search(automaton);

        if let Some(json_path_bytes) = &self.json_path_bytes {
            term_stream_builder = term_stream_builder.ge(json_path_bytes);
            if let Some(end) = prefix_end(json_path_bytes) {
                term_stream_builder = term_stream_builder.lt(&end);
            }
        }

        term_stream_builder
    }

    fn automaton_stream<'a>(
        &'a self,
        term_dict: &'a TermDictionary,
    ) -> io::Result<TermStreamer<'a, &'a A, WithoutKeys>> {
        // Matching terms are only used for their `TermInfo`.
        self.automaton_stream_builder(term_dict)
            .without_keys()
            .into_stream()
    }

    /// Calls `on_term_info` for every matching term, and records the matching terms into the sink
    /// if one is attached.
    fn for_each_matching_term_info(
        &self,
        term_dict: &TermDictionary,
        mut on_term_info: impl FnMut(&TermInfo) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let Some(sink) = &self.matched_terms_sink else {
            let mut term_stream = self.automaton_stream(term_dict)?;
            while term_stream.advance() {
                on_term_info(term_stream.value())?;
            }
            return Ok(());
        };
        let mut term_stream = self.automaton_stream_builder(term_dict).into_stream()?;
        // Collected locally and merged once, so that segments searched in parallel do not contend
        // on the sink during the walk.
        let mut matched_terms = Vec::new();
        while term_stream.advance() {
            matched_terms.push(self.recorded_term(term_stream.key()).to_vec());
            on_term_info(term_stream.value())?;
        }
        sink.extend(matched_terms);
        Ok(())
    }

    /// The part of a matching term dictionary key that is reported to the sink.
    fn recorded_term<'k>(&self, key: &'k [u8]) -> &'k [u8] {
        // Under a JSON path, keys are `[JSON path][JSON_END_OF_PATH][type code][value]`: only the
        // value is recorded.
        let value_start = self
            .json_path_bytes
            .as_ref()
            .map_or(0, |json_path_bytes| json_path_bytes.len() + 1);
        key.get(value_start..).unwrap_or_default()
    }

    /// Returns the term infos that match the automaton
    ///
    /// The matching terms are recorded into the [`MatchedTermsSink`], if one is attached.
    pub fn get_match_term_infos(&self, reader: &SegmentReader) -> crate::Result<Vec<TermInfo>> {
        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        let mut term_infos = Vec::new();
        self.for_each_matching_term_info(term_dict, |term_info| {
            term_infos.push(term_info.clone());
            Ok(())
        })?;
        Ok(term_infos)
    }
}

impl<A> Weight for AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);
        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        self.for_each_matching_term_info(term_dict, |term_info| {
            let mut block_segment_postings = inverted_index
                .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
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
        })?;
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        let const_scorer = ConstScorer::new(doc_bitset, boost);
        Ok(Box::new(const_scorer))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) == doc {
            Ok(Explanation::new("AutomatonScorer", 1.0))
        } else {
            Err(TantivyError::InvalidArgument(
                "Document does not exist".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy_fst::Automaton;

    use super::{AutomatonWeight, MatchedTermsSink};
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
        let automaton_weight = AutomatonWeight::new(field, PrefixedByA);
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
    fn test_automaton_weight_matched_terms_sink() -> crate::Result<()> {
        let index = create_index()?;
        let field = index.schema().get_field("title").unwrap();
        let mut automaton_weight = AutomatonWeight::new(field, PrefixedByA);
        let sink = MatchedTermsSink::new();
        automaton_weight.set_matched_terms_sink(sink.clone());
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut scorer = automaton_weight.scorer(searcher.segment_reader(0u32), 1.0)?;
        assert_eq!(scorer.doc(), 0u32);
        assert_eq!(scorer.advance(), 2u32);
        assert_eq!(scorer.advance(), TERMINATED);
        assert_eq!(
            sink.take_strings(),
            vec!["abc".to_string(), "abcd".to_string()]
        );
        assert!(sink.is_empty());

        let term_infos = automaton_weight.get_match_term_infos(searcher.segment_reader(0u32))?;
        assert_eq!(term_infos.len(), 2);
        assert_eq!(sink.len(), 2);
        Ok(())
    }

    #[test]
    fn test_matched_terms_sink_keeps_non_utf8_bytes() {
        let sink = MatchedTermsSink::new();
        sink.extend(vec![
            vec![0xFF],
            b"b".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
        ]);
        assert_eq!(sink.len(), 3);
        let clone = sink.clone();
        assert_eq!(
            clone.take_bytes(),
            vec![b"a".to_vec(), b"b".to_vec(), vec![0xFF]]
        );
        assert!(sink.is_empty());
        sink.extend(vec![vec![0xFF], b"a".to_vec()]);
        assert_eq!(sink.take_strings(), vec!["a".to_string()]);
    }

    #[test]
    fn test_automaton_weight_boost() -> crate::Result<()> {
        let index = create_index()?;
        let field = index.schema().get_field("title").unwrap();
        let automaton_weight = AutomatonWeight::new(field, PrefixedByA);
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut scorer = automaton_weight.scorer(searcher.segment_reader(0u32), 1.32)?;
        assert_eq!(scorer.doc(), 0u32);
        assert_eq!(scorer.score(), 1.32);
        Ok(())
    }
}
