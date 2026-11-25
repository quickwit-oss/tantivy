use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
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
    truncated: Arc<AtomicBool>,
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

    /// Returns true if an [expansion cap](AutomatonWeight::set_max_expansions) left out at least
    /// one matching term, in any segment.
    ///
    /// The flag is not reset by [`take_bytes`](Self::take_bytes) or
    /// [`take_strings`](Self::take_strings).
    pub fn is_truncated(&self) -> bool {
        self.truncated.load(AtomicOrdering::Relaxed)
    }

    /// Marks the expansion as truncated, for callers that cap an expansion themselves and report
    /// it through this sink.
    pub fn mark_truncated(&self) {
        self.truncated.store(true, AtomicOrdering::Relaxed);
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
            .field("truncated", &self.is_truncated())
            .finish()
    }
}

/// Ranks a matching term for [`AutomatonWeight::set_max_expansions`]: lower is better.
///
/// It is called with the term dictionary key the automaton matched (for a JSON path weight, this
/// includes the JSON path).
pub type ExpansionRank = Arc<dyn Fn(&[u8]) -> u32 + Send + Sync>;

/// A weight struct for Fuzzy Term and Regex Queries
pub struct AutomatonWeight<A> {
    field: Field,
    automaton: Arc<A>,
    // For JSON fields, the term dictionary include terms from all paths.
    // We apply additional filtering based on the given JSON path, when searching within the term
    // dictionary. This prevents terms from unrelated paths from matching the search criteria.
    json_path_bytes: Option<Box<[u8]>>,
    matched_terms_sink: Option<MatchedTermsSink>,
    max_expansions: Option<usize>,
    expansion_rank: Option<ExpansionRank>,
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
            max_expansions: None,
            expansion_rank: None,
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
            max_expansions: None,
            expansion_rank: None,
        }
    }

    /// Records the terms this weight matches into `sink`. See [`MatchedTermsSink`].
    pub fn set_matched_terms_sink(&mut self, sink: MatchedTermsSink) {
        self.matched_terms_sink = Some(sink);
    }

    /// Caps the number of matching terms used in each segment.
    ///
    /// When the automaton matches more than `cap` terms in a segment's term dictionary, only the
    /// best `cap` of them are used, and an attached [`MatchedTermsSink`] receives only those and
    /// is [marked truncated](MatchedTermsSink::is_truncated). Without a cap, every matching term
    /// is used.
    ///
    /// The best terms are those with the lowest [rank](Self::set_expansion_rank), then the
    /// fewest characters, then the highest document frequency, then the lowest bytes. The number
    /// of characters is the number of bytes that are not UTF-8 continuation bytes, which is the
    /// number of `char`s for valid UTF-8.
    ///
    /// The cap applies to each segment separately, like the max expansion error of
    /// [`RegexPhraseQuery`](crate::query::RegexPhraseQuery): an index with several segments may
    /// use up to `cap` distinct terms per segment, and which terms are used depends on how the
    /// documents are split into segments. The whole matching expansion is still walked, but only
    /// `cap` terms are held in memory at a time.
    ///
    /// A cap of `0` matches no document; the sink is then marked truncated if any term matched.
    ///
    /// Unlike [`RegexPhraseQuery::set_max_expansions`], which returns an error past its limit,
    /// this keeps the selected terms and leaves the others out; the sink, if any, is then marked
    /// truncated.
    ///
    /// [`RegexPhraseQuery::set_max_expansions`]: crate::query::RegexPhraseQuery::set_max_expansions
    pub fn set_max_expansions(&mut self, cap: usize) {
        self.max_expansions = Some(cap);
    }

    /// Sets the primary key [`set_max_expansions`](Self::set_max_expansions) selects terms by.
    /// Without a rank, every term has rank `0`. It has no effect without a cap.
    pub fn set_expansion_rank(&mut self, rank: ExpansionRank) {
        self.expansion_rank = Some(rank);
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
        if let Some(cap) = self.max_expansions {
            return self.for_each_capped_term_info(term_dict, cap, on_term_info);
        }
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

    /// Like [`for_each_matching_term_info`](Self::for_each_matching_term_info), for the best `cap`
    /// matching terms only.
    fn for_each_capped_term_info(
        &self,
        term_dict: &TermDictionary,
        cap: usize,
        mut on_term_info: impl FnMut(&TermInfo) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let mut term_stream = self.automaton_stream_builder(term_dict).into_stream()?;
        let mut best_terms = BestTerms::new(cap);
        while term_stream.advance() {
            if cap == 0 {
                best_terms.truncated = true;
                break;
            }
            let key = term_stream.key();
            let rank = self.expansion_rank.as_ref().map_or(0, |rank| rank(key));
            let num_chars = num_chars(self.recorded_term(key));
            best_terms.offer(rank, num_chars, key, term_stream.value());
        }
        let truncated = best_terms.truncated;
        let kept_terms = best_terms.into_sorted_vec();
        if let Some(sink) = &self.matched_terms_sink {
            if truncated {
                sink.mark_truncated();
            }
            sink.extend(
                kept_terms
                    .iter()
                    .map(|candidate| self.recorded_term(&candidate.key).to_vec())
                    .collect(),
            );
        }
        for candidate in &kept_terms {
            on_term_info(&candidate.term_info)?;
        }
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

/// The number of bytes of `term` that are not UTF-8 continuation bytes: the number of `char`s if
/// `term` is valid UTF-8.
fn num_chars(term: &[u8]) -> usize {
    term.iter().filter(|&&byte| byte & 0xC0 != 0x80).count()
}

/// A matching term competing for a slot of a capped expansion.
struct ExpansionCandidate {
    rank: u32,
    num_chars: usize,
    key: Vec<u8>,
    term_info: TermInfo,
}

impl ExpansionCandidate {
    /// Lower is better.
    fn sort_key(&self) -> (u32, usize, Reverse<u32>, &[u8]) {
        (
            self.rank,
            self.num_chars,
            Reverse(self.term_info.doc_freq),
            &self.key,
        )
    }
}

impl PartialEq for ExpansionCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key() == other.sort_key()
    }
}

impl Eq for ExpansionCandidate {}

impl PartialOrd for ExpansionCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExpansionCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sort_key().cmp(&other.sort_key())
    }
}

/// Keeps the best `cap` terms offered to it, in `O(cap)` memory.
struct BestTerms {
    cap: usize,
    // A max-heap: the worst kept term is on top, ready to be replaced.
    heap: BinaryHeap<ExpansionCandidate>,
    truncated: bool,
}

impl BestTerms {
    fn new(cap: usize) -> BestTerms {
        BestTerms {
            cap,
            heap: BinaryHeap::new(),
            truncated: false,
        }
    }

    fn offer(&mut self, rank: u32, num_chars: usize, key: &[u8], term_info: &TermInfo) {
        if self.heap.len() < self.cap {
            self.heap.push(ExpansionCandidate {
                rank,
                num_chars,
                key: key.to_vec(),
                term_info: term_info.clone(),
            });
            return;
        }
        self.truncated = true;
        let Some(mut worst) = self.heap.peek_mut() else {
            return;
        };
        // Compared before copying the key: most terms of a broad expansion are rejected here.
        if (rank, num_chars, Reverse(term_info.doc_freq), key) < worst.sort_key() {
            worst.rank = rank;
            worst.num_chars = num_chars;
            worst.key.clear();
            worst.key.extend_from_slice(key);
            worst.term_info = term_info.clone();
        }
    }

    /// The kept terms, best first.
    fn into_sorted_vec(self) -> Vec<ExpansionCandidate> {
        self.heap.into_sorted_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use proptest::prelude::*;
    use tantivy_fst::automaton::AlwaysMatch;
    use tantivy_fst::Automaton;

    use super::{num_chars, AutomatonWeight, BestTerms, MatchedTermsSink};
    use crate::docset::{DocSet, TERMINATED};
    use crate::index::SegmentReader;
    use crate::postings::TermInfo;
    use crate::query::Weight;
    use crate::schema::{Schema, INDEXED, STRING};
    use crate::{DocId, Index, IndexWriter};

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

    fn matching_docs<A>(weight: &AutomatonWeight<A>, reader: &SegmentReader) -> Vec<DocId>
    where
        A: Automaton + Send + Sync + 'static,
        A::State: Clone,
    {
        let mut scorer = weight.scorer(reader, 1.0).unwrap();
        let mut docs = Vec::new();
        while scorer.doc() != TERMINATED {
            docs.push(scorer.doc());
            scorer.advance();
        }
        docs
    }

    #[test]
    fn test_automaton_weight_max_expansions() -> crate::Result<()> {
        let mut schema = Schema::builder();
        let title = schema.add_text_field("title", STRING);
        let index = Index::create_in_ram(schema.build());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        // docs:          0      1      2      3      4      5      6       7
        for word in ["abcd", "abe", "abd", "abc", "abd", "abe", "ab\u{e9}", "ab"] {
            index_writer.add_document(doc!(title => word))?;
        }
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);

        // Fewest chars first ("ab\u{e9}" is 3 chars in 4 bytes), then the highest doc
        // frequency, then bytes.
        let expected: [(&[&str], &[DocId]); 7] = [
            (&[], &[]),
            (&["ab"], &[7]),
            (&["ab", "abd"], &[2, 4, 7]),
            (&["ab", "abd", "abe"], &[1, 2, 4, 5, 7]),
            (&["ab", "abc", "abd", "abe"], &[1, 2, 3, 4, 5, 7]),
            (
                &["ab", "abc", "abd", "abe", "ab\u{e9}"],
                &[1, 2, 3, 4, 5, 6, 7],
            ),
            (
                &["ab", "abc", "abcd", "abd", "abe", "ab\u{e9}"],
                &[0, 1, 2, 3, 4, 5, 6, 7],
            ),
        ];
        for (cap, (expected_terms, expected_docs)) in expected.into_iter().enumerate() {
            let mut automaton_weight = AutomatonWeight::new(title, PrefixedByA);
            let sink = MatchedTermsSink::new();
            automaton_weight.set_matched_terms_sink(sink.clone());
            automaton_weight.set_max_expansions(cap);
            assert_eq!(
                matching_docs(&automaton_weight, segment_reader),
                expected_docs,
                "cap {cap}"
            );
            assert_eq!(sink.take_strings(), expected_terms, "cap {cap}");
            assert_eq!(sink.is_truncated(), cap < 6, "cap {cap}");
            let term_infos = automaton_weight.get_match_term_infos(segment_reader)?;
            assert_eq!(term_infos.len(), expected_terms.len(), "cap {cap}");
        }
        Ok(())
    }

    #[test]
    fn test_automaton_weight_max_expansions_zero() -> crate::Result<()> {
        let index = create_index()?;
        let field = index.schema().get_field("title").unwrap();
        let searcher = index.reader()?.searcher();
        let mut automaton_weight = AutomatonWeight::new(field, PrefixedByA);
        automaton_weight.set_max_expansions(0);
        assert!(matching_docs(&automaton_weight, searcher.segment_reader(0)).is_empty());

        let sink = MatchedTermsSink::new();
        automaton_weight.set_matched_terms_sink(sink.clone());
        assert!(matching_docs(&automaton_weight, searcher.segment_reader(0)).is_empty());
        assert!(sink.is_empty());
        assert!(sink.is_truncated());
        Ok(())
    }

    #[test]
    fn test_automaton_weight_max_expansions_rank_first() -> crate::Result<()> {
        let index = create_index()?;
        let field = index.schema().get_field("title").unwrap();
        let searcher = index.reader()?.searcher();
        let mut automaton_weight = AutomatonWeight::new(field, PrefixedByA);
        let sink = MatchedTermsSink::new();
        automaton_weight.set_matched_terms_sink(sink.clone());
        automaton_weight.set_max_expansions(1);
        automaton_weight.set_expansion_rank(Arc::new(|key: &[u8]| u32::from(key != b"abcd")));
        assert_eq!(
            matching_docs(&automaton_weight, searcher.segment_reader(0)),
            [2]
        );
        assert_eq!(sink.take_strings(), ["abcd"]);
        assert!(sink.is_truncated());
        Ok(())
    }

    #[test]
    fn test_automaton_weight_max_expansions_non_utf8_terms() -> crate::Result<()> {
        let mut schema = Schema::builder();
        let bytes_field = schema.add_bytes_field("bytes", INDEXED);
        let index = Index::create_in_ram(schema.build());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        let terms: [&[u8]; 5] = [&[0x41], &[0x80], &[0xC3], &[0xC3, 0x41], &[0xFF]];
        for term in terms {
            index_writer.add_document(doc!(bytes_field => term.to_vec()))?;
        }
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();

        let expected: [&[&[u8]]; 5] = [
            &[&[0x80]],
            &[&[0x41], &[0x80]],
            &[&[0x41], &[0x80], &[0xC3]],
            &[&[0x41], &[0x80], &[0xC3], &[0xFF]],
            &[&[0x41], &[0x80], &[0xC3], &[0xC3, 0x41], &[0xFF]],
        ];
        for (cap, expected_terms) in (1..).zip(expected) {
            let mut automaton_weight = AutomatonWeight::new(bytes_field, AlwaysMatch);
            let sink = MatchedTermsSink::new();
            automaton_weight.set_matched_terms_sink(sink.clone());
            automaton_weight.set_max_expansions(cap);
            let num_docs = matching_docs(&automaton_weight, searcher.segment_reader(0)).len();
            assert_eq!(num_docs, cap, "cap {cap}");
            assert_eq!(sink.take_bytes(), expected_terms, "cap {cap}");
        }
        Ok(())
    }

    #[test]
    fn test_num_chars() {
        assert_eq!(num_chars(b""), 0);
        assert_eq!(num_chars("abc".as_bytes()), 3);
        assert_eq!(num_chars("e\u{301}\u{e9}\u{1F600}".as_bytes()), 4);
        // Not UTF-8: every byte that is not a continuation byte counts as one char.
        assert_eq!(num_chars(&[0x41]), 1);
        assert_eq!(num_chars(&[0x80]), 0);
        assert_eq!(num_chars(&[0xC3]), 1);
        assert_eq!(num_chars(&[0xC3, 0x41]), 2);
        assert_eq!(num_chars(&[0xFF]), 1);
    }

    proptest! {
        #[test]
        fn test_best_terms_keeps_the_best(
            terms in proptest::collection::btree_map(
                proptest::collection::vec(any::<u8>(), 0..4),
                (0u32..3, 0u32..3),
                0..40,
            ),
            cap in 0usize..45,
        ) {
            let mut best_terms = BestTerms::new(cap);
            let mut all_terms = Vec::new();
            for (key, (rank, doc_freq)) in &terms {
                let term_info = TermInfo {
                    doc_freq: *doc_freq,
                    ..TermInfo::default()
                };
                best_terms.offer(*rank, num_chars(key), key, &term_info);
                all_terms.push((*rank, num_chars(key), std::cmp::Reverse(*doc_freq), key.clone()));
            }
            all_terms.sort();
            prop_assert_eq!(best_terms.truncated, terms.len() > cap);
            let kept: Vec<Vec<u8>> = best_terms
                .into_sorted_vec()
                .into_iter()
                .map(|candidate| candidate.key)
                .collect();
            let expected: Vec<Vec<u8>> = all_terms
                .into_iter()
                .take(cap)
                .map(|(_, _, _, key)| key)
                .collect();
            prop_assert_eq!(kept, expected);
        }
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
