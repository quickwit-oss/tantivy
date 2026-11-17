use std::io;
use std::sync::Arc;

use common::BitSet;
use tantivy_fst::Automaton;

use super::phrase_prefix_query::prefix_end;
use crate::index::SegmentReader;
use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
use crate::postings::TermInfo;
use crate::query::{BitSetDocSet, ConstScorer, Explanation, Scorer, VecDocSet, Weight};
use crate::schema::{Field, IndexRecordOption};
use crate::termdict::{TermDictionary, TermStreamer};
use crate::{DocId, Score, TantivyError};

/// A weight struct for Fuzzy Term and Regex Queries
pub struct AutomatonWeight<A> {
    field: Field,
    automaton: Arc<A>,
    // For JSON fields, the term dictionary include terms from all paths.
    // We apply additional filtering based on the given JSON path, when searching within the term
    // dictionary. This prevents terms from unrelated paths from matching the search criteria.
    json_path_bytes: Option<Box<[u8]>>,
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
        }
    }

    /// Create a new AutomationWeight for a json path
    pub fn new_for_json_path<IntoArcA: Into<Arc<A>>>(
        field: Field,
        automaton: IntoArcA,
        json_path_bytes: &[u8],
    ) -> AutomatonWeight<A> {
        AutomatonWeight {
            field,
            automaton: automaton.into(),
            json_path_bytes: Some(json_path_bytes.to_vec().into_boxed_slice()),
        }
    }

    fn automaton_stream<'a>(
        &'a self,
        term_dict: &'a TermDictionary,
    ) -> io::Result<TermStreamer<'a, &'a A>> {
        let automaton: &A = &self.automaton;
        let mut term_stream_builder = term_dict.search(automaton);

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
}

/// Accumulates the doc ids matched by an automaton.
///
/// Starts as a plain vector, and only switches to a dense bitset (whose cost is
/// proportional to `max_doc`, regardless of the number of matches) once the
/// number of collected doc ids makes the bitset the cheaper representation.
enum MatchingDocs {
    Sparse {
        docs: Vec<DocId>,
        max_doc: DocId,
        dense_threshold: usize,
    },
    Dense(BitSet),
}

impl MatchingDocs {
    fn new(max_doc: DocId) -> MatchingDocs {
        // A bitset costs `max_doc / 8` bytes to allocate and `max_doc / 64` words to scan.
        // Past `max_doc / 64` doc ids, the vector would use more than half the memory of
        // the bitset and require an increasingly expensive sort: switch to dense.
        MatchingDocs::Sparse {
            docs: Vec::new(),
            max_doc,
            dense_threshold: max_doc as usize / 64,
        }
    }

    #[inline]
    fn extend(&mut self, new_docs: &[DocId]) {
        match self {
            MatchingDocs::Sparse {
                docs,
                max_doc,
                dense_threshold,
            } => {
                docs.extend_from_slice(new_docs);
                if docs.len() > *dense_threshold {
                    let mut doc_bitset = BitSet::with_max_value(*max_doc);
                    for &doc in docs.iter() {
                        doc_bitset.insert(doc);
                    }
                    *self = MatchingDocs::Dense(doc_bitset);
                }
            }
            MatchingDocs::Dense(doc_bitset) => {
                for &doc in new_docs {
                    doc_bitset.insert(doc);
                }
            }
        }
    }
}

impl<A> Weight for AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        let mut term_stream = self.automaton_stream(term_dict)?;
        let mut matching_docs = MatchingDocs::new(max_doc);
        let mut small_postings_buffer = [0u32; COMPRESSION_BLOCK_SIZE];
        while term_stream.advance() {
            let term_info = term_stream.value();
            if (term_info.doc_freq as usize) < COMPRESSION_BLOCK_SIZE {
                // Fast path for small posting lists (the vast majority of terms for
                // n-gram/suffix-like tokenizers): decode the single VInt block directly
                // instead of building a `BlockSegmentPostings` for every term.
                let num_docs = inverted_index
                    .read_small_postings_docs(term_info, &mut small_postings_buffer)?;
                matching_docs.extend(&small_postings_buffer[..num_docs]);
                continue;
            }
            let mut block_segment_postings = inverted_index
                .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            loop {
                let docs = block_segment_postings.docs();
                if docs.is_empty() {
                    break;
                }
                matching_docs.extend(docs);
                block_segment_postings.advance();
            }
        }
        let scorer: Box<dyn Scorer> = match matching_docs {
            MatchingDocs::Sparse { mut docs, .. } => {
                docs.sort_unstable();
                docs.dedup();
                Box::new(ConstScorer::new(VecDocSet::from(docs), boost))
            }
            MatchingDocs::Dense(doc_bitset) => {
                Box::new(ConstScorer::new(BitSetDocSet::from(doc_bitset), boost))
            }
        };
        Ok(scorer)
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

    fn create_index_with_sparse_and_dense_terms() -> crate::Result<(Index, Vec<Vec<String>>)> {
        let mut schema = Schema::builder();
        let title = schema.add_text_field("title", STRING);
        let index = Index::create_in_ram(schema.build());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        let mut all_terms = Vec::new();
        for i in 0..5_000u32 {
            let mut terms = vec![
                format!("a{}", i % 300),
                format!("d{}", i % 300),
                format!("e{}", i % 300),
            ];
            if i % 2 == 0 {
                // doc_freq >= 128: goes through the block postings path.
                terms.push("abig".to_string());
            }
            let mut doc = crate::TantivyDocument::default();
            for term in &terms {
                doc.add_text(title, term);
            }
            index_writer.add_document(doc)?;
            all_terms.push(terms);
        }
        index_writer.commit()?;
        Ok((index, all_terms))
    }

    #[test]
    fn test_automaton_weight_sparse_and_dense_match_brute_force() -> crate::Result<()> {
        use std::collections::BTreeSet;

        use crate::collector::DocSetCollector;
        use crate::query::{Query, RegexQuery};

        let (index, all_terms) = create_index_with_sparse_and_dense_terms()?;
        let field = index.schema().get_field("title").unwrap();
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        for pattern in [
            "a1",         // single small term: sparse
            "[de]5",      // two small terms matching the same docs: sparse with duplicates
            "a1.*",       // many small terms: switches to dense
            "abig",       // single large term: dense
            "a.*",        // everything, with duplicates
            "[de]5|abig", // mix
            "zzz",        // no match
        ] {
            let regex = regex::Regex::new(&format!("^(?:{pattern})$")).unwrap();
            let expected: BTreeSet<u32> = all_terms
                .iter()
                .enumerate()
                .filter(|(_, terms)| terms.iter().any(|term| regex.is_match(term)))
                .map(|(doc, _)| doc as u32)
                .collect();
            let query = RegexQuery::from_pattern(pattern, field)?;
            let docs: BTreeSet<u32> = searcher
                .search(&query, &DocSetCollector)?
                .into_iter()
                .map(|doc_address| doc_address.doc_id)
                .collect();
            assert_eq!(docs, expected, "pattern {pattern}");

            // Exercise advance/seek on the scorer as well.
            let weight = query.weight(crate::query::EnableScoring::disabled_from_searcher(
                &searcher,
            ))?;
            let mut scorer = weight.scorer(searcher.segment_reader(0u32), 1.0)?;
            let expected: Vec<u32> = expected.into_iter().collect();
            for &target in expected.iter().step_by(7) {
                if target < scorer.doc() {
                    continue;
                }
                assert_eq!(scorer.seek(target), target, "pattern {pattern}");
            }
            let last = expected.last().copied().unwrap_or(0);
            if scorer.doc() <= last + 1 {
                assert_eq!(scorer.seek(last + 1), TERMINATED);
            }
        }
        Ok(())
    }
}
