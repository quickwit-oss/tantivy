use std::fmt;
use std::sync::Arc;

use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA};
use once_cell::sync::OnceCell;
use tantivy_fst::Automaton;

use crate::query::{AutomatonWeight, EnableScoring, MatchedTermsSink, Query, Weight};
use crate::schema::{Term, Type};
use crate::TantivyError::InvalidArgument;

/// The Levenshtein automaton a [`FuzzyTermQuery`] matches terms with.
///
/// Obtained from [`FuzzyTermQuery::automaton`], e.g. to stream the term dictionary
/// and collect the terms the query expands to.
pub struct DfaWrapper(pub(crate) DFA);

impl Automaton for DfaWrapper {
    type State = u32;

    fn start(&self) -> Self::State {
        self.0.initial_state()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        match self.0.distance(*state) {
            Distance::Exact(_) => true,
            Distance::AtLeast(_) => false,
        }
    }

    fn can_match(&self, state: &u32) -> bool {
        *state != levenshtein_automata::SINK_STATE
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0.transition(*state, byte)
    }
}

/// A Fuzzy Query matches all of the documents
/// containing a specific term that is within
/// Levenshtein distance
/// ```rust
/// use tantivy::collector::{Count, TopDocs};
/// use tantivy::query::FuzzyTermQuery;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, Index, IndexWriter, Term};
///
/// fn example() -> tantivy::Result<()> {
///     let mut schema_builder = Schema::builder();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer: IndexWriter = index.writer(15_000_000)?;
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///         ))?;
///         index_writer.add_document(doc!(
///             title => "The Diary of Muadib",
///         ))?;
///         index_writer.add_document(doc!(
///             title => "A Dairy Cow",
///         ))?;
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///         ))?;
///         index_writer.commit()?;
///     }
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///
///     {
///         let term = Term::from_field_text(title, "Diary");
///         let query = FuzzyTermQuery::new(term, 1, true);
///         let (top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2).order_by_score(), Count)).unwrap();
///         assert_eq!(count, 2);
///         assert_eq!(top_docs.len(), 2);
///     }
///
///     Ok(())
/// }
/// # assert!(example().is_ok());
/// ```
#[derive(Clone)]
pub struct FuzzyTermQuery {
    /// What term are we searching
    term: Term,
    /// How many changes are we going to allow
    distance: u8,
    /// Should a transposition cost 1 or 2?
    transposition_cost_one: bool,
    /// is a starts with query
    prefix: bool,
    matched_terms_sink: Option<MatchedTermsSink>,
    max_expansions: Option<usize>,
}

impl fmt::Debug for FuzzyTermQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("FuzzyTermQuery");
        debug_struct
            .field("term", &self.term)
            .field("distance", &self.distance)
            .field("transposition_cost_one", &self.transposition_cost_one)
            .field("prefix", &self.prefix);
        if let Some(sink) = &self.matched_terms_sink {
            debug_struct.field("matched_terms_sink", sink);
        }
        if let Some(cap) = &self.max_expansions {
            debug_struct.field("max_expansions", cap);
        }
        debug_struct.finish()
    }
}

impl FuzzyTermQuery {
    /// Creates a new Fuzzy Query
    pub fn new(term: Term, distance: u8, transposition_cost_one: bool) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            distance,
            transposition_cost_one,
            prefix: false,
            matched_terms_sink: None,
            max_expansions: None,
        }
    }

    /// Creates a new Fuzzy Query of the Term prefix
    pub fn new_prefix(term: Term, distance: u8, transposition_cost_one: bool) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            distance,
            transposition_cost_one,
            prefix: true,
            matched_terms_sink: None,
            max_expansions: None,
        }
    }

    /// Records the terms this query matches in the term dictionary into `sink` while the search
    /// runs. See [`MatchedTermsSink`].
    pub fn set_matched_terms_sink(&mut self, sink: MatchedTermsSink) {
        self.matched_terms_sink = Some(sink);
    }

    /// Uses at most `cap` matching terms per segment: the closest (by edit distance), then the
    /// shortest, then the most frequent. See [`AutomatonWeight::set_max_expansions`].
    ///
    /// Unlike [`RegexPhraseQuery::set_max_expansions`], which returns an error past its limit,
    /// this keeps the selected terms and leaves the others out; the sink, if any, is then marked
    /// truncated.
    ///
    /// [`RegexPhraseQuery::set_max_expansions`]: crate::query::RegexPhraseQuery::set_max_expansions
    pub fn set_max_expansions(&mut self, cap: usize) {
        self.max_expansions = Some(cap);
    }

    /// Returns the automaton this query matches terms with.
    ///
    /// For a JSON term, it matches the term's text only, not its JSON path.
    ///
    /// ```rust
    /// use tantivy::query::{DfaWrapper, FuzzyTermQuery};
    /// use tantivy::schema::{Schema, TEXT};
    /// use tantivy::Term;
    ///
    /// let mut schema_builder = Schema::builder();
    /// let title = schema_builder.add_text_field("title", TEXT);
    /// let query = FuzzyTermQuery::new(Term::from_field_text(title, "diary"), 1, true);
    /// let _automaton: DfaWrapper = query.automaton().unwrap();
    /// ```
    pub fn automaton(&self) -> crate::Result<DfaWrapper> {
        static AUTOMATON_BUILDER: [[OnceCell<LevenshteinAutomatonBuilder>; 2]; 3] = [
            [OnceCell::new(), OnceCell::new()],
            [OnceCell::new(), OnceCell::new()],
            [OnceCell::new(), OnceCell::new()],
        ];

        let automaton_builder = AUTOMATON_BUILDER
            .get(self.distance as usize)
            .ok_or_else(|| {
                InvalidArgument(format!(
                    "Levenshtein distance of {} is not allowed. Choose a value less than {}",
                    self.distance,
                    AUTOMATON_BUILDER.len()
                ))
            })?
            .get(self.transposition_cost_one as usize)
            .unwrap()
            .get_or_init(|| {
                LevenshteinAutomatonBuilder::new(self.distance, self.transposition_cost_one)
            });

        let term_value = self.term.value();

        let term_text = if term_value.typ() == Type::Json {
            if let Some(json_path_type) = term_value.json_path_type() {
                if json_path_type != Type::Str {
                    return Err(InvalidArgument(format!(
                        "The fuzzy term query requires a string path type for a json term. Found \
                         {json_path_type:?}"
                    )));
                }
            }

            std::str::from_utf8(self.term.serialized_value_bytes()).map_err(|_| {
                InvalidArgument(
                    "Failed to convert json term value bytes to utf8 string.".to_string(),
                )
            })?
        } else {
            term_value.as_str().ok_or_else(|| {
                InvalidArgument("The fuzzy term query requires a string term.".to_string())
            })?
        };
        let automaton = if self.prefix {
            automaton_builder.build_prefix_dfa(term_text)
        } else {
            automaton_builder.build_dfa(term_text)
        };
        Ok(DfaWrapper(automaton))
    }

    fn specialized_weight(&self) -> crate::Result<AutomatonWeight<DfaWrapper>> {
        let automaton = Arc::new(self.automaton()?);
        let mut weight = if let Some((json_path_bytes, _)) = self.term.value().as_json() {
            AutomatonWeight::new_for_json_path(
                self.term.field(),
                automaton.clone(),
                json_path_bytes,
            )
        } else {
            AutomatonWeight::new(self.term.field(), automaton.clone())
        };
        if let Some(sink) = &self.matched_terms_sink {
            weight.set_matched_terms_sink(sink.clone());
        }
        if let Some(cap) = self.max_expansions {
            weight.set_max_expansions(cap);
            weight.set_expansion_rank(Arc::new(move |key: &[u8]| match automaton.0.eval(key) {
                Distance::Exact(distance) | Distance::AtLeast(distance) => u32::from(distance),
            }));
        }
        Ok(weight)
    }
}

impl Query for FuzzyTermQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight()?))
    }
}

#[cfg(test)]
mod test {
    use super::FuzzyTermQuery;
    use crate::collector::{Count, TopDocs};
    use crate::indexer::NoMergePolicy;
    use crate::query::{MatchedTermsSink, QueryParser};
    use crate::schema::{Schema, STORED, TEXT};
    use crate::{assert_nearly_equals, Index, IndexWriter, TantivyDocument, Term};

    #[test]
    pub fn test_fuzzy_automaton_streams_expanded_terms() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(title => "The Diary of a Dairy Cow"))?;
        index_writer.add_document(doc!(title => "A Daily Log"))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();

        let query = FuzzyTermQuery::new(Term::from_field_text(title, "diary"), 1, true);
        let automaton = query.automaton()?;
        let inverted_index = searcher.segment_reader(0).inverted_index(title)?;
        let mut stream = inverted_index.terms().search(automaton).into_stream()?;
        let mut terms = Vec::new();
        while stream.advance() {
            terms.push(String::from_utf8(stream.key().to_vec()).unwrap());
        }
        assert_eq!(terms, vec!["dairy", "diary"]);
        assert_eq!(searcher.search(&query, &Count)?, 1);
        Ok(())
    }

    #[test]
    pub fn test_fuzzy_json_path() -> crate::Result<()> {
        // # Defining the schema
        let mut schema_builder = Schema::builder();
        let attributes = schema_builder.add_json_field("attributes", TEXT | STORED);
        let schema = schema_builder.build();

        // # Indexing documents
        let index = Index::create_in_ram(schema.clone());

        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        let doc = TantivyDocument::parse_json(
            &schema,
            r#"{
            "attributes": {
                "a": "japan"
            }
        }"#,
        )?;
        index_writer.add_document(doc)?;
        let doc = TantivyDocument::parse_json(
            &schema,
            r#"{
            "attributes": {
                "aa": "japan"
            }
        }"#,
        )?;
        index_writer.add_document(doc)?;
        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // # Fuzzy search
        let query_parser = QueryParser::for_index(&index, vec![attributes]);

        let get_json_path_term = |query: &str| -> crate::Result<Term> {
            let query = query_parser.parse_query(query)?;
            let mut terms = Vec::new();
            query.query_terms(&mut |term, _| {
                terms.push(term.clone());
            });

            Ok(terms[0].clone())
        };

        // shall not match the first document due to json path mismatch
        {
            let term = get_json_path_term("attributes.aa:japan")?;
            let mut fuzzy_query = FuzzyTermQuery::new(term, 2, true);
            let sink = MatchedTermsSink::new();
            fuzzy_query.set_matched_terms_sink(sink.clone());
            let top_docs =
                searcher.search(&fuzzy_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            assert_eq!(top_docs[0].1.doc_id, 1, "Expected the second document");
            // the JSON path and the type code are not part of the recorded terms
            assert_eq!(sink.take_strings(), vec!["japan".to_string()]);
        }

        // shall match the first document because Levenshtein distance is 1 (substitute 'o' with
        // 'a')
        {
            let term = get_json_path_term("attributes.a:japon")?;

            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs =
                searcher.search(&fuzzy_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            assert_eq!(top_docs[0].1.doc_id, 0, "Expected the first document");
        }

        // shall not match because non-prefix Levenshtein distance is more than 1 (add 'a' and 'n')
        {
            let term = get_json_path_term("attributes.a:jap")?;

            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs =
                searcher.search(&fuzzy_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 0, "Expected no document");
        }

        Ok(())
    }

    #[test]
    pub fn test_fuzzy_term_max_expansions() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        // distance to "abcd": "abxd" 1, "abcde" 1, "abd" 1, "ab" 2, "bd" 2 (with "abcd" absent)
        for word in ["abxd", "abcde", "abd", "abd", "ab", "bd"] {
            index_writer.add_document(doc!(field => word))?;
        }
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let term = Term::from_field_text(field, "abcd");

        let expected_terms: [&[&str]; 4] = [
            &["abd"],
            &["abd", "abxd"],
            &["abcde", "abd", "abxd"],
            &["ab", "abcde", "abd", "abxd"],
        ];
        for (cap, expected) in (1..).zip(expected_terms) {
            let mut query = FuzzyTermQuery::new(term.clone(), 2, true);
            let sink = MatchedTermsSink::new();
            query.set_matched_terms_sink(sink.clone());
            query.set_max_expansions(cap);
            searcher.search(&query, &Count)?;
            assert_eq!(sink.take_strings(), expected, "cap {cap}");
            assert!(sink.is_truncated(), "cap {cap}");
        }

        // a closer term wins even if it is longer and rarer
        let mut query = FuzzyTermQuery::new(Term::from_field_text(field, "abcdx"), 2, true);
        let sink = MatchedTermsSink::new();
        query.set_matched_terms_sink(sink.clone());
        query.set_max_expansions(1);
        assert_eq!(searcher.search(&query, &Count)?, 1);
        assert_eq!(sink.take_strings(), ["abcde"]);

        // the cap applies per segment
        index_writer.add_document(doc!(field => "abcf"))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        let mut query = FuzzyTermQuery::new(term, 2, true);
        let sink = MatchedTermsSink::new();
        query.set_matched_terms_sink(sink.clone());
        query.set_max_expansions(1);
        assert_eq!(searcher.search(&query, &Count)?, 3);
        assert_eq!(sink.take_strings(), ["abcf", "abd"]);
        assert!(sink.is_truncated());
        Ok(())
    }

    #[test]
    pub fn test_fuzzy_term() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let country_field = schema_builder.add_text_field("country", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(
                country_field => "japan",
            ))?;
            index_writer.add_document(doc!(
                country_field => "korea",
            ))?;
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // passes because Levenshtein distance is 1 (substitute 'o' with 'a')
        {
            let term = Term::from_field_text(country_field, "japon");
            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs =
                searcher.search(&fuzzy_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);
        }

        // a sink records the matched terms, and only those
        {
            let term = Term::from_field_text(country_field, "japon");
            let mut fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let sink = MatchedTermsSink::new();
            fuzzy_query.set_matched_terms_sink(sink.clone());
            let top_docs =
                searcher.search(&fuzzy_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            assert_eq!(sink.take_strings(), vec!["japan".to_string()]);
        }

        // fails because non-prefix Levenshtein distance is more than 1 (add 'a' and 'n')
        {
            let term = Term::from_field_text(country_field, "jap");

            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs =
                searcher.search(&fuzzy_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 0, "Expected no document");
        }

        // passes because prefix Levenshtein distance is 0
        {
            let term = Term::from_field_text(country_field, "jap");
            let fuzzy_query = FuzzyTermQuery::new_prefix(term, 1, true);
            let top_docs =
                searcher.search(&fuzzy_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);
        }
        Ok(())
    }

    #[test]
    pub fn test_fuzzy_term_transposition_cost_one() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let country_field = schema_builder.add_text_field("country", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(country_field => "japan"))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let term_jaapn = Term::from_field_text(country_field, "jaapn");
        {
            let fuzzy_query_transposition = FuzzyTermQuery::new(term_jaapn.clone(), 1, true);
            let count = searcher.search(&fuzzy_query_transposition, &Count)?;
            assert_eq!(count, 1);
        }
        {
            let fuzzy_query_transposition = FuzzyTermQuery::new(term_jaapn, 1, false);
            let count = searcher.search(&fuzzy_query_transposition, &Count)?;
            assert_eq!(count, 0);
        }
        Ok(())
    }
}
