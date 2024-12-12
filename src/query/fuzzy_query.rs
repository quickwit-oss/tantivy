use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA};
use once_cell::sync::OnceCell;
use tantivy_fst::Automaton;

use crate::query::{AutomatonWeight, EnableScoring, Query, Weight};
use crate::schema::{Term, Type};
use crate::TantivyError::InvalidArgument;

#[derive(Clone, Debug)]
pub(crate) struct Str {
    string: Vec<u8>,
}

impl Str {
    /// Constructs automaton that matches an exact string.
    #[inline]
    pub fn new(string: &str) -> Str {
        Str {
            string: string.as_bytes().to_vec(),
        }
    }
}

impl Automaton for Str {
    type State = Option<usize>;

    #[inline]
    fn start(&self) -> Option<usize> {
        Some(0)
    }

    #[inline]
    fn is_match(&self, pos: &Option<usize>) -> bool {
        *pos == Some(self.string.len())
    }

    #[inline]
    fn can_match(&self, pos: &Option<usize>) -> bool {
        pos.is_some()
    }

    #[inline]
    fn accept(&self, pos: &Option<usize>, byte: u8) -> Option<usize> {
        // if we aren't already past the end...
        if let Some(pos) = *pos {
            // and there is still a matching byte at the current position...
            if self.string.get(pos).cloned() == Some(byte) {
                // then move forward
                return Some(pos + 1);
            }
        }
        // otherwise we're either past the end or didn't match the byte
        None
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Intersection<A, B, StateA, StateB>
where
    A: Automaton<State = StateA>,
    B: Automaton<State = StateB>,
    StateA: Clone,
    StateB: Clone,
{
    pub automaton_a: A,
    pub automaton_b: B,
}

/// The `Automaton` state for `Intersection<A, B>`.
#[derive(Clone, Debug)]
pub(crate) struct IntersectionState<A: Clone, B: Clone>(pub A, pub B);

impl<A, B, StateA, StateB> Automaton for Intersection<A, B, StateA, StateB>
where
    A: Automaton<State = StateA>,
    B: Automaton<State = StateB>,
    StateA: Clone,
    StateB: Clone,
{
    type State = IntersectionState<StateA, StateB>;

    fn start(&self) -> Self::State {
        IntersectionState(self.automaton_a.start(), self.automaton_b.start())
    }

    fn is_match(&self, state: &Self::State) -> bool {
        self.automaton_a.is_match(&state.0) && self.automaton_b.is_match(&state.1)
    }

    fn can_match(&self, state: &Self::State) -> bool {
        self.automaton_a.can_match(&state.0) && self.automaton_b.can_match(&state.1)
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        self.automaton_a.will_always_match(&state.0) && self.automaton_b.will_always_match(&state.1)
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        IntersectionState(
            self.automaton_a.accept(&state.0, byte),
            self.automaton_b.accept(&state.1, byte),
        )
    }
}

#[derive(Clone, Debug)]
pub struct StartsWithAutomaton<A, StateA>
where
    A: Automaton<State = StateA>,
    StateA: Clone,
{
    pub automaton: A,
}

/// The `Automaton` state for `StartsWith<A>`.
#[derive(Clone, Debug)]
pub struct StartsWithAutomatonState<A: Clone>(StartsWithAutomatonStateInternal<A>);

#[derive(Clone, Debug)]
enum StartsWithAutomatonStateInternal<A: Clone> {
    Done,
    Running(A),
}

impl<A, StateA> Automaton for StartsWithAutomaton<A, StateA>
where
    A: Automaton<State = StateA>,
    StateA: Clone,
{
    type State = StartsWithAutomatonState<StateA>;

    fn start(&self) -> Self::State {
        StartsWithAutomatonState({
            let inner = self.automaton.start();
            if self.automaton.is_match(&inner) {
                StartsWithAutomatonStateInternal::Done
            } else {
                StartsWithAutomatonStateInternal::Running(inner)
            }
        })
    }

    fn is_match(&self, state: &Self::State) -> bool {
        match state.0 {
            StartsWithAutomatonStateInternal::Done => true,
            StartsWithAutomatonStateInternal::Running(_) => false,
        }
    }

    fn can_match(&self, state: &Self::State) -> bool {
        match state.0 {
            StartsWithAutomatonStateInternal::Done => true,
            StartsWithAutomatonStateInternal::Running(ref inner) => self.automaton.can_match(inner),
        }
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        match state.0 {
            StartsWithAutomatonStateInternal::Done => true,
            StartsWithAutomatonStateInternal::Running(_) => false,
        }
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        StartsWithAutomatonState(match state.0 {
            StartsWithAutomatonStateInternal::Done => StartsWithAutomatonStateInternal::Done,
            StartsWithAutomatonStateInternal::Running(ref inner) => {
                let next_inner = self.automaton.accept(inner, byte);
                if self.automaton.is_match(&next_inner) {
                    StartsWithAutomatonStateInternal::Done
                } else {
                    StartsWithAutomatonStateInternal::Running(next_inner)
                }
            }
        })
    }
}

pub(crate) struct DfaWrapper(pub DFA);

const DEFAULT_MAX_EXPANSIONS: u32 = 50;

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
///         let (top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count)).unwrap();
///         assert_eq!(count, 2);
///         assert_eq!(top_docs.len(), 2);
///     }
///
///     Ok(())
/// }
/// # assert!(example().is_ok());
/// ```
#[derive(Debug, Clone)]
pub struct FuzzyTermQuery {
    /// What term are we searching
    term: Term,
    /// How many changes are we going to allow
    distance: u8,
    /// Should a transposition cost 1 or 2?
    transposition_cost_one: bool,
    /// is a starts with query
    prefix: bool,
    /// max expansions allowed
    max_expansions: Option<u32>,
    prefix_length: Option<usize>,
    /// score based on edit distance
    fuzzy_scoring: bool,
}

impl FuzzyTermQuery {
    /// Creates a new Fuzzy Query
    pub fn new(term: Term, distance: u8, transposition_cost_one: bool) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            distance,
            transposition_cost_one,
            prefix: false,
            max_expansions: Some(DEFAULT_MAX_EXPANSIONS),
            prefix_length: None,
            fuzzy_scoring: true,
        }
    }

    /// Creates a new Fuzzy Query of the Term prefix
    pub fn new_prefix(term: Term, distance: u8, transposition_cost_one: bool) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            distance,
            transposition_cost_one,
            prefix: true,
            max_expansions: Some(DEFAULT_MAX_EXPANSIONS),
            prefix_length: None,
            fuzzy_scoring: true,
        }
    }

    /// maximum expansions to allow for fuzzy match
    pub fn set_max_expansions(&mut self, max_expansions: Option<u32>) {
        self.max_expansions = max_expansions;
    }

    /// enable/disable fuzzy scoring
    pub fn set_fuzzy_scoring(&mut self, fuzzy_scoring: bool) {
        self.fuzzy_scoring = fuzzy_scoring;
    }

    /// prefix length to allow for fuzzy match
    pub fn set_prefix_length(&mut self, prefix_len: Option<usize>) {
        self.prefix_length = prefix_len;
    }

    fn specialized_weight(&self) -> crate::Result<AutomatonWeight<DfaWrapper>> {
        static AUTOMATON_BUILDER: [[OnceCell<LevenshteinAutomatonBuilder>; 2]; 4] = [
            [OnceCell::new(), OnceCell::new()],
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
            automaton_builder.build_prefix_dfa(&term_text)
        } else {
            automaton_builder.build_dfa(&term_text)
        };

        if let Some((json_path_bytes, _)) = term_value.as_json() {
            Ok(AutomatonWeight::new_for_json_path(
                self.term.field(),
                DfaWrapper(automaton),
                json_path_bytes,
                self.max_expansions,
                self.fuzzy_scoring,
            ))
        } else {
            Ok(AutomatonWeight::new(
                self.term.field(),
                DfaWrapper(automaton),
                self.max_expansions,
                self.fuzzy_scoring,
            ))
        }
    }

    fn prefix_weight(
        &self,
    ) -> crate::Result<
        AutomatonWeight<
            Intersection<
                DfaWrapper,
                StartsWithAutomaton<Str, Option<usize>>,
                <DfaWrapper as tantivy_fst::Automaton>::State,
                <StartsWithAutomaton<Str, Option<usize>> as tantivy_fst::Automaton>::State,
            >,
        >,
    > {
        static AUTOMATON_BUILDER: [[OnceCell<LevenshteinAutomatonBuilder>; 2]; 4] = [
            [OnceCell::new(), OnceCell::new()],
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

        let prefix_len = self.prefix_length.unwrap_or(0);

        let automaton = if self.prefix {
            automaton_builder.build_prefix_dfa(&term_text)
        } else {
            automaton_builder.build_dfa(&term_text)
        };

        let prefix_text: &str = &term_text[..prefix_len];
        let prefix_automaton = StartsWithAutomaton {
            automaton: Str::new(prefix_text),
        };

        if let Some((json_path_bytes, _)) = term_value.as_json() {
            Ok(AutomatonWeight::new_for_json_path(
                self.term.field(),
                Intersection {
                    automaton_a: DfaWrapper(automaton),
                    automaton_b: prefix_automaton,
                },
                json_path_bytes,
                self.max_expansions,
                self.fuzzy_scoring,
            ))
        } else {
            Ok(AutomatonWeight::new(
                self.term.field(),
                Intersection {
                    automaton_a: DfaWrapper(automaton),
                    automaton_b: prefix_automaton,
                },
                self.max_expansions,
                self.fuzzy_scoring,
            ))
        }
    }
}

impl Query for FuzzyTermQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        match self.prefix_length {
            Some(prefix_len) => {
                if prefix_len > 0 {
                    Ok(Box::new(self.prefix_weight()?))
                } else {
                    Ok(Box::new(self.specialized_weight()?))
                }
            }
            None => Ok(Box::new(self.specialized_weight()?)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::FuzzyTermQuery;
    use crate::collector::{Count, TopDocs};
    use crate::indexer::NoMergePolicy;
    use crate::query::QueryParser;
    use crate::schema::{Schema, STORED, TEXT};
    use crate::{assert_nearly_equals, Index, IndexWriter, TantivyDocument, Term};

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
            let fuzzy_query = FuzzyTermQuery::new(term, 2, true);
            let top_docs = searcher.search(&fuzzy_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            assert_eq!(top_docs[0].1.doc_id, 1, "Expected the second document");
        }

        // shall match the first document because Levenshtein distance is 1 (substitute 'o' with
        // 'a')
        {
            let term = get_json_path_term("attributes.a:japon")?;

            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs = searcher.search(&fuzzy_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            assert_eq!(top_docs[0].1.doc_id, 0, "Expected the first document");
        }

        // shall not match because non-prefix Levenshtein distance is more than 1 (add 'a' and 'n')
        {
            let term = get_json_path_term("attributes.a:jap")?;

            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs = searcher.search(&fuzzy_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 0, "Expected no document");
        }

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
            let top_docs = searcher.search(&fuzzy_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(0.5, score);
        }

        // fails because non-prefix Levenshtein distance is more than 1 (add 'a' and 'n')
        {
            let term = Term::from_field_text(country_field, "jap");

            let fuzzy_query = FuzzyTermQuery::new(term, 1, true);
            let top_docs = searcher.search(&fuzzy_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 0, "Expected no document");
        }

        // passes because prefix Levenshtein distance is 0
        {
            let term = Term::from_field_text(country_field, "jap");
            let fuzzy_query = FuzzyTermQuery::new_prefix(term, 1, true);
            let top_docs = searcher.search(&fuzzy_query, &TopDocs::with_limit(2))?;
            assert_eq!(top_docs.len(), 1, "Expected only 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(0.5, score);
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
