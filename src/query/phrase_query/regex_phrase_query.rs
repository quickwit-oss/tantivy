use std::fmt;
use std::sync::Arc;

use once_cell::sync::OnceCell;
use tantivy_fst::{Automaton, Regex};

use super::regex_phrase_weight::RegexPhraseWeight;
use crate::query::bm25::Bm25Weight;
use crate::query::{EnableScoring, Query, Weight};
use crate::schema::{Field, IndexRecordOption, Term, Type};

/// `RegexPhraseQuery` matches a specific sequence of regex queries.
///
/// For instance, the phrase query for `"pa.* time"` will match
/// the sentence:
///
/// **Alan just got a part time job.**
///
/// On the other hand it will not match the sentence.
///
/// **This is my favorite part of the job.**
///
/// [Slop](RegexPhraseQuery::set_slop) allows leniency in term proximity
/// for some performance trade-off.
///
/// Using a `RegexPhraseQuery` on a field requires positions
/// to be indexed for this field.
///
/// The term automaton type `A` defaults to [`Regex`]. Any other
/// [`Automaton`] (e.g. a DFA built by another regex engine) can be used
/// through [`RegexPhraseQuery::from_automata`].
pub struct RegexPhraseQuery<A = Regex> {
    field: Field,
    phrase_terms: Vec<(usize, String)>,
    slop: u32,
    max_expansions: u32,
    automata: CompiledAutomata<A>,
}

impl<A> Clone for RegexPhraseQuery<A> {
    fn clone(&self) -> Self {
        RegexPhraseQuery {
            field: self.field,
            phrase_terms: self.phrase_terms.clone(),
            slop: self.slop,
            max_expansions: self.max_expansions,
            automata: self.automata.clone(),
        }
    }
}

impl<A> fmt::Debug for RegexPhraseQuery<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegexPhraseQuery")
            .field("field", &self.field)
            .field("phrase_terms", &self.phrase_terms)
            .field("slop", &self.slop)
            .field("max_expansions", &self.max_expansions)
            .field("automata", &self.automata)
            .finish()
    }
}

/// The automata of `phrase_terms`: given up front, or built on first use by `compile`.
/// Its `Debug` omits the automata.
struct CompiledAutomata<A> {
    automata: OnceCell<Vec<Arc<A>>>,
    compile: Option<fn(&str) -> crate::Result<A>>,
}

impl<A> Clone for CompiledAutomata<A> {
    fn clone(&self) -> Self {
        CompiledAutomata {
            automata: self.automata.clone(),
            compile: self.compile,
        }
    }
}

impl<A> fmt::Debug for CompiledAutomata<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.automata.get().is_some() {
            "CompiledAutomata(compiled)"
        } else {
            "CompiledAutomata(pending)"
        })
    }
}

fn compile_regex(pattern: &str) -> crate::Result<Regex> {
    Regex::new(pattern)
        .map_err(|e| crate::TantivyError::InvalidArgument(format!("Invalid regex: {e}")))
}

/// Transform a wildcard query to a regex string.
///
/// `AB*CD` for example is converted to `AB.*CD`
///
/// All other chars are regex escaped.
pub fn wildcard_query_to_regex_str(term: &str) -> String {
    regex::escape(term).replace(r"\*", ".*")
}

impl RegexPhraseQuery<Regex> {
    /// Creates a new `RegexPhraseQuery` given a list of terms.
    ///
    /// There must be at least two terms, and all terms
    /// must belong to the same field.
    ///
    /// Offset for each term will be same as index in the Vector
    pub fn new(field: Field, terms: Vec<String>) -> RegexPhraseQuery {
        let terms_with_offset = terms.into_iter().enumerate().collect();
        RegexPhraseQuery::new_with_offset(field, terms_with_offset)
    }

    /// Creates a new `RegexPhraseQuery` given a list of terms and their offsets.
    ///
    /// Can be used to provide custom offset for each term.
    pub fn new_with_offset(field: Field, terms: Vec<(usize, String)>) -> RegexPhraseQuery {
        RegexPhraseQuery::new_with_offset_and_slop(field, terms, 0)
    }

    /// Creates a new `RegexPhraseQuery` given a list of terms, their offsets and a slop
    pub fn new_with_offset_and_slop(
        field: Field,
        mut terms: Vec<(usize, String)>,
        slop: u32,
    ) -> RegexPhraseQuery {
        assert!(
            terms.len() > 1,
            "A phrase query is required to have strictly more than one term."
        );
        terms.sort_by_key(|&(offset, _)| offset);
        RegexPhraseQuery {
            field,
            phrase_terms: terms,
            slop,
            max_expansions: 1 << 14,
            automata: CompiledAutomata {
                automata: OnceCell::new(),
                compile: Some(compile_regex),
            },
        }
    }

    /// Creates a new `RegexPhraseQuery` from already compiled regexes, e.g. built with a
    /// non-default state limit.
    ///
    /// Each term is `(offset, pattern, regex)`, where `regex` is the compilation of
    /// `pattern`; the pattern is what [`RegexPhraseQuery::phrase_terms`] returns.
    pub fn from_regexes(
        field: Field,
        terms: Vec<(usize, String, Arc<Regex>)>,
        slop: u32,
    ) -> RegexPhraseQuery {
        RegexPhraseQuery::from_automata(field, terms, slop)
    }

    /// The compiled regex of each phrase term, in offset order.
    ///
    /// Compiled on first call and cached, so a query that is reused across searchers,
    /// or inspected before searching, determinizes each pattern once.
    pub fn regexes(&self) -> crate::Result<&[Arc<Regex>]> {
        self.automata()
    }
}

impl<A> RegexPhraseQuery<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    /// Creates a new `RegexPhraseQuery` from one prebuilt term automaton per phrase term,
    /// e.g. a DFA built by another regex engine and wrapped as an [`Automaton`].
    ///
    /// Each term is `(offset, pattern, automaton)`. The automaton alone decides which
    /// dictionary terms match; `pattern` is what [`RegexPhraseQuery::phrase_terms`]
    /// returns, and is used for the BM25 statistics when scoring is enabled.
    pub fn from_automata(
        field: Field,
        mut terms: Vec<(usize, String, Arc<A>)>,
        slop: u32,
    ) -> RegexPhraseQuery<A> {
        assert!(
            terms.len() > 1,
            "A phrase query is required to have strictly more than one term."
        );
        terms.sort_by_key(|&(offset, _, _)| offset);
        let (phrase_terms, automata): (Vec<_>, Vec<_>) = terms
            .into_iter()
            .map(|(offset, pattern, automaton)| ((offset, pattern), automaton))
            .unzip();
        RegexPhraseQuery {
            field,
            phrase_terms,
            slop,
            max_expansions: 1 << 14,
            automata: CompiledAutomata {
                automata: OnceCell::with_value(automata),
                compile: None,
            },
        }
    }

    /// The automaton of each phrase term, in offset order.
    pub fn automata(&self) -> crate::Result<&[Arc<A>]> {
        let automata = self.automata.automata.get_or_try_init(|| {
            let compile = self
                .automata
                .compile
                .expect("automata without a compiler are set at construction");
            self.phrase_terms
                .iter()
                .map(|(_, pattern)| compile(pattern).map(Arc::new))
                .collect::<crate::Result<Vec<_>>>()
        })?;
        Ok(automata)
    }

    /// Slop allowed for the phrase.
    ///
    /// The query will match if its terms are separated by `slop` terms at most.
    /// The slop can be considered a budget between all terms.
    /// E.g. "A B C" with slop 1 allows "A X B C", "A B X C", but not "A X B X C".
    ///
    /// Transposition costs 2, e.g. "A B" with slop 1 will not match "B A" but it would with slop 2
    /// Transposition is not a special case, in the example above A is moved 1 position and B is
    /// moved 1 position, so the slop is 2.
    ///
    /// As a result slop works in both directions, so the order of the terms may changed as long as
    /// they respect the slop.
    ///
    /// By default the slop is 0 meaning query terms need to be adjacent.
    pub fn set_slop(&mut self, value: u32) {
        self.slop = value;
    }

    /// Sets the max expansions a regex term can match. The limit will be over all terms.
    /// After the limit is hit an error will be returned.
    pub fn set_max_expansions(&mut self, value: u32) {
        self.max_expansions = value;
    }

    /// The [`Field`] this `RegexPhraseQuery` is targeting.
    pub fn field(&self) -> Field {
        self.field
    }

    /// `Term`s in the phrase without the associated offsets.
    pub fn phrase_terms(&self) -> Vec<Term> {
        self.phrase_terms
            .iter()
            .map(|(_, term)| Term::from_field_text(self.field, term))
            .collect::<Vec<Term>>()
    }

    /// Returns the [`RegexPhraseWeight`] for the given phrase query given a specific `searcher`.
    ///
    /// This function is the same as [`Query::weight()`] except it returns
    /// a specialized type [`RegexPhraseWeight`] instead of a Boxed trait.
    pub(crate) fn regex_phrase_weight(
        &self,
        enable_scoring: EnableScoring<'_>,
    ) -> crate::Result<RegexPhraseWeight<A>> {
        let schema = enable_scoring.schema();
        let field_type = schema.get_field_entry(self.field).field_type().value_type();
        if field_type != Type::Str {
            return Err(crate::TantivyError::SchemaError(format!(
                "RegexPhraseQuery can only be used with a field of type text currently, but got \
                 {field_type:?}"
            )));
        }

        let field_entry = schema.get_field_entry(self.field);
        let has_positions = field_entry
            .field_type()
            .get_index_record_option()
            .map(IndexRecordOption::has_positions)
            .unwrap_or(false);
        if !has_positions {
            let field_name = field_entry.name();
            return Err(crate::TantivyError::SchemaError(format!(
                "Applied phrase query on field {field_name:?}, which does not have positions \
                 indexed"
            )));
        }
        let terms = self.phrase_terms();
        let bm25_weight_opt = match enable_scoring {
            EnableScoring::Enabled {
                statistics_provider,
                ..
            } => Some(Bm25Weight::for_terms(statistics_provider, &terms)?),
            EnableScoring::Disabled { .. } => None,
        };
        let phrase_terms = self
            .phrase_terms
            .iter()
            .map(|(offset, _)| *offset)
            .zip(self.automata()?.iter().cloned())
            .collect();
        let weight = RegexPhraseWeight::new(
            self.field,
            phrase_terms,
            bm25_weight_opt,
            self.max_expansions,
            self.slop,
        );
        Ok(weight)
    }
}

impl<A> Query for RegexPhraseQuery<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    /// Create the weight associated with a query.
    ///
    /// See [`Weight`].
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let phrase_weight = self.regex_phrase_weight(enable_scoring)?;
        Ok(Box::new(phrase_weight))
    }
}
