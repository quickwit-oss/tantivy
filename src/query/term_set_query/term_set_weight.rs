//! `TermSetWeight` — a single `Weight` impl that holds every
//! representation needed to dispatch any of the term-set strategies
//! (`Gallop` / `LinearScan` / `BitsetFromPostings` / `Automaton` /
//! `Empty`), built once at construction and consulted per-segment.
//!
//! ## Two orthogonal concerns, one type
//!
//! Dispatch separates two questions:
//!
//!   - "Which field representations are available?" — `is_fast()`, `is_indexed()`.
//!   - "Which strategy wins on the current K/N/D?" — the planner's job.
//!
//! `BitsetFromPostings` only needs the field to be **indexed** (it
//! reads posting lists, not a column), so non-fast indexed fields
//! (text / string) still reach the bitset path — the planner picks
//! between `BitsetFromPostings` (low K) and `Automaton` (high K) on
//! those.
//!
//! `TermSetWeight` holds the bytes representation (consumed by Bitset
//! + Automaton), an optional u64 representation (consumed by Gallop +
//! Linear + Bitset on numeric-fast), a pre-built FST (consumed by
//! Automaton), and a `fast_strategies_available` flag set at
//! construction. The per-segment scorer opens the column only when
//! fast is available, calls `select_strategy`, and dispatches all
//! five variants including `Automaton` for non-fast indexed fields.

use std::io;
use std::net::Ipv6Addr;
use std::sync::Arc;

use columnar::{Column, ColumnType};
use rustc_hash::FxHashSet;
use tantivy_fst::raw::CompiledAddr;
use tantivy_fst::{Automaton, Map};

use super::term_set_bitset::bitset_from_postings_scorer;
use super::term_set_gallop::TermSetGallopDocSet;
use super::term_set_strategy::{
    select_strategy, PlannerInputs, StrategyTag, TermSetStrategy, TermSetStrategyConfig,
};
use crate::index::SegmentReader;
use crate::query::score_combiner::DoNothingCombiner;
use crate::query::{
    AutomatonWeight, BooleanWeight, ConstScorer, EmptyScorer, EnableScoring, Explanation, Occur,
    Query, Scorer, Weight,
};
use crate::schema::{Field, FieldType, Type};
use crate::{DocId, DocSet, Score, TantivyError, Term, TERMINATED};

// ---------------------------------------------------------------------------
// FastFieldTermSetQuery — preserved as a thin public Query type
// ---------------------------------------------------------------------------

/// Thin `Query` wrapper that delegates to `TermSetWeight`. Provided
/// alongside [`TermSetQuery`] for consumers that prefer the "fast-only"
/// name at the call site; the underlying dispatch path is identical.
///
/// [`TermSetQuery`]: super::TermSetQuery
#[derive(Debug, Clone)]
pub struct FastFieldTermSetQuery {
    terms_map: std::collections::HashMap<Field, Vec<Term>>,
    strategy_config: TermSetStrategyConfig,
}

impl FastFieldTermSetQuery {
    /// Create a new `FastFieldTermSetQuery`.
    pub fn new<T: IntoIterator<Item = Term>>(terms: T) -> Self {
        let mut terms_map: std::collections::HashMap<Field, Vec<Term>> =
            std::collections::HashMap::new();
        for term in terms {
            terms_map.entry(term.field()).or_default().push(term);
        }
        FastFieldTermSetQuery {
            terms_map,
            strategy_config: TermSetStrategyConfig::default(),
        }
    }

    /// Override the strategy thresholds and gates used when this query
    /// runs. Consumers (paradedb) build the config from GUCs.
    pub fn with_strategy_config(mut self, cfg: TermSetStrategyConfig) -> Self {
        self.strategy_config = cfg;
        self
    }
}

impl Query for FastFieldTermSetQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let schema = enable_scoring.schema();
        let mut sub_queries: Vec<(_, Box<dyn Weight>)> = Vec::with_capacity(self.terms_map.len());
        for (&field, terms) in &self.terms_map {
            let field_entry = schema.get_field_entry(field);
            let field_type = field_entry.field_type();
            sub_queries.push((
                Occur::Should,
                Box::new(TermSetWeight::new(
                    field,
                    terms,
                    field_type,
                    self.strategy_config.clone(),
                )?),
            ));
        }
        Ok(Box::new(BooleanWeight::new(
            sub_queries,
            false,
            Box::new(DoNothingCombiner::default),
        )))
    }
}

// ---------------------------------------------------------------------------
// TermSetWeight — the unified Weight
// ---------------------------------------------------------------------------

/// Internal representation of the input terms in their post-validation
/// type. `Numeric` covers U64/I64/F64/Date (each Term decoded via
/// `as_u64_lenient` to a u64 the column dispatch consumes). `Ipv6`
/// holds parsed Ipv6Addr values. `Bytes` is the catch-all for fields
/// whose dispatch only needs the byte encoding (text/string/json — and
/// also numeric/IP when used purely on the inverted index).
#[derive(Clone, Debug)]
enum TermRepr {
    /// Numeric Terms decoded as `u64`. Present when at least one Term's
    /// value can be decoded via `as_u64_lenient`. The FF strategies
    /// (Gallop / Linear / Bitset-on-fast) consume this.
    Numeric(Vec<u64>),
    /// Ipv6Addr Terms. Present when at least one Term has an
    /// `as_ip_addr` value. IP fields don't go through the planner — they
    /// always land on `TermSetDocSet` over the ipv6 column.
    Ipv6(Vec<Ipv6Addr>),
    /// Bytes-only (text / string / json). Dispatch is inverted-index
    /// only: Bitset (low K) vs. Automaton (high K).
    Bytes,
}

/// `Weight` for `TermSetQuery` over a single field. Holds every
/// representation needed by the strategy dispatcher; the per-segment
/// scorer picks one and dispatches.
#[derive(Clone, Debug)]
pub struct TermSetWeight {
    field: Field,
    /// Pre-sorted, deduped serialized bytes of every Term in the input.
    /// Built once at construction. Consumed by `BitsetFromPostings`
    /// (passed straight to `bitset_from_postings_scorer`) and indirectly
    /// by `Automaton` (via `terms_fst`).
    terms_bytes: Vec<Vec<u8>>,
    /// Pre-built FST set-membership map. Consumed by the `Automaton`
    /// dispatch (`AutomatonWeight::new(field, SetDfaWrapper(map))`).
    /// Built once at construction so per-segment dispatch is alloc-free.
    /// `None` when the input was empty. Wrapped in `Arc` because
    /// `Map<Vec<u8>>` isn't `Clone` (and the surrounding `TermSetWeight`
    /// needs to be — `BooleanWeight` requires it).
    terms_fst: Option<Arc<Map<Vec<u8>>>>,
    /// Typed representation. Drives the per-segment dispatch path.
    repr: TermRepr,
    /// `true` iff `field_type.is_fast()` and the field's value type has
    /// a column representation the planner can open as `Column<u64>` or
    /// `Column<Ipv6Addr>`. When `false`, the planner never returns
    /// `Gallop` / `LinearScan` (fast-field-bound); it routes to
    /// `Automaton` instead.
    fast_strategies_available: bool,
    /// `true` iff `field_type.is_indexed()`. Required for
    /// `BitsetFromPostings` (which reads posting lists from the inverted
    /// index) and for `Automaton`. Without an inverted index neither can
    /// build; the constructor rejects fields that are neither fast nor
    /// indexed.
    is_indexed: bool,
    strategy_config: TermSetStrategyConfig,
}

/// Returns whether `field_type.value_type()` has a fast-field column
/// representation the planner's FF strategies can open. Keep this in
/// sync with `FastFieldTermSetWeight::scorer`'s old `ColumnType` allow
/// list (U64/I64/F64/Date) plus IPv6.
fn type_supported_for_fast_field(field_type: &FieldType) -> bool {
    matches!(
        field_type.value_type(),
        Type::U64 | Type::I64 | Type::F64 | Type::Date | Type::IpAddr
    )
}

impl TermSetWeight {
    /// Construct a `TermSetWeight` for one field's term subset.
    ///
    /// Side effects: sorts + dedupes the bytes representation, builds
    /// the FST, decodes numeric/IP Terms if applicable. All O(K log K)
    /// or smaller; runs once.
    ///
    /// Errors when the field is neither fast nor indexed (no dispatch
    /// path can build), or when an IP-typed Term is mixed with a
    /// non-IP-typed field.
    pub fn new(
        field: Field,
        terms: &[Term],
        field_type: &FieldType,
        strategy_config: TermSetStrategyConfig,
    ) -> crate::Result<Self> {
        let fast_strategies_available =
            field_type.is_fast() && type_supported_for_fast_field(field_type);
        let is_indexed = field_type.is_indexed();
        if !fast_strategies_available && !is_indexed {
            return Err(TantivyError::SchemaError(format!(
                "Field {field:?} is neither fast nor indexed; TermSetQuery has no execution path"
            )));
        }

        // Build the sorted, deduped bytes representation. This is the
        // primary representation; the FST and (optional) numeric / IP
        // representations are derived from the input Terms separately.
        let mut terms_bytes: Vec<Vec<u8>> = terms
            .iter()
            .map(|t| t.serialized_value_bytes().to_vec())
            .collect();
        terms_bytes.sort_unstable();
        terms_bytes.dedup();

        // Build the FST once. `Map::from_iter` requires strictly
        // ascending keys, which the sort+dedup above guarantees. Skip
        // FST construction when there are no terms — the scorer
        // short-circuits to `EmptyScorer` before consulting the FST.
        let terms_fst = if terms_bytes.is_empty() {
            None
        } else {
            let map = Map::from_iter(terms_bytes.iter().map(|k| (k.as_slice(), 0u64)))
                .map_err(io::Error::other)?;
            Some(Arc::new(map))
        };

        // Pick a typed representation based on the first Term's value
        // type. Numeric / IP terms decode to typed arms that the
        // fast-field dispatch path consumes; everything else (text /
        // string / json) lands on `Bytes` and goes through the
        // inverted-index dispatch path.
        let repr = if let Some(first) = terms.first() {
            let first_val = first.value();
            if first_val.as_ip_addr().is_some() {
                let mut values = Vec::with_capacity(terms.len());
                for term in terms {
                    match term.value().as_ip_addr() {
                        Some(v) => values.push(v),
                        None => {
                            return Err(TantivyError::InvalidArgument(format!(
                                "Expected term with ip address, but got {term:?}",
                            )))
                        }
                    }
                }
                TermRepr::Ipv6(values)
            } else if first_val.as_u64_lenient().is_some() {
                let mut values = Vec::with_capacity(terms.len());
                for term in terms {
                    match term.value().as_u64_lenient() {
                        Some(v) => values.push(v),
                        None => {
                            return Err(TantivyError::InvalidArgument(format!(
                                "Expected term with u64, i64, f64, or date, but got {term:?}",
                            )))
                        }
                    }
                }
                TermRepr::Numeric(values)
            } else {
                // Text / string / json — bytes-only dispatch.
                TermRepr::Bytes
            }
        } else {
            // Empty input — repr doesn't matter; scorer short-circuits.
            TermRepr::Bytes
        };

        Ok(Self {
            field,
            terms_bytes,
            terms_fst,
            repr,
            fast_strategies_available,
            is_indexed,
            strategy_config,
        })
    }

    /// Common epilogue: write the chosen tag to `strategy_sink` if one
    /// is configured. Used by the non-planner dispatch paths
    /// (Ipv6 / non-fast bytes); the planner-driven path writes the sink
    /// inside `select_strategy`.
    fn record_strategy_tag(&self, tag: StrategyTag) {
        if let Some(sink) = self.strategy_config.strategy_sink.as_ref() {
            sink.store(tag as u8, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Build the `Automaton` scorer: compose an `AutomatonWeight` over
    /// the pre-built term FST and ask it for its scorer. The
    /// `AutomatonWeight` impl handles per-segment FST traversal across
    /// the dictionary streamer; we don't re-implement it here.
    fn automaton_scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<Box<dyn Scorer>> {
        let Some(map_arc) = self.terms_fst.clone() else {
            return Ok(Box::new(EmptyScorer));
        };
        let weight = AutomatonWeight::new(self.field, SetDfaWrapper(map_arc));
        weight.scorer(reader, boost)
    }
}

impl Weight for TermSetWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        if self.terms_bytes.is_empty() {
            return Ok(Box::new(EmptyScorer));
        }

        let field_entry = reader.schema().get_field_entry(self.field);
        let field_type = field_entry.field_type();
        let field_name = field_entry.name();

        match &self.repr {
            TermRepr::Ipv6(values) => {
                if !field_type.is_ip_addr() {
                    return Err(TantivyError::InvalidArgument(format!(
                        "fast fields TermSet for field `{field_name}` contains IP addresses, but \
                         the field type is {field_type:?}"
                    )));
                }
                let Some(ip_addr_column): Option<Column<Ipv6Addr>> =
                    reader.fast_fields().column_opt(field_name)?
                else {
                    return Ok(Box::new(EmptyScorer));
                };
                // IPv6 always routes through TermSetDocSet — no Gallop /
                // Bitset / Automaton dispatch for IP — so the HashSet is
                // built unconditionally here. Sink gets `Linear`.
                self.record_strategy_tag(StrategyTag::Linear);
                let term_set: FxHashSet<Ipv6Addr> = values.iter().copied().collect();
                let docset = TermSetDocSet::new(ip_addr_column, term_set);
                Ok(Box::new(ConstScorer::new(docset, boost)))
            }

            TermRepr::Numeric(values) => {
                if field_type.is_ip_addr() {
                    return Err(TantivyError::InvalidArgument(format!(
                        "fast fields TermSet for field `{field_name}` contains numeric values, \
                         but the field type is {field_type:?}"
                    )));
                }
                if self.fast_strategies_available {
                    self.dispatch_numeric_fast(reader, field_name, values, boost)
                } else {
                    self.dispatch_non_fast(reader, boost)
                }
            }

            TermRepr::Bytes => {
                // Non-numeric, non-IP fields. Fast strategies don't
                // apply (no `Column<u64>` decoding for arbitrary bytes);
                // dispatch is purely between Bitset and Automaton.
                self.dispatch_non_fast(reader, boost)
            }
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        Ok(Explanation::new("TermSetScorer", scorer.score()))
    }
}

impl TermSetWeight {
    /// Fast-numeric dispatch path: open the `Column<u64>`, estimate D,
    /// run the planner, dispatch on the returned variant.
    fn dispatch_numeric_fast(
        &self,
        reader: &SegmentReader,
        field_name: &str,
        values: &[u64],
        boost: Score,
    ) -> crate::Result<Box<dyn Scorer>> {
        let fast_field_reader = reader.fast_fields();
        let Some((column, _col_type)) = fast_field_reader.u64_lenient_for_type(
            Some(&[
                ColumnType::U64,
                ColumnType::I64,
                ColumnType::F64,
                ColumnType::DateTime,
            ]),
            field_name,
        )?
        else {
            return Ok(Box::new(EmptyScorer));
        };

        // Estimate `D = N / dict_size`. Pre-gate by K'/N before opening
        // the inverted index — when K'/N already exceeds the loosest
        // admissible bitset density, no D value can rescue bitset
        // admission. Saves the dict open on high-K queries.
        let n = column.num_docs() as u64;
        let k_prime = super::term_set_strategy::k_prime_in_range(values, &column);
        let density = k_prime as f64 / n as f64;
        let loosest_gate = self
            .strategy_config
            .bitset_max_density_unique
            .max(self.strategy_config.bitset_max_density_multi);
        let d = if density <= loosest_gate && self.is_indexed {
            let dict_size = reader
                .inverted_index(self.field)?
                .terms()
                .num_terms()
                .max(1) as u64;
            ((n / dict_size).max(1)).min(u32::MAX as u64) as u32
        } else {
            1
        };

        let strategy = select_strategy(
            reader,
            &column,
            PlannerInputs {
                field_name,
                candidate_size: None,
                avg_docs_per_term: Some(d),
                fast_available: true,
            },
            values,
            &self.strategy_config,
        );

        match strategy {
            TermSetStrategy::Empty => Ok(Box::new(EmptyScorer)),
            TermSetStrategy::Gallop {
                sort_order,
                sorted_terms,
            } => {
                let cardinality = column.get_cardinality();
                let docset =
                    TermSetGallopDocSet::new(column, sort_order, sorted_terms, cardinality);
                Ok(Box::new(ConstScorer::new(docset, boost)))
            }
            TermSetStrategy::BitsetFromPostings if self.is_indexed => {
                // `terms_bytes` was pre-sorted+deduped at construction.
                bitset_from_postings_scorer(reader, self.field, &self.terms_bytes, boost)
            }
            TermSetStrategy::LinearScan | TermSetStrategy::BitsetFromPostings => {
                let term_set: FxHashSet<u64> = values.iter().copied().collect();
                let docset = TermSetDocSet::new(column, term_set);
                Ok(Box::new(ConstScorer::new(docset, boost)))
            }
            TermSetStrategy::Automaton => {
                // Planner shouldn't emit Automaton when fast_available=true.
                // Belt-and-suspenders: route to AutomatonWeight as a
                // graceful fallback rather than panicking, in case future
                // planner changes loosen this invariant.
                self.automaton_scorer(reader, boost)
            }
        }
    }

    /// Non-fast indexed dispatch path. Used for:
    ///   - fields with FAST set but the value type isn't supported for FF (today: none — see
    ///     `type_supported_for_fast_field`),
    ///   - numeric fields without FAST,
    ///   - text / string / json fields (always non-fast for FF purposes).
    ///
    /// Decision is a simple K/max_doc density gate against the
    /// `bitset_max_density_multi` threshold. We don't have a
    /// `Column<u64>` so we can't run `k_prime_in_range` pruning —
    /// `K' = K` here. `D` is approximated as ≥2 (typical for non-fast
    /// text columns); the user can override via
    /// `bitset_max_density_unique` if their column is unique-valued
    /// indexed-only.
    fn dispatch_non_fast(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<Box<dyn Scorer>> {
        let n = reader.max_doc() as u64;
        if n == 0 {
            self.record_strategy_tag(StrategyTag::Empty);
            return Ok(Box::new(EmptyScorer));
        }
        if !self.is_indexed {
            // FAST-only field whose value type isn't supported by FF
            // strategies and has no inverted index — neither Bitset nor
            // Automaton can build. Return `EmptyScorer` silently rather
            // than erroring, matching the rest of the dispatch surface
            // which prefers to drop unscorable cells over surfacing a
            // hard schema error mid-query.
            self.record_strategy_tag(StrategyTag::Empty);
            return Ok(Box::new(EmptyScorer));
        }

        let k = self.terms_bytes.len() as u64;
        let density = (k as f64) / (n as f64);
        // Non-fast indexed fields are overwhelmingly text/string in
        // practice — the multi-value threshold applies. Numeric-non-fast
        // (unusual) lands on the same threshold by default.
        if density <= self.strategy_config.bitset_max_density_multi {
            self.record_strategy_tag(StrategyTag::Bitset);
            bitset_from_postings_scorer(reader, self.field, &self.terms_bytes, boost)
        } else {
            self.record_strategy_tag(StrategyTag::Automaton);
            self.automaton_scorer(reader, boost)
        }
    }
}

// ---------------------------------------------------------------------------
// SetDfaWrapper — FST-as-set automaton driving the `Automaton` dispatch
// ---------------------------------------------------------------------------

pub(crate) struct SetDfaWrapper(pub(crate) Arc<Map<Vec<u8>>>);

impl Automaton for SetDfaWrapper {
    type State = Option<CompiledAddr>;

    fn start(&self) -> Option<CompiledAddr> {
        // `self.0` is `Arc<Map<Vec<u8>>>`; `(*self.0).as_ref()` reaches
        // the inner `Fst` via `Map::as_ref` (not `Arc::as_ref`, which
        // would only yield `&Map`).
        Some((*self.0).as_ref().root().addr())
    }

    fn is_match(&self, state_opt: &Option<CompiledAddr>) -> bool {
        if let Some(state) = state_opt {
            (*self.0).as_ref().node(*state).is_final()
        } else {
            false
        }
    }

    fn accept(&self, state_opt: &Option<CompiledAddr>, byte: u8) -> Option<CompiledAddr> {
        let state = state_opt.as_ref()?;
        let node = (*self.0).as_ref().node(*state);
        let transition = node.find_input(byte)?;
        Some(node.transition_addr(transition))
    }

    fn can_match(&self, state: &Self::State) -> bool {
        state.is_some()
    }
}

// ---------------------------------------------------------------------------
// TermSetDocSet — fast-field linear scan over a `Column<T>` filtered by an
// in-memory `FxHashSet<T>` of accepted values
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct TermSetDocSet<T: Copy + Eq + std::hash::Hash> {
    column: Column<T>,
    values: FxHashSet<T>,
    doc_id: DocId,
    max_doc: DocId,
}

impl<T: Copy + Eq + std::hash::Hash + PartialOrd + std::fmt::Debug + Send + Sync + 'static>
    TermSetDocSet<T>
{
    pub fn new(column: Column<T>, values: FxHashSet<T>) -> Self {
        let max_doc = column.num_docs();
        let mut doc_set = Self {
            column,
            values,
            doc_id: TERMINATED,
            max_doc,
        };
        doc_set.advance();
        doc_set
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::str::FromStr;

    use super::FastFieldTermSetQuery;
    use crate::collector::{Count, TopDocs};
    use crate::query::QueryParser;
    use crate::schema::{IntoIpv6Addr, Schema, FAST, INDEXED, STRING, TEXT};
    use crate::{Index, IndexWriter, Term};

    fn create_test_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let text_field_fast = schema_builder.add_text_field("text_fast", STRING | FAST);
        let u64_field_fast = schema_builder.add_u64_field("u64_fast", FAST | INDEXED);
        let ip_field_fast = schema_builder.add_ip_addr_field("ip_fast", FAST | INDEXED);
        let bool_field_fast = schema_builder.add_bool_field("bool_fast", FAST | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(
                text_field => "doc1",
                text_field_fast => "doc1",
                u64_field_fast => 1u64,
                ip_field_fast => IpAddr::from_str("127.0.0.1").unwrap().into_ipv6_addr(),
                bool_field_fast => true,
            ))?;
            index_writer.add_document(doc!(
                text_field => "doc2",
                text_field_fast => "doc2",
                u64_field_fast => 2u64,
                ip_field_fast => IpAddr::from_str("127.0.0.2").unwrap().into_ipv6_addr(),
                bool_field_fast => false,
            ))?;
            index_writer.add_document(doc!(
                text_field => "doc3",
                text_field_fast => "doc3",
                u64_field_fast => 3u64,
                ip_field_fast => IpAddr::from_str("::ffff:127.0.0.3").unwrap().into_ipv6_addr(),
                bool_field_fast => true,
            ))?;
            index_writer.commit()?;
        }
        Ok(index)
    }

    #[test]
    pub fn test_term_set_query_fast_field_u64() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let u64_field_fast = index.schema().get_field("u64_fast").unwrap();
        let query = FastFieldTermSetQuery::new(vec![
            Term::from_field_u64(u64_field_fast, 1),
            Term::from_field_u64(u64_field_fast, 3),
        ]);
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);
        Ok(())
    }

    /// String fields (STRING | FAST) dispatch through `TermSetWeight`'s
    /// inverted-index path, which routes to `BitsetFromPostings` or
    /// `Automaton` depending on K. Pins the contract that string-field
    /// term sets are scorable.
    #[test]
    pub fn test_term_set_query_fast_field_string() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let text_field_fast = index.schema().get_field("text_fast").unwrap();
        let query = FastFieldTermSetQuery::new(vec![
            Term::from_field_text(text_field_fast, "doc1"),
            Term::from_field_text(text_field_fast, "doc3"),
        ]);
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);
        Ok(())
    }

    #[test]
    pub fn test_term_set_query_fast_field_ip() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let ip_field_fast = index.schema().get_field("ip_fast").unwrap();
        let query = FastFieldTermSetQuery::new(vec![
            Term::from_field_ip_addr(
                ip_field_fast,
                IpAddr::from_str("127.0.0.1").unwrap().into_ipv6_addr(),
            ),
            Term::from_field_ip_addr(
                ip_field_fast,
                IpAddr::from_str("127.0.0.3").unwrap().into_ipv6_addr(),
            ),
        ]);
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);
        Ok(())
    }

    #[test]
    pub fn test_term_set_query_fast_field_no_match() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let u64_field_fast = index.schema().get_field("u64_fast").unwrap();
        let query = FastFieldTermSetQuery::new(vec![
            Term::from_field_u64(u64_field_fast, 4),
            Term::from_field_u64(u64_field_fast, 5),
        ]);
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    pub fn test_term_set_query_fast_field_empty() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = FastFieldTermSetQuery::new(Vec::<Term>::new());
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    pub fn test_term_set_query_parser_fast_field() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![]);
        let query = query_parser.parse_query("u64_fast: IN [1 3]")?;
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);
        let query = query_parser.parse_query("text_fast: IN [doc1 doc3]")?;
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);
        let query = query_parser.parse_query("ip_fast: IN [127.0.0.1 127.0.0.3]")?;
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);
        let query = query_parser.parse_query("bool_fast: IN [true]")?;
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);
        Ok(())
    }

    fn create_test_index_with_missing_values() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let u64_field_fast = schema_builder.add_u64_field("u64_fast", FAST | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(u64_field_fast => 1u64))?;
            index_writer.add_document(doc!(text_field => "doc2"))?;
            index_writer.add_document(doc!(u64_field_fast => 3u64))?;
            index_writer.commit()?;
        }
        Ok(index)
    }

    #[test]
    pub fn test_term_set_query_fast_field_missing_value() -> crate::Result<()> {
        let index = create_test_index_with_missing_values()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let u64_field_fast = index.schema().get_field("u64_fast").unwrap();
        let query = FastFieldTermSetQuery::new(vec![Term::from_field_u64(u64_field_fast, 0)]);
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
        assert_eq!(top_docs.len(), 0);
        let query = FastFieldTermSetQuery::new(vec![Term::from_field_u64(u64_field_fast, 1)]);
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
        assert_eq!(top_docs.len(), 1);
        let (_, doc_address) = top_docs[0];
        assert_eq!(doc_address.doc_id, 0);
        Ok(())
    }
}

impl<T: Copy + Eq + std::hash::Hash + PartialOrd + std::fmt::Debug + Send + Sync + 'static> DocSet
    for TermSetDocSet<T>
{
    fn advance(&mut self) -> DocId {
        let mut next_doc_id = if self.doc_id == TERMINATED {
            0
        } else {
            self.doc_id + 1
        };
        while next_doc_id < self.max_doc {
            for value in self.column.values_for_doc(next_doc_id) {
                if self.values.contains(&value) {
                    self.doc_id = next_doc_id;
                    return self.doc_id;
                }
            }
            next_doc_id += 1;
        }
        self.doc_id = TERMINATED;
        TERMINATED
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc_id || self.doc_id == TERMINATED);
        if target >= self.max_doc {
            self.doc_id = TERMINATED;
            return TERMINATED;
        }
        let mut next_doc_id = target;
        while next_doc_id < self.max_doc {
            for value in self.column.values_for_doc(next_doc_id) {
                if self.values.contains(&value) {
                    self.doc_id = next_doc_id;
                    return next_doc_id;
                }
            }
            next_doc_id += 1;
        }
        self.doc_id = TERMINATED;
        TERMINATED
    }

    fn doc(&self) -> DocId {
        self.doc_id
    }

    fn size_hint(&self) -> u32 {
        self.max_doc - self.doc_id.saturating_add(1)
    }
}
