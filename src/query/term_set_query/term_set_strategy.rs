//! Strategy selection for `TermSetWeight`.
//!
//! Mirrors the structure of `range_query::range_query_fastfield`:
//! `Weight::scorer()` consults this module to decide between `Gallop`,
//! `BitsetFromPostings`, `LinearScan`, and `Automaton`, then constructs
//! the right `DocSet`. The decision tree and threshold calibration live
//! on [`select_strategy`]. `TermSetWeight`'s non-fast dispatch path
//! (text / string fields) inlines a simpler bitset-vs-automaton density
//! gate instead of calling [`select_strategy`], because it has no
//! `Column<u64>` to feed the planner — see the "Non-fast indexed
//! fields" section of [`select_strategy`]'s doc comment.
//!
//! ## `inputs.avg_docs_per_term` (`D`)
//!
//! Computing `D` precisely from the column alone would require
//! posting-list lookups (which is some of the work
//! `BitsetFromPostings` does anyway). `select_strategy` accepts
//! `None` and treats it as `D = 1`. The first-column dispatch uses
//! `D` to pick between two `BitsetFromPostings`-vs-`LinearScan`
//! thresholds (see [`select_strategy`] for the cost-model reasoning);
//! a missing estimate degrades to the tighter D=1 threshold.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use columnar::{Cardinality, Column};

use crate::index::SegmentReader;
use crate::Order;

/// Numeric tag for the chosen strategy. Used by the optional
/// `TermSetStrategyConfig::strategy_sink` so consumers (paradedb) can surface
/// the per-segment dispatch decision in `EXPLAIN ANALYZE` without depending
/// on `TermSetStrategy` directly.
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum StrategyTag {
    None = 0,
    Gallop = 1,
    Linear = 2,
    Bitset = 3,
    Empty = 4,
    /// Streaming dictionary walk via FST set-membership automaton. Picked
    /// when the field has no fast representation but is indexed — Gallop /
    /// LinearScan / BitsetFromPostings all require a `Column<u64>` so the
    /// planner routes here instead.
    Automaton = 5,
}

impl TryFrom<u8> for StrategyTag {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(StrategyTag::None),
            1 => Ok(StrategyTag::Gallop),
            2 => Ok(StrategyTag::Linear),
            3 => Ok(StrategyTag::Bitset),
            4 => Ok(StrategyTag::Empty),
            5 => Ok(StrategyTag::Automaton),
            _ => Err("unknown StrategyTag value"),
        }
    }
}

/// User-tunable density thresholds for `BitsetFromPostings`. Defaults are bench-calibrated
/// against the SSTable backend (see [`select_strategy`] for the
/// cost-model reasoning) and overridable via the
/// `paradedb.term_set_*_max_density` GUCs at the consumer side.
///
/// "Density" is a unitless ratio of *matching count over corpus
/// count*. For bitset on the first column it reduces to `K' / N` (D
/// is used only to pick which threshold applies, not multiplied into
/// the gate variable); for the subsequent-column branch, `K' · D / C`.
/// Bitset fires when its density is at or below the corresponding
/// `_max_density` threshold. Gallop has no density gate — see
/// `gallop_enabled`.
///
/// We use `f64` rather than integer denominators so bench-derived
/// thresholds don't have to round to `1/N` for small `N`.
#[derive(Clone, Debug)]
pub struct TermSetStrategyConfig {
    /// Kill-switch: when `false`, the planner never returns `Gallop` even if
    /// every other gate passes.
    pub gallop_enabled: bool,
    /// `BitsetFromPostings` first-column threshold for *unique-valued
    /// columns* (`D = 1`, i.e. `dict_size = N`, e.g., pri,mary keys).
    /// The strategy fires when `'K' / N <= bitset_max_density_unique`.
    /// Default `1/2000 = 0.0005` — calibrated against the SSTable
    /// backend where per-`dict.get` zstd block decompress dominates
    /// batched cost on unique columns; the empirical crossover with
    /// `LinearScan` lands at `'K'/N ≈ 1/1000` and this default sits at
    /// half that to give linear the boundary cells.
    pub bitset_max_density_unique: f64,
    /// `BitsetFromPostings` a first-column threshold for *non-unique
    /// columns* (`D >= 2`). The strategy fires when
    /// `'K' / N <= bitset_max_density_multi`. Default `1/200 = 0.005` —
    /// the `cost_model` bench tier pinned the batched-vs-linear
    /// crossover to K/N ∈ [0.0055, 0.0088] across D ∈ {2, 5, 20, 50}
    /// (spread 1.59×), and `1/200` sits just below the tightest
    /// measured crossover (D=2). Exactly 10× looser than the unique
    /// threshold, matching the ~10× per-K cost gap: batched on a
    /// non-unique column amortizes the zstd block decompressed across
    /// multiple keys per block, dropping per-K dict cost relative to
    /// the D=1 case where every key maps to a distinct block.
    pub bitset_max_density_multi: f64,
    /// Subsequent-column `BitsetFromPostings` threshold: `K' · D / C` cutoff.
    pub subsequent_bitset_max_density: f64,
    /// Optional sink for the per-segment strategy choice. When `Some`,
    /// `select_strategy` writes the chosen `StrategyTag` (as `u8`) on its way
    /// out — last-segment-wins is fine because `EXPLAIN` only asks "did any
    /// segment use it?". When `None`, no atomic store happens and the
    /// hot-path cost is one `Option::is_some` check.
    pub strategy_sink: Option<Arc<AtomicU8>>,
}

impl Default for TermSetStrategyConfig {
    fn default() -> Self {
        Self {
            gallop_enabled: true,
            // Defaults are bench-derived. See per-field doc-comments for the
            // calibration story. `subsequent_bitset_max_density` is an
            // unverified starting estimate — no bench data for the
            // subsequent-column branch yet.
            bitset_max_density_unique: 1.0 / 2000.0,
            bitset_max_density_multi: 1.0 / 200.0,
            subsequent_bitset_max_density: 1.0 / 4.0,
            strategy_sink: None,
        }
    }
}

/// Selected execution strategy. Set of variants that can be dispatched today.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TermSetStrategy {
    /// Sorted-segment fast path. Carries the pruned, ascending-sorted term list
    /// because the planner already had to compute it for min/max pruning.
    Gallop {
        sort_order: Order,
        sorted_terms: Vec<u64>,
    },
    /// `TermSetDocSet` over the fast field. Terminal fallback when no other
    /// strategy qualifies.
    LinearScan,
    /// Direct `TermDictionary::get(key)` lookups + bitset OR. The K-bounded
    /// inverted-index path; see `term_set_bitset.rs`.
    BitsetFromPostings,
    /// Streaming dictionary walk via FST set-membership automaton.
    /// Picked when the field has no fast representation (so Gallop /
    /// LinearScan / BitsetFromPostings would all fail to open a
    /// `Column<u64>`) but is indexed. The dispatch site composes an
    /// `AutomatonWeight` over the pre-built term FST.
    Automaton,
    /// The result is definitively empty; no posting list reads or
    /// column scans needed. Returned when the planner can prove no
    /// docs can match:
    ///   - `n == 0` (empty segment)
    ///   - `term_set.is_empty()` (no query terms)
    ///   - `k_prime == 0` (all query terms pruned outside `[min, max]`)
    ///   - `candidate_size == 0` (subsequent-column branch with empty upstream)
    ///
    /// The dispatch site emits an `EmptyScorer` for this variant —
    /// returning `LinearScan` here instead would have the dispatch
    /// site walk every doc in the segment doing hashset probes that
    /// all fail by construction.
    Empty,
}

/// Inputs to the planner that aren't already on `Column`.
pub struct PlannerInputs<'a> {
    pub field_name: &'a str,
    /// Upstream candidate-set size. `None` ≡ full segment scan (first column).
    pub candidate_size: Option<u32>,
    /// Average documents per term in this column. `None` is treated as `D = 1`.
    pub avg_docs_per_term: Option<u32>,
    /// Whether the field has a fast-field representation the planner can
    /// open as `Column<u64>`. When `false`, the planner never returns
    /// `Gallop`, `LinearScan`, or `BitsetFromPostings`'s fast-field
    /// fallback; the terminal route becomes `Automaton` instead. The
    /// `BitsetFromPostings` strategy is fast-field-independent (it reads
    /// posting lists, not the column) and remains admissible for
    /// non-fast indexed fields.
    pub fast_available: bool,
}

/// Strategy dispatch for `TermSetWeight`.
///
/// # How this function gets called
///
/// `TermSetQuery::build_weight` constructs one `TermSetWeight` per
/// field; `TermSetWeight::scorer` calls `select_strategy` once per
/// segment when the field has a fast-field representation (so a
/// `Column<u64>` can be opened). `inputs.fast_available` tells the
/// planner whether the column-bound strategies (Gallop / LinearScan)
/// are admissible — when `false`, the terminal fallback is `Automaton`
/// instead.
///
/// `BitsetFromPostings` is fast-field-independent (it reads posting
/// lists from the inverted index, not the column) and is admissible
/// regardless of `fast_available` as long as the field is indexed.
///
/// # Decision tree
///
/// Each step is "decide and return"; later steps run only if the
/// earlier ones declined. `terminal_fallback()` is shorthand for
/// "`LinearScan` if `inputs.fast_available`, else `Automaton`" — both
/// shapes scale ~O(N) but on different substrates.
///
/// ```text
/// 1. Edge cases
///      n == 0  ||  term_set.is_empty()            →  Empty
///      k_prime == 0  (all terms outside [min,max]) →  Empty
///
/// 2. Gallop  (sorted-segment fast path)
///      inputs.fast_available                       AND
///      cfg.gallop_enabled                          AND
///      inputs.candidate_size is None or == n       AND
///      cardinality in {Full, Optional}             AND
///      reader.sort_by_field matches field_name     →  Gallop
///
/// 3. Subsequent-column branch  (candidate_size < n)
///      candidate_size == 0                         →  Empty
///      K' · D / C <=                                  BitsetFromPostings
///        cfg.subsequent_bitset_max_density         →
///      otherwise                                   →  terminal_fallback()
///
/// 4. First-column branch  (full segment, no upstream filter)
///      threshold = if D >= 2 { bitset_max_density_multi }
///                  else      { bitset_max_density_unique }
///      K' / N <= threshold                         →  BitsetFromPostings
///      otherwise                                   →  terminal_fallback()
/// ```
///
/// # Gallop admission
///
/// Gallop walks the fast-field column directly with exponential jumps
/// once the segment is sorted by the queried field; no other strategy
/// exploits the sort. Every benched cell on a sort-matching segment
/// showed Gallop at least as fast as the alternatives, so once the
/// structural preconditions hold the planner commits — no density
/// component to the gate. `cfg.gallop_enabled` is the only knob.
///
/// # Why two bitset thresholds (D=1 vs D >= 2)
///
/// On the SSTable backend each `dict.get` pays a zstd block
/// decompress. For unique-valued columns (`D = 1`, i.e.
/// `dict_size = N`) every query term lands in a distinct dict block,
/// so per-K dict cost is large and `BatchedBitsetFromPostings` only
/// wins against `LinearScan` for very small K (crossover at K/N ≈
/// 1/1000). For non-unique columns (`D >= 2`) multiple query terms
/// share dict blocks and the batched API amortizes the decompress;
/// the crossover with linear moves out to K/N ≈ 1/100. The two-arm
/// gate picks the right side of that ~10× cost gap based on
/// `inputs.avg_docs_per_term`, which the dispatch site derives from
/// `dict_size = TermDictionary::num_terms()` (a constant-time field
/// read on both backends) as `D = N / dict_size`.
///
/// Both defaults sit at half the empirical crossover (the
/// strict-less-than form would set them exactly at it; the `<=`
/// gate gives the simpler-strategy fallback the benefit at the
/// boundary). All density values are unitless ratios in `[0, 1]`.
///
/// # Non-fast indexed fields
///
/// `select_strategy` still requires `column: &Column<u64>` and is
/// therefore only called by the fast-numeric dispatch path in
/// `TermSetWeight::dispatch_numeric_fast`. The non-fast dispatch path
/// (`TermSetWeight::dispatch_non_fast`, used for text / string fields)
/// can't open a column and inlines its own simpler density gate —
/// `K / max_doc <= bitset_max_density_multi` → `BitsetFromPostings`,
/// else `Automaton`. It writes to `strategy_sink` directly. The
/// `inputs.fast_available = false` substitution rule here exists for
/// future callers that may have a column but want to suppress
/// fast-field-bound strategies anyway.
///
/// # Constants
///
///   - `cfg.gallop_enabled`                    default true
///   - `cfg.bitset_max_density_unique`         default 1/2000 = 0.0005
///   - `cfg.bitset_max_density_multi`          default 1/200  = 0.005
///   - `cfg.subsequent_bitset_max_density`     default 1/4    = 0.25
///
/// # Strategy sink for EXPLAIN
///
/// When `cfg.strategy_sink` is `Some`, the chosen `StrategyTag` is
/// stored (as `u8`) on the sink before returning so consumers can
/// surface it in `EXPLAIN ANALYZE`. A thin wrapper around the inner
/// planner keeps the per-segment hot path uncluttered when no sink
/// is configured. `StrategyTag::Automaton` is included alongside the
/// fast-field-bound tags so a single sink covers both dispatch paths.
pub fn select_strategy(
    reader: &SegmentReader,
    column: &Column<u64>,
    inputs: PlannerInputs<'_>,
    term_set: &[u64],
    cfg: &TermSetStrategyConfig,
) -> TermSetStrategy {
    let strat = select_strategy_inner(reader, column, inputs, term_set, cfg);
    if let Some(sink) = cfg.strategy_sink.as_ref() {
        let tag = match &strat {
            TermSetStrategy::Gallop { .. } => StrategyTag::Gallop,
            TermSetStrategy::LinearScan => StrategyTag::Linear,
            TermSetStrategy::BitsetFromPostings => StrategyTag::Bitset,
            TermSetStrategy::Automaton => StrategyTag::Automaton,
            TermSetStrategy::Empty => StrategyTag::Empty,
        };
        sink.store(tag as u8, Ordering::Relaxed);
    }
    strat
}

fn select_strategy_inner(
    reader: &SegmentReader,
    column: &Column<u64>,
    inputs: PlannerInputs<'_>,
    term_set: &[u64],
    cfg: &TermSetStrategyConfig,
) -> TermSetStrategy {
    // --- Edge cases: nothing to do.
    let n = column.num_docs();
    if n == 0 || term_set.is_empty() {
        return TermSetStrategy::Empty;
    }
    let k_prime = k_prime_in_range(term_set, column);
    if k_prime == 0 {
        // All query terms outside [min, max]. Returning `LinearScan` would
        // walk every doc in the segment doing hashset probes guaranteed to
        // miss; `Empty` lets the dispatch site emit `EmptyScorer` instead.
        return TermSetStrategy::Empty;
    }

    // Gallop: sorted-segment fast path. Dominates whenever the segment
    // is sorted by the queried field. Skipped when `!fast_available`
    // because Gallop walks a `Column<u64>`.
    if inputs.fast_available {
        if let Some(strategy) = try_gallop(reader, column, &inputs, term_set, cfg) {
            return strategy;
        }
    }

    // --- Subsequent-column branch (upstream filter narrowed work to c < n).
    if let Some(c) = inputs.candidate_size.filter(|&c| c < n) {
        if c == 0 {
            return TermSetStrategy::Empty;
        }
        let d = inputs.avg_docs_per_term.unwrap_or(1) as u64;
        let kd = (k_prime as u64).saturating_mul(d) as f64;
        if kd / (c as f64) <= cfg.subsequent_bitset_max_density {
            return TermSetStrategy::BitsetFromPostings;
        }
        return terminal_fallback(&inputs);
    }

    // --- Tier 2: first-column bitset vs linear, parameterized by D.
    // The cost driver is K/N. Picking the right threshold needs D:
    //   - D = 1 (unique-valued, dict_size >= N): per-K dict cost is ~10× higher than D >= 2 because
    //     every key maps to a distinct SSTable block; tight gate keeps linear winning past
    //     K/N≈1/2000.
    //   - D >= 2: batched amortizes the zstd block decompress across multiple keys per block; loose
    //     gate admits up to K/N≈1/200.
    let d = inputs.avg_docs_per_term.unwrap_or(1);
    let density = (k_prime as f64) / (n as f64);
    let bitset_threshold = if d >= 2 {
        cfg.bitset_max_density_multi
    } else {
        cfg.bitset_max_density_unique
    };
    if density <= bitset_threshold {
        TermSetStrategy::BitsetFromPostings
    } else {
        terminal_fallback(&inputs)
    }
}

/// Terminal fallback when no specialized strategy committed: picks the
/// scan-shaped path. Fast-field-available fields land on `LinearScan`
/// (FxHashSet-probing column scan); non-fast indexed fields land on
/// `Automaton` (FST set-membership streaming walk over the inverted
/// index). Both shapes scale ~O(N) but on different substrates.
fn terminal_fallback(inputs: &PlannerInputs<'_>) -> TermSetStrategy {
    if inputs.fast_available {
        TermSetStrategy::LinearScan
    } else {
        TermSetStrategy::Automaton
    }
}

/// Count the query terms that survive the `[column.min, column.max]`
/// range prune — `K'` in the dispatcher's gate formulas. Public-in-crate
/// so the dispatch site (`FastFieldTermSetWeight::scorer`) can pre-compute
/// the density gate's left-hand-side without re-running the filter or
/// having to call the full planner first. Keeping a single definition
/// here ensures dispatch and planner stay consistent by construction.
///
/// Duplicates are *not* removed: counts occurrences. Production input
/// flows through HashSet-derived sources so duplicates are rare; the
/// Gallop branch dedups defensively when it commits to that strategy.
pub(crate) fn k_prime_in_range(term_set: &[u64], column: &Column<u64>) -> u32 {
    let lo = column.min_value();
    let hi = column.max_value();
    term_set.iter().filter(|&&v| v >= lo && v <= hi).count() as u32
}

/// Gallop admissibility: returns `Some(Gallop { .. })` when the segment is
/// sorted by `field_name`, the column shape supports gallop, and no upstream
/// filter has narrowed the work. Builds the pruned+sorted+deduped
/// `sorted_terms` Vec only on commit. `cfg.gallop_enabled` is the only
/// configurable gate.
fn try_gallop(
    reader: &SegmentReader,
    column: &Column<u64>,
    inputs: &PlannerInputs<'_>,
    term_set: &[u64],
    cfg: &TermSetStrategyConfig,
) -> Option<TermSetStrategy> {
    if !cfg.gallop_enabled {
        return None;
    }
    let n = column.num_docs();
    let candidate_eq_n = inputs.candidate_size.is_none_or(|c| c == n);
    if !candidate_eq_n {
        return None;
    }
    if !matches!(
        column.get_cardinality(),
        Cardinality::Full | Cardinality::Optional
    ) {
        return None;
    }
    let order = reader
        .sort_by_field()
        .filter(|sbf| sbf.field == inputs.field_name)
        .map(|sbf| sbf.order)?;
    // Gallop tolerates duplicate input terms (the second occurrence finds
    // an empty range and skips), but each duplicate still pays two
    // `gallop_search_sorted` probes. Production input is HashSet-derived
    // so dedup is usually a no-op; `TermSetQuery::new` doesn't enforce
    // uniqueness on its input, so we dedup defensively here.
    let lo = column.min_value();
    let hi = column.max_value();
    let mut sorted_terms: Vec<u64> = term_set
        .iter()
        .copied()
        .filter(|&v| v >= lo && v <= hi)
        .collect();
    sorted_terms.sort_unstable();
    sorted_terms.dedup();
    Some(TermSetStrategy::Gallop {
        sort_order: order,
        sorted_terms,
    })
}

#[cfg(test)]
mod tests {
    //! Unit tests pin the planner's *output shape* on cases it can decide
    //! today. Tests that need to exercise gate semantics on specific K/N
    //! cells pass `avg_docs_per_term` and density-threshold overrides
    //! directly so they remain stable across future calibration changes.
    use super::*;
    use crate::schema::{NumericOptions, SchemaBuilder};
    use crate::{Index, IndexSettings, IndexSortByField};

    fn build_index(
        n: u64,
        sort: Option<IndexSortByField>,
        value_for_doc: impl Fn(u64) -> u64,
    ) -> (Index, crate::schema::Field, String) {
        let mut sb = SchemaBuilder::new();
        let field = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
        let schema = sb.build();
        let mut builder = Index::builder().schema(schema);
        if let Some(sbf) = sort {
            builder = builder.settings(IndexSettings {
                sort_by_field: Some(sbf),
                ..Default::default()
            });
        }
        let index = builder.create_in_ram().unwrap();
        let mut writer = index.writer_for_tests().unwrap();
        for d_idx in 0..n {
            writer
                .add_document(doc!(field => value_for_doc(d_idx)))
                .unwrap();
        }
        writer.commit().unwrap();
        (index, field, "fk".to_string())
    }

    fn open_column(index: &Index, field_name: &str) -> (SegmentReader, Column<u64>) {
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0).clone();
        let (column, _) = segment_reader
            .fast_fields()
            .u64_lenient_for_type(None, field_name)
            .unwrap()
            .unwrap();
        (segment_reader, column)
    }

    fn term_set(values: impl IntoIterator<Item = u64>) -> Vec<u64> {
        values.into_iter().collect()
    }

    #[test]
    fn select_handles_min_max_pruning_dropping_all_terms() {
        // Column values are 0..N, so terms entirely outside [0, N-1] all prune.
        let (index, _field, name) = build_index(1024, None, |i| i);
        let (reader, column) = open_column(&index, &name);
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: None,
                fast_available: true,
            },
            &term_set([10_000_000u64, 99_999_999, u64::MAX]),
            &TermSetStrategyConfig::default(),
        );
        // All query terms fall outside the column's [min, max]. The
        // planner proves no docs can match and returns `Empty` so the
        // dispatch site emits `EmptyScorer` instead of walking the
        // segment with a hashset of guaranteed-to-miss values.
        assert_eq!(strat, TermSetStrategy::Empty);
    }

    #[test]
    fn select_returns_bitset_from_postings_when_gallop_kill_switch_engaged() {
        // Sorted segment, K above the D=1 bitset gate but below D=2 gate.
        // With gallop disabled, dispatch falls through to the first-column
        // bitset gate. We pass D=2 so the looser threshold admits.
        // N = 4096, K = 60 → K/N ≈ 0.0146.
        //   gallop_enabled = false → gallop skipped
        //   D = 2 → use bitset_max_density_multi = 1/200 = 0.005
        //   0.0146 > 0.005 → LinearScan
        // To verify the gate-shape assertion (bitset accepted at K/N below
        // its threshold), override bitset_max_density_multi to 1/50.
        let n: u64 = 4096;
        let (index, _field, name) = build_index(
            n,
            Some(IndexSortByField {
                field: "fk".to_string(),
                order: Order::Asc,
            }),
            |i| i, // every value distinct, all in range
        );
        let (reader, column) = open_column(&index, &name);
        let cfg = TermSetStrategyConfig {
            gallop_enabled: false,
            bitset_max_density_multi: 1.0 / 50.0,
            ..TermSetStrategyConfig::default()
        };
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: Some(2),
                fast_available: true,
            },
            &term_set(0..60),
            &cfg,
        );
        assert_eq!(strat, TermSetStrategy::BitsetFromPostings);
    }

    #[test]
    fn select_returns_bitset_when_first_column_highly_selective() {
        // Unsorted, K well below the D=1 bitset gate when overridden to 1/50.
        // N = 4096, K = 8, D = 1 → K/N ≈ 0.00195 ≤ 1/50.
        let n: u64 = 4096;
        let (index, _field, name) = build_index(n, None, |i| i);
        let (reader, column) = open_column(&index, &name);
        let cfg = TermSetStrategyConfig {
            bitset_max_density_unique: 1.0 / 50.0,
            ..TermSetStrategyConfig::default()
        };
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: Some(1),
                fast_available: true,
            },
            &term_set(0..8),
            &cfg,
        );
        assert_eq!(strat, TermSetStrategy::BitsetFromPostings);
    }

    #[test]
    fn select_falls_back_to_linear_scan_when_density_exceeds_bitset_threshold() {
        // Unsorted, K large enough that K/N exceeds the D=1 gate.
        // N = 4096, K = 2000 → K/N ≈ 0.488; default unique gate is
        // 1/2000 = 0.0005, far below. Falls through to LinearScan.
        let n: u64 = 4096;
        let (index, _field, name) = build_index(n, None, |i| i);
        let (reader, column) = open_column(&index, &name);
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: Some(1),
                fast_available: true,
            },
            &term_set(0..2000),
            &TermSetStrategyConfig::default(),
        );
        assert_eq!(strat, TermSetStrategy::LinearScan);
    }

    #[test]
    fn select_returns_linear_scan_for_subsequent_column_when_kd_exceeds_threshold() {
        // Subsequent column with c=100, K·D/c = 50/100 = 0.5 > 0.25 →
        // bitset rejected, falls through to LinearScan.
        let n: u64 = 4096;
        let (index, _field, name) = build_index(n, None, |i| i);
        let (reader, column) = open_column(&index, &name);
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: Some(100),
                avg_docs_per_term: None,
                fast_available: true,
            },
            &term_set(0..50),
            &TermSetStrategyConfig::default(),
        );
        assert_eq!(strat, TermSetStrategy::LinearScan);
    }

    #[test]
    fn select_returns_bitset_from_postings_when_subsequent_column_kd_below_threshold() {
        // Subsequent column: K·D / C < subsequent_bitset_max_density.
        // K·D / C = 50 / 1024 ≈ 0.0488 < 1/4 = 0.25 → BitsetFromPostings.
        let n: u64 = 4096;
        let (index, _field, name) = build_index(n, None, |i| i);
        let (reader, column) = open_column(&index, &name);
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: Some(1024),
                avg_docs_per_term: None,
                fast_available: true,
            },
            &term_set(0..50),
            &TermSetStrategyConfig::default(),
        );
        assert_eq!(strat, TermSetStrategy::BitsetFromPostings);
    }

    /// Gallop precondition: only fires on sorted segments. The decision
    /// tree must still produce *some* sensible non-Gallop variant on
    /// unsorted input.
    #[test]
    fn select_skips_gallop_when_unsorted() {
        let n: u64 = 4096;
        let (index, _field, name) = build_index(n, None, |i| i);
        let (reader, column) = open_column(&index, &name);
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: None,
                fast_available: true,
            },
            &term_set(0..8),
            &TermSetStrategyConfig::default(),
        );
        assert!(!matches!(strat, TermSetStrategy::Gallop { .. }));
    }

    /// Gallop precondition: even with sort_by, a *different* sort field
    /// disables gallop. We build an index with two fast fields, sort by the
    /// second, and query the first — so
    /// `sort_by_field().field != inputs.field_name` fires.
    #[test]
    fn select_skips_gallop_when_field_mismatches_sort_field() {
        let n: u64 = 4096;
        let mut sb = SchemaBuilder::new();
        let fk = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
        let other = sb.add_u64_field("other", NumericOptions::default().set_fast().set_indexed());
        let schema = sb.build();
        let index = Index::builder()
            .schema(schema)
            .settings(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "other".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            })
            .create_in_ram()
            .unwrap();
        {
            let mut writer = index.writer_for_tests().unwrap();
            for i in 0..n {
                writer.add_document(doc!(fk => i, other => n - i)).unwrap();
            }
            writer.commit().unwrap();
        }
        let (reader, column) = open_column(&index, "fk");
        let cfg = TermSetStrategyConfig {
            bitset_max_density_unique: 1.0 / 50.0,
            ..TermSetStrategyConfig::default()
        };
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: "fk",
                candidate_size: None,
                avg_docs_per_term: Some(1),
                fast_available: true,
            },
            &term_set(0..8),
            &cfg,
        );
        assert!(!matches!(strat, TermSetStrategy::Gallop { .. }));
        // With K=8 on N=4096, K/N = 0.00195 ≤ 1/50 (overridden D=1 gate),
        // so the first-column branch picks BitsetFromPostings.
        assert_eq!(strat, TermSetStrategy::BitsetFromPostings);
    }

    /// Positive gallop assertion. Sorted ASC, small K, planner commits
    /// to Gallop because all structural preconditions hold.
    #[test]
    fn select_returns_gallop_when_sorted_and_small_k() {
        let n: u64 = 4096;
        let (index, _field, name) = build_index(
            n,
            Some(IndexSortByField {
                field: "fk".to_string(),
                order: Order::Asc,
            }),
            |i| i,
        );
        let (reader, column) = open_column(&index, &name);
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: None,
                fast_available: true,
            },
            &term_set(0..8), // K' = 8 < 4096/64 = 64
            &TermSetStrategyConfig::default(),
        );
        match strat {
            TermSetStrategy::Gallop {
                sort_order,
                sorted_terms,
            } => {
                assert!(matches!(sort_order, Order::Asc));
                // Pruned + sorted ascending; pruning preserves all 8 (all in [0, 4095]).
                assert_eq!(sorted_terms, vec![0, 1, 2, 3, 4, 5, 6, 7]);
            }
            other => panic!("expected Gallop, got {other:?}"),
        }
    }

    /// `strategy_sink` records the chosen variant on every call.
    /// `select_strategy` is invoked twice against differently-shaped corpora;
    /// the sink should reflect the *latest* strategy on each call (last-write
    /// semantics).
    #[test]
    fn select_writes_chosen_tag_to_strategy_sink() {
        use std::sync::atomic::{AtomicU8, Ordering};
        use std::sync::Arc;

        let n: u64 = 4096;
        let sink = Arc::new(AtomicU8::new(StrategyTag::None as u8));
        let cfg = TermSetStrategyConfig {
            strategy_sink: Some(sink.clone()),
            ..TermSetStrategyConfig::default()
        };

        // Sorted ASC, K=8 → Gallop.
        let (sorted_idx, _f, name_s) = build_index(
            n,
            Some(IndexSortByField {
                field: "fk".to_string(),
                order: Order::Asc,
            }),
            |i| i,
        );
        let (reader_s, col_s) = open_column(&sorted_idx, &name_s);
        let _ = select_strategy(
            &reader_s,
            &col_s,
            PlannerInputs {
                field_name: &name_s,
                candidate_size: None,
                avg_docs_per_term: None,
                fast_available: true,
            },
            &term_set(0..8),
            &cfg,
        );
        assert_eq!(
            StrategyTag::try_from(sink.load(Ordering::Relaxed)).unwrap(),
            StrategyTag::Gallop
        );

        // Unsorted, K=2000 → LinearScan (terminal fallback). Last-write-wins
        // overwrites the GALLOP tag from the previous call.
        let (unsorted_idx, _f, name_u) = build_index(n, None, |i| i);
        let (reader_u, col_u) = open_column(&unsorted_idx, &name_u);
        let _ = select_strategy(
            &reader_u,
            &col_u,
            PlannerInputs {
                field_name: &name_u,
                candidate_size: None,
                avg_docs_per_term: None,
                fast_available: true,
            },
            &term_set(0..2000),
            &cfg,
        );
        assert_eq!(
            StrategyTag::try_from(sink.load(Ordering::Relaxed)).unwrap(),
            StrategyTag::Linear
        );
    }

    /// Kill-switch: gallop_enabled=false forces a non-gallop strategy
    /// even when the segment is sorted and K is tiny.
    #[test]
    fn select_respects_gallop_enabled_kill_switch() {
        let n: u64 = 4096;
        let (index, _field, name) = build_index(
            n,
            Some(IndexSortByField {
                field: "fk".to_string(),
                order: Order::Asc,
            }),
            |i| i,
        );
        let (reader, column) = open_column(&index, &name);
        let cfg = TermSetStrategyConfig {
            gallop_enabled: false,
            bitset_max_density_unique: 1.0 / 50.0,
            ..TermSetStrategyConfig::default()
        };
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: Some(1),
                fast_available: true,
            },
            &term_set(0..8),
            &cfg,
        );
        assert!(!matches!(strat, TermSetStrategy::Gallop { .. }));
        // K=8 on N=4096, K/N = 0.00195 ≤ 1/50 (overridden D=1 gate),
        // so the first-column branch picks BitsetFromPostings.
        assert_eq!(strat, TermSetStrategy::BitsetFromPostings);
    }

    // ---- D-aware first-column bitset gate ----

    /// Build a generic unsorted N=20_000 column, K terms in range.
    fn run_dispatch_at(d: u32, k: usize) -> TermSetStrategy {
        let n: u64 = 20_000;
        let (index, _field, name) = build_index(n, None, |i| i);
        let (reader, column) = open_column(&index, &name);
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: Some(d),
                fast_available: true,
            },
            &term_set(0..k as u64),
            &TermSetStrategyConfig::default(),
        );
        strat
    }

    #[test]
    fn d1_gate_admits_bitset_just_below_threshold() {
        // N=20_000, threshold 1/2000 = 0.0005, so admit K such that
        // K/N ≤ 0.0005, i.e. K ≤ 10. Use K=10 → K/N = 0.0005 exactly.
        assert_eq!(run_dispatch_at(1, 10), TermSetStrategy::BitsetFromPostings);
    }

    #[test]
    fn d1_gate_rejects_bitset_just_above_threshold() {
        // K=11 → K/N = 0.00055 > 0.0005 → LinearScan.
        assert_eq!(run_dispatch_at(1, 11), TermSetStrategy::LinearScan);
    }

    #[test]
    fn d2_gate_admits_bitset_just_below_threshold() {
        // Threshold 1/200 = 0.005. K=100 → K/N = 0.005 exactly. Admit.
        assert_eq!(run_dispatch_at(2, 100), TermSetStrategy::BitsetFromPostings);
    }

    #[test]
    fn d2_gate_rejects_bitset_just_above_threshold() {
        // K=101 → K/N = 0.00505 > 0.005 → LinearScan.
        assert_eq!(run_dispatch_at(2, 101), TermSetStrategy::LinearScan);
    }

    #[test]
    fn d50_uses_the_multi_value_gate() {
        // K=20 → K/N = 0.001, well below the D≥2 gate of 1/200. Bitset.
        // (Confirms D=50 routes through the same threshold as D=2 —
        // any D ≥ 2 hits the multi-value gate.)
        assert_eq!(run_dispatch_at(50, 20), TermSetStrategy::BitsetFromPostings);
    }

    #[test]
    fn gallop_admits_at_any_density_when_segment_is_sorted() {
        // Gallop commits on any K/N as long as the structural
        // preconditions hold (sorted segment, matching field,
        // supported cardinality) — no density-component gate. Pick
        // K/N ≈ 0.012 to confirm.
        let n: u64 = 4096;
        let (index, _field, name) = build_index(
            n,
            Some(IndexSortByField {
                field: "fk".to_string(),
                order: Order::Asc,
            }),
            |i| i,
        );
        let (reader, column) = open_column(&index, &name);
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: Some(1),
                fast_available: true,
            },
            &term_set(0..50),
            &TermSetStrategyConfig::default(),
        );
        assert!(matches!(strat, TermSetStrategy::Gallop { .. }));
    }

    #[test]
    fn empty_inputs_route_to_empty_strategy() {
        let n: u64 = 4096;
        let (index, _field, name) = build_index(n, None, |i| i);
        let (reader, column) = open_column(&index, &name);
        // term_set empty → Empty.
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: Some(1),
                fast_available: true,
            },
            &[],
            &TermSetStrategyConfig::default(),
        );
        assert_eq!(strat, TermSetStrategy::Empty);
    }
}
