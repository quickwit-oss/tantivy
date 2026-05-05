//! Strategy selection for `FastFieldTermSetQuery`.
//!
//! Mirrors the structure of `range_query::range_query_fastfield`:
//! `Weight::scorer()` consults this module to decide between gallop, linear, or
//! future strategies, then constructs the right `DocSet`.
//!
//! See `design.md` §4 (decision tree) and `implementation.md` §2.1 for the
//! background. For #4895 the planner returns its full set of variants from day
//! one — the dispatch in `FastFieldTermSetWeight::scorer()` will route the
//! non-`Gallop` / non-`LinearScan` variants to the existing `TermSetDocSet`
//! until follow-ups A and B fill them in.
//!
//! ## Note on `inputs.avg_docs_per_term` (`D`)
//!
//! Computing `D` precisely from the column alone would require posting-list
//! lookups (which are exactly the work Strategy 4 wants to amortize). We
//! accept `None`, which `select_strategy` treats as `D = 1`. That biases
//! the first-column branch toward `PostingListDirect`, but since the
//! dispatch site stubs both `PostingListDirect` and `BitsetFromPostings`
//! to `LinearScan` until follow-ups A and B fill them in, the bias is
//! invisible at runtime and only shows up in unit tests on the planner.

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
    Posting = 4,
    Hash = 5,
}

impl TryFrom<u8> for StrategyTag {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(StrategyTag::None),
            1 => Ok(StrategyTag::Gallop),
            2 => Ok(StrategyTag::Linear),
            3 => Ok(StrategyTag::Bitset),
            4 => Ok(StrategyTag::Posting),
            5 => Ok(StrategyTag::Hash),
            _ => Err("unknown StrategyTag value"),
        }
    }
}

/// User-tunable density thresholds. Defaults match the starting estimates in
/// `design.md` §4 and are overridden via the `paradedb.term_set_*_max_density`
/// GUCs at the consumer side.
///
/// "Density" is a unitless ratio of *matching count over corpus count*. For
/// gallop, that's `K' / N`; for posting/bitset on the first column, it's
/// `K' · D / N`; for the subsequent-column branch, `K' · D / C` (or `C / N`
/// for the hash-probe gate). Each strategy fires when its density is *below*
/// the corresponding `_max_density` threshold.
///
/// We use `f64` rather than integer denominators so bench-derived
/// thresholds don't have to round to `1/N` for small `N`.
#[derive(Clone, Debug)]
pub struct TermSetStrategyConfig {
    /// Kill-switch: when `false`, the planner never returns `Gallop` even if
    /// every other gate would pass.
    pub gallop_enabled: bool,
    /// Gallop fires when `K' / N < gallop_max_density` on a sorted segment.
    /// Default `1/100 = 0.01`. At K/N = 0.01 gallop wins ~2.6× over linear
    /// on LowFk-shaped corpora with consistent behavior across N values.
    /// The corpus generator caps `distinct = N/100`, so the empirical
    /// crossover above 0.01 isn't pinned for LowFk; PK-shaped corpora
    /// show a different (D-dependent) crossover — see Follow-up H.
    pub gallop_max_density: f64,
    /// First-column `PostingListDirect` threshold: `K' · D / N` cutoff.
    pub posting_max_density: f64,
    /// First-column `BitsetFromPostings` threshold: `K' · D / N` cutoff.
    pub bitset_max_density: f64,
    /// Subsequent-column `HashProbe` gate: `C / N` cutoff (`C` = upstream
    /// candidate-set size). Below this, the candidate set is small enough to
    /// hash-probe rather than build a bitset.
    pub hash_probe_max_density: f64,
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
            // gallop_max_density: 1/100 — bench-tuned (see field doc).
            // posting/bitset/hash_probe/subsequent_bitset: 1/256, 1/4, 1/16,
            // 1/4 — starting estimates; only consulted by strategies that
            // are stubs in #4895, so they don't affect runtime today.
            gallop_max_density: 1.0 / 100.0,
            posting_max_density: 1.0 / 256.0,
            bitset_max_density: 1.0 / 4.0,
            hash_probe_max_density: 1.0 / 16.0,
            subsequent_bitset_max_density: 1.0 / 4.0,
            strategy_sink: None,
        }
    }
}

/// Selected execution strategy. The non-`Gallop` / non-`LinearScan` variants
/// are reserved for follow-ups A and B; `select_strategy` returns them today
/// so unit tests can pin the planner shape, but `scorer()` routes them through
/// the `TermSetDocSet` linear path until those strategies are implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TermSetStrategy {
    /// Sorted-segment fast path. Carries the pruned, ascending-sorted term list
    /// because the planner already had to compute it for min/max pruning.
    Gallop {
        sort_order: Order,
        sorted_terms: Vec<u64>,
    },
    /// Today's `TermSetDocSet`. Also the terminal fallback when no other
    /// strategy qualifies.
    LinearScan,
    /// Reserved (follow-up B).
    BitsetFromPostings,
    /// Reserved (follow-up A).
    PostingListDirect,
    /// Reserved (subsequent-column work, post-#4895).
    HashProbe,
}

/// Inputs to the planner that aren't already on `Column`.
pub struct PlannerInputs<'a> {
    pub field_name: &'a str,
    /// Upstream candidate-set size. `None` ≡ full segment scan (first column).
    pub candidate_size: Option<u32>,
    /// Average documents per term in this column. `None` is treated as `D = 1`.
    pub avg_docs_per_term: Option<u32>,
}

/// Run the decision tree.
///
/// First, prune the term set against the column's `[min, max]`. Then try
/// the sort-dependent gallop fast path. Otherwise fall through to the
/// sort-agnostic dispatch, which branches on `C < N` (subsequent column)
/// vs `C == N` (first column).
///
/// When `cfg.strategy_sink` is `Some`, the chosen `StrategyTag` is stored
/// (as `u8`) on the sink before returning so consumers can surface it in
/// `EXPLAIN ANALYZE`. The thin wrapper around the inner planner keeps the
/// per-segment hot path uncluttered.
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
            TermSetStrategy::PostingListDirect => StrategyTag::Posting,
            TermSetStrategy::HashProbe => StrategyTag::Hash,
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
    let n = column.num_docs();
    if n == 0 || term_set.is_empty() {
        // The empty-input case is handled upstream as `EmptyScorer`; falling
        // through to `LinearScan` here is a no-op for correctness.
        return TermSetStrategy::LinearScan;
    }

    // Count terms surviving min/max pruning. Non-gallop branches only need
    // K' to evaluate density gates; allocating a `Vec<u64>` here just to
    // discard it on those branches is wasted work. The Gallop branch below
    // does the allocation only when it commits to that strategy.
    let lo = column.min_value();
    let hi = column.max_value();
    let in_range = |v: u64| v >= lo && v <= hi;
    let k_prime = term_set.iter().copied().filter(|&v| in_range(v)).count() as u32;
    if k_prime == 0 {
        // All terms pruned — return `LinearScan` as the natural fallback.
        // `scorer()` upstream will detect the empty result via its own
        // empty-range checks.
        return TermSetStrategy::LinearScan;
    }

    let candidate_eq_n = inputs.candidate_size.is_none_or(|c| c == n);
    let cardinality = column.get_cardinality();
    let n_f = n as f64;

    // Try gallop (sort-dependent fast path).
    if cfg.gallop_enabled
        && candidate_eq_n
        && matches!(cardinality, Cardinality::Full | Cardinality::Optional)
    {
        if let Some(order) = reader
            .sort_by_field()
            .filter(|sbf| sbf.field == inputs.field_name)
            .map(|sbf| sbf.order)
        {
            // K' / N < gallop_max_density. Strict less-than matches the
            // pre-refactor integer form `K' < N / R_GALLOP` at every
            // exactly-representable density (the defaults are 1/2^k).
            if (k_prime as f64) / n_f < cfg.gallop_max_density {
                // Gallop is the only branch that needs the pruned Vec
                // (sorted + deduped + handed to `TermSetGallopDocSet`), so
                // allocate it only here.
                let mut pruned: Vec<u64> =
                    term_set.iter().copied().filter(|&v| in_range(v)).collect();
                pruned.sort_unstable();
                // Dedup duplicate input terms. `TermSetGallopDocSet`
                // tolerates duplicates (the second occurrence finds an
                // empty range and skips), but each duplicate still pays
                // two `gallop_search_sorted` probes — cheap insurance on
                // the hot path. Production input today flows through
                // HashSet-derived sources so this is a no-op there;
                // `TermSetQuery::new` doesn't enforce uniqueness on its
                // input, hence the belt-and-suspenders dedup.
                pruned.dedup();
                return TermSetStrategy::Gallop {
                    sort_order: order,
                    sorted_terms: pruned,
                };
            }
        }
    }

    // Sort-agnostic selection.
    let d = inputs.avg_docs_per_term.unwrap_or(1) as u64;
    let kd = (k_prime as u64).saturating_mul(d) as f64;

    if let Some(c) = inputs.candidate_size.filter(|&c| c < n) {
        // Subsequent column. `c == 0` is degenerate (the upstream is empty),
        // but guard the division anyway: with no candidates, no work to do
        // — HashProbe is the cheapest fallthrough.
        if c == 0 {
            return TermSetStrategy::HashProbe;
        }
        if (c as f64) / n_f < cfg.hash_probe_max_density {
            return TermSetStrategy::HashProbe;
        }
        if kd / (c as f64) < cfg.subsequent_bitset_max_density {
            return TermSetStrategy::BitsetFromPostings;
        }
        TermSetStrategy::HashProbe
    } else {
        // First column, post-gallop.
        let kd_density = kd / n_f;
        if kd_density < cfg.posting_max_density {
            return TermSetStrategy::PostingListDirect;
        }
        if kd_density < cfg.bitset_max_density {
            return TermSetStrategy::BitsetFromPostings;
        }
        TermSetStrategy::LinearScan
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests pin the planner's *output shape* on cases it can decide
    //! today. The `_planner_shape_only` suffix is a deliberate signal:
    //! the dispatch site maps every non-Gallop / non-LinearScan variant to
    //! `TermSetDocSet`. Once Follow-up A populates `inputs.avg_docs_per_term`
    //! properly, the `K' · D < threshold` arithmetic will shift and these
    //! tests will need to be revisited.
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
            },
            &term_set([10_000_000u64, 99_999_999, u64::MAX]),
            &TermSetStrategyConfig::default(),
        );
        // All terms pruned → LinearScan (scorer() will turn it into EmptyScorer).
        assert_eq!(strat, TermSetStrategy::LinearScan);
    }

    #[test]
    fn select_returns_bitset_from_postings_when_gallop_rejected_due_to_high_k_planner_shape_only() {
        // Sorted ASC, but K is *too big* for gallop: K'/N >= gallop_max_density.
        // With D = 1, we want K'·D / N < bitset_max_density as well so the
        // sort-agnostic branch lands on BitsetFromPostings (and not
        // PostingListDirect, which requires K'·D / N < posting_max_density
        // — a tighter threshold).
        // Defaults: gallop=1/64≈0.0156, posting=1/256≈0.00391, bitset=1/4=0.25.
        // N = 4096, K = 200 → K/N = 200/4096 ≈ 0.0488:
        //   0.0488 ≥ 0.0156 → gallop rejected
        //   0.0488 ≥ 0.00391 → posting rejected
        //   0.0488 < 0.25  → bitset accepted
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
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: None,
            },
            &term_set(0..200), // K' = 200 (all in [0, 4095])
            &TermSetStrategyConfig::default(),
        );
        assert_eq!(strat, TermSetStrategy::BitsetFromPostings);
    }

    #[test]
    fn select_returns_posting_direct_when_first_column_highly_selective_planner_shape_only() {
        // Unsorted, K small enough that K·D / N < posting_max_density.
        // N = 4096, K = 8, D = 1 → K/N = 0.00195 < 1/256 ≈ 0.00391.
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
            },
            &term_set(0..8),
            &TermSetStrategyConfig::default(),
        );
        assert_eq!(strat, TermSetStrategy::PostingListDirect);
    }

    #[test]
    fn select_falls_back_to_linear_scan_only_when_kd_exceeds_all_thresholds() {
        // Unsorted, K large enough that K·D / N >= every threshold in the
        // first-column branch. With D = 1 and bitset_max_density = 1/4 = 0.25,
        // N = 4096 and K = 2000 → K/N ≈ 0.488 > 0.25 → LinearScan.
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
            },
            &term_set(0..2000),
            &TermSetStrategyConfig::default(),
        );
        assert_eq!(strat, TermSetStrategy::LinearScan);
    }

    #[test]
    fn select_returns_hash_probe_when_subsequent_column_with_small_c_planner_shape_only() {
        // Subsequent column: C / N < hash_probe_max_density.
        // N = 4096, C = 100 → 100/4096 ≈ 0.0244 < 1/16 = 0.0625.
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
            },
            &term_set(0..50),
            &TermSetStrategyConfig::default(),
        );
        assert_eq!(strat, TermSetStrategy::HashProbe);
    }

    #[test]
    fn select_returns_bitset_from_postings_when_subsequent_column_kd_below_threshold_planner_shape_only(
    ) {
        // Subsequent column: K·D / C < subsequent_bitset_max_density.
        // N = 4096. C = 1024 → C/N = 0.25 ≥ hash_probe_max_density (0.0625),
        // so the HashProbe gate is bypassed.
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
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: "fk",
                candidate_size: None,
                avg_docs_per_term: None,
            },
            &term_set(0..8),
            &TermSetStrategyConfig::default(),
        );
        assert!(!matches!(strat, TermSetStrategy::Gallop { .. }));
        // With K=8, the first-column branch picks PostingListDirect.
        assert_eq!(strat, TermSetStrategy::PostingListDirect);
    }

    /// Positive gallop assertion. Sorted ASC, K small, K'/N below the
    /// gallop_max_density threshold.
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
            ..TermSetStrategyConfig::default()
        };
        let strat = select_strategy(
            &reader,
            &column,
            PlannerInputs {
                field_name: &name,
                candidate_size: None,
                avg_docs_per_term: None,
            },
            &term_set(0..8),
            &cfg,
        );
        assert!(!matches!(strat, TermSetStrategy::Gallop { .. }));
        // Selection given K=8: PostingListDirect
        // (K · D = 8 < N · posting_max_density = 16).
        assert_eq!(strat, TermSetStrategy::PostingListDirect);
    }
}
