//! Microbenchmarks for `FastFieldTermSetQuery` (paradedb/paradedb#4895).
//!
//! # Tiers
//!
//! Three tiers driven by the `TERM_SET_BENCH_TIER` environment variable:
//!
//!   - `smoke` (default, target < 60s wall-clock): N ∈ {1M}, K ∈ {100, 10_000}, two corpus kinds ×
//!     two sort orders × four strategies plus the multi-column AND-intersection panel. Catches
//!     regressions; not for threshold derivation.
//!   - `full` (manual, ~30min): N ∈ {1M, 10M, 50M}, K ∈ {10, 100, 1_000, 10_000, 100_000}. 360-cell
//!     matrix used to derive `TermSetStrategyConfig::default()` densities.
//!   - `threshold` (~1min): targeted LowFk panel through K/N ∈ [0.002, 0.01] for fine-grained
//!     crossover characterization between the full tier's 10× geometric K spacing.
//!
//! # Corpus shapes
//!
//! The bench varies `D` (average documents per distinct value) across three
//! shapes that span the customer workloads we care about:
//!
//!   - `PrimaryKey` (`D = 1`): `value_for_doc(d) = d`, so every doc has a unique value. `distinct =
//!     N`. Models hash-join build sides on unique keys (UUIDs, surrogate IDs).
//!   - `LowFk` (`D ≈ 100`): `value_for_doc(d) = d / 100`, so each value appears in ~100 contiguous
//!     docs after sorting. `distinct = N/100`. Models foreign-key joins on moderate-cardinality
//!     columns — the typical paradedb hash-join pattern.
//!   - `HighFk` (`D ≈ 100_000`): `value_for_doc(d) = d / 100_000`. `distinct = N/100_000`. Models
//!     very-low-cardinality columns (status enums, region codes). Only present in the full tier at
//!     `N ≥ 10M` where there are enough distinct values to sample from.
//!
//! D shape matters for strategy choice: with `LowFk`, gallop emits ~D docs
//! per term through `RangeUnionDocSet`, which adds linear-equivalent cost
//! on top of the per-term search. PK doesn't pay that emission cost
//! (single-doc ranges). The `gallop_max_density` default is tuned to
//! LowFk because that's the dominant customer shape; PK queries with
//! K/N just above the threshold underutilize gallop slightly as a
//! tradeoff.
//!
//! # Strategy mapping
//!
//!   - `Gallop`: planner forced via `gallop_max_density = 1.0` so any K < N qualifies.
//!   - `Linear`: planner forced to terminal `LinearScan` via `gallop_enabled = false` + zero
//!     densities.
//!   - `PostingDirect`: synthetic — `BooleanQuery` of `TermQuery::Should` over the same terms.
//!     Strategy 4 in production isn't implemented yet; this gives a representative
//!     posting-list-union measurement.
//!   - `BitsetFromPostings`: synthetic — execute the same posting-union but materialize matched
//!     DocIds into a `BitSet` and iterate. Strategy 3 in production isn't implemented yet; this
//!     captures bitset construction + iteration cost on top of posting-list iteration.
//!
//! # Timing boundaries
//!
//! Inside the timed closure (work `searcher.search` does end-to-end per call):
//! `Term::from_field_u64` for each term; `FastFieldTermSetQuery::new` +
//! `with_strategy_config`; the inner `query.weight()` build; per-segment
//! `weight.scorer()` build (which runs `select_strategy`); the collector walk.
//! Multi-column `and_intersect` cells additionally pay one `BooleanQuery::new`
//! per iteration.
//!
//! Outside the timed closure (paid once per cell as setup): corpus build
//! (schema, index, writer, all `add_document` calls, commit), reader and
//! `Searcher` instantiation, and the deterministic `sample_terms` shuffle.
//! Only the raw `Vec<u64>` of sampled values is captured into the closure;
//! `Term::from_field_u64` runs per iteration.
//!
//! For cells with K ≥ ~1_000 the per-iteration construction cost is a
//! small fraction of total work (≤ ~5–10%), so the reported throughput
//! reflects scoring-loop performance. For very small K (10, 100) the
//! construction overhead is a larger fraction and binggan's
//! input-size-divided-by-time formula reaches artifact territory when
//! iterations finish in microseconds — relative ratios between strategies
//! at the same cell remain meaningful but absolute throughput numbers
//! below ~10ms are not reliable.
//!
//! # Captured outputs
//!
//! Captured outputs from the full and threshold tiers live in
//! `benches/term_set_queries.full-tier.txt` and
//! `benches/term_set_queries.threshold-tier.txt`. Re-run with
//!
//! ```text
//! TERM_SET_BENCH_TIER=full      cargo bench --bench term_set_queries 2>&1 \
//!     | tee benches/term_set_queries.full-tier.txt
//! TERM_SET_BENCH_TIER=threshold cargo bench --bench term_set_queries 2>&1 \
//!     | tee benches/term_set_queries.threshold-tier.txt
//! ```
//!
//! when the gallop algorithm or `TermSetStrategyConfig::default()`
//! changes meaningfully. Comment-only and test-only changes don't
//! warrant a refresh.

use binggan::{black_box, BenchRunner};
use common::BitSet;
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::{Collector, Count, DocSetCollector, SegmentCollector};
use tantivy::query::{
    BooleanQuery, FastFieldTermSetQuery, Occur, Query, TermQuery, TermSetStrategyConfig, Weight,
};
use tantivy::schema::{IndexRecordOption, NumericOptions, SchemaBuilder};
use tantivy::{
    doc, DocId, DocSet, Index, IndexSettings, IndexSortByField, Order, ReloadPolicy, Searcher,
    SegmentOrdinal, SegmentReader, Term,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CorpusKind {
    /// D = 1, every value unique.
    PrimaryKey,
    /// D ≈ 100, foreign-key shape.
    LowFk,
    /// D ≈ 100_000.
    HighFk,
}

impl CorpusKind {
    fn label(self) -> &'static str {
        match self {
            CorpusKind::PrimaryKey => "pk",
            CorpusKind::LowFk => "lowfk",
            CorpusKind::HighFk => "highfk",
        }
    }

    fn distinct_count(self, n: u64) -> u64 {
        match self {
            CorpusKind::PrimaryKey => n,
            CorpusKind::LowFk => n.div_ceil(100),
            CorpusKind::HighFk => n.div_ceil(100_000).max(1),
        }
    }

    fn value_for_doc(self, doc_id: u64) -> u64 {
        match self {
            CorpusKind::PrimaryKey => doc_id,
            CorpusKind::LowFk => doc_id / 100,
            CorpusKind::HighFk => doc_id / 100_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sort {
    Asc,
    None,
}

impl Sort {
    fn label(self) -> &'static str {
        match self {
            Sort::Asc => "asc",
            Sort::None => "unsorted",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Strat {
    Gallop,
    Linear,
    PostingDirect,
    BitsetFromPostings,
}

impl Strat {
    fn label(self) -> &'static str {
        match self {
            Strat::Gallop => "gallop",
            Strat::Linear => "linear",
            Strat::PostingDirect => "posting_direct",
            Strat::BitsetFromPostings => "bitset_from_postings",
        }
    }
}

fn applicable(sort: Sort, strat: Strat) -> bool {
    !(sort == Sort::None && strat == Strat::Gallop)
}

fn bench_tier() -> &'static str {
    match std::env::var("TERM_SET_BENCH_TIER").as_deref() {
        Ok("full") => "full",
        Ok("threshold") => "threshold",
        _ => "smoke",
    }
}

fn build_corpus(n: u64, kind: CorpusKind, sort: Sort) -> (Searcher, tantivy::schema::Field) {
    let mut sb = SchemaBuilder::new();
    let field = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
    let schema = sb.build();
    let mut builder = Index::builder().schema(schema);
    if let Sort::Asc = sort {
        builder = builder.settings(IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "fk".to_string(),
                order: Order::Asc,
            }),
            ..Default::default()
        });
    }
    let index = builder.create_in_ram().unwrap();

    {
        let writer_mem = (200_000_000u64).max(n * 32);
        let mut writer = index
            .writer_with_num_threads(1, writer_mem as usize)
            .unwrap();
        for d in 0..n {
            writer
                .add_document(doc!(field => kind.value_for_doc(d)))
                .unwrap();
        }
        writer.commit().unwrap();
    }
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    (reader.searcher(), field)
}

fn sample_terms(distinct: u64, k: usize, seed: u64) -> Vec<u64> {
    let k = k.min(distinct as usize);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut chosen: Vec<u64> = (0..distinct).collect();
    chosen.shuffle(&mut rng);
    chosen.truncate(k);
    chosen
}

fn cfg_force_gallop() -> TermSetStrategyConfig {
    TermSetStrategyConfig {
        gallop_enabled: true,
        // Strict less-than: K/N < 1.0 admits any K < N.
        gallop_max_density: 1.0,
        // The other thresholds don't matter for sorted+small-K cases —
        // once gallop is taken, the sort-agnostic branch isn't reached.
        ..TermSetStrategyConfig::default()
    }
}

fn cfg_force_linear() -> TermSetStrategyConfig {
    TermSetStrategyConfig {
        gallop_enabled: false,
        gallop_max_density: 0.0,
        // Strict less-than: nothing is < 0.0, so the sort-agnostic posting
        // / bitset arms are rejected and we land on the LinearScan terminal.
        posting_max_density: 0.0,
        bitset_max_density: 0.0,
        hash_probe_max_density: 0.0,
        subsequent_bitset_max_density: 0.0,
        strategy_sink: None,
    }
}

/// Run the planner-driven path with the given config. `K` distinct terms
/// from `[0, distinct_count)` are emitted as `Term::from_field_u64`.
fn run_planner_path(
    searcher: &Searcher,
    field: tantivy::schema::Field,
    terms: &[u64],
    cfg: TermSetStrategyConfig,
) -> usize {
    let q = FastFieldTermSetQuery::new(terms.iter().map(|v| Term::from_field_u64(field, *v)))
        .with_strategy_config(cfg);
    searcher.search(&q, &Count).unwrap()
}

/// Synthetic Strategy 4: posting-list union via `BooleanQuery` of
/// `TermQuery::Should`. Each `TermQuery` walks its posting list; the union
/// scorer interleaves them. No bitset materialization.
fn run_posting_direct_synthetic(
    searcher: &Searcher,
    field: tantivy::schema::Field,
    terms: &[u64],
) -> usize {
    let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
        .iter()
        .map(|&v| {
            (
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_u64(field, v),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>,
            )
        })
        .collect();
    let q = BooleanQuery::new(subqueries);
    searcher.search(&q, &Count).unwrap()
}

/// Synthetic Strategy 3: posting-list union into a `BitSet`, then iterate
/// the bitset. Captures bitset construction + iteration cost on top of
/// posting-list iteration. We use a custom `Collector` to write each
/// matching DocId into a per-segment `BitSet`, then merge by iteration.
fn run_bitset_from_postings_synthetic(
    searcher: &Searcher,
    field: tantivy::schema::Field,
    terms: &[u64],
) -> usize {
    let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
        .iter()
        .map(|&v| {
            (
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_u64(field, v),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>,
            )
        })
        .collect();
    let q = BooleanQuery::new(subqueries);
    searcher.search(&q, &BitSetCounter).unwrap()
}

/// Collector that, per segment, materializes matches into a `BitSet` sized
/// to the segment's `max_doc`, then walks the bitset to count.
struct BitSetCounter;

impl Collector for BitSetCounter {
    type Fruit = usize;
    type Child = BitSetSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let max_doc = segment_reader.max_doc();
        Ok(BitSetSegmentCollector {
            bitset: BitSet::with_max_value(max_doc),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<usize>) -> tantivy::Result<usize> {
        Ok(segment_fruits.into_iter().sum())
    }
}

struct BitSetSegmentCollector {
    bitset: BitSet,
}

impl SegmentCollector for BitSetSegmentCollector {
    type Fruit = usize;

    fn collect(&mut self, doc: DocId, _score: f32) {
        self.bitset.insert(doc);
    }

    fn harvest(self) -> usize {
        // Iterate the bitset to simulate the "walk bitset to emit DocIds"
        // step that Strategy 3 performs after the per-term phase.
        let mut docset: tantivy::query::BitSetDocSet = self.bitset.into();
        let mut count = 0usize;
        while docset.doc() != tantivy::TERMINATED {
            count += 1;
            docset.advance();
        }
        count
    }
}

#[allow(dead_code)]
fn _docset_collector_smoke(searcher: &Searcher) {
    // Reserved: anchor the DocSetCollector import for cells that may want
    // sorted-DocId outputs. Currently unused; left here so adding such
    // cells doesn't require import gymnastics.
    let _ = searcher.search(
        &FastFieldTermSetQuery::new(Vec::<Term>::new()),
        &DocSetCollector,
    );
}
#[allow(dead_code)]
fn _weight_smoke<W: Weight>(_w: &W) {}

fn matrix_for_tier(tier: &str) -> (Vec<u64>, Vec<usize>, Vec<CorpusKind>) {
    match tier {
        "full" => (
            vec![1_000_000, 10_000_000, 50_000_000],
            vec![10, 100, 1_000, 10_000, 100_000],
            vec![
                CorpusKind::PrimaryKey,
                CorpusKind::LowFk,
                CorpusKind::HighFk,
            ],
        ),
        // Smoke skips HighFk at N=1M because its `distinct = 10` excludes
        // every smoke K level — building its corpus would be wasted work.
        // HighFk is exercised only in the full tier at N >= 10M where larger
        // K values can apply.
        _ => (
            vec![1_000_000],
            vec![100, 10_000],
            vec![CorpusKind::PrimaryKey, CorpusKind::LowFk],
        ),
    }
}

fn main() {
    let tier = bench_tier();

    // Threshold tier: targeted measurements to close the LowFk gallop-vs-linear
    // crossover gap left by the full tier's 10x geometric K spacing. Runs only
    // a focused (LowFk, ASC, gallop+linear) panel at K values distributed
    // log-uniformly through K/N ∈ (0.001, 0.01). Bypasses the matrix loop
    // entirely; doesn't touch smoke or full tier code paths.
    if tier == "threshold" {
        run_threshold_tier();
        return;
    }

    let (n_levels, k_levels, kinds) = matrix_for_tier(tier);
    let sorts = [Sort::Asc, Sort::None];
    let strategies = [
        Strat::Gallop,
        Strat::Linear,
        Strat::PostingDirect,
        Strat::BitsetFromPostings,
    ];

    let mut runner = BenchRunner::new();

    for &n in &n_levels {
        for &kind in kinds.iter() {
            for &sort in &sorts {
                let (searcher, field) = build_corpus(n, kind, sort);
                let mut group = runner.new_group();
                group.set_name(format!("n={n} kind={} sort={}", kind.label(), sort.label()));
                group.set_input_size(n as usize);
                let distinct = kind.distinct_count(n);
                for &k in &k_levels {
                    if (k as u64) > distinct {
                        continue;
                    }
                    // Smoke trim: K=10K cells on the unsorted groups duplicate
                    // information already in the corresponding asc groups
                    // (linear/posting/bitset are sort-insensitive). Drop them
                    // in smoke to keep wall-clock under 90s after the
                    // and_intersect cells were added. Full tier still has them.
                    if tier != "full" && sort == Sort::None && k == 10_000 {
                        continue;
                    }
                    let terms = sample_terms(distinct, k, 7);
                    for &strat in &strategies {
                        if !applicable(sort, strat) {
                            continue;
                        }
                        // Force-gallop requires K < N strictly; with K = N
                        // the strict-less-than would still fail. Skip the
                        // degenerate cell so timings aren't reported as
                        // gallop misses.
                        if strat == Strat::Gallop && (k as u64) >= n {
                            continue;
                        }
                        let name = format!("k={k} strat={}", strat.label());
                        let searcher = searcher.clone();
                        let terms_v = terms.clone();
                        group.register(name, move |_| match strat {
                            Strat::Gallop => black_box(run_planner_path(
                                &searcher,
                                field,
                                &terms_v,
                                cfg_force_gallop(),
                            )),
                            Strat::Linear => black_box(run_planner_path(
                                &searcher,
                                field,
                                &terms_v,
                                cfg_force_linear(),
                            )),
                            Strat::PostingDirect => {
                                black_box(run_posting_direct_synthetic(&searcher, field, &terms_v))
                            }
                            Strat::BitsetFromPostings => black_box(
                                run_bitset_from_postings_synthetic(&searcher, field, &terms_v),
                            ),
                        });
                    }
                }
                group.run();
            }
        }
    }

    // Multi-column AND-intersection cells. Probes the smart-seek win on the
    // linear-scan path: column A is sorted (gallop), column B is unsorted
    // (linear) — BooleanQuery::Must of two FastFieldTermSetQueries. With the
    // trait-default seek, column B's per-`seek(target)` walk dilutes
    // column A's gallop selectivity; with the smart-seek override on
    // TermSetDocSet, column B jumps directly to each target.
    //
    // Two corpus shapes are exercised:
    //   - dense FK (D ≈ 100): gallop output is ~1500 contiguous ranges of ~100 docs each. B's seeks
    //     rarely cross range boundaries, so the smart-vs-default delta is bounded — the lower-bound
    //     case.
    //   - sparse PK (D = 1): gallop output is 1500 *isolated* DocIds spread across the segment.
    //     Every gallop emit forces B to skip a large gap, which is exactly where smart seek pays
    //     off — the upper-bound case for typical hash-join build sides.
    run_and_intersect_cells(&mut runner);
}

/// Targeted threshold-tier panel: closes the LowFk K/N ∈ (0.001, 0.01) gap
/// the full-tier matrix skipped. Eight (K, N) cells × two strategies
/// (gallop, linear), all on LowFk + sorted ASC. Reuses the existing
/// build_corpus / sample_terms / cfg_force_* primitives so per-cell
/// behavior is identical to the matrix cells — just different K values.
fn run_threshold_tier() {
    // (K, N) pairs spread log-uniformly through K/N ∈ (0.001, 0.01).
    // Five at N=1M (K/N = 0.002, 0.003, 0.005, 0.007, 0.010) and three at
    // N=10M (K/N = 0.003, 0.005, 0.007) so we can also check whether the
    // crossover is N-dependent.
    let cells: &[(usize, u64)] = &[
        (2_000, 1_000_000),
        (3_000, 1_000_000),
        (5_000, 1_000_000),
        (7_000, 1_000_000),
        (10_000, 1_000_000),
        (30_000, 10_000_000),
        (50_000, 10_000_000),
        (70_000, 10_000_000),
    ];

    // Build one corpus per N (LowFk distinct = N/100, comfortably above max
    // K=70K at N=10M).
    let mut runner = BenchRunner::new();
    for &n in &[1_000_000u64, 10_000_000u64] {
        let (searcher, field) = build_corpus(n, CorpusKind::LowFk, Sort::Asc);
        let mut group = runner.new_group();
        group.set_name(format!("threshold n={n} kind=lowfk sort=asc"));
        group.set_input_size(n as usize);

        let distinct = CorpusKind::LowFk.distinct_count(n);
        for &(k, n_cell) in cells {
            if n_cell != n {
                continue;
            }
            assert!(
                (k as u64) <= distinct,
                "k={k} exceeds LowFk distinct={distinct} at n={n}",
            );
            let terms = sample_terms(distinct, k, 7);
            for &strat in &[Strat::Gallop, Strat::Linear] {
                let s = searcher.clone();
                let terms_v = terms.clone();
                let cell_name = format!("k={k} strat={}", strat.label());
                group.register(cell_name, move |_| match strat {
                    Strat::Gallop => {
                        black_box(run_planner_path(&s, field, &terms_v, cfg_force_gallop()))
                    }
                    Strat::Linear => {
                        black_box(run_planner_path(&s, field, &terms_v, cfg_force_linear()))
                    }
                    _ => unreachable!(),
                });
            }
        }
        group.run();
    }
}

/// Build the (a sorted, b unsorted) two-column AND-intersection bench cells.
/// Extracted so the corpus-build cost is paid once per (kind) shape.
fn run_and_intersect_cells(runner: &mut BenchRunner) {
    for &(label, density_label) in &[("dense_fk", "D=100"), ("sparse_pk", "D=1")] {
        let (searcher, a, b, n, a_distinct, b_distinct) = build_and_intersect_corpus(label);

        let mut group = runner.new_group();
        group.set_name(format!("and_intersect n=1M kind={label} ({density_label})"));
        group.set_input_size(n as usize);

        // Sample terms uniformly across each column's distinct value space
        // (rather than using the contiguous prefix `0..1500`). This is the
        // load-bearing detail for the sparse_pk cell: with a = doc_id (D=1),
        // a contiguous prefix `0..1500` would map gallop's output to a SINGLE
        // contiguous DocId range [0, 1500), defeating the very thing we're
        // trying to measure (sparse, scattered gallop hits forcing many big
        // seeks into B). Random sampling spreads the 1500 hits across the
        // full segment so each forces a real ~N/K-doc gap-skip.
        let a_terms: Vec<u64> = sample_terms(a_distinct, 1500, 7);
        let b_terms: Vec<u64> = sample_terms(b_distinct, 1500, 11);

        let s = searcher.clone();
        let at = a_terms.clone();
        let bt = b_terms.clone();
        group.register("a=gallop b=linear", move |_| {
            let qa = Box::new(
                FastFieldTermSetQuery::new(at.iter().map(|v| Term::from_field_u64(a, *v)))
                    .with_strategy_config(cfg_force_gallop()),
            ) as Box<dyn Query>;
            let qb = Box::new(
                FastFieldTermSetQuery::new(bt.iter().map(|v| Term::from_field_u64(b, *v)))
                    .with_strategy_config(cfg_force_linear()),
            ) as Box<dyn Query>;
            let bq = BooleanQuery::new(vec![(Occur::Must, qa), (Occur::Must, qb)]);
            black_box(s.search(&bq, &Count).unwrap())
        });

        let s = searcher.clone();
        let at = a_terms.clone();
        let bt = b_terms.clone();
        group.register("a=linear b=linear", move |_| {
            let qa = Box::new(
                FastFieldTermSetQuery::new(at.iter().map(|v| Term::from_field_u64(a, *v)))
                    .with_strategy_config(cfg_force_linear()),
            ) as Box<dyn Query>;
            let qb = Box::new(
                FastFieldTermSetQuery::new(bt.iter().map(|v| Term::from_field_u64(b, *v)))
                    .with_strategy_config(cfg_force_linear()),
            ) as Box<dyn Query>;
            let bq = BooleanQuery::new(vec![(Occur::Must, qa), (Occur::Must, qb)]);
            black_box(s.search(&bq, &Count).unwrap())
        });

        group.run();
    }
}

fn build_and_intersect_corpus(
    kind: &str,
) -> (
    Searcher,
    tantivy::schema::Field,
    tantivy::schema::Field,
    u64,
    u64, // a_distinct
    u64, // b_distinct
) {
    let n: u64 = 1_000_000;
    let mut sb = SchemaBuilder::new();
    let a = sb.add_u64_field("a", NumericOptions::default().set_fast().set_indexed());
    let b = sb.add_u64_field("b", NumericOptions::default().set_fast().set_indexed());
    let schema = sb.build();
    let index = Index::builder()
        .schema(schema)
        .settings(IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "a".to_string(),
                order: Order::Asc,
            }),
            ..Default::default()
        })
        .create_in_ram()
        .unwrap();
    {
        let mut writer = index.writer_with_num_threads(1, 200_000_000).unwrap();
        for d in 0..n {
            // dense_fk:  a = d / 100   → ~10K distinct values, ~100 docs each (D≈100).
            // sparse_pk: a = d         → 1M distinct values, 1 doc each (D=1).
            // b is the same in both: an unsorted spread of ~9973 distinct values.
            let a_val = if kind == "sparse_pk" { d } else { d / 100 };
            writer
                .add_document(doc!(a => a_val, b => (d * 7 + 13) % 9973))
                .unwrap();
        }
        writer.commit().unwrap();
    }
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let a_distinct = if kind == "sparse_pk" {
        n
    } else {
        n.div_ceil(100)
    };
    let b_distinct = 9973u64;
    (reader.searcher(), a, b, n, a_distinct, b_distinct)
}
