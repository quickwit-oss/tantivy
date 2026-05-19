//! Microbenchmarks for `FastFieldTermSetQuery`.
//!
//! # Tiers
//!
//! Four tiers driven by the `TERM_SET_BENCH_TIER` environment variable.
//! All four are designed for the SSTable backend (run with
//! `--features quickwit`). Run a tier with:
//!
//! ```text
//! TERM_SET_BENCH_TIER=<tier> cargo bench --bench term_set_queries --features quickwit
//! ```
//!
//!   - `smoke` (default; ~1 min): regression-guard sweep. N=1M, K ∈ {100, 10K}, two corpus kinds ×
//!     two sort orders × six force-dispatched strategies, plus the multi-column AND-intersection
//!     panel. Catches functional regressions; not for threshold derivation.
//!   - `production` (~5 min): planner-default dispatch on three corpora (PK_1M, PK_20M, LowFk_20M)
//!     × {sorted, unsorted} × {planner, linear_forced, batched_forced, gallop_forced}. Prints the
//!     strategy the planner picked per cell via a probe call against
//!     `TermSetStrategyConfig::strategy_sink`. This is what reviewers run to validate dispatch
//!     end-to-end.
//!   - `cost_model` (~10 min): force-dispatched `linear` vs `batched_bitset` sweep across D ∈ {2,
//!     5, 20, 50} and K ∈ {200, 1K, 5K, 10K, 50K, 100K}. Justifies the `1/2000` (D=1) and `1/200`
//!     (D ≥ 2) defaults in `TermSetStrategyConfig` — batched/linear crossovers for D ≥ 2 cluster at
//!     K/N ≈ 0.0055–0.0088; the threshold sits just below the tightest measured crossover.
//!   - `compare` (~3 min): force-dispatched 4-way (linear, gallop, direct_bitset, batched_bitset)
//!     on the production matrix. Useful when iterating on the bitset scorer or comparing batched vs
//!     direct dictionary lookups.
//!
//! # Corpus shapes
//!
//! `D` is the average number of documents per distinct value.
//!
//!   - `smoke` exercises two named shapes via `CorpusKind`:
//!       - `PrimaryKey` (D = 1): every doc has a unique value; `distinct = N`. Hash-join build
//!         sides on unique keys.
//!       - `LowFk` (D ≈ 100): each value appears in ~100 contiguous docs after sorting; `distinct =
//!         N/100`. The typical paradedb foreign-key shape.
//!   - `production`, `cost_model`, `compare` instead parameterize D directly via
//!     `build_corpus_parametric_d{,_sorted}(N, D)`, with `value_for_doc(i) = i / D`. `cost_model`
//!     sweeps D ∈ {2, 5, 20, 50}; the other tiers use D ∈ {1, 100} to match the production PK and
//!     LowFk shapes at N=20M.
//!
//! # Strategy mapping (smoke / compare)
//!
//!   - `Gallop`: planner forced via `cfg_force_gallop()`.
//!   - `Linear`: planner forced to terminal `LinearScan` via `cfg_force_linear()` (gallop disabled
//!     + zero densities).
//!   - `BitsetFromPostings` (real, exercised as `bitset_from_postings_real`): planner forced via
//!     `cfg_force_bitset_real()` (both bitset densities = 1.0).
//!   - `DirectBitset`: bench-only `DirectBitsetQuery` wrapper that drives K individual
//!     `term_dict.get(key)` calls into a bitset OR (no batched dictionary lookup). Used by
//!     `cost_model` to measure the per-block-decode amortization the batched API provides.
//!   - `PostingDirect`: synthetic baseline — `BooleanQuery` of `TermQuery::Should`. Exercises
//!     `BufferedUnionScorer`. Retained to characterize union scaling cost.
//!   - `BitsetFromPostings` (synthetic, `bitset_from_postings`): synthetic baseline —
//!     `BooleanQuery::Should` union materialized into a `BitSet` via a custom collector.
//!     Characterizes bitset construction + iteration cost on top of the union scorer.
//!
//! # Timing boundaries
//!
//! Inside the timed closure (work `searcher.search` does end-to-end per
//! call): `Term::from_field_u64` for each term; `FastFieldTermSetQuery::new`
//! + `with_strategy_config`; the inner `query.weight()` build; per-segment
//! `weight.scorer()` build (which runs `select_strategy`); the collector
//! walk. Multi-column `and_intersect` cells additionally pay one
//! `BooleanQuery::new` per iteration.
//!
//! Outside the timed closure (paid once per cell as setup): corpus build
//! (schema, index, writer, all `add_document` calls, commit), reader and
//! `Searcher` instantiation, and the deterministic `sample_terms`
//! shuffle. Only the raw `Vec<u64>` of sampled values is captured into
//! the closure; `Term::from_field_u64` runs per iteration.
//!
//! For cells with K ≥ ~1_000 the per-iteration construction cost is a
//! small fraction of total work (≤ ~5–10%), so the reported throughput
//! reflects scoring-loop performance. For very small K (10, 100) the
//! construction overhead is a larger fraction and binggan's
//! input-size-divided-by-time formula reaches artifact territory when
//! iterations finish in microseconds — relative ratios between
//! strategies at the same cell remain meaningful but absolute throughput
//! numbers below ~10ms are not reliable.

use binggan::{black_box, BenchRunner};
use common::BitSet;
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::{Collector, Count, DocSetCollector, SegmentCollector};
use tantivy::query::{
    BitSetDocSet, BooleanQuery, ConstScorer, EmptyScorer, EnableScoring, Explanation,
    FastFieldTermSetQuery, Occur, Query, Scorer, StrategyTag, TermQuery, TermSetStrategyConfig,
    Weight,
};
use tantivy::schema::{Field, IndexRecordOption, NumericOptions, SchemaBuilder};
use tantivy::{
    doc, DocId, DocSet, Index, IndexSettings, IndexSortByField, Order, ReloadPolicy, Score,
    Searcher, SegmentOrdinal, SegmentReader, TantivyError, Term,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CorpusKind {
    /// D = 1, every value unique.
    PrimaryKey,
    /// D ≈ 100, foreign-key shape.
    LowFk,
}

impl CorpusKind {
    fn label(self) -> &'static str {
        match self {
            CorpusKind::PrimaryKey => "pk",
            CorpusKind::LowFk => "lowfk",
        }
    }

    fn distinct_count(self, n: u64) -> u64 {
        match self {
            CorpusKind::PrimaryKey => n,
            CorpusKind::LowFk => n.div_ceil(100),
        }
    }

    fn value_for_doc(self, doc_id: u64) -> u64 {
        match self {
            CorpusKind::PrimaryKey => doc_id,
            CorpusKind::LowFk => doc_id / 100,
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
    /// Real planner-driven `BitsetFromPostings` strategy. Forced via
    /// `bitset_max_density = 1.0` so the dispatch always picks it on
    /// indexed numeric fast fields. The other `BitsetFromPostings`
    /// variant is the synthetic posting-union-into-bitset baseline that
    /// retains `BufferedUnionScorer`; this one exercises the real
    /// strategy that bypasses the union scorer entirely.
    BitsetFromPostingsReal,
    /// K independent `TermDictionary::get(key)` lookups (no streaming
    /// automaton, no batched dictionary API) + OR each posting list
    /// into one `BitSet`. Bench-only — lives in this file, not in
    /// production code. Measured by `compare` to quantify the
    /// per-block-decode amortization the batched dictionary API
    /// provides relative to per-key dispatch.
    DirectBitset,
}

impl Strat {
    fn label(self) -> &'static str {
        match self {
            Strat::Gallop => "gallop",
            Strat::Linear => "linear",
            Strat::PostingDirect => "posting_direct",
            Strat::BitsetFromPostings => "bitset_from_postings",
            Strat::BitsetFromPostingsReal => "bitset_from_postings_real",
            Strat::DirectBitset => "direct_bitset",
        }
    }
}

fn applicable(sort: Sort, strat: Strat) -> bool {
    !(sort == Sort::None && strat == Strat::Gallop)
}

/// Tier names that `TERM_SET_BENCH_TIER` accepts. Anything else falls
/// back to `smoke`. Each tier's behaviour is documented on its
/// `run_*_tier` function.
const TIER_SMOKE: &str = "smoke";
const TIER_PRODUCTION: &str = "production";
const TIER_COST_MODEL: &str = "cost_model";
const TIER_COMPARE: &str = "compare";

fn bench_tier() -> &'static str {
    match std::env::var("TERM_SET_BENCH_TIER").as_deref() {
        Ok(TIER_PRODUCTION) => TIER_PRODUCTION,
        Ok(TIER_COST_MODEL) => TIER_COST_MODEL,
        Ok(TIER_COMPARE) => TIER_COMPARE,
        _ => TIER_SMOKE,
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
        // The other thresholds don't matter for sorted+small-K cases —
        // once gallop is taken, the sort-agnostic branch isn't reached.
        ..TermSetStrategyConfig::default()
    }
}

fn cfg_force_bitset_real() -> TermSetStrategyConfig {
    TermSetStrategyConfig {
        gallop_enabled: false,
        bitset_max_density_unique: 1.0,
        bitset_max_density_multi: 1.0,
        subsequent_bitset_max_density: 1.0,
        strategy_sink: None,
    }
}

fn cfg_force_linear() -> TermSetStrategyConfig {
    TermSetStrategyConfig {
        gallop_enabled: false,
        // `<=` gate with threshold 0.0 admits only density == 0 (i.e.
        // k_prime == 0, which short-circuits to Empty earlier). Every
        // non-empty term set falls through to LinearScan.
        bitset_max_density_unique: 0.0,
        bitset_max_density_multi: 0.0,
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

// --- Direct (non-batched) dict lookups + bitset OR --------------------------

/// `direct_bitset_scorer`: K independent `TermDictionary::get(key)` lookups,
/// each opening a `BlockSegmentPostings` and OR'ing its docs into a single
/// per-segment `BitSet`. No streaming automaton, no `BufferedUnionScorer`.
///
/// Bench-only — lives in this file, not in `term_set_bitset.rs`.
fn direct_bitset_scorer(
    reader: &SegmentReader,
    field: Field,
    values: &[u64],
    boost: Score,
) -> tantivy::Result<Box<dyn Scorer>> {
    if values.is_empty() || reader.max_doc() == 0 {
        return Ok(Box::new(EmptyScorer));
    }
    let inverted_index = reader.inverted_index(field)?;
    let term_dict = inverted_index.terms();
    let mut bitset = BitSet::with_max_value(reader.max_doc());
    for &v in values {
        let key_buf = v.to_be_bytes();
        let term_info = term_dict.get(&key_buf[..])?;
        let Some(term_info) = term_info else { continue };
        let mut block_postings = inverted_index
            .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic)?;
        loop {
            let docs = block_postings.docs();
            if docs.is_empty() {
                break;
            }
            for &doc in docs {
                bitset.insert(doc);
            }
            block_postings.advance();
        }
    }
    let docset = BitSetDocSet::from(bitset);
    Ok(Box::new(ConstScorer::new(docset, boost)))
}

/// Minimal `Query` wrapper so `Searcher::search` can drive the fourth
/// variant the same way it drives the other strategies — apples-to-apples
/// on `Searcher::search` overhead.
struct DirectBitsetQuery {
    field: Field,
    values: Vec<u64>,
}

impl std::fmt::Debug for DirectBitsetQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectBitsetQuery")
            .field("field", &self.field)
            .field("values_len", &self.values.len())
            .finish()
    }
}

impl Clone for DirectBitsetQuery {
    fn clone(&self) -> Self {
        Self {
            field: self.field,
            values: self.values.clone(),
        }
    }
}

impl Query for DirectBitsetQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(DirectBitsetWeight {
            field: self.field,
            values: self.values.clone(),
        }))
    }
}

struct DirectBitsetWeight {
    field: Field,
    values: Vec<u64>,
}

impl Weight for DirectBitsetWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        direct_bitset_scorer(reader, self.field, &self.values, boost)
    }
    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "doc {doc} does not match"
            )));
        }
        Ok(Explanation::new("DirectBitsetScorer", scorer.score()))
    }
}

fn run_direct_bitset(searcher: &Searcher, field: tantivy::schema::Field, terms: &[u64]) -> usize {
    let q = DirectBitsetQuery {
        field,
        values: terms.to_vec(),
    };
    searcher.search(&q, &Count).unwrap()
}

// --- Batched-dictionary variant ---------------------------------------------
//
// Gated on `quickwit` because the batched dictionary API
// (`batch_term_info_exact`, `SortedTermSlice`, `sort_and_dedupe_terms`)
// is only re-exported from `tantivy::termdict` under that feature.
// The FST backend has no zstd block-decode to amortize, so there's no
// batched counterpart to measure. Under default features the bench
// still compiles — it just skips registering this variant.
#[cfg(feature = "quickwit")]
/// Scorer mirror of `direct_bitset_scorer` that uses the batched
/// dictionary API. Same algorithm, but K independent
/// `term_dict.get(key)` calls collapse into one forward pass through
/// the dictionary that decompresses each touched block exactly once.
fn batched_bitset_scorer(
    reader: &SegmentReader,
    field: Field,
    values: &[u64],
    boost: Score,
) -> tantivy::Result<Box<dyn Scorer>> {
    use tantivy::termdict::{sort_and_dedupe_terms, SortedTermSlice};

    if values.is_empty() || reader.max_doc() == 0 {
        return Ok(Box::new(EmptyScorer));
    }
    let inverted_index = reader.inverted_index(field)?;
    let term_dict = inverted_index.terms();

    // Pre-encode + sort + dedupe the BE u64 keys so we satisfy
    // `SortedTermSlice`'s precondition without re-validating.
    let mut keys: Vec<[u8; 8]> = values.iter().map(|v| v.to_be_bytes()).collect();
    sort_and_dedupe_terms(&mut keys);
    let sorted = SortedTermSlice::new_assume_sorted(&keys);

    let mut bitset = BitSet::with_max_value(reader.max_doc());
    for result in term_dict.batch_term_info_exact(sorted) {
        let (_idx, term_info) = result?;
        let mut block_postings = inverted_index
            .read_block_postings_from_terminfo(&term_info, IndexRecordOption::Basic)?;
        loop {
            let docs = block_postings.docs();
            if docs.is_empty() {
                break;
            }
            for &doc in docs {
                bitset.insert(doc);
            }
            block_postings.advance();
        }
    }
    let docset = BitSetDocSet::from(bitset);
    Ok(Box::new(ConstScorer::new(docset, boost)))
}

#[cfg(feature = "quickwit")]
struct BatchedBitsetQuery {
    field: Field,
    values: Vec<u64>,
}

#[cfg(feature = "quickwit")]
impl std::fmt::Debug for BatchedBitsetQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchedBitsetQuery")
            .field("field", &self.field)
            .field("values_len", &self.values.len())
            .finish()
    }
}

#[cfg(feature = "quickwit")]
impl Clone for BatchedBitsetQuery {
    fn clone(&self) -> Self {
        Self {
            field: self.field,
            values: self.values.clone(),
        }
    }
}

#[cfg(feature = "quickwit")]
impl Query for BatchedBitsetQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(BatchedBitsetWeight {
            field: self.field,
            values: self.values.clone(),
        }))
    }
}

#[cfg(feature = "quickwit")]
struct BatchedBitsetWeight {
    field: Field,
    values: Vec<u64>,
}

#[cfg(feature = "quickwit")]
impl Weight for BatchedBitsetWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        batched_bitset_scorer(reader, self.field, &self.values, boost)
    }
    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "doc {doc} does not match"
            )));
        }
        Ok(Explanation::new("BatchedBitsetScorer", scorer.score()))
    }
}

#[cfg(feature = "quickwit")]
fn run_batched_bitset(searcher: &Searcher, field: tantivy::schema::Field, terms: &[u64]) -> usize {
    let q = BatchedBitsetQuery {
        field,
        values: terms.to_vec(),
    };
    searcher.search(&q, &Count).unwrap()
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

/// Matrix axes for the smoke tier. HighFk is omitted because its
/// `distinct = N/100_000` at N=1M gives only 10 distinct values, below
/// every smoke K level — building the corpus would be wasted work.
fn smoke_matrix() -> (Vec<u64>, Vec<usize>, Vec<CorpusKind>) {
    (
        vec![1_000_000],
        vec![100, 10_000],
        vec![CorpusKind::PrimaryKey, CorpusKind::LowFk],
    )
}

/// Build an unsorted corpus with parametric `D` (average docs per
/// distinct value). `value_for_doc(i) = i / d`, so
/// `distinct = ceil(n / d)` and each value appears in exactly `d`
/// contiguous docs. Used by the `production`, `cost_model`, and
/// `compare` tiers to construct corpora at specific (N, D) shapes
/// without going through `CorpusKind`'s named enum.
fn build_corpus_parametric_d(n: u64, d: u64) -> (Searcher, tantivy::schema::Field) {
    let mut sb = SchemaBuilder::new();
    let field = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
    let schema = sb.build();
    let index = Index::builder().schema(schema).create_in_ram().unwrap();
    {
        let writer_mem = (200_000_000u64).max(n * 32);
        let mut writer = index
            .writer_with_num_threads(1, writer_mem as usize)
            .unwrap();
        for doc_id in 0..n {
            writer.add_document(doc!(field => doc_id / d)).unwrap();
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

/// Sorted variant of `build_corpus_parametric_d`. `sort_by_field` is
/// set to the parametric-D column ascending, so the resulting
/// segment has `reader.sort_by_field()` matching `"fk"` — the
/// precondition `select_strategy` checks to admit `Gallop`.
fn build_corpus_parametric_d_sorted(n: u64, d: u64) -> (Searcher, tantivy::schema::Field) {
    let mut sb = SchemaBuilder::new();
    let field = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
    let schema = sb.build();
    let index = Index::builder()
        .schema(schema)
        .settings(IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "fk".to_string(),
                order: Order::Asc,
            }),
            ..Default::default()
        })
        .create_in_ram()
        .unwrap();
    {
        let writer_mem = (200_000_000u64).max(n * 32);
        let mut writer = index
            .writer_with_num_threads(1, writer_mem as usize)
            .unwrap();
        for doc_id in 0..n {
            writer.add_document(doc!(field => doc_id / d)).unwrap();
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

/// Planner-default dispatch over the production cell matrix. For each
/// (cell, sort variant, K) tuple, registers four measurements:
///
/// - `planner_default` — the production code path with default `TermSetStrategyConfig`. Also probes
///   the planner's choice once via `strategy_sink` and prints `BENCH_PICK cell=... pick=...` so the
///   capture can be parsed into a per-cell dispatch decision.
/// - `linear_forced`   — `cfg_force_linear()`.
/// - `batched_forced`  — bench-only `BatchedBitsetQuery` wrapper.
/// - `gallop_forced`   — `cfg_force_gallop()`. Sorted corpora only; on unsorted segments the
///   planner correctly rejects gallop even when force-configured.
///
/// Quickwit-gated: the batched-bitset wrapper depends on the
/// quickwit-only re-exports of `SortedTermSlice` and
/// `batch_term_info_exact`.
#[cfg(feature = "quickwit")]
fn run_production_tier() {
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;

    let mut runner = BenchRunner::new();

    let cells: &[(u64, u64, &str, &[usize])] = &[
        (1_000_000, 1, "pk_1m", &[100, 1_000, 10_000]),
        (20_000_000, 1, "pk_20m", &[1_000, 10_000, 100_000]),
        (20_000_000, 100, "lowfk_20m", &[100, 1_000, 10_000]),
    ];

    for &(n, d, label, ks) in cells {
        for sort_kind in &["asc", "unsorted"] {
            let (searcher, field) = match *sort_kind {
                "asc" => build_corpus_parametric_d_sorted(n, d),
                _ => build_corpus_parametric_d(n, d),
            };
            let mut group = runner.new_group();
            group.set_name(format!("production n={n} D={d} ({label}) sort={sort_kind}"));
            group.set_input_size(n as usize);
            let distinct = n.div_ceil(d).max(1);

            for &k in ks {
                let k_eff = k.min(distinct as usize);
                let terms = sample_terms(distinct, k_eff, 7);

                // Probe the planner's pick once, outside the timed
                // closures, so we know what `planner_default` actually
                // dispatched to. The sink writes the chosen tag on
                // every `select_strategy` call; one warm-up call is
                // enough to capture it.
                let sink = Arc::new(AtomicU8::new(StrategyTag::None as u8));
                let probe_cfg = TermSetStrategyConfig {
                    strategy_sink: Some(sink.clone()),
                    ..TermSetStrategyConfig::default()
                };
                let _ = run_planner_path(&searcher, field, &terms, probe_cfg);
                let pick = StrategyTag::try_from(sink.load(Ordering::Relaxed)).unwrap();
                eprintln!("BENCH_PICK cell={label} sort={sort_kind} k={k_eff} pick={pick:?}");

                let s = searcher.clone();
                let t = terms.clone();
                group.register(format!("k={k_eff} run=planner_default"), move |_| {
                    black_box(run_planner_path(
                        &s,
                        field,
                        &t,
                        TermSetStrategyConfig::default(),
                    ))
                });
                let s = searcher.clone();
                let t = terms.clone();
                group.register(format!("k={k_eff} run=linear_forced"), move |_| {
                    black_box(run_planner_path(&s, field, &t, cfg_force_linear()))
                });
                let s = searcher.clone();
                let t = terms.clone();
                group.register(format!("k={k_eff} run=batched_forced"), move |_| {
                    black_box(run_batched_bitset(&s, field, &t))
                });

                if *sort_kind == "asc" {
                    let s = searcher.clone();
                    let t = terms;
                    group.register(format!("k={k_eff} run=gallop_forced"), move |_| {
                        black_box(run_planner_path(&s, field, &t, cfg_force_gallop()))
                    });
                }
            }
            group.run();
        }
    }

    // ---- Non-fast indexed (text/string) cell ----
    // Exercises `TermSetWeight`'s inverted-index dispatch path:
    // `BitsetFromPostings` at low K, `Automaton` at high K.
    run_production_text_cell(&mut runner);
}

#[cfg(feature = "quickwit")]
fn run_production_text_cell(runner: &mut BenchRunner) {
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;

    let n: u64 = 20_000_000;
    let distinct: u64 = 200_000; // D ≈ 100
    let (searcher, field, vocab) = build_text_corpus_parametric(n, distinct);

    let mut group = runner.new_group();
    group.set_name(format!(
        "production n={n} text_indexed (distinct={distinct})"
    ));
    group.set_input_size(n as usize);

    for &k in &[100usize, 1_000, 100_000] {
        let k_eff = k.min(vocab.len());
        let terms: Vec<String> = vocab.iter().take(k_eff).cloned().collect();

        let sink = Arc::new(AtomicU8::new(StrategyTag::None as u8));
        let probe_cfg = TermSetStrategyConfig {
            strategy_sink: Some(sink.clone()),
            ..TermSetStrategyConfig::default()
        };
        let _ = run_planner_path_text(&searcher, field, &terms, probe_cfg);
        let pick = StrategyTag::try_from(sink.load(Ordering::Relaxed)).unwrap();
        eprintln!("BENCH_PICK cell=text_indexed_20m k={k_eff} pick={pick:?}");

        let s = searcher.clone();
        let t = terms.clone();
        group.register(format!("k={k_eff} run=planner_default"), move |_| {
            black_box(run_planner_path_text(
                &s,
                field,
                &t,
                TermSetStrategyConfig::default(),
            ))
        });

        // Force-bitset cfg: density gate admits all K.
        let mut cfg_bitset = TermSetStrategyConfig::default();
        cfg_bitset.gallop_enabled = false;
        cfg_bitset.bitset_max_density_unique = 1.0;
        cfg_bitset.bitset_max_density_multi = 1.0;
        let s = searcher.clone();
        let t = terms.clone();
        group.register(format!("k={k_eff} run=bitset_forced"), move |_| {
            black_box(run_planner_path_text(&s, field, &t, cfg_bitset.clone()))
        });

        // Force-automaton cfg: density gate rejects all K → Automaton.
        let mut cfg_auto = TermSetStrategyConfig::default();
        cfg_auto.gallop_enabled = false;
        cfg_auto.bitset_max_density_unique = 0.0;
        cfg_auto.bitset_max_density_multi = 0.0;
        let s = searcher.clone();
        let t = terms;
        group.register(format!("k={k_eff} run=automaton_forced"), move |_| {
            black_box(run_planner_path_text(&s, field, &t, cfg_auto.clone()))
        });
    }
    group.run();
}

#[cfg(feature = "quickwit")]
fn build_text_corpus_parametric(
    n: u64,
    distinct: u64,
) -> (Searcher, tantivy::schema::Field, Vec<String>) {
    use rand::{Rng, SeedableRng};
    use tantivy::schema::STRING;

    let mut sb = SchemaBuilder::new();
    // STRING (not TEXT) for exact-match semantics — no tokenization, no
    // analysis. Indexed only (no FAST), which routes the dispatch
    // through the unified `TermSetWeight`'s non-fast path.
    let field = sb.add_text_field("text", STRING);
    let schema = sb.build();
    let index = Index::builder().schema(schema).create_in_ram().unwrap();
    let vocab: Vec<String> = (0..distinct).map(|i| format!("term-{i:08}")).collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(0xbeef);
    {
        let mut writer = index.writer_with_num_threads(1, 200_000_000).unwrap();
        for _ in 0..n {
            let idx = rng.random_range(0..vocab.len());
            writer
                .add_document(doc!(field => vocab[idx].as_str()))
                .unwrap();
        }
        writer.commit().unwrap();
    }
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    (reader.searcher(), field, vocab)
}

#[cfg(feature = "quickwit")]
fn run_planner_path_text(
    searcher: &Searcher,
    field: tantivy::schema::Field,
    terms: &[String],
    cfg: TermSetStrategyConfig,
) -> usize {
    let q = FastFieldTermSetQuery::new(terms.iter().map(|s| Term::from_field_text(field, s)))
        .with_strategy_config(cfg);
    searcher.search(&q, &Count).unwrap()
}

#[cfg(not(feature = "quickwit"))]
fn run_production_tier() {
    eprintln!("production tier requires `--features quickwit`");
}

/// Force-dispatched `linear` vs `batched_bitset` sweep across the
/// (D, K) matrix the bitset thresholds are calibrated against. For
/// each D ∈ {2, 5, 20, 50} the sweep walks K ∈ {200, 1K, 5K, 10K,
/// 50K, 100K} on a sorted N=20M corpus; the post-processed `batched
/// / linear` ratio per cell tells you where batched stops winning at
/// that D. Combined with the existing measurements at D=1 (PK shape;
/// crossover at K/N ≈ 0.0005) and D=100 (LowFk shape; crossover well
/// past K/N=0.05), this sweep justifies the `bitset_max_density_unique
/// = 1/2000` (D=1) and `bitset_max_density_multi = 1/200` (D >= 2)
/// defaults in `TermSetStrategyConfig`.
///
/// Quickwit-gated: `run_batched_bitset` depends on the quickwit-only
/// re-exports of `SortedTermSlice` and `batch_term_info_exact`.
#[cfg(feature = "quickwit")]
fn run_cost_model_tier() {
    let mut runner = BenchRunner::new();

    let n: u64 = 20_000_000;
    // `dict_size = N/D`, exact because `build_corpus_parametric_d_sorted`
    // writes value `doc_id / D` per doc — each value appears exactly D times.
    let ds: &[u64] = &[2, 5, 20, 50];
    let ks: &[usize] = &[200, 1_000, 5_000, 10_000, 50_000, 100_000];

    for &d in ds {
        let (searcher, field) = build_corpus_parametric_d_sorted(n, d);
        let mut group = runner.new_group();
        group.set_name(format!("cost_model n={n} D={d}"));
        group.set_input_size(n as usize);
        let distinct = n.div_ceil(d).max(1);
        for &k in ks {
            let k_eff = k.min(distinct as usize);
            let terms = sample_terms(distinct, k_eff, 7);
            let s = searcher.clone();
            let t = terms.clone();
            group.register(format!("k={k_eff} run=linear_forced"), move |_| {
                black_box(run_planner_path(&s, field, &t, cfg_force_linear()))
            });
            let s = searcher.clone();
            let t = terms;
            group.register(format!("k={k_eff} run=batched_forced"), move |_| {
                black_box(run_batched_bitset(&s, field, &t))
            });
        }
        group.run();
    }
}

#[cfg(not(feature = "quickwit"))]
fn run_cost_model_tier() {
    eprintln!("cost_model tier requires `--features quickwit`");
}

/// Force-dispatched four-way comparison — `linear`, `gallop`,
/// `direct_bitset`, `batched_bitset` — on three corpora (PK_1M,
/// PK_20M, LowFk_20M) at three K levels each. All corpora are sorted
/// so the planner admits Gallop; the other three strategies are
/// segment-sort-insensitive, so the cross-strategy comparison stays
/// apples-to-apples.
///
/// `linear`         — `cfg_force_linear()`.
/// `gallop`         — `cfg_force_gallop()` (includes the upfront sort cost).
/// `direct_bitset`  — bench-only `DirectBitsetQuery`. K individual
///                    `term_dict.get(key)` calls + bitset OR. Used
///                    here as the per-key baseline for the batched
///                    variant.
/// `batched_bitset` — bench-only `BatchedBitsetQuery`. The
///                    `bitset_from_postings_scorer` under
///                    `--features quickwit`. Registered only when the
///                    `quickwit` feature is enabled — the FST backend
///                    has no block decode to amortize.
fn run_compare_tier() {
    let mut runner = BenchRunner::new();

    // PK_1M (D=1, dict_size=1M, single segment).
    {
        let n: u64 = 1_000_000;
        let d: u64 = 1;
        let (searcher, field) = build_corpus_parametric_d_sorted(n, d);
        let mut group = runner.new_group();
        group.set_name(format!("compare n={n} D={d} (pk_1m)"));
        group.set_input_size(n as usize);
        let distinct = n.div_ceil(d).max(1);
        for &k in &[100usize, 1_000, 10_000] {
            let k_eff = k.min(distinct as usize);
            let terms = sample_terms(distinct, k_eff, 7);
            register_compare_cell(&mut group, &searcher, field, k_eff, terms);
        }
        group.run();
    }

    // PK_20M (D=1, dict_size=20M, multi-segment).
    {
        let n: u64 = 20_000_000;
        let d: u64 = 1;
        let (searcher, field) = build_corpus_parametric_d_sorted(n, d);
        let mut group = runner.new_group();
        group.set_name(format!("compare n={n} D={d} (pk_20m)"));
        group.set_input_size(n as usize);
        let distinct = n.div_ceil(d).max(1);
        for &k in &[1_000usize, 10_000, 100_000] {
            let k_eff = k.min(distinct as usize);
            let terms = sample_terms(distinct, k_eff, 7);
            register_compare_cell(&mut group, &searcher, field, k_eff, terms);
        }
        group.run();
    }

    // LowFk_20M (D=100, dict_size=200K, multi-segment).
    {
        let n: u64 = 20_000_000;
        let d: u64 = 100;
        let (searcher, field) = build_corpus_parametric_d_sorted(n, d);
        let mut group = runner.new_group();
        group.set_name(format!("compare n={n} D={d} (lowfk_20m)"));
        group.set_input_size(n as usize);
        let distinct = n.div_ceil(d).max(1);
        for &k in &[100usize, 1_000, 10_000] {
            let k_eff = k.min(distinct as usize);
            let terms = sample_terms(distinct, k_eff, 7);
            register_compare_cell(&mut group, &searcher, field, k_eff, terms);
        }
        group.run();
    }
}

/// Register the four `compare` strategies on a single cell.
/// `batched_bitset` is only registered under `--features quickwit`
/// because its scorer pulls in `batch_term_info_exact`, which is
/// quickwit-gated.
fn register_compare_cell(
    group: &mut binggan::BenchGroup<'_, '_>,
    searcher: &Searcher,
    field: Field,
    k_eff: usize,
    terms: Vec<u64>,
) {
    let s = searcher.clone();
    let t = terms.clone();
    group.register(format!("k={k_eff} strat=linear"), move |_| {
        black_box(run_planner_path(&s, field, &t, cfg_force_linear()))
    });
    let s = searcher.clone();
    let t = terms.clone();
    group.register(format!("k={k_eff} strat=gallop"), move |_| {
        black_box(run_planner_path(&s, field, &t, cfg_force_gallop()))
    });
    #[cfg(feature = "quickwit")]
    let terms_for_batched = terms.clone();
    let s = searcher.clone();
    let t = terms;
    group.register(format!("k={k_eff} strat=direct_bitset"), move |_| {
        black_box(run_direct_bitset(&s, field, &t))
    });
    #[cfg(feature = "quickwit")]
    {
        let s = searcher.clone();
        let t = terms_for_batched;
        group.register(format!("k={k_eff} strat=batched_bitset"), move |_| {
            black_box(run_batched_bitset(&s, field, &t))
        });
    }
}

fn main() {
    match bench_tier() {
        TIER_PRODUCTION => run_production_tier(),
        TIER_COST_MODEL => run_cost_model_tier(),
        TIER_COMPARE => run_compare_tier(),
        _ => run_smoke_tier(),
    }
}

/// Default tier. Catches functional regressions across the
/// force-dispatched strategy matrix on a small corpus. N=1M, K ∈
/// {100, 10K}, kinds = {PrimaryKey, LowFk}, both sort orders. Plus the
/// multi-column AND-intersection panel that exercises cross-column
/// seek behavior.
fn run_smoke_tier() {
    let (n_levels, k_levels, kinds) = smoke_matrix();
    let sorts = [Sort::Asc, Sort::None];
    let strategies = [
        Strat::Gallop,
        Strat::Linear,
        Strat::PostingDirect,
        Strat::BitsetFromPostings,
        Strat::BitsetFromPostingsReal,
        Strat::DirectBitset,
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
                    // K=10K on the unsorted groups duplicates information
                    // already in the corresponding asc groups (linear,
                    // posting, and bitset are sort-insensitive). Drop them
                    // to keep smoke under a minute on top of the
                    // and-intersect panel.
                    if sort == Sort::None && k == 10_000 {
                        continue;
                    }
                    let terms = sample_terms(distinct, k, 7);
                    for &strat in &strategies {
                        if !applicable(sort, strat) {
                            continue;
                        }
                        // Force-gallop requires K < N strictly; skip the
                        // degenerate K=N cell so timings aren't reported
                        // as gallop misses.
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
                            Strat::BitsetFromPostingsReal => black_box(run_planner_path(
                                &searcher,
                                field,
                                &terms_v,
                                cfg_force_bitset_real(),
                            )),
                            Strat::DirectBitset => {
                                black_box(run_direct_bitset(&searcher, field, &terms_v))
                            }
                        });
                    }
                }
                group.run();
            }
        }
    }

    // Multi-column AND-intersection cells. Probes the smart-seek win
    // on the linear-scan path: column A is sorted (gallop), column B
    // is unsorted (linear) — BooleanQuery::Must of two
    // FastFieldTermSetQueries. With the trait-default seek, column B's
    // per-`seek(target)` walk dilutes column A's gallop selectivity;
    // with the smart-seek override on TermSetDocSet, column B jumps
    // directly to each target.
    //
    // Two corpus shapes:
    //   - dense FK (D ≈ 100): gallop output is ~1500 contiguous ranges of ~100 docs each. B's seeks
    //     rarely cross range boundaries, so the smart-vs-default delta is bounded — lower-bound
    //     case.
    //   - sparse PK (D = 1): gallop output is 1500 *isolated* DocIds spread across the segment.
    //     Every gallop emit forces B to skip a large gap — upper-bound case for typical hash-join
    //     build sides.
    run_and_intersect_cells(&mut runner);
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
