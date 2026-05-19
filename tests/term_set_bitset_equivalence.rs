//! End-to-end equivalence test for the `BitsetFromPostings` strategy.
//!
//! For the same `(corpus, term_set)`, the `BitsetFromPostings` path and the
//! `LinearScan` path (today's `TermSetDocSet`) must return identical sorted
//! `DocId` vectors. Both are driven via the public `FastFieldTermSetQuery`
//! API; only `TermSetStrategyConfig` densities differ between the runs.
//!
//! This is the strongest correctness invariant for the
//! BitsetFromPostings rollout — every other test pins planner shape or
//! algorithm details, but this one would catch any semantic divergence
//! between the two strategies before it lands.

use proptest::prelude::*;
use rand::prelude::*;
use rand::rngs::StdRng;
use tantivy::collector::DocSetCollector;
use tantivy::query::{FastFieldTermSetQuery, TermSetStrategyConfig};
use tantivy::schema::{NumericOptions, SchemaBuilder};
use tantivy::{doc, Index, IndexSettings, IndexSortByField, Order, ReloadPolicy, Searcher, Term};

#[derive(Clone, Copy, Debug)]
enum CorpusKind {
    /// D = 1, every value unique.
    PrimaryKey,
    /// D ≈ 100, foreign-key shape — the DSG contact_list shape this
    /// strategy targets.
    LowFk,
    /// D ≈ 1_000, very-low-cardinality column (status enums, region codes).
    HighFk,
}

#[derive(Clone, Copy, Debug)]
enum SortMode {
    Asc,
    None,
}

fn value_for(doc_id: u64, kind: CorpusKind) -> u64 {
    match kind {
        CorpusKind::PrimaryKey => doc_id,
        CorpusKind::LowFk => doc_id / 100,
        CorpusKind::HighFk => doc_id / 1_000,
    }
}

fn distinct_count(n: u64, kind: CorpusKind) -> u64 {
    match kind {
        CorpusKind::PrimaryKey => n,
        CorpusKind::LowFk => n.div_ceil(100),
        CorpusKind::HighFk => n.div_ceil(1_000).max(1),
    }
}

fn build_corpus(n: u64, kind: CorpusKind, sort: SortMode) -> (Searcher, tantivy::schema::Field) {
    let mut sb = SchemaBuilder::new();
    let field = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
    let schema = sb.build();
    let mut builder = Index::builder().schema(schema);
    if let SortMode::Asc = sort {
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
        let mut writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
        for d in 0..n {
            writer
                .add_document(doc!(field => value_for(d, kind)))
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

/// Force `BitsetFromPostings` for any non-empty term set on any unsorted
/// segment by setting both bitset densities to `1.0`. Gallop is gated off
/// so the planner always lands on bitset regardless of D.
fn cfg_force_bitset() -> TermSetStrategyConfig {
    TermSetStrategyConfig {
        gallop_enabled: false,
        bitset_max_density_unique: 1.0,
        bitset_max_density_multi: 1.0,
        subsequent_bitset_max_density: 1.0,
        strategy_sink: None,
    }
}

/// Force the terminal `LinearScan` fallback by setting every density to 0.0.
fn cfg_force_linear() -> TermSetStrategyConfig {
    TermSetStrategyConfig {
        gallop_enabled: false,
        bitset_max_density_unique: 0.0,
        bitset_max_density_multi: 0.0,
        subsequent_bitset_max_density: 0.0,
        strategy_sink: None,
    }
}

fn run_query(
    searcher: &Searcher,
    field: tantivy::schema::Field,
    terms: &[u64],
    cfg: TermSetStrategyConfig,
) -> Vec<u32> {
    let q = FastFieldTermSetQuery::new(terms.iter().map(|v| Term::from_field_u64(field, *v)))
        .with_strategy_config(cfg);
    let result = searcher.search(&q, &DocSetCollector).unwrap();
    let mut docs: Vec<u32> = result.into_iter().map(|addr| addr.doc_id).collect();
    docs.sort_unstable();
    docs
}

fn assert_equivalent(n: u64, kind: CorpusKind, sort: SortMode, k_values: &[usize]) {
    let (searcher, field) = build_corpus(n, kind, sort);
    let distinct = distinct_count(n, kind);
    for &k in k_values {
        if (k as u64) > distinct {
            continue;
        }
        let terms = sample_terms(distinct, k, 7);
        let bitset_docs = run_query(&searcher, field, &terms, cfg_force_bitset());
        let linear_docs = run_query(&searcher, field, &terms, cfg_force_linear());
        assert_eq!(
            bitset_docs, linear_docs,
            "strategy divergence: corpus={kind:?} sort={sort:?} N={n} K={k}",
        );
    }
}

#[test]
fn bitset_matches_linear_pk_unsorted() {
    assert_equivalent(
        10_000,
        CorpusKind::PrimaryKey,
        SortMode::None,
        &[1, 10, 100, 1_000, 5_000],
    );
}

#[test]
fn bitset_matches_linear_pk_asc() {
    // Even on a sorted corpus the planner can still land on bitset when
    // gallop is gated off — proves bitset doesn't depend on sort.
    assert_equivalent(
        10_000,
        CorpusKind::PrimaryKey,
        SortMode::Asc,
        &[1, 10, 100, 1_000, 5_000],
    );
}

#[test]
fn bitset_matches_linear_lowfk_unsorted() {
    // The DSG contact_list shape: column unsorted relative to the filter,
    // D ≈ 100. This is the regime BitsetFromPostings exists to optimize.
    assert_equivalent(
        10_000,
        CorpusKind::LowFk,
        SortMode::None,
        &[1, 5, 25, 50, 99],
    );
}

#[test]
fn bitset_matches_linear_lowfk_asc() {
    assert_equivalent(
        10_000,
        CorpusKind::LowFk,
        SortMode::Asc,
        &[1, 5, 25, 50, 99],
    );
}

#[test]
fn bitset_matches_linear_highfk_unsorted() {
    // Very-low-cardinality column. With N=10K and HighFk's D≈1000,
    // distinct = 10, so K only ranges up to 10. Confirms the strategy
    // handles dense per-term posting lists.
    assert_equivalent(10_000, CorpusKind::HighFk, SortMode::None, &[1, 5, 10]);
}

#[test]
fn bitset_matches_linear_highfk_asc() {
    assert_equivalent(10_000, CorpusKind::HighFk, SortMode::Asc, &[1, 5, 10]);
}

/// 7th-shape coverage: mixed in-range and out-of-range query terms on
/// a LowFk corpus. The bitset path streams the dictionary and only
/// touches dictionary blocks that intersect the query FST, so terms
/// outside [min, max] contribute zero posting walks. The output must
/// match LinearScan, which filters per-doc.
#[test]
fn bitset_matches_linear_lowfk_with_out_of_range_terms() {
    let (searcher, field) = build_corpus(10_000, CorpusKind::LowFk, SortMode::None);
    // Distinct values are [0, 100). Mix in some out-of-range terms.
    let terms = vec![5u64, 17, 42, 99, 200, 999, u64::MAX];
    let bitset_docs = run_query(&searcher, field, &terms, cfg_force_bitset());
    let linear_docs = run_query(&searcher, field, &terms, cfg_force_linear());
    assert_eq!(bitset_docs, linear_docs);
    assert!(!bitset_docs.is_empty(), "fixture degenerate");
}

/// Edge case: K = 0. Both strategies must return empty.
#[test]
fn bitset_matches_linear_empty_terms() {
    let (searcher, field) = build_corpus(1_000, CorpusKind::PrimaryKey, SortMode::None);
    let terms: Vec<u64> = vec![];
    let bitset_docs = run_query(&searcher, field, &terms, cfg_force_bitset());
    let linear_docs = run_query(&searcher, field, &terms, cfg_force_linear());
    assert!(bitset_docs.is_empty());
    assert!(linear_docs.is_empty());
}

/// Edge case: K = 1. A single-term query is the simplest non-trivial
/// case; the streamer walks the dictionary to one TermInfo, OR's one
/// posting list into the bitset.
#[test]
fn bitset_matches_linear_single_term() {
    let (searcher, field) = build_corpus(1_000, CorpusKind::LowFk, SortMode::None);
    let terms = vec![5u64];
    let bitset_docs = run_query(&searcher, field, &terms, cfg_force_bitset());
    let linear_docs = run_query(&searcher, field, &terms, cfg_force_linear());
    assert_eq!(bitset_docs, linear_docs);
    assert!(!bitset_docs.is_empty());
}

/// Edge case: every query term is missing from the segment. The
/// streamer yields zero TermInfos; the bitset stays empty; the result
/// is empty.
#[test]
fn bitset_matches_linear_all_terms_missing() {
    let (searcher, field) = build_corpus(1_000, CorpusKind::PrimaryKey, SortMode::None);
    // Distinct values are [0, 1000). Pick terms well outside that.
    let terms = vec![10_000u64, 50_000, 999_999, u64::MAX];
    let bitset_docs = run_query(&searcher, field, &terms, cfg_force_bitset());
    let linear_docs = run_query(&searcher, field, &terms, cfg_force_linear());
    assert!(bitset_docs.is_empty());
    assert!(linear_docs.is_empty());
}

/// Edge case: a single very-dense term whose posting list nearly
/// fills the segment. Tests that the bitset insert path scales when
/// most bits in the bitset get set.
#[test]
fn bitset_matches_linear_single_very_dense_term() {
    // HighFk at N=1000 → distinct = 1, every doc matches the single value 0.
    let (searcher, field) = build_corpus(1_000, CorpusKind::HighFk, SortMode::None);
    let terms = vec![0u64];
    let bitset_docs = run_query(&searcher, field, &terms, cfg_force_bitset());
    let linear_docs = run_query(&searcher, field, &terms, cfg_force_linear());
    assert_eq!(bitset_docs, linear_docs);
    assert_eq!(bitset_docs.len(), 1_000, "every doc should match");
}

/// Edge case: duplicate input terms. The framework doesn't dedupe
/// input on the non-Gallop path; the bitset helper sort+dedups bytes
/// internally before building the FST. Result must match LinearScan.
#[test]
fn bitset_matches_linear_with_duplicate_input_terms() {
    let (searcher, field) = build_corpus(1_000, CorpusKind::LowFk, SortMode::None);
    let terms = vec![5u64, 5, 5, 7, 7, 5, 8];
    let bitset_docs = run_query(&searcher, field, &terms, cfg_force_bitset());
    let linear_docs = run_query(&searcher, field, &terms, cfg_force_linear());
    assert_eq!(bitset_docs, linear_docs);
    assert!(!bitset_docs.is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        ..ProptestConfig::default()
    })]

    /// Randomized equivalence across corpus size, value range, query size,
    /// and corpus shape. Each input picks a fresh seed for the term sampler
    /// so distinct strategy paths are exercised within the budget.
    #[test]
    fn bitset_equivalent_to_linear_scan(
        num_docs in 100usize..2_000,
        num_queries in 1usize..500,
        value_range in 1u64..50_000,
        seed in 0u64..1_000_000,
    ) {
        let n = num_docs as u64;
        // Use a generic random-value generator (not the structured CorpusKind
        // shapes) — the property must hold over arbitrary u64 fast-field
        // contents, not just the three benchmark shapes above.
        let mut sb = SchemaBuilder::new();
        let field = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
        let schema = sb.build();
        let index = Index::builder().schema(schema).create_in_ram().unwrap();
        let mut rng = StdRng::seed_from_u64(seed);
        {
            let mut writer = index.writer_with_num_threads(1, 30_000_000).unwrap();
            for _ in 0..n {
                let v = rng.random_range(0..value_range);
                writer.add_document(doc!(field => v)).unwrap();
            }
            writer.commit().unwrap();
        }
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let searcher = reader.searcher();

        let queries: Vec<u64> = (0..num_queries)
            .map(|_| rng.random_range(0..value_range))
            .collect();

        let bitset_docs = run_query(&searcher, field, &queries, cfg_force_bitset());
        let linear_docs = run_query(&searcher, field, &queries, cfg_force_linear());
        prop_assert_eq!(bitset_docs, linear_docs);
    }
}

// ---------------------------------------------------------------------------
// Text-field equivalence (non-fast indexed) — BitsetFromPostings vs Automaton
// ---------------------------------------------------------------------------

mod text_equivalence {
    //! Equivalence on text fields, where the dispatch path is
    //! `BitsetFromPostings` (low K) vs `Automaton` (high K) — both
    //! reading the inverted index. The two strategies must yield
    //! identical docs on every input.

    use proptest::prelude::*;
    use rand::prelude::*;
    use rand::rngs::StdRng;
    use tantivy::collector::DocSetCollector;
    use tantivy::query::{FastFieldTermSetQuery, TermSetStrategyConfig};
    use tantivy::schema::{SchemaBuilder, STRING};
    use tantivy::{doc, Index, ReloadPolicy, Searcher, Term};

    /// Force `BitsetFromPostings` on the non-fast path: density gate
    /// admits everything.
    fn cfg_force_bitset_text() -> TermSetStrategyConfig {
        TermSetStrategyConfig {
            gallop_enabled: false,
            bitset_max_density_unique: 1.0,
            bitset_max_density_multi: 1.0,
            subsequent_bitset_max_density: 1.0,
            strategy_sink: None,
        }
    }

    /// Force `Automaton` on the non-fast path: density gate rejects
    /// everything, so the dispatcher falls through to Automaton.
    fn cfg_force_automaton_text() -> TermSetStrategyConfig {
        TermSetStrategyConfig {
            gallop_enabled: false,
            bitset_max_density_unique: 0.0,
            bitset_max_density_multi: 0.0,
            subsequent_bitset_max_density: 0.0,
            strategy_sink: None,
        }
    }

    fn build_text_corpus(
        n: u64,
        distinct_terms: u64,
        seed: u64,
    ) -> (Searcher, tantivy::schema::Field, Vec<String>) {
        let mut sb = SchemaBuilder::new();
        // STRING (not TEXT) for tokenization-free, exact-match
        // semantics — every doc has one whole-string value.
        let field = sb.add_text_field("text", STRING);
        let schema = sb.build();
        let index = Index::builder().schema(schema).create_in_ram().unwrap();
        let vocab: Vec<String> = (0..distinct_terms).map(|i| format!("t{i:06}")).collect();
        let mut rng = StdRng::seed_from_u64(seed);
        {
            let mut writer = index.writer_with_num_threads(1, 30_000_000).unwrap();
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

    fn run_text_query(
        searcher: &Searcher,
        field: tantivy::schema::Field,
        terms: &[String],
        cfg: TermSetStrategyConfig,
    ) -> Vec<u32> {
        let q = FastFieldTermSetQuery::new(terms.iter().map(|s| Term::from_field_text(field, s)))
            .with_strategy_config(cfg);
        let result = searcher.search(&q, &DocSetCollector).unwrap();
        let mut docs: Vec<u32> = result.into_iter().map(|addr| addr.doc_id).collect();
        docs.sort_unstable();
        docs
    }

    #[test]
    fn text_bitset_matches_automaton_small_k() {
        let (searcher, field, vocab) = build_text_corpus(500, 50, 1);
        let terms: Vec<String> = vocab.iter().take(3).cloned().collect();
        let bitset_docs = run_text_query(&searcher, field, &terms, cfg_force_bitset_text());
        let automaton_docs = run_text_query(&searcher, field, &terms, cfg_force_automaton_text());
        assert_eq!(bitset_docs, automaton_docs);
        assert!(!bitset_docs.is_empty(), "fixture degenerate");
    }

    #[test]
    fn text_bitset_matches_automaton_large_k() {
        let (searcher, field, vocab) = build_text_corpus(1_000, 100, 2);
        let terms: Vec<String> = vocab.iter().take(80).cloned().collect();
        let bitset_docs = run_text_query(&searcher, field, &terms, cfg_force_bitset_text());
        let automaton_docs = run_text_query(&searcher, field, &terms, cfg_force_automaton_text());
        assert_eq!(bitset_docs, automaton_docs);
        assert!(!bitset_docs.is_empty());
    }

    #[test]
    fn text_bitset_matches_automaton_all_missing() {
        let (searcher, field, _vocab) = build_text_corpus(500, 50, 3);
        let terms: Vec<String> = (1000..1010).map(|i| format!("missing-{i}")).collect();
        let bitset_docs = run_text_query(&searcher, field, &terms, cfg_force_bitset_text());
        let automaton_docs = run_text_query(&searcher, field, &terms, cfg_force_automaton_text());
        assert!(bitset_docs.is_empty());
        assert!(automaton_docs.is_empty());
    }

    #[test]
    fn text_bitset_matches_automaton_empty_input() {
        let (searcher, field, _vocab) = build_text_corpus(200, 30, 4);
        let terms: Vec<String> = vec![];
        let bitset_docs = run_text_query(&searcher, field, &terms, cfg_force_bitset_text());
        let automaton_docs = run_text_query(&searcher, field, &terms, cfg_force_automaton_text());
        assert!(bitset_docs.is_empty());
        assert!(automaton_docs.is_empty());
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 48,
            ..ProptestConfig::default()
        })]

        /// Randomized equivalence on text fields: `BitsetFromPostings`
        /// and `Automaton` must yield identical docs on every
        /// (corpus, query-set) pair. Pins the bitset path's correctness
        /// against the `AutomatonWeight` baseline.
        #[test]
        fn text_bitset_equivalent_to_automaton(
            num_docs in 50usize..1_000,
            distinct_terms in 4u64..200,
            num_queries in 1usize..120,
            seed in 0u64..1_000_000,
        ) {
            let n = num_docs as u64;
            let (searcher, field, vocab) = build_text_corpus(n, distinct_terms, seed);
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(0x9e37));

            // Half query terms drawn from the vocab (probably present),
            // half random fresh strings (probably absent).
            let mut queries: Vec<String> = Vec::with_capacity(num_queries);
            for i in 0..num_queries {
                if i % 2 == 0 {
                    let idx = rng.random_range(0..vocab.len());
                    queries.push(vocab[idx].clone());
                } else {
                    let r: u32 = rng.random();
                    queries.push(format!("absent-{r:08x}"));
                }
            }

            let bitset_docs = run_text_query(&searcher, field, &queries, cfg_force_bitset_text());
            let automaton_docs =
                run_text_query(&searcher, field, &queries, cfg_force_automaton_text());
            prop_assert_eq!(bitset_docs, automaton_docs);
        }
    }
}
