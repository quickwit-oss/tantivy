//! End-to-end equivalence test for the gallop strategy.
//!
//! For the same `(corpus, term_set)`, the gallop path and the linear path
//! (today's `TermSetDocSet`) must return identical sorted DocId vectors. We
//! drive both via the public `FastFieldTermSetQuery` API: identical corpus,
//! identical query, only `TermSetStrategyConfig::gallop_enabled` differs.
//!
//! This is the strongest correctness invariant for #4895 — every other test
//! pins planner shape or algorithm details, but this one would catch any
//! semantic divergence between the two strategies before it lands.

use rand::prelude::*;
use rand::rngs::StdRng;
use tantivy::collector::DocSetCollector;
use tantivy::query::{BooleanQuery, FastFieldTermSetQuery, Occur, Query, TermSetStrategyConfig};
use tantivy::schema::{NumericOptions, SchemaBuilder};
use tantivy::{doc, Index, IndexSettings, IndexSortByField, Order, ReloadPolicy, Searcher, Term};

#[derive(Clone, Copy, Debug)]
enum CorpusKind {
    /// D = 1, every value unique.
    PrimaryKey,
    /// D ≈ 100, foreign-key shape.
    ForeignKey,
}

fn value_for(doc_id: u64, kind: CorpusKind) -> u64 {
    match kind {
        CorpusKind::PrimaryKey => doc_id,
        CorpusKind::ForeignKey => doc_id / 100,
    }
}

fn distinct_count(n: u64, kind: CorpusKind) -> u64 {
    match kind {
        CorpusKind::PrimaryKey => n,
        CorpusKind::ForeignKey => n.div_ceil(100),
    }
}

fn build_sorted_corpus(
    n: u64,
    kind: CorpusKind,
    order: Order,
) -> (Searcher, tantivy::schema::Field) {
    let mut sb = SchemaBuilder::new();
    let field = sb.add_u64_field("fk", NumericOptions::default().set_fast().set_indexed());
    let schema = sb.build();
    let index = Index::builder()
        .schema(schema)
        .settings(IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "fk".to_string(),
                order,
            }),
            ..Default::default()
        })
        .create_in_ram()
        .unwrap();
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
    let mut rng = StdRng::seed_from_u64(seed);
    let mut chosen: Vec<u64> = (0..distinct).collect();
    chosen.shuffle(&mut rng);
    chosen.truncate(k);
    chosen
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

fn cfg_gallop_on() -> TermSetStrategyConfig {
    TermSetStrategyConfig::default()
}

fn cfg_gallop_off() -> TermSetStrategyConfig {
    TermSetStrategyConfig {
        gallop_enabled: false,
        ..TermSetStrategyConfig::default()
    }
}

fn assert_equivalent(n: u64, kind: CorpusKind, order: Order, k_values: &[usize]) {
    let (searcher, field) = build_sorted_corpus(n, kind, order);
    let distinct = distinct_count(n, kind);
    for &k in k_values {
        if (k as u64) > distinct {
            continue;
        }
        let terms = sample_terms(distinct, k, 7);
        let gallop_docs = run_query(&searcher, field, &terms, cfg_gallop_on());
        let linear_docs = run_query(&searcher, field, &terms, cfg_gallop_off());
        assert_eq!(
            gallop_docs, linear_docs,
            "strategy divergence: corpus={kind:?} order={order:?} N={n} K={k}",
        );
    }
}

#[test]
fn gallop_matches_linear_pk_asc() {
    assert_equivalent(
        10_000,
        CorpusKind::PrimaryKey,
        Order::Asc,
        &[1, 10, 100, 1_000],
    );
}

#[test]
fn gallop_matches_linear_pk_desc() {
    assert_equivalent(
        10_000,
        CorpusKind::PrimaryKey,
        Order::Desc,
        &[1, 10, 100, 1_000],
    );
}

#[test]
fn gallop_matches_linear_fk_asc() {
    assert_equivalent(10_000, CorpusKind::ForeignKey, Order::Asc, &[1, 5, 25, 100]);
}

#[test]
fn gallop_matches_linear_fk_desc() {
    assert_equivalent(
        10_000,
        CorpusKind::ForeignKey,
        Order::Desc,
        &[1, 5, 25, 100],
    );
}

/// Edge case: term set partially outside [min, max]. Pruning must agree
/// between the two strategies; both should produce the same DocIds.
#[test]
fn gallop_matches_linear_with_out_of_range_terms() {
    let (searcher, field) = build_sorted_corpus(10_000, CorpusKind::ForeignKey, Order::Asc);
    // Distinct values are [0, 100). Mix in some out-of-range terms (200, 999).
    let terms = vec![5u64, 17, 42, 99, 200, 999];
    let gallop_docs = run_query(&searcher, field, &terms, cfg_gallop_on());
    let linear_docs = run_query(&searcher, field, &terms, cfg_gallop_off());
    assert_eq!(gallop_docs, linear_docs);
    // And it should be non-empty (5, 17, 42, 99 are all in range with ~100 docs each).
    assert!(!gallop_docs.is_empty());
}

/// Multi-column AND-intersection: column A is sorted (gallop fires when
/// enabled), column B is unsorted (always linear scan). The gallop-enabled
/// and gallop-disabled runs of the same `BooleanQuery` must produce
/// identical DocId vectors — proving that the smart `seek` override on
/// `TermSetDocSet` preserves matching semantics under intersection-driven
/// seeks.
#[test]
fn gallop_matches_linear_two_column_and_intersection() {
    // Two fast+indexed u64 fields. `a` is the sort key (so a sorted segment
    // can gallop on it); `b` is unsorted and always lands on the linear path.
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

    // 10K rows with `a` sweeping low-cardinality FK shape (~100 docs/value)
    // and `b` an unrelated u64 derived from a different formula so the AND
    // intersection is non-degenerate. Values for `b` deliberately spread
    // across DocIds so smart-seek is exercised on the linear-scan path.
    let n: u64 = 10_000;
    {
        let mut writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
        for d in 0..n {
            writer
                .add_document(doc!(a => d / 100, b => (d * 7 + 13) % 1234))
                .unwrap();
        }
        writer.commit().unwrap();
    }
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let searcher = reader.searcher();

    // Sample a handful of values from each column. Pick `a` terms that the
    // sorted-segment gallop will compress into a few contiguous DocId
    // ranges, and `b` terms that match docs scattered through the segment —
    // forcing many `seek` calls into the linear scanner.
    let a_terms: Vec<u64> = vec![5, 17, 42, 71, 88];
    let b_terms: Vec<u64> = vec![7, 100, 200, 333, 500, 800, 1000];

    let run = |cfg_a: TermSetStrategyConfig, cfg_b: TermSetStrategyConfig| -> Vec<u32> {
        let qa = Box::new(
            FastFieldTermSetQuery::new(a_terms.iter().map(|v| Term::from_field_u64(a, *v)))
                .with_strategy_config(cfg_a),
        ) as Box<dyn Query>;
        let qb = Box::new(
            FastFieldTermSetQuery::new(b_terms.iter().map(|v| Term::from_field_u64(b, *v)))
                .with_strategy_config(cfg_b),
        ) as Box<dyn Query>;
        let bq = BooleanQuery::new(vec![(Occur::Must, qa), (Occur::Must, qb)]);
        let result = searcher.search(&bq, &DocSetCollector).unwrap();
        let mut docs: Vec<u32> = result.into_iter().map(|addr| addr.doc_id).collect();
        docs.sort_unstable();
        docs
    };

    let gallop_docs = run(cfg_gallop_on(), cfg_gallop_on());
    let linear_docs = run(cfg_gallop_off(), cfg_gallop_off());

    assert_eq!(
        gallop_docs, linear_docs,
        "two-column AND intersection diverged under smart seek",
    );
    // Sanity: the AND should produce a non-empty intersection on this corpus.
    // (If it were empty, the test would still pass tautologically — guard
    // against silently building a degenerate fixture.)
    assert!(
        !gallop_docs.is_empty(),
        "fixture is degenerate: AND-intersection produced no docs",
    );

    // Belt-and-suspenders: also exercise the mixed config — column A
    // gallops while column B linear-scans (with smart seek). This is
    // exactly the production shape post-step-3, and the multi-column case
    // the perf change targets. Output must agree with both sides linear.
    let mixed_docs = run(cfg_gallop_on(), cfg_gallop_off());
    assert_eq!(
        mixed_docs, linear_docs,
        "gallop-A + linear-B (smart seek) diverged from all-linear baseline",
    );
}

/// Edge case: term set entirely outside [min, max]. Both strategies must
/// return EmptyScorer / no docs.
#[test]
fn gallop_and_linear_both_empty_when_all_terms_pruned() {
    let (searcher, field) = build_sorted_corpus(10_000, CorpusKind::ForeignKey, Order::Asc);
    let terms = vec![10_000u64, 99_999, u64::MAX];
    let gallop_docs = run_query(&searcher, field, &terms, cfg_gallop_on());
    let linear_docs = run_query(&searcher, field, &terms, cfg_gallop_off());
    assert!(gallop_docs.is_empty());
    assert!(linear_docs.is_empty());
}
