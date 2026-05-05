//! Random integration sanity-check: the production gallop path
//! (`term_set_gallop::run`) and the linear path (`TermSetDocSet::advance`)
//! must produce identical sorted DocId sets across random sorted corpora
//! and random query terms. Any divergence between the gallop helper and
//! the linear ground truth surfaces as a symmetric-difference failure
//! here, so this test catches regressions in either path.
//!
//! Direct head-to-head comparison of `binary_search_sorted` vs
//! `gallop_search_sorted` (the helpers themselves) lives inside the crate
//! at `sorted_internals::gallop_tests`, where both `pub(crate)` helpers
//! are reachable.

use proptest::prelude::*;
use tantivy::collector::DocSetCollector;
use tantivy::query::{FastFieldTermSetQuery, TermSetStrategyConfig};
use tantivy::schema::{NumericOptions, SchemaBuilder};
use tantivy::{doc, Index, IndexSettings, IndexSortByField, Order, ReloadPolicy, Searcher, Term};

fn cfg_gallop() -> TermSetStrategyConfig {
    TermSetStrategyConfig {
        gallop_enabled: true,
        gallop_max_density: 1.0,
        ..TermSetStrategyConfig::default()
    }
}

fn cfg_linear() -> TermSetStrategyConfig {
    TermSetStrategyConfig {
        gallop_enabled: false,
        gallop_max_density: 0.0,
        posting_max_density: 0.0,
        bitset_max_density: 0.0,
        hash_probe_max_density: 0.0,
        subsequent_bitset_max_density: 0.0,
        strategy_sink: None,
    }
}

fn build_sorted_index_from_values(
    values: &[u64],
    order: Order,
) -> (Searcher, tantivy::schema::Field) {
    let mut sb = SchemaBuilder::new();
    let f = sb.add_u64_field("v", NumericOptions::default().set_fast().set_indexed());
    let schema = sb.build();
    let index = Index::builder()
        .schema(schema)
        .settings(IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "v".to_string(),
                order,
            }),
            ..Default::default()
        })
        .create_in_ram()
        .unwrap();
    let mut writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
    for &v in values {
        writer.add_document(doc!(f => v)).unwrap();
    }
    writer.commit().unwrap();
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    (reader.searcher(), f)
}

fn collect(
    searcher: &Searcher,
    field: tantivy::schema::Field,
    term: u64,
    cfg: TermSetStrategyConfig,
) -> Vec<u32> {
    let q = FastFieldTermSetQuery::new(std::iter::once(Term::from_field_u64(field, term)))
        .with_strategy_config(cfg);
    let mut docs: Vec<u32> = searcher
        .search(&q, &DocSetCollector)
        .unwrap()
        .into_iter()
        .map(|a| a.doc_id)
        .collect();
    docs.sort_unstable();
    docs
}

// Strategy: random sorted corpus values, ASC or DESC. Bounded so each
// proptest case stays cheap (Index build dominates per-case cost).
fn corpus_values_strategy() -> impl Strategy<Value = Vec<u64>> {
    proptest::collection::vec(0u64..5000, 32usize..2000)
}

fn order_strategy() -> impl Strategy<Value = Order> {
    prop_oneof![Just(Order::Asc), Just(Order::Desc)]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..Default::default() })]

    /// Differential: single-term gallop vs linear must agree across random
    /// sorted corpora and random target values (some in-range, some out).
    #[test]
    fn prop_gallop_matches_linear_path_across_random_corpora(
        values in corpus_values_strategy(),
        order in order_strategy(),
        target in 0u64..6000,
    ) {
        let (searcher, f) = build_sorted_index_from_values(&values, order);
        let gallop_docs = collect(&searcher, f, target, cfg_gallop());
        let linear_docs = collect(&searcher, f, target, cfg_linear());
        prop_assert_eq!(
            gallop_docs, linear_docs,
            "differential mismatch: order={:?} target={}",
            order, target
        );
    }

    /// Multi-term differential: a TermSetQuery with K distinct terms must
    /// produce identical DocId sets under gallop and linear strategies.
    /// Exercises the per-term loop in term_set_gallop::run.
    #[test]
    fn prop_gallop_matches_linear_path_on_multi_term_queries(
        values in corpus_values_strategy(),
        order in order_strategy(),
        terms_raw in proptest::collection::vec(0u64..6000, 1usize..30),
    ) {
        let (searcher, f) = build_sorted_index_from_values(&values, order);
        let mut terms = terms_raw;
        terms.sort_unstable();
        terms.dedup();

        let q_gallop = FastFieldTermSetQuery::new(
            terms.iter().map(|v| Term::from_field_u64(f, *v)),
        )
        .with_strategy_config(cfg_gallop());
        let q_linear = FastFieldTermSetQuery::new(
            terms.iter().map(|v| Term::from_field_u64(f, *v)),
        )
        .with_strategy_config(cfg_linear());

        let mut g: Vec<u32> = searcher
            .search(&q_gallop, &DocSetCollector)
            .unwrap()
            .into_iter()
            .map(|a| a.doc_id)
            .collect();
        let mut l: Vec<u32> = searcher
            .search(&q_linear, &DocSetCollector)
            .unwrap()
            .into_iter()
            .map(|a| a.doc_id)
            .collect();
        g.sort_unstable();
        l.sort_unstable();
        prop_assert_eq!(g, l, "multi-term mismatch");
    }
}
