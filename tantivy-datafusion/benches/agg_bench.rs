//! Benchmark: tantivy native AggregationCollector vs tantivy-datafusion translate_aggregations.
//!
//! Reuses the same index and aggregation JSON for apples-to-apples comparison.
//! Run with: `cargo bench -p tantivy-datafusion`

use std::sync::Arc;

use binggan::{black_box, InputGroup};
use datafusion::prelude::*;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, TextFieldIndexing, IndexRecordOption, FAST, STRING};
use tantivy::{doc, Index};
use tantivy_datafusion::{translate_aggregations, TantivyTableProvider};

// ---------------------------------------------------------------------------
// Index builder (mirrors root agg_bench, simplified to "full" cardinality)
// ---------------------------------------------------------------------------

fn build_bench_index(num_docs: usize) -> Index {
    let mut schema_builder = Schema::builder();
    let text_fieldtype = tantivy::schema::TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
        )
        .set_stored();
    let text_field = schema_builder.add_text_field("text", text_fieldtype);
    let text_field_many_terms = schema_builder.add_text_field("text_many_terms", STRING | FAST);
    let text_field_few_terms_status =
        schema_builder.add_text_field("text_few_terms_status", STRING | FAST);
    let score_fieldtype = tantivy::schema::NumericOptions::default().set_fast();
    let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
    let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype);

    let index = Index::create_from_tempdir(schema_builder.build()).unwrap();

    let status_labels = ["INFO", "ERROR", "WARN", "DEBUG", "OK", "CRITICAL", "EMERGENCY"];
    let status_weights = [8000u32, 300, 1200, 500, 500, 20, 1];
    let status_dist =
        rand::distr::weighted::WeightedIndex::new(status_weights.iter().copied()).unwrap();

    let lg_norm = rand_distr::LogNormal::new(2.996f64, 0.979f64).unwrap();

    let many_terms: Vec<String> = (0..150_000).map(|n| format!("author{n}")).collect();

    let mut rng = StdRng::from_seed([1u8; 32]);
    let mut writer = index.writer_with_num_threads(1, 200_000_000).unwrap();

    for _ in 0..num_docs {
        let val: f64 = rng.random_range(0.0..1_000_000.0);
        writer
            .add_document(doc!(
                text_field => "cool",
                text_field_many_terms => many_terms.choose(&mut rng).unwrap().to_string(),
                text_field_few_terms_status => status_labels[status_dist.sample(&mut rng)],
                score_field => val as u64,
                score_field_f64 => lg_norm.sample(&mut rng),
            ))
            .unwrap();
    }
    writer.commit().unwrap();
    index
}

// ---------------------------------------------------------------------------
// Aggregation definitions
// ---------------------------------------------------------------------------

struct AggCase {
    name: &'static str,
    json: serde_json::Value,
    /// Key in the translate_aggregations result map to collect
    df_key: &'static str,
}

fn agg_cases() -> Vec<AggCase> {
    vec![
        AggCase {
            name: "avg_f64",
            json: json!({ "average": { "avg": { "field": "score_f64" } } }),
            df_key: "average",
        },
        AggCase {
            name: "stats_f64",
            json: json!({ "s": { "stats": { "field": "score_f64" } } }),
            df_key: "s",
        },
        AggCase {
            name: "terms_few",
            json: json!({ "t": { "terms": { "field": "text_few_terms_status" } } }),
            df_key: "t",
        },
        AggCase {
            name: "terms_few_with_avg",
            json: json!({
                "t": {
                    "terms": { "field": "text_few_terms_status" },
                    "aggs": { "avg_score": { "avg": { "field": "score_f64" } } }
                }
            }),
            df_key: "t",
        },
        AggCase {
            name: "histogram_100",
            json: json!({
                "h": { "histogram": { "field": "score_f64", "interval": 100 } }
            }),
            df_key: "h",
        },
        AggCase {
            name: "range_6",
            json: json!({
                "r": { "range": { "field": "score_f64", "ranges": [
                    { "from": 3, "to": 7000 },
                    { "from": 7000, "to": 20000 },
                    { "from": 20000, "to": 30000 },
                    { "from": 30000, "to": 40000 },
                    { "from": 40000, "to": 50000 },
                    { "from": 50000, "to": 60000 }
                ] } }
            }),
            df_key: "r",
        },
        AggCase {
            name: "range_with_avg",
            json: json!({
                "r": {
                    "range": { "field": "score_f64", "ranges": [
                        { "from": 3, "to": 7000 },
                        { "from": 7000, "to": 20000 },
                        { "from": 20000, "to": 60000 }
                    ] },
                    "aggs": { "avg_in_range": { "avg": { "field": "score_f64" } } }
                }
            }),
            df_key: "r",
        },
    ]
}

// ---------------------------------------------------------------------------
// Tantivy native runner
// ---------------------------------------------------------------------------

fn run_native(index: &Index, agg_req: &serde_json::Value) {
    let aggs: Aggregations = serde_json::from_value(agg_req.clone()).unwrap();
    let collector = AggregationCollector::from_aggs(aggs, Default::default());
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    black_box(searcher.search(&AllQuery, &collector).unwrap());
}

// ---------------------------------------------------------------------------
// DataFusion runner
// ---------------------------------------------------------------------------

fn run_datafusion(index: &Index, agg_req: &serde_json::Value, df_key: &str) {
    run_datafusion_with_partitions(index, agg_req, df_key, 1);
}

fn run_datafusion_parallel(index: &Index, agg_req: &serde_json::Value, df_key: &str) {
    // Use default target_partitions (num CPUs)
    run_datafusion_with_partitions(index, agg_req, df_key, 0);
}

fn run_datafusion_with_partitions(
    index: &Index,
    agg_req: &serde_json::Value,
    df_key: &str,
    target_partitions: usize,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let ctx = if target_partitions > 0 {
            let config = SessionConfig::new().with_target_partitions(target_partitions);
            SessionContext::new_with_config(config)
        } else {
            SessionContext::new() // default = num_cpus
        };
        ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
            .unwrap();

        let df = ctx.table("f").await.unwrap();
        let aggs: Aggregations = serde_json::from_value(agg_req.clone()).unwrap();
        let results = translate_aggregations(df, &aggs).unwrap();
        let batches = results[df_key].clone().collect().await.unwrap();
        black_box(batches);
    });
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let cases = agg_cases();

    // Build inputs: different sizes to see scaling behavior
    let inputs: Vec<(&str, Index)> = vec![
        ("1M_docs", build_bench_index(1_000_000)),
    ];

    for case in &cases {
        let mut group = InputGroup::new_with_inputs(
            inputs
                .iter()
                .map(|(name, idx)| (*name, idx.clone()))
                .collect(),
        );

        let json_native = case.json.clone();
        group.register(&format!("native/{}", case.name), move |index| {
            run_native(index, &json_native);
        });

        let json_df = case.json.clone();
        let key = case.df_key;
        group.register(&format!("datafusion_1t/{}", case.name), move |index| {
            run_datafusion(index, &json_df, key);
        });

        let json_df_par = case.json.clone();
        let key_par = case.df_key;
        group.register(&format!("datafusion_par/{}", case.name), move |index| {
            run_datafusion_parallel(index, &json_df_par, key_par);
        });

        group.run();
    }
}
