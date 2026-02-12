//! Benchmark: tantivy native AggregationCollector vs tantivy-datafusion translate_aggregations
//! vs native pushdown via execute_aggregations.
//!
//! Run with: `cargo bench -p tantivy-datafusion`

use std::sync::Arc;

use binggan::{black_box, InputGroup};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, FAST, STRING};
use tantivy::{doc, Index};
use datafusion::execution::SessionStateBuilder;
use tantivy_datafusion::{
    create_session_with_pushdown, translate_aggregations, OrdinalGroupByOptimization,
    TantivyTableProvider,
};

// ---------------------------------------------------------------------------
// Index builder (mirrors root agg_bench, "full" cardinality, 1M docs)
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
// Shared tokio runtime + pre-warmed SessionContext
// ---------------------------------------------------------------------------

/// Pre-built context that avoids per-iteration setup overhead.
struct DfContext {
    rt: tokio::runtime::Runtime,
    ctx: SessionContext,
}

impl DfContext {
    fn new(index: &Index) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
            .unwrap();
        Self { rt, ctx }
    }

    fn run(&self, agg_req: &serde_json::Value, df_key: &str) {
        self.rt.block_on(async {
            let df = self.ctx.table("f").await.unwrap();
            let aggs: Aggregations = serde_json::from_value(agg_req.clone()).unwrap();
            let results = translate_aggregations(df, &aggs).unwrap();
            let batches = results[df_key].clone().collect().await.unwrap();
            black_box(batches);
        });
    }
}

/// Pre-built context with AggPushdown rule and aggregations stashed.
struct PushdownContext {
    rt: tokio::runtime::Runtime,
    ctx: SessionContext,
    aggs: Aggregations,
    df_key: String,
}

impl PushdownContext {
    fn new(index: &Index, agg_json: &serde_json::Value, df_key: &str) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let aggs: Aggregations = serde_json::from_value(agg_json.clone()).unwrap();
        let ctx = create_session_with_pushdown(index, &aggs).unwrap();
        Self {
            rt,
            ctx,
            aggs,
            df_key: df_key.to_string(),
        }
    }

    fn run(&self) {
        self.rt.block_on(async {
            let df = self.ctx.table("f").await.unwrap();
            let results = translate_aggregations(df, &self.aggs).unwrap();
            let batches = results[&self.df_key].clone().collect().await.unwrap();
            black_box(batches);
        });
    }
}

/// Pre-built context with OrdinalGroupByOptimization rule (no pushdown).
struct OrdinalDfContext {
    rt: tokio::runtime::Runtime,
    ctx: SessionContext,
}

impl OrdinalDfContext {
    fn new(index: &Index) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        // Use default target_partitions (= num CPUs) for parallelism,
        // same as DfContext, so the comparison is fair.
        let config = SessionConfig::new();
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(OrdinalGroupByOptimization::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
            .unwrap();
        Self { rt, ctx }
    }

    fn run(&self, agg_req: &serde_json::Value, df_key: &str) {
        self.rt.block_on(async {
            let df = self.ctx.table("f").await.unwrap();
            let aggs: Aggregations = serde_json::from_value(agg_req.clone()).unwrap();
            let results = translate_aggregations(df, &aggs).unwrap();
            let batches = results[df_key].clone().collect().await.unwrap();
            black_box(batches);
        });
    }
}

/// Single-threaded ordinal: target_partitions=1 so no chunking, no repartition.
/// Shows true single-threaded ordinal performance vs native tantivy.
struct OrdinalDf1T {
    rt: tokio::runtime::Runtime,
    ctx: SessionContext,
}

impl OrdinalDf1T {
    fn new(index: &Index) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let config = SessionConfig::new().with_target_partitions(1);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(OrdinalGroupByOptimization::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
            .unwrap();
        Self { rt, ctx }
    }

    fn run(&self, agg_req: &serde_json::Value, df_key: &str) {
        self.rt.block_on(async {
            let df = self.ctx.table("f").await.unwrap();
            let aggs: Aggregations = serde_json::from_value(agg_req.clone()).unwrap();
            let results = translate_aggregations(df, &aggs).unwrap();
            let batches = results[df_key].clone().collect().await.unwrap();
            black_box(batches);
        });
    }
}

/// Pre-planned pushdown: physical plan built once, only execution is benchmarked.
struct PrePlannedPushdown {
    rt: tokio::runtime::Runtime,
    plan: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
}

impl PrePlannedPushdown {
    fn new(index: &Index, agg_json: &serde_json::Value, df_key: &str) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let aggs: Aggregations = serde_json::from_value(agg_json.clone()).unwrap();
        let ctx = create_session_with_pushdown(index, &aggs).unwrap();
        let plan = rt.block_on(async {
            let df = ctx.table("f").await.unwrap();
            let results = translate_aggregations(df, &aggs).unwrap();
            results[df_key]
                .clone()
                .create_physical_plan()
                .await
                .unwrap()
        });
        Self { rt, plan, ctx }
    }

    fn run(&self) {
        self.rt.block_on(async {
            let task_ctx = self.ctx.task_ctx();
            let batches =
                datafusion::physical_plan::collect(self.plan.clone(), task_ctx).await.unwrap();
            black_box(batches);
        });
    }
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
// Main
// ---------------------------------------------------------------------------

fn main() {
    let cases = agg_cases();

    let index = build_bench_index(1_000_000);

    // Pre-warm DF context once (reused across all cases)
    let df_ctx = Arc::new(DfContext::new(&index));
    let ordinal_ctx = Arc::new(OrdinalDfContext::new(&index));
    let ordinal_1t = Arc::new(OrdinalDf1T::new(&index));

    let inputs: Vec<(&str, Index)> = vec![("1M_docs", index.clone())];

    for case in &cases {
        // Pre-warm contexts per case
        let pd_ctx = Arc::new(PushdownContext::new(&index, &case.json, case.df_key));
        let pp_pd = Arc::new(PrePlannedPushdown::new(&index, &case.json, case.df_key));

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

        let pd = pd_ctx.clone();
        group.register(
            &format!("native_pushdown/{}", case.name),
            move |_index| {
                pd.run();
            },
        );

        let ctx = df_ctx.clone();
        let json_df = case.json.clone();
        let key_df = case.df_key;
        group.register(&format!("datafusion/{}", case.name), move |_index| {
            ctx.run(&json_df, key_df);
        });

        let ord_ctx = ordinal_ctx.clone();
        let json_ord = case.json.clone();
        let key_ord = case.df_key;
        group.register(&format!("ordinal_df/{}", case.name), move |_index| {
            ord_ctx.run(&json_ord, key_ord);
        });

        let o1t = ordinal_1t.clone();
        let json_1t = case.json.clone();
        let key_1t = case.df_key;
        group.register(&format!("ordinal_1t/{}", case.name), move |_index| {
            o1t.run(&json_1t, key_1t);
        });

        // Pre-planned variants: execution only, no planning overhead
        let pp = pp_pd.clone();
        group.register(
            &format!("pushdown_exec/{}", case.name),
            move |_index| {
                pp.run();
            },
        );

        group.run();
    }
}

