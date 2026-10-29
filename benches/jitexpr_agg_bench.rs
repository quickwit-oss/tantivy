//! Terms aggregations over jitexpr value sources.
//!
//! The text inputs mimic log attributes: `service` has a few hundred distinct values per segment,
//! like service names, and `request_id` is unique per doc.

use std::sync::Arc;

use binggan::plugins::PeakMemAllocPlugin;
use binggan::{black_box, BenchRunner, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::{
    AggContextParams, AggregationCollector, JitExprValueSourceProvider, ValueSourceRegistry,
};
use tantivy::indexer::NoMergePolicy;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, STRING};
use tantivy::{doc, Index, Searcher};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

const NUM_DOCS: usize = 2_000_000;
const NUM_SEGMENTS: usize = 4;
const NUM_SERVICES: usize = 200;

fn main() {
    let index = create_index();
    let searcher = index.reader().unwrap().searcher();
    let mut runner = BenchRunner::new();
    runner.add_plugin(PeakMemAllocPlugin::new(GLOBAL));
    let mut group = runner.new_group();
    group.set_name("jitexpr_terms");
    for (name, field, expression) in [
        ("terms_physical_service", "service", None),
        (
            "terms_regexp_extract_service",
            "#first",
            Some(r#"(REGEXP_EXTRACT service "^([^-]+)" 1u64)"#),
        ),
        ("terms_upper_service", "#upper", Some("(UPPER service)")),
        (
            "terms_regexp_extract_request_id",
            "#first",
            Some(r#"(REGEXP_EXTRACT request_id "^([^-]+)" 1u64)"#),
        ),
    ] {
        let mut registry = ValueSourceRegistry::default();
        if let Some(expression) = expression {
            let expression = tantivy::jitexpr::ast::deserialize(expression).unwrap();
            let provider = JitExprValueSourceProvider::new(expression).unwrap();
            registry.register(field, Arc::new(provider));
        }
        let context = AggContextParams::default().with_value_sources(Arc::new(registry));
        let aggs: Aggregations =
            serde_json::from_value(json!({ "t": { "terms": { "field": field, "size": 10 } } }))
                .unwrap();
        group.register_with_input(name, &searcher, move |searcher: &Searcher| {
            let collector = AggregationCollector::from_aggs(aggs.clone(), context.clone());
            black_box(searcher.search(&AllQuery, &collector).unwrap());
        });
    }
    group.run();
}

fn create_index() -> Index {
    let mut schema_builder = Schema::builder();
    let service = schema_builder.add_text_field("service", STRING | FAST);
    let request_id = schema_builder.add_text_field("request_id", STRING | FAST);
    let index = Index::create_in_ram(schema_builder.build());
    let mut writer = index.writer_with_num_threads(1, 500_000_000).unwrap();
    writer.set_merge_policy(Box::new(NoMergePolicy));
    let services: Vec<String> = (0..NUM_SERVICES)
        .map(|idx| format!("team{}-service{idx}-api", idx % 20))
        .collect();
    let mut rng = StdRng::seed_from_u64(42);
    for doc_pos in 0..NUM_DOCS {
        let service_value = &services[rng.random_range(0..NUM_SERVICES)];
        writer
            .add_document(doc!(
                service => service_value.as_str(),
                request_id => format!("req{}-{doc_pos}", doc_pos % 7),
            ))
            .unwrap();
        if (doc_pos + 1) % (NUM_DOCS / NUM_SEGMENTS) == 0 {
            writer.commit().unwrap();
        }
    }
    writer.commit().unwrap();
    index
}
