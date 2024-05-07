use binggan::{black_box, InputGroup, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::{AllQuery, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, FAST, STRING};
use tantivy::{doc, Index, Term};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

/// Mini macro to register a function via its name
/// runner.register("average_u64", move |index| average_u64(index));
macro_rules! register {
    ($runner:expr, $func:ident) => {
        $runner.register(stringify!($func), move |index| $func(index))
    };
}

fn main() {
    let inputs = vec![
        ("full", get_test_index_bench(Cardinality::Full).unwrap()),
        (
            "dense",
            get_test_index_bench(Cardinality::OptionalDense).unwrap(),
        ),
        (
            "sparse",
            get_test_index_bench(Cardinality::OptionalSparse).unwrap(),
        ),
        (
            "multivalue",
            get_test_index_bench(Cardinality::Multivalued).unwrap(),
        ),
    ];

    bench_agg(InputGroup::new_with_inputs(inputs));
}

fn bench_agg(mut group: InputGroup<Index>) {
    group.set_alloc(GLOBAL); // Set the peak mem allocator. This will enable peak memory reporting.
    register!(group, average_u64);
    register!(group, average_f64);
    register!(group, average_f64_u64);
    register!(group, stats_f64);
    register!(group, percentiles_f64);
    register!(group, terms_few);
    register!(group, terms_many);
    register!(group, terms_many_order_by_term);
    register!(group, terms_many_with_top_hits);
    register!(group, terms_many_with_avg_sub_agg);
    register!(group, terms_many_json_mixed_type_with_sub_agg_card);
    register!(group, range_agg);
    register!(group, range_agg_with_avg_sub_agg);
    register!(group, range_agg_with_term_agg_few);
    register!(group, range_agg_with_term_agg_many);
    register!(group, histogram);
    register!(group, histogram_hard_bounds);
    register!(group, histogram_with_avg_sub_agg);
    register!(group, avg_and_range_with_avg_sub_agg);

    group.run();
}

fn exec_term_with_agg(index: &Index, agg_req: serde_json::Value) {
    let agg_req: Aggregations = serde_json::from_value(agg_req).unwrap();

    let reader = index.reader().unwrap();
    let text_field = reader.searcher().schema().get_field("text").unwrap();
    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );
    let collector = get_collector(agg_req);
    let searcher = reader.searcher();
    black_box(searcher.search(&term_query, &collector).unwrap());
}

fn average_u64(index: &Index) {
    let agg_req = json!({
        "average": { "avg": { "field": "score", } }
    });
    exec_term_with_agg(index, agg_req)
}
fn average_f64(index: &Index) {
    let agg_req = json!({
        "average": { "avg": { "field": "score_f64", } }
    });
    exec_term_with_agg(index, agg_req)
}
fn average_f64_u64(index: &Index) {
    let agg_req = json!({
        "average_f64": { "avg": { "field": "score_f64" } },
        "average": { "avg": { "field": "score" } },
    });
    exec_term_with_agg(index, agg_req)
}
fn stats_f64(index: &Index) {
    let agg_req = json!({
        "average_f64": { "stats": { "field": "score_f64", } }
    });
    exec_term_with_agg(index, agg_req)
}

fn percentiles_f64(index: &Index) {
    let agg_req = json!({
      "mypercentiles": {
        "percentiles": {
          "field": "score_f64",
          "percents": [ 95, 99, 99.9 ]
        }
      }
    });
    execute_agg(index, agg_req);
}
fn terms_few(index: &Index) {
    let agg_req = json!({
        "my_texts": { "terms": { "field": "text_few_terms" } },
    });
    execute_agg(index, agg_req);
}
fn terms_many(index: &Index) {
    let agg_req = json!({
        "my_texts": { "terms": { "field": "text_many_terms" } },
    });
    execute_agg(index, agg_req);
}
fn terms_many_order_by_term(index: &Index) {
    let agg_req = json!({
        "my_texts": { "terms": { "field": "text_many_terms", "order": { "_key": "desc" } } },
    });
    execute_agg(index, agg_req);
}
fn terms_many_with_top_hits(index: &Index) {
    let agg_req = json!({
        "my_texts": {
            "terms": { "field": "text_many_terms" },
            "aggs": {
                "top_hits": { "top_hits":
                    {
                        "sort": [
                            { "score": "desc" }
                        ],
                        "size": 2,
                        "doc_value_fields": ["score_f64"]
                    }
                }
            }
        },
    });
    execute_agg(index, agg_req);
}
fn terms_many_with_avg_sub_agg(index: &Index) {
    let agg_req = json!({
        "my_texts": {
            "terms": { "field": "text_many_terms" },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        },
    });
    execute_agg(index, agg_req);
}
fn terms_many_json_mixed_type_with_sub_agg_card(index: &Index) {
    let agg_req = json!({
        "my_texts": {
            "terms": { "field": "json.mixed_type" },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        },
    });
    execute_agg(index, agg_req);
}

fn execute_agg(index: &Index, agg_req: serde_json::Value) {
    let agg_req: Aggregations = serde_json::from_value(agg_req).unwrap();
    let collector = get_collector(agg_req);

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    black_box(searcher.search(&AllQuery, &collector).unwrap());
}
fn range_agg(index: &Index) {
    let agg_req = json!({
        "range_f64": { "range": { "field": "score_f64", "ranges": [
            { "from": 3, "to": 7000 },
            { "from": 7000, "to": 20000 },
            { "from": 20000, "to": 30000 },
            { "from": 30000, "to": 40000 },
            { "from": 40000, "to": 50000 },
            { "from": 50000, "to": 60000 }
        ] } },
    });
    execute_agg(index, agg_req);
}
fn range_agg_with_avg_sub_agg(index: &Index) {
    let agg_req = json!({
        "rangef64": {
            "range": {
                "field": "score_f64",
                "ranges": [
                    { "from": 3, "to": 7000 },
                    { "from": 7000, "to": 20000 },
                    { "from": 20000, "to": 30000 },
                    { "from": 30000, "to": 40000 },
                    { "from": 40000, "to": 50000 },
                    { "from": 50000, "to": 60000 }
                ]
            },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        },
    });
    execute_agg(index, agg_req);
}

fn range_agg_with_term_agg_few(index: &Index) {
    let agg_req = json!({
        "rangef64": {
            "range": {
                "field": "score_f64",
                "ranges": [
                    { "from": 3, "to": 7000 },
                    { "from": 7000, "to": 20000 },
                    { "from": 20000, "to": 30000 },
                    { "from": 30000, "to": 40000 },
                    { "from": 40000, "to": 50000 },
                    { "from": 50000, "to": 60000 }
                ]
            },
            "aggs": {
                "my_texts": { "terms": { "field": "text_few_terms" } },
            }
        },
    });
    execute_agg(index, agg_req);
}
fn range_agg_with_term_agg_many(index: &Index) {
    let agg_req = json!({
        "rangef64": {
            "range": {
                "field": "score_f64",
                "ranges": [
                    { "from": 3, "to": 7000 },
                    { "from": 7000, "to": 20000 },
                    { "from": 20000, "to": 30000 },
                    { "from": 30000, "to": 40000 },
                    { "from": 40000, "to": 50000 },
                    { "from": 50000, "to": 60000 }
                ]
            },
            "aggs": {
                "my_texts": { "terms": { "field": "text_many_terms" } },
            }
        },
    });
    execute_agg(index, agg_req);
}
fn histogram(index: &Index) {
    let agg_req = json!({
        "rangef64": {
            "histogram": {
                "field": "score_f64",
                "interval": 100 // 1000 buckets
            },
        }
    });
    execute_agg(index, agg_req);
}
fn histogram_hard_bounds(index: &Index) {
    let agg_req = json!({
        "rangef64": { "histogram": { "field": "score_f64", "interval": 100, "hard_bounds": { "min": 1000, "max": 300000 } } },
    });
    execute_agg(index, agg_req);
}
fn histogram_with_avg_sub_agg(index: &Index) {
    let agg_req = json!({
        "rangef64": {
            "histogram": { "field": "score_f64", "interval": 100 },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        }
    });
    execute_agg(index, agg_req);
}
fn avg_and_range_with_avg_sub_agg(index: &Index) {
    let agg_req = json!({
        "rangef64": {
            "range": {
                "field": "score_f64",
                "ranges": [
                    { "from": 3, "to": 7000 },
                    { "from": 7000, "to": 20000 },
                    { "from": 20000, "to": 60000 }
                ]
            },
            "aggs": {
                "average_in_range": { "avg": { "field": "score" } }
            }
        },
        "average": { "avg": { "field": "score" } }
    });
    execute_agg(index, agg_req);
}

#[derive(Clone, Copy, Hash, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cardinality {
    /// All documents contain exactly one value.
    /// `Full` is the default for auto-detecting the Cardinality, since it is the most strict.
    #[default]
    Full = 0,
    /// All documents contain at most one value.
    OptionalDense = 1,
    /// All documents may contain any number of values.
    Multivalued = 2,
    /// 1 / 20 documents has a value
    OptionalSparse = 3,
}

fn get_collector(agg_req: Aggregations) -> AggregationCollector {
    AggregationCollector::from_aggs(agg_req, Default::default())
}

fn get_test_index_bench(cardinality: Cardinality) -> tantivy::Result<Index> {
    let mut schema_builder = Schema::builder();
    let text_fieldtype = tantivy::schema::TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
        )
        .set_stored();
    let text_field = schema_builder.add_text_field("text", text_fieldtype);
    let json_field = schema_builder.add_json_field("json", FAST);
    let text_field_many_terms = schema_builder.add_text_field("text_many_terms", STRING | FAST);
    let text_field_few_terms = schema_builder.add_text_field("text_few_terms", STRING | FAST);
    let score_fieldtype = tantivy::schema::NumericOptions::default().set_fast();
    let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
    let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
    let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
    let index = Index::create_from_tempdir(schema_builder.build())?;
    let few_terms_data = ["INFO", "ERROR", "WARN", "DEBUG"];

    let lg_norm = rand_distr::LogNormal::new(2.996f64, 0.979f64).unwrap();

    let many_terms_data = (0..150_000)
        .map(|num| format!("author{}", num))
        .collect::<Vec<_>>();
    {
        let mut rng = StdRng::from_seed([1u8; 32]);
        let mut index_writer = index.writer_with_num_threads(1, 200_000_000)?;
        // To make the different test cases comparable we just change one doc to force the
        // cardinality
        if cardinality == Cardinality::OptionalDense {
            index_writer.add_document(doc!())?;
        }
        if cardinality == Cardinality::Multivalued {
            index_writer.add_document(doc!(
                json_field => json!({"mixed_type": 10.0}),
                json_field => json!({"mixed_type": 10.0}),
                text_field => "cool",
                text_field => "cool",
                text_field_many_terms => "cool",
                text_field_many_terms => "cool",
                text_field_few_terms => "cool",
                text_field_few_terms => "cool",
                score_field => 1u64,
                score_field => 1u64,
                score_field_f64 => lg_norm.sample(&mut rng),
                score_field_f64 => lg_norm.sample(&mut rng),
                score_field_i64 => 1i64,
                score_field_i64 => 1i64,
            ))?;
        }
        let mut doc_with_value = 1_000_000;
        if cardinality == Cardinality::OptionalSparse {
            doc_with_value /= 20;
        }
        let _val_max = 1_000_000.0;
        for _ in 0..doc_with_value {
            let val: f64 = rng.gen_range(0.0..1_000_000.0);
            let json = if rng.gen_bool(0.1) {
                // 10% are numeric values
                json!({ "mixed_type": val })
            } else {
                json!({"mixed_type": many_terms_data.choose(&mut rng).unwrap().to_string()})
            };
            index_writer.add_document(doc!(
                text_field => "cool",
                json_field => json,
                text_field_many_terms => many_terms_data.choose(&mut rng).unwrap().to_string(),
                text_field_few_terms => few_terms_data.choose(&mut rng).unwrap().to_string(),
                score_field => val as u64,
                score_field_f64 => lg_norm.sample(&mut rng),
                score_field_i64 => val as i64,
            ))?;
            if cardinality == Cardinality::OptionalSparse {
                for _ in 0..20 {
                    index_writer.add_document(doc!(text_field => "cool"))?;
                }
            }
        }
        // writing the segment
        index_writer.commit()?;
    }

    Ok(index)
}
