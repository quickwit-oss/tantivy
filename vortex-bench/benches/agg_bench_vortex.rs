//! Copy of `benches/agg_bench.rs`, comparing three ways of reading the columns.
//!
//! - `columnar`: the regular tantivy fast fields (exactly what `agg_bench` measures).
//! - `opaque`: the same tantivy columns, registered as `ValueSource`s. The aggregation does not
//!   see the physical column (`as_column()` returns `None`), which disables some fast paths.
//! - `vortex`: the columns written by the `VortexPlugin`, registered as `ValueSource`s.
//!
//! The three variants of a request share a binggan group, so that they are interleaved.
//!
//! Only the request definitions and the index generation are kept from the original bench.
//! The index generation registers the `VortexPlugin`.
//!
//! Environment variables:
//! - `VORTEX_BENCH_OUT`: JSON lines output of the timings (default `results.jsonl`).
//! - `VORTEX_BENCH_VERIFY_OUT`: JSON lines output of the result checks (default `verify.jsonl`).
//! - `VORTEX_BENCH_INPUTS`: comma separated inputs (default `full,dense,sparse,multivalue`).
//! - `VORTEX_BENCH_SEGMENTS`: comma separated segment counts for the many segments bench
//!   (default `100,1000`, empty to skip).
//! - `VORTEX_BENCH_VARIANTS`: comma separated variants (default `columnar,opaque,vortex`).
//! - `VORTEX_BENCH_REQUESTS`: if set, comma separated request names to run.
//! - `VORTEX_BENCH_GROUPS`: if set, comma separated group names to run.

use std::any::Any;
use std::io::Write;
use std::sync::Arc;

use binggan::plugins::{EventListener, PeakMemAllocPlugin, PluginEvents};
use binggan::{black_box, BenchRunner, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use rand::distr::weighted::WeightedIndex;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::{AggContextParams, AggregationCollector};
use tantivy::indexer::NoMergePolicy;
use tantivy::query::{AllQuery, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, FAST, STRING};
use tantivy::{doc, DateTime, Index, Searcher, Term};
use vortex_bench::{opaque_registry, vortex_registry, VortexPlugin, VORTEX_EXTENSION};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

type AggregationRequest = serde_json::Value;
type AggregationExecutor =
    fn(&Searcher, &AggContextParams, AggregationRequest) -> tantivy::Result<AggregationResults>;
type BenchmarkConfig = (&'static str, AggregationRequest);
type BenchmarkGroup = (&'static str, AggregationExecutor, Vec<BenchmarkConfig>);

macro_rules! benchmark_config {
    ($request:ident) => {
        (stringify!($request), $request())
    };
}

/// The fast fields read through a registered value source in the `opaque` and `vortex`
/// variants. JSON fields are not converted by the `VortexPlugin`: they always use the columnar.
const REGISTERED_FIELDS: &[&str] = &[
    "single_term",
    "text_all_unique_terms",
    "text_many_terms",
    "text_few_terms",
    "text_few_terms_status",
    "text_90_terms_zipf",
    "text_1000_terms_zipf",
    "score",
    "score_f64",
    "score_i64",
    "timestamp",
    "timestamp_26_bits",
];

struct Variant {
    name: &'static str,
    context: AggContextParams,
}

fn env_list(var_name: &str) -> Option<Vec<String>> {
    let value = std::env::var(var_name).ok()?;
    Some(
        value
            .split(',')
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
            .collect(),
    )
}

fn env_list_contains(var_name: &str, item: &str, default: bool) -> bool {
    match env_list(var_name) {
        Some(items) => items.iter().any(|candidate| candidate == item),
        None => default,
    }
}

fn build_variants() -> Vec<Variant> {
    let all_variants = vec![
        Variant {
            name: "columnar",
            context: AggContextParams::default(),
        },
        Variant {
            name: "opaque",
            context: AggContextParams::default()
                .with_value_sources(Arc::new(opaque_registry(REGISTERED_FIELDS))),
        },
        Variant {
            name: "vortex",
            context: AggContextParams::default()
                .with_value_sources(Arc::new(vortex_registry(REGISTERED_FIELDS))),
        },
    ];
    all_variants
        .into_iter()
        .filter(|variant| env_list_contains("VORTEX_BENCH_VARIANTS", variant.name, true))
        .collect()
}

/// Appends one JSON line per bench result.
struct JsonResultsListener {
    output: std::fs::File,
}

impl JsonResultsListener {
    fn new() -> Self {
        let path = std::env::var("VORTEX_BENCH_OUT").unwrap_or_else(|_| "results.jsonl".into());
        let output = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("could not open the results file");
        JsonResultsListener { output }
    }
}

impl EventListener for JsonResultsListener {
    fn name(&self) -> &'static str {
        "json_results"
    }

    fn on_event(&mut self, event: PluginEvents) {
        let PluginEvents::GroupStop {
            runner_name,
            group_name,
            results,
            ..
        } = event
        else {
            return;
        };
        let timestamp_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for result in results {
            let line = json!({
                "input": runner_name,
                "group": group_name,
                "variant": result.bench_id.bench_name,
                "median_ns": result.stats.median_ns,
                "min_ns": result.stats.min_ns,
                "max_ns": result.stats.max_ns,
                "average_ns": result.stats.average_ns,
                "avg_memory": result.stats.avg_memory,
                "timestamp": timestamp_secs,
            });
            writeln!(self.output, "{line}").unwrap();
        }
        self.output.flush().unwrap();
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

/// Appends one JSON line per (request, variant), telling whether the variant returns the same
/// result as the columnar.
struct Verifier {
    output: std::fs::File,
}

impl Verifier {
    fn new() -> Self {
        let path =
            std::env::var("VORTEX_BENCH_VERIFY_OUT").unwrap_or_else(|_| "verify.jsonl".into());
        let output = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("could not open the verify file");
        Verifier { output }
    }

    /// Returns the variants able to run the request.
    fn supported_variants<'a>(
        &mut self,
        input_name: &str,
        group_name: &str,
        searcher: &Searcher,
        execute: AggregationExecutor,
        agg_req: &AggregationRequest,
        variants: &'a [Variant],
    ) -> Vec<&'a Variant> {
        let reference: Option<serde_json::Value> =
            execute(searcher, &AggContextParams::default(), agg_req.clone())
                .ok()
                .map(|result| serde_json::to_value(result).unwrap());
        let truncate = |value: &serde_json::Value| -> String {
            let mut text = value.to_string();
            text.truncate(400);
            text
        };
        let mut supported_variants = Vec::with_capacity(variants.len());
        for variant in variants {
            let mut diff: Option<(String, String)> = None;
            let mut result_hash: Option<String> = None;
            let (status, error) = match execute(searcher, &variant.context, agg_req.clone()) {
                Ok(result) => {
                    let result = serde_json::to_value(result).unwrap();
                    result_hash = Some(hash_json(&result));
                    supported_variants.push(variant);
                    if Some(&result) == reference.as_ref() {
                        ("same", None)
                    } else {
                        let reference_text =
                            reference.as_ref().map(truncate).unwrap_or_default();
                        diff = Some((reference_text, truncate(&result)));
                        ("different", None)
                    }
                }
                Err(err) => ("error", Some(err.to_string())),
            };
            let line = json!({
                "input": input_name,
                "group": group_name,
                "variant": variant.name,
                "status": status,
                "error": error,
                "reference": diff.as_ref().map(|diff| diff.0.clone()),
                "result": diff.as_ref().map(|diff| diff.1.clone()),
                "result_hash": result_hash,
            });
            writeln!(self.output, "{line}").unwrap();
        }
        self.output.flush().unwrap();
        supported_variants
    }
}

fn hash_json(value: &serde_json::Value) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.to_string().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn new_runner(input_name: &str) -> BenchRunner {
    let mut runner = BenchRunner::new();
    runner.add_plugin(PeakMemAllocPlugin::new(GLOBAL));
    runner.add_plugin(JsonResultsListener::new());
    runner.set_name(input_name);
    runner
}

fn main() {
    let inputs = [
        ("full", Cardinality::Full),
        ("dense", Cardinality::OptionalDense),
        ("sparse", Cardinality::OptionalSparse),
        ("multivalue", Cardinality::Multivalued),
    ];
    let variants = build_variants();
    let mut verifier = Verifier::new();

    for (input_name, cardinality) in inputs {
        if !env_list_contains("VORTEX_BENCH_INPUTS", input_name, true) {
            continue;
        }
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        // On sparse this will not effectively filter anything. This should simulate co-located
        // data which are sparse.
        // So for sparse aggregation, although the value is sparse all values in the aggregation
        // that are fetched do exist.
        // We want to make sure we perform well in these cases.
        let execute_filtered: AggregationExecutor = if cardinality == Cardinality::OptionalSparse {
            execute_agg_filtered_on_single_term
        } else {
            execute_agg_filtered
        };
        let mut runner = new_runner(input_name);
        bench_agg(
            &mut runner,
            input_name,
            &searcher,
            execute_filtered,
            &variants,
            &mut verifier,
        );
    }

    let segment_counts: Vec<usize> = env_list("VORTEX_BENCH_SEGMENTS")
        .unwrap_or_else(|| vec!["100".to_string(), "1000".to_string()])
        .iter()
        .map(|num_segments| num_segments.parse().unwrap())
        .collect();
    for num_segments in segment_counts {
        bench_many_segments(num_segments, &variants, &mut verifier);
    }
}

/// Registers the variants of a request in a single (interleaved) group and runs it.
fn run_request_group(
    runner: &mut BenchRunner,
    input_name: &str,
    group_name: &str,
    searcher: &Searcher,
    execute: AggregationExecutor,
    agg_req: AggregationRequest,
    variants: &[Variant],
    verifier: &mut Verifier,
) {
    let supported_variants = verifier.supported_variants(
        input_name, group_name, searcher, execute, &agg_req, variants,
    );
    if supported_variants.is_empty() {
        return;
    }
    let mut group = runner.new_group();
    group.set_name(group_name);
    for variant in supported_variants {
        let agg_req = agg_req.clone();
        let context = variant.context.clone();
        group.register_with_input(variant.name, searcher, move |searcher| {
            black_box(execute(searcher, &context, agg_req.clone()).unwrap());
        });
    }
    group.run();
}

fn bench_many_segments(num_segments: usize, variants: &[Variant], verifier: &mut Verifier) {
    let input_name = format!("{num_segments}_segments");
    let mut runner = new_runner(&input_name);
    runner.config().set_num_iter_for_group(1);
    let index = get_test_index_bench_with_num_segments(Cardinality::Full, num_segments).unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let mut multi_terms_top500 = multi_terms_many_and_zipf_1000();
    multi_terms_top500["mt"]["multi_terms"]["size"] = json!(500);
    let mut nested_terms_top500 = nested_terms_many_and_zipf_1000();
    // This limits outer buckets, unlike the global tuple limit for multi_terms.
    nested_terms_top500["my_texts"]["terms"]["size"] = json!(500);
    for (benchmark_name, agg_req) in [
        ("terms_7", terms_on_field("text_few_terms_status")),
        ("terms_zipfs_1000", terms_on_field("text_1000_terms_zipf")),
        ("terms_150_000", terms_on_field("text_many_terms")),
        ("terms_all_unique", terms_on_field("text_all_unique_terms")),
        benchmark_config!(nested_terms_status_and_zipf_1000),
        benchmark_config!(multi_terms_status_and_zipf_1000),
        benchmark_config!(nested_terms_many_and_zipf_1000),
        benchmark_config!(multi_terms_many_and_zipf_1000),
        (
            "nested_terms_many_and_zipf_1000_top500",
            nested_terms_top500,
        ),
        ("multi_terms_many_and_zipf_1000_top500", multi_terms_top500),
    ] {
        if !request_selected("segments", benchmark_name) {
            continue;
        }
        let group_name = format!("segments/{benchmark_name}");
        run_request_group(
            &mut runner,
            &input_name,
            &group_name,
            &searcher,
            execute_agg,
            agg_req,
            variants,
            verifier,
        );
    }
}

fn request_selected(group_name: &str, request_name: &str) -> bool {
    env_list_contains("VORTEX_BENCH_GROUPS", group_name, true)
        && env_list_contains("VORTEX_BENCH_REQUESTS", request_name, true)
}

fn terms_on_field(field: &str) -> AggregationRequest {
    json!({
        "terms": { "terms": { "field": field, "size": 1000 } }
    })
}

fn bench_agg(
    runner: &mut BenchRunner,
    input_name: &str,
    searcher: &Searcher,
    execute_filtered: AggregationExecutor,
    variants: &[Variant],
    verifier: &mut Verifier,
) {
    let multi_terms_vs_nested = vec![
        benchmark_config!(nested_terms_status_and_zipf_1000),
        benchmark_config!(multi_terms_status_and_zipf_1000),
        benchmark_config!(nested_terms_status_and_zipf_1000_with_missing),
        benchmark_config!(multi_terms_status_and_zipf_1000_with_missing),
        benchmark_config!(nested_terms_zipf_1000_and_status),
        benchmark_config!(multi_terms_zipf_1000_and_status),
        benchmark_config!(nested_terms_many_and_zipf_1000),
        benchmark_config!(multi_terms_many_and_zipf_1000),
        benchmark_config!(nested_terms_many_and_zipf_1000_and_status),
        benchmark_config!(multi_terms_many_and_zipf_1000_and_status),
        benchmark_config!(nested_terms_status_and_zipf_1000_avg_sub_agg),
        benchmark_config!(multi_terms_status_and_zipf_1000_avg_sub_agg),
    ];

    let mut groups: Vec<BenchmarkGroup> = Vec::new();
    groups.push((
        "metrics",
        execute_agg,
        vec![
            benchmark_config!(average_u64),
            benchmark_config!(average_f64),
            benchmark_config!(average_f64_u64),
            benchmark_config!(stats_f64),
            benchmark_config!(extendedstats_f64),
            benchmark_config!(percentiles_f64),
            benchmark_config!(cardinality_agg_low_card),
            benchmark_config!(cardinality_agg),
            benchmark_config!(cardinality_agg_high_card),
            benchmark_config!(terms_status_with_cardinality_agg),
            benchmark_config!(terms_100_buckets_with_cardinality_agg),
            benchmark_config!(terms_many_with_single_term_order_by_card),
            benchmark_config!(terms_many_with_single_term_2_order_by_card),
        ],
    ));
    groups.push((
        "terms",
        execute_agg,
        vec![
            benchmark_config!(terms_7),
            benchmark_config!(terms_zipf_1000),
            benchmark_config!(terms_zipf_90),
            benchmark_config!(terms_zipf_90_with_sum_sub_agg),
            benchmark_config!(terms_150_000),
            benchmark_config!(terms_many_top_1000),
            benchmark_config!(terms_many_order_by_term),
            benchmark_config!(terms_all_unique),
            benchmark_config!(terms_all_unique_order_by_key),
            benchmark_config!(terms_all_unique_with_avg_sub_agg),
            benchmark_config!(terms_many_with_avg_sub_agg),
            benchmark_config!(terms_status_with_avg_sub_agg),
            benchmark_config!(terms_zipf_1000_with_avg_sub_agg),
            benchmark_config!(terms_many_json_mixed_type_with_avg_sub_agg),
            benchmark_config!(terms_many_with_top_hits),
            benchmark_config!(terms_status_with_histogram),
            benchmark_config!(terms_zipf_1000_with_histogram),
            benchmark_config!(terms_status_with_date_histogram),
            benchmark_config!(terms_status_with_date_histogram_26_bits),
            benchmark_config!(terms_status_with_date_histogram_single_bucket),
            benchmark_config!(terms_status_with_date_histogram_4_buckets),
            benchmark_config!(terms_status_with_date_histogram_8_buckets),
            benchmark_config!(terms_status_with_date_histogram_hard_bounds),
            benchmark_config!(terms_status_with_date_histogram_and_sibling_terms),
        ],
    ));
    groups.push((
        "multi_terms_vs_nested",
        execute_agg,
        multi_terms_vs_nested.clone(),
    ));
    groups.push((
        "multi_terms_vs_nested_with_1%_term_query",
        execute_filtered,
        multi_terms_vs_nested,
    ));
    groups.push((
        "range_and_histogram",
        execute_agg,
        vec![
            benchmark_config!(range_agg),
            benchmark_config!(range_agg_with_avg_sub_agg),
            benchmark_config!(range_agg_with_term_agg_status),
            benchmark_config!(range_agg_with_term_agg_many),
            benchmark_config!(avg_and_range_with_avg_sub_agg),
            benchmark_config!(histogram),
            benchmark_config!(histogram_hard_bounds),
            benchmark_config!(histogram_with_avg_sub_agg),
            benchmark_config!(histogram_with_term_agg_status),
            benchmark_config!(composite_term_few),
            benchmark_config!(composite_term_many_page_1000),
            benchmark_config!(composite_term_many_page_1000_with_avg_sub_agg),
            benchmark_config!(composite_histogram),
            benchmark_config!(composite_histogram_calendar),
        ],
    ));
    groups.push((
        "filter_agg",
        execute_agg,
        vec![
            benchmark_config!(filter_agg_all_query_count_agg),
            benchmark_config!(filter_agg_term_query_count_agg),
            benchmark_config!(filter_agg_all_query_with_sub_aggs),
            benchmark_config!(filter_agg_term_query_with_sub_aggs),
        ],
    ));

    for (group_name, execute, configs) in groups {
        for (benchmark_name, agg_req) in configs {
            if !request_selected(group_name, benchmark_name) {
                continue;
            }
            let full_group_name = format!("{group_name}/{benchmark_name}");
            run_request_group(
                runner,
                input_name,
                &full_group_name,
                searcher,
                execute,
                agg_req,
                variants,
                verifier,
            );
        }
    }
}

fn average_u64() -> AggregationRequest {
    json!({
        "average": { "avg": { "field": "score", } }
    })
}
fn average_f64() -> AggregationRequest {
    json!({
        "average": { "avg": { "field": "score_f64", } }
    })
}
fn average_f64_u64() -> AggregationRequest {
    json!({
        "average_f64": { "avg": { "field": "score_f64" } },
        "average": { "avg": { "field": "score" } },
    })
}
fn stats_f64() -> AggregationRequest {
    json!({
        "average_f64": { "stats": { "field": "score_f64", } }
    })
}
fn extendedstats_f64() -> AggregationRequest {
    json!({
        "extendedstats_f64": { "extended_stats": { "field": "score_f64", } }
    })
}
fn percentiles_f64() -> AggregationRequest {
    json!({
        "mypercentiles": {
            "percentiles": {
                "field": "score_f64",
                "percents": [ 95, 99, 99.9 ]
            }
        }
    })
}

fn cardinality_agg() -> AggregationRequest {
    json!({
        "cardinality": {
            "cardinality": {
                "field": "text_many_terms"
            },
        }
    })
}
// Full-scan cardinality on a near-1M-cardinality string field.
// Hits the dense (PagedBitset) path: every doc has a unique term,
// so the bucket promotes from FxHashSet shortly into the scan.
fn cardinality_agg_high_card() -> AggregationRequest {
    json!({
        "cardinality": {
            "cardinality": {
                "field": "text_all_unique_terms"
            },
        }
    })
}
// Full-scan cardinality on a tiny-cardinality string field (7 distinct
// values). Stays on the FxHashSet path — the promotion threshold is
// never crossed. Validates no regression on the sparse path.
fn cardinality_agg_low_card() -> AggregationRequest {
    json!({
        "cardinality": {
            "cardinality": {
                "field": "text_few_terms_status"
            },
        }
    })
}
fn terms_status_with_cardinality_agg() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "cardinality": {
                    "cardinality": {
                        "field": "text_few_terms_status"
                    },
                }
            }
        },
    })
}

fn terms_100_buckets_with_cardinality_agg() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_1000_terms_zipf", "size": 100 },
            "aggs": {
                "cardinality": {
                    "cardinality": {
                        "field": "text_many_terms"
                    },
                }
            }
        },
    })
}

fn terms_many_with_single_term_order_by_card() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_many_terms" },
            "aggs": {
                "nested_terms": {
                    "terms": {
                        "field": "single_term",
                        "order": { "cardinality": "desc" }
                    },
                    "aggs": {
                        "cardinality": {
                            "cardinality": { "field": "text_few_terms" }
                        }
                    }
                }
            }
        },
    })
}

// Two-level terms ordered by cardinality at each level: a high-card outer terms
// (text_many_terms) ordered by a cardinality sub-agg, with a nested low-card terms
// (text_few_terms_status) also ordered by a cardinality sub-agg, plus an avg.
fn terms_many_with_single_term_2_order_by_card() -> AggregationRequest {
    json!({
        "by_ip": {
            "terms": {
                "field": "text_many_terms",
                "order": { "card_few_terms": "desc" }
            },
            "aggs": {
                "card_few_terms": {
                    "cardinality": { "field": "text_few_terms" }
                },
                "nested_terms": {
                    "terms": {
                        "field": " single_term",
                        "order": { "distinct_path2": "desc" }
                    },
                    "aggs": {
                        "avg_botscore": { "avg": { "field": "score" } },
                        "distinct_path2": { "cardinality": { "field": "text_few_terms" } }
                    }
                }
            }
        }
    })
}

fn terms_7() -> AggregationRequest {
    json!({
        "my_texts": { "terms": { "field": "text_few_terms_status" } },
    })
}
fn terms_all_unique() -> AggregationRequest {
    json!({
        "my_texts": { "terms": { "field": "text_all_unique_terms" } },
    })
}

fn terms_all_unique_order_by_key() -> AggregationRequest {
    json!({
        "my_texts": { "terms": { "field": "text_all_unique_terms", "order": { "_key": "asc" } } },
    })
}

fn terms_150_000() -> AggregationRequest {
    json!({
        "my_texts": { "terms": { "field": "text_many_terms" } },
    })
}
fn terms_many_top_1000() -> AggregationRequest {
    json!({
        "my_texts": { "terms": { "field": "text_many_terms", "size": 1000 } },
    })
}
fn terms_many_order_by_term() -> AggregationRequest {
    json!({
        "my_texts": { "terms": { "field": "text_many_terms", "order": { "_key": "desc" } } },
    })
}
fn terms_many_with_top_hits() -> AggregationRequest {
    json!({
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
    })
}
fn terms_many_with_avg_sub_agg() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_many_terms" },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        },
    })
}
fn terms_all_unique_with_avg_sub_agg() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_all_unique_terms" },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        },
    })
}
fn nested_terms_status_and_zipf_1000() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "nested_terms": { "terms": { "field": "text_1000_terms_zipf" } }
            }
        }
    })
}

fn nested_terms_status_and_zipf_1000_with_missing() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": {
                "field": "text_few_terms_status",
                "missing": "MISSING_STATUS"
            },
            "aggs": {
                "nested_terms": {
                    "terms": {
                        "field": "text_1000_terms_zipf",
                        "missing": "MISSING_ZIPF"
                    }
                }
            }
        }
    })
}

fn nested_terms_zipf_1000_and_status() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_1000_terms_zipf" },
            "aggs": {
                "nested_terms": { "terms": { "field": "text_few_terms_status" } }
            }
        }
    })
}

fn nested_terms_many_and_zipf_1000() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_many_terms" },
            "aggs": {
                "nested_terms": { "terms": { "field": "text_1000_terms_zipf" } }
            }
        }
    })
}

fn nested_terms_many_and_zipf_1000_and_status() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_many_terms" },
            "aggs": {
                "nested_zipf_terms": {
                    "terms": { "field": "text_1000_terms_zipf" },
                    "aggs": {
                        "nested_status_terms": {
                            "terms": { "field": "text_few_terms_status" }
                        }
                    }
                }
            }
        }
    })
}

fn nested_terms_status_and_zipf_1000_avg_sub_agg() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "nested_terms": {
                    "terms": { "field": "text_1000_terms_zipf" },
                    "aggs": {
                        "average_f64": { "avg": { "field": "score_f64" } }
                    }
                }
            }
        }
    })
}

fn terms_status_with_histogram() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "histo": {"histogram": { "field": "score_f64", "interval": 10 }}
            }
        }
    })
}

fn terms_status_with_date_histogram() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "over_time": { "date_histogram": { "field": "timestamp", "fixed_interval": "1h" } }
            }
        }
    })
}

fn terms_status_with_date_histogram_26_bits() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "over_time": { "date_histogram": { "field": "timestamp_26_bits", "fixed_interval": "134h" } }
            }
        }
    })
}

/// Same flattened terms × date_histogram, but with `hard_bounds`. The timestamps span 0..120h; the
/// bounds drop only the first and last hour (ms: 1h=3_600_000, 119h=428_400_000), so almost every
/// doc is in-bounds. This exercises the collector's hard-bounds path: `bounds.contains` runs per
/// doc (the `all_docs_in_bounds` short-circuit is off) and the rare out-of-bounds doc takes the
/// `term_counts` branch.
/// The timestamps span 0..120h, so a seven-day interval puts the entire index in one histogram
/// bucket. Combined with the skewed status distribution, this repeatedly updates the same grid
/// cell and highlights the benefit of independent count lanes.
fn terms_status_with_date_histogram_single_bucket() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "over_time": { "date_histogram": { "field": "timestamp", "fixed_interval": "7d" } }
            }
        }
    })
}

/// A thirty-two-hour interval divides the 0..120h timestamp span into exactly four buckets and
/// exercises the four-bucket linear resolver.
fn terms_status_with_date_histogram_4_buckets() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "over_time": { "date_histogram": { "field": "timestamp", "fixed_interval": "32h" } }
            }
        }
    })
}

/// A sixteen-hour interval divides the 0..120h timestamp span into exactly eight buckets and
/// exercises the eight-bucket linear resolver.
fn terms_status_with_date_histogram_8_buckets() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "over_time": { "date_histogram": { "field": "timestamp", "fixed_interval": "16h" } }
            }
        }
    })
}

fn terms_status_with_date_histogram_hard_bounds() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "fixed_interval": "1h",
                        "hard_bounds": { "min": 3_600_000, "max": 428_400_000 }
                    }
                }
            }
        }
    })
}

/// Same flattened terms × date_histogram, but with a sibling terms aggregation next to it. The
/// flattened fast path should still trigger for `my_texts` (sibling aggregations are independent
/// top-level aggregations, so they don't change its eligibility).
fn terms_status_with_date_histogram_and_sibling_terms() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "over_time": { "date_histogram": { "field": "timestamp", "fixed_interval": "1h" } }
            }
        },
        "other_texts": { "terms": { "field": "text_few_terms" } }
    })
}

fn terms_zipf_1000_with_histogram() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_1000_terms_zipf" },
            "aggs": {
                "histo": {"histogram": { "field": "score_f64", "interval": 10 }}
            }
        }
    })
}

fn terms_status_with_avg_sub_agg() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_few_terms_status" },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        },
    })
}

fn terms_zipf_1000_with_avg_sub_agg() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_1000_terms_zipf" },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        },
    })
}

fn terms_zipf_1000() -> AggregationRequest {
    json!({
        "my_texts": { "terms": { "field": "text_1000_terms_zipf" } },
    })
}

fn terms_zipf_90() -> AggregationRequest {
    json!({
        "my_texts": { "terms": { "field": "text_90_terms_zipf", "size": 100 } },
    })
}

// 90-term (low-cardinality Vec path) terms agg with a metric sub-agg. The skewed distribution keeps
// a dominant bucket crossing the sub-agg flush threshold, exercising the buffer flush path.
fn terms_zipf_90_with_sum_sub_agg() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "text_90_terms_zipf", "size": 100 },
            "aggs": {
                "sum_score": { "sum": { "field": "score" } }
            }
        },
    })
}

fn terms_many_json_mixed_type_with_avg_sub_agg() -> AggregationRequest {
    json!({
        "my_texts": {
            "terms": { "field": "json.mixed_type" },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        },
    })
}

fn composite_term_few() -> AggregationRequest {
    json!({
        "my_ctf": {
            "composite": {
                "sources": [
                    { "text_few_terms": { "terms": { "field": "text_few_terms" } } }
                ],
                "size": 1000
            }
        },
    })
}
fn composite_term_many_page_1000() -> AggregationRequest {
    json!({
        "my_ctmp1000": {
            "composite": {
                "sources": [
                    { "text_many_terms": { "terms": { "field": "text_many_terms" } } }
                ],
                "size": 1000
            }
        },
    })
}
fn composite_term_many_page_1000_with_avg_sub_agg() -> AggregationRequest {
    json!({
        "my_ctmp1000wasa": {
            "composite": {
                "sources": [
                    { "text_many_terms": { "terms": { "field": "text_many_terms" } } }
                ],
                "size": 1000,
            },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        },
    })
}
fn composite_histogram() -> AggregationRequest {
    json!({
        "my_ch": {
            "composite": {
                "sources": [
                    { "f64_histogram": { "histogram": { "field": "score_f64", "interval": 1 } } }
                ],
                "size": 1000
            }
        },
    })
}
fn composite_histogram_calendar() -> AggregationRequest {
    json!({
        "my_chc": {
            "composite": {
                "sources": [
                    { "time_histogram": { "date_histogram": { "field": "timestamp", "calendar_interval": "month" } } }
                ],
                "size": 1000
            }
        },
    })
}

/// multi_terms equivalent of nested_terms_status_and_zipf_1000:
/// flat GroupBy(status, zipf_1000) vs nested terms(status) -> terms(zipf_1000)
fn multi_terms_status_and_zipf_1000() -> AggregationRequest {
    json!({
        "mt": {
            "multi_terms": {
                "terms": [
                    {"field": "text_few_terms_status"},
                    {"field": "text_1000_terms_zipf"}
                ],
                "size": 100
            }
        }
    })
}

/// multi_terms equivalent of nested_terms_status_and_zipf_1000_with_missing.
fn multi_terms_status_and_zipf_1000_with_missing() -> AggregationRequest {
    json!({
        "mt": {
            "multi_terms": {
                "terms": [
                    {"field": "text_few_terms_status", "missing": "MISSING_STATUS"},
                    {"field": "text_1000_terms_zipf", "missing": "MISSING_ZIPF"}
                ],
                "size": 100
            }
        }
    })
}

/// multi_terms equivalent of nested_terms_zipf_1000_and_status:
/// flat GroupBy(zipf_1000, status) vs nested terms(zipf_1000) -> terms(status)
fn multi_terms_zipf_1000_and_status() -> AggregationRequest {
    json!({
        "mt": {
            "multi_terms": {
                "terms": [
                    {"field": "text_1000_terms_zipf"},
                    {"field": "text_few_terms_status"}
                ],
                "size": 100
            }
        }
    })
}

/// multi_terms equivalent of nested_terms_many_and_zipf_1000:
/// flat GroupBy(many, zipf_1000) vs nested terms(many) -> terms(zipf_1000)
fn multi_terms_many_and_zipf_1000() -> AggregationRequest {
    json!({
        "mt": {
            "multi_terms": {
                "terms": [
                    {"field": "text_many_terms"},
                    {"field": "text_1000_terms_zipf"}
                ],
                "size": 100
            }
        }
    })
}

/// multi_terms equivalent of nested_terms_many_and_zipf_1000_and_status:
/// flat GroupBy(many, zipf_1000, status) vs three nested terms levels
fn multi_terms_many_and_zipf_1000_and_status() -> AggregationRequest {
    json!({
        "mt": {
            "multi_terms": {
                "terms": [
                    {"field": "text_many_terms"},
                    {"field": "text_1000_terms_zipf"},
                    {"field": "text_few_terms_status"}
                ],
                "size": 100
            }
        }
    })
}

/// multi_terms equivalent of nested_terms_status_and_zipf_1000_avg_sub_agg.
fn multi_terms_status_and_zipf_1000_avg_sub_agg() -> AggregationRequest {
    json!({
        "mt": {
            "multi_terms": {
                "terms": [
                    {"field": "text_few_terms_status"},
                    {"field": "text_1000_terms_zipf"}
                ]
            },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        }
    })
}

fn execute_agg(
    searcher: &Searcher,
    context: &AggContextParams,
    agg_req: AggregationRequest,
) -> tantivy::Result<AggregationResults> {
    execute_agg_with_query(searcher, context, agg_req, &AllQuery)
}

fn execute_agg_filtered(
    searcher: &Searcher,
    context: &AggContextParams,
    agg_req: AggregationRequest,
) -> tantivy::Result<AggregationResults> {
    let filter_field = searcher.schema().get_field("filter_field").unwrap();
    let filter_query = TermQuery::new(
        Term::from_field_text(filter_field, "a"),
        IndexRecordOption::Basic,
    );
    execute_agg_with_query(searcher, context, agg_req, &filter_query)
}

fn execute_agg_filtered_on_single_term(
    searcher: &Searcher,
    context: &AggContextParams,
    agg_req: AggregationRequest,
) -> tantivy::Result<AggregationResults> {
    let filter_field = searcher.schema().get_field("single_term").unwrap();
    let filter_query = TermQuery::new(
        Term::from_field_text(filter_field, "single_term"),
        IndexRecordOption::Basic,
    );
    execute_agg_with_query(searcher, context, agg_req, &filter_query)
}

fn execute_agg_with_query(
    searcher: &Searcher,
    context: &AggContextParams,
    agg_req: AggregationRequest,
    query: &dyn Query,
) -> tantivy::Result<AggregationResults> {
    let agg_req: Aggregations = serde_json::from_value(agg_req).unwrap();
    let collector = AggregationCollector::from_aggs(agg_req, context.clone());
    searcher.search(query, &collector)
}

fn range_agg() -> AggregationRequest {
    json!({
        "range_f64": { "range": { "field": "score_f64", "ranges": [
            { "from": 3, "to": 7000 },
            { "from": 7000, "to": 20000 },
            { "from": 20000, "to": 30000 },
            { "from": 30000, "to": 40000 },
            { "from": 40000, "to": 50000 },
            { "from": 50000, "to": 60000 }
        ] } },
    })
}
fn range_agg_with_avg_sub_agg() -> AggregationRequest {
    json!({
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
    })
}

fn range_agg_with_term_agg_status() -> AggregationRequest {
    json!({
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
                "my_texts": { "terms": { "field": "text_few_terms_status" } },
            }
        },
    })
}
fn range_agg_with_term_agg_many() -> AggregationRequest {
    json!({
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
    })
}

fn histogram() -> AggregationRequest {
    json!({
        "rangef64": {
            "histogram": {
                "field": "score_f64",
                "interval": 100 // 1000 buckets
            },
        }
    })
}
fn histogram_hard_bounds() -> AggregationRequest {
    json!({
        "rangef64": { "histogram": { "field": "score_f64", "interval": 100, "hard_bounds": { "min": 1000, "max": 300000 } } },
    })
}
fn histogram_with_avg_sub_agg() -> AggregationRequest {
    json!({
        "rangef64": {
            "histogram": { "field": "score_f64", "interval": 100 },
            "aggs": {
                "average_f64": { "avg": { "field": "score_f64" } }
            }
        }
    })
}
fn histogram_with_term_agg_status() -> AggregationRequest {
    json!({
        "rangef64": {
            "histogram": { "field": "score_f64", "interval": 10 },
            "aggs": {
                "my_texts": { "terms": { "field": "text_few_terms_status" } }
            }
        }
    })
}
fn avg_and_range_with_avg_sub_agg() -> AggregationRequest {
    json!({
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
    })
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

fn get_test_index_bench(cardinality: Cardinality) -> tantivy::Result<Index> {
    get_test_index_bench_with_num_segments(cardinality, 1)
}

fn get_test_index_bench_with_num_segments(
    cardinality: Cardinality,
    num_segments: usize,
) -> tantivy::Result<Index> {
    assert!(num_segments > 0);
    assert!(num_segments == 1 || cardinality == Cardinality::Full);
    assert_eq!(1_000_000 % num_segments, 0);
    // Flag to reuse an on-disk index across runs. The generated data differs per cardinality and
    // segment count, so the path must include both.
    let reuse_index = std::env::var("REUSE_AGG_BENCH_INDEX")
        .map(|v| v == "true")
        .unwrap_or(true);
    let index_dir = if num_segments == 1 {
        format!("agg_bench_vortex/{cardinality:?}")
    } else {
        format!("agg_bench_vortex/{cardinality:?}_{num_segments}_segments")
    };
    let mut schema_builder = Schema::builder();
    let text_fieldtype = tantivy::schema::TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
        )
        .set_stored();
    let text_field = schema_builder.add_text_field("text", text_fieldtype.clone());
    let filter_field = schema_builder.add_text_field("filter_field", STRING);
    let single_term = schema_builder.add_text_field("single_term", STRING | FAST);
    let json_field = schema_builder.add_json_field("json", FAST);
    let text_field_all_unique_terms =
        schema_builder.add_text_field("text_all_unique_terms", STRING | FAST);
    let text_field_many_terms = schema_builder.add_text_field("text_many_terms", STRING | FAST);
    let text_field_few_terms = schema_builder.add_text_field("text_few_terms", STRING | FAST);
    let text_field_few_terms_status =
        schema_builder.add_text_field("text_few_terms_status", STRING | FAST);
    let text_field_90_terms_zipf =
        schema_builder.add_text_field("text_90_terms_zipf", STRING | FAST);
    let text_field_1000_terms_zipf =
        schema_builder.add_text_field("text_1000_terms_zipf", STRING | FAST);
    let score_fieldtype = tantivy::schema::NumericOptions::default().set_fast();
    let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
    let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
    let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
    let date_field = schema_builder.add_date_field("timestamp", FAST);
    let date_26_bits_field = schema_builder.add_date_field("timestamp_26_bits", FAST);
    let schema = schema_builder.build();

    if reuse_index && std::path::Path::new(&index_dir).try_exists()? {
        let index = Index::open_in_dir(&index_dir)?;
        let has_vortex_component = index
            .load_metas()?
            .persisted_custom_extensions
            .iter()
            .any(|extension| extension == VORTEX_EXTENSION);
        if index.schema() == schema
            && has_vortex_component
            && index.searchable_segment_ids()?.len() == num_segments
        {
            return Ok(index);
        }
        drop(index);
        std::fs::remove_dir_all(&index_dir)?;
    }

    // use tmp dir
    let index_builder = Index::builder()
        .schema(schema)
        .register_plugin(Arc::new(VortexPlugin));
    let index = if reuse_index {
        std::fs::create_dir_all(&index_dir)?;
        index_builder.create_in_dir(&index_dir)?
    } else {
        index_builder.create_from_tempdir()?
    };
    // Approximate log proportions
    let status_field_data = [
        ("INFO", 8000),
        ("ERROR", 300),
        ("WARN", 1200),
        ("DEBUG", 500),
        ("OK", 500),
        ("CRITICAL", 20),
        ("EMERGENCY", 1),
    ];
    let log_level_distribution =
        WeightedIndex::new(status_field_data.iter().map(|item| item.1)).unwrap();

    let few_terms_data = ["INFO", "ERROR", "WARN", "DEBUG"];
    let lg_norm = rand_distr::LogNormal::new(2.996f64, 0.979f64).unwrap();

    let many_terms_data = (0..150_000)
        .map(|num| format!("author{num}"))
        .collect::<Vec<_>>();

    // Prepare 1000 unique terms sampled using a Zipf distribution.
    // Exponent 1.1: heavy head — term_1 alone ~18%, top-20 terms ~57%.
    let terms_1000: Vec<String> = (1..=1000).map(|i| format!("term_{i}")).collect();
    let zipf_1000 = rand_distr::Zipf::new(1000.0, 1.1f64).unwrap();

    // 90 terms (< MAX_NUM_TERMS_FOR_VEC), skewed via Zipf so a dominant bucket keeps crossing the
    // sub-agg flush threshold while minority buckets stay small. Exercises the low-cardinality
    // sub-agg buffer flush path (see issue #2992).
    let terms_90: Vec<String> = (1..=90).map(|i| format!("term_{i}")).collect();
    let zipf_90 = rand_distr::Zipf::new(90.0, 1.1f64).unwrap();

    {
        let mut rng = StdRng::from_seed([1u8; 32]);
        let mut filter_rng = StdRng::from_seed([2u8; 32]);
        let mut timestamp_26_bits_rng = StdRng::from_seed([3u8; 32]);
        let mut index_writer = index.writer_with_num_threads(1, 400_000_000)?;
        if num_segments > 1 {
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
        }
        let docs_per_segment = 1_000_000 / num_segments;
        let mut num_indexed_docs = 0usize;
        // 1% steady-state match rate, with random clusters averaging four documents.
        const MATCH_RATE: f64 = 0.01;
        const CLUSTER_END_PROBABILITY: f64 = 0.25;
        // Example: 25% ends average 4 matches; 1% requires 396 non-matches, so start at 1/396.
        let cluster_start_probability = CLUSTER_END_PROBABILITY * MATCH_RATE / (1.0 - MATCH_RATE);
        let mut filter_matches = false;
        let mut add_document = |mut document: tantivy::TantivyDocument| -> tantivy::Result<()> {
            filter_matches = if filter_matches {
                !filter_rng.random_bool(CLUSTER_END_PROBABILITY)
            } else {
                filter_rng.random_bool(cluster_start_probability)
            };
            document.add_text(filter_field, if filter_matches { "a" } else { "b" });
            index_writer.add_document(document)?;
            num_indexed_docs += 1;
            if num_segments > 1 && num_indexed_docs.is_multiple_of(docs_per_segment) {
                index_writer.commit()?;
            }
            Ok(())
        };
        // To make the different test cases comparable we just change one doc to force the
        // cardinality
        if cardinality == Cardinality::OptionalDense {
            add_document(doc!())?;
        }
        if cardinality == Cardinality::Multivalued {
            let log_level_sample_a = status_field_data[log_level_distribution.sample(&mut rng)].0;
            let log_level_sample_b = status_field_data[log_level_distribution.sample(&mut rng)].0;
            let idx_a = zipf_1000.sample(&mut rng) as usize - 1;
            let idx_b = zipf_1000.sample(&mut rng) as usize - 1;
            let term_1000_a = &terms_1000[idx_a];
            let term_1000_b = &terms_1000[idx_b];
            let term_90_a = &terms_90[zipf_90.sample(&mut rng) as usize - 1];
            let term_90_b = &terms_90[zipf_90.sample(&mut rng) as usize - 1];
            add_document(doc!(
                json_field => json!({"mixed_type": 10.0}),
                json_field => json!({"mixed_type": 10.0}),
                single_term => "single_term",
                single_term => "single_term",
                text_field => "cool",
                text_field => "cool",
                text_field_all_unique_terms => "cool",
                text_field_all_unique_terms => "coolo",
                text_field_many_terms => "cool",
                text_field_many_terms => "cool",
                text_field_few_terms => "cool",
                text_field_few_terms => "cool",
                text_field_few_terms_status => log_level_sample_a,
                text_field_few_terms_status => log_level_sample_b,
                text_field_90_terms_zipf => term_90_a.as_str(),
                text_field_90_terms_zipf => term_90_b.as_str(),
                text_field_1000_terms_zipf => term_1000_a.as_str(),
                text_field_1000_terms_zipf => term_1000_b.as_str(),
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
        const SPAN_MS: i64 = 120 * 3600 * 1000; // 120 hours in ms
        const NOISE_MS: i64 = 2 * 3600 * 1000; // ±2h noise
        const MAX_26_BIT_TIMESTAMP_SECS: i64 = (1 << 26) - 1;
        for i in 0..doc_with_value {
            let val: f64 = rng.random_range(0.0..1_000_000.0);
            let json = if rng.random_bool(0.1) {
                // 10% are numeric values
                json!({ "mixed_type": val })
            } else {
                json!({"mixed_type": many_terms_data.choose(&mut rng).unwrap().to_string()})
            };
            let base_ms = (i as i64 * SPAN_MS) / doc_with_value as i64;
            let noise_ms = rng.random_range(-NOISE_MS..NOISE_MS);
            let ts_ms = (base_ms + noise_ms).clamp(0, SPAN_MS);
            // Force the endpoints and randomize the interior so the column uses a 26-bit packed
            // representation rather than the blockwise-linear codec.
            let ts_26_bits_secs = match i {
                0 => 0,
                1 => 1,
                i if i + 1 == doc_with_value => MAX_26_BIT_TIMESTAMP_SECS,
                _ => timestamp_26_bits_rng.random_range(0..=MAX_26_BIT_TIMESTAMP_SECS),
            };
            add_document(doc!(
                single_term => "single_term",
                text_field => "cool",
                json_field => json,
                text_field_all_unique_terms => format!("unique_term_{}", rng.random::<u64>()),
                text_field_many_terms => many_terms_data.choose(&mut rng).unwrap().to_string(),
                text_field_few_terms => few_terms_data.choose(&mut rng).unwrap().to_string(),
                text_field_few_terms_status => status_field_data[log_level_distribution.sample(&mut rng)].0,
                text_field_90_terms_zipf => terms_90[zipf_90.sample(&mut rng) as usize - 1].as_str(),
                text_field_1000_terms_zipf => terms_1000[zipf_1000.sample(&mut rng) as usize - 1].as_str(),
                score_field => val as u64,
                score_field_f64 => lg_norm.sample(&mut rng),
                score_field_i64 => val as i64,
                date_field => DateTime::from_timestamp_millis(ts_ms),
                date_26_bits_field => DateTime::from_timestamp_secs(ts_26_bits_secs),
            ))?;
            if cardinality == Cardinality::OptionalSparse {
                for _ in 0..20 {
                    add_document(doc!(text_field => "cool"))?;
                }
            }
        }
        drop(add_document);
        // writing the segment
        index_writer.commit()?;
    }

    assert_eq!(index.searchable_segment_ids()?.len(), num_segments);
    Ok(index)
}

// Filter aggregation benchmarks

fn filter_agg_all_query_count_agg() -> AggregationRequest {
    json!({
        "filtered": {
            "filter": "*",
            "aggs": {
                "count": { "value_count": { "field": "score" } }
            }
        }
    })
}

fn filter_agg_term_query_count_agg() -> AggregationRequest {
    json!({
        "filtered": {
            "filter": "text:cool",
            "aggs": {
                "count": { "value_count": { "field": "score" } }
            }
        }
    })
}

fn filter_agg_all_query_with_sub_aggs() -> AggregationRequest {
    json!({
        "filtered": {
            "filter": "*",
            "aggs": {
                "avg_score": { "avg": { "field": "score" } },
                "stats_score": { "stats": { "field": "score_f64" } },
                "terms_text": {
                    "terms": { "field": "text_few_terms_status" }
                }
            }
        }
    })
}

fn filter_agg_term_query_with_sub_aggs() -> AggregationRequest {
    json!({
        "filtered": {
            "filter": "text:cool",
            "aggs": {
                "avg_score": { "avg": { "field": "score" } },
                "stats_score": { "stats": { "field": "score_f64" } },
                "terms_text": {
                    "terms": { "field": "text_few_terms_status" }
                }
            }
        }
    })
}
