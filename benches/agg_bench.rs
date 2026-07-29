use binggan::plugins::PeakMemAllocPlugin;
use binggan::{black_box, BenchRunner, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use rand::distr::weighted::WeightedIndex;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::{AllQuery, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, FAST, STRING};
use tantivy::{doc, DateTime, Index, Term};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

type AggregationRequest = serde_json::Value;
type AggregationExecutor = fn(&Index, AggregationRequest);
type BenchmarkConfig = (&'static str, AggregationRequest);
type BenchmarkGroup = (&'static str, AggregationExecutor, Vec<BenchmarkConfig>);

macro_rules! benchmark_config {
    ($request:ident) => {
        (stringify!($request), $request())
    };
}

fn main() {
    let inputs = [
        ("full", Cardinality::Full),
        ("dense", Cardinality::OptionalDense),
        ("sparse", Cardinality::OptionalSparse),
        ("multivalue", Cardinality::Multivalued),
    ];

    let mut runner = BenchRunner::new();
    runner.add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    for (input_name, cardinality) in inputs {
        let index = get_test_index_bench(cardinality).unwrap();
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
        runner.set_name(input_name);
        bench_agg(&mut runner, &index, execute_filtered);
    }
}

fn bench_agg(runner: &mut BenchRunner, index: &Index, execute_filtered: AggregationExecutor) {
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
        let mut group = runner.new_group();
        group.set_name(group_name);
        for (benchmark_name, agg_req) in configs {
            group.register_with_input(benchmark_name, index, move |index| {
                execute(index, agg_req.clone())
            });
        }
        group.run();
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

/// Same fused terms × date_histogram, but with `hard_bounds`. The timestamps span 0..120h; the
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

/// Same fused terms × date_histogram, but with a sibling terms aggregation next to it. The fused
/// fast path should still trigger for `my_texts` (sibling aggregations are independent top-level
/// aggregations, so they don't change its eligibility).
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

fn execute_agg(index: &Index, agg_req: AggregationRequest) {
    execute_agg_with_query(index, agg_req, &AllQuery);
}

fn execute_agg_filtered(index: &Index, agg_req: AggregationRequest) {
    let filter_field = index.schema().get_field("filter_field").unwrap();
    let filter_query = TermQuery::new(
        Term::from_field_text(filter_field, "a"),
        IndexRecordOption::Basic,
    );
    execute_agg_with_query(index, agg_req, &filter_query);
}

fn execute_agg_filtered_on_single_term(index: &Index, agg_req: AggregationRequest) {
    let filter_field = index.schema().get_field("single_term").unwrap();
    let filter_query = TermQuery::new(
        Term::from_field_text(filter_field, "single_term"),
        IndexRecordOption::Basic,
    );
    execute_agg_with_query(index, agg_req, &filter_query);
}

fn execute_agg_with_query(index: &Index, agg_req: AggregationRequest, query: &dyn Query) {
    let agg_req: Aggregations = serde_json::from_value(agg_req).unwrap();
    let collector = get_collector(agg_req);

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    black_box(searcher.search(query, &collector).unwrap());
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

fn get_collector(agg_req: Aggregations) -> AggregationCollector {
    AggregationCollector::from_aggs(agg_req, Default::default())
}

fn get_test_index_bench(cardinality: Cardinality) -> tantivy::Result<Index> {
    // Flag to reuse an on-disk index across runs. The generated data differs per cardinality, so
    // the path must include the cardinality — otherwise one cardinality reuses another's index.
    let reuse_index = std::env::var("REUSE_AGG_BENCH_INDEX")
        .map(|v| v == "true")
        .unwrap_or(true);
    let index_dir = format!("agg_bench/{cardinality:?}");
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
    let schema = schema_builder.build();

    if reuse_index && std::path::Path::new(&index_dir).try_exists()? {
        let index = Index::open_in_dir(&index_dir)?;
        if index.schema() == schema {
            return Ok(index);
        }
        drop(index);
        std::fs::remove_dir_all(&index_dir)?;
    }

    // use tmp dir
    let index = if reuse_index {
        std::fs::create_dir_all(&index_dir)?;
        Index::create_in_dir(&index_dir, schema)?
    } else {
        Index::create_from_tempdir(schema)?
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
        let mut index_writer = index.writer_with_num_threads(1, 200_000_000)?;
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
