use serde_json::Value;

use crate::aggregation::agg_req::{
    Aggregation, Aggregations, BucketAggregation, BucketAggregationType, MetricAggregation,
};
use crate::aggregation::agg_result::AggregationResults;
use crate::aggregation::bucket::{RangeAggregation, TermsAggregation};
use crate::aggregation::buf_collector::DOC_BLOCK_SIZE;
use crate::aggregation::collector::AggregationCollector;
use crate::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use crate::aggregation::metric::AverageAggregation;
use crate::aggregation::tests::{get_test_index_2_segments, get_test_index_from_values_and_terms};
use crate::aggregation::DistributedAggregationCollector;
use crate::query::{AllQuery, TermQuery};
use crate::schema::IndexRecordOption;
use crate::Term;

fn get_avg_req(field_name: &str) -> Aggregation {
    Aggregation::Metric(MetricAggregation::Average(
        AverageAggregation::from_field_name(field_name.to_string()),
    ))
}

// *** EVERY BUCKET-TYPE SHOULD BE TESTED HERE ***
fn test_aggregation_flushing(
    merge_segments: bool,
    use_distributed_collector: bool,
) -> crate::Result<()> {
    let mut values_and_terms = (0..80)
        .map(|val| vec![(val as f64, "terma".to_string())])
        .collect::<Vec<_>>();
    values_and_terms.last_mut().unwrap()[0].1 = "termb".to_string();
    let index = get_test_index_from_values_and_terms(merge_segments, &values_and_terms)?;

    let reader = index.reader()?;

    assert_eq!(DOC_BLOCK_SIZE, 64);
    // In the tree we cache Documents of DOC_BLOCK_SIZE, before passing them down as one block.
    //
    // Build a request so that on the first level we have one full cache, which is then flushed.
    // The same cache should have some residue docs at the end, which are flushed (Range 0-70)
    // -> 70 docs
    //
    // The second level should also have some residue docs in the cache that are flushed at the
    // end.
    //
    // A second bucket on the first level should have the cache unfilled

    // let elasticsearch_compatible_json_req = r#"
    let elasticsearch_compatible_json = json!(
    {
    "bucketsL1": {
        "range": {
            "field": "score",
            "ranges": [ { "to": 3.0f64 }, { "from": 3.0f64, "to": 70.0f64 }, { "from": 70.0f64 } ]
        },
        "aggs": {
            "bucketsL2": {
                "range": {
                    "field": "score",
                    "ranges": [ { "to": 30.0f64 }, { "from": 30.0f64, "to": 70.0f64 }, { "from": 70.0f64 } ]
                }
            }
        }
    },
    "histogram_test":{
        "histogram": {
            "field": "score",
            "interval":  70.0,
            "offset": 3.0
        },
        "aggs": {
            "bucketsL2": {
                "histogram": {
                    "field": "score",
                    "interval":  70.0
                }
            }
        }
    },
    "term_agg_test":{
        "terms": {
            "field": "string_id"
        },
        "aggs": {
            "bucketsL2": {
                "histogram": {
                    "field": "score",
                    "interval":  70.0
                }
            }
        }
    }
    });

    let agg_req: Aggregations =
        serde_json::from_str(&serde_json::to_string(&elasticsearch_compatible_json).unwrap())
            .unwrap();

    let agg_res: AggregationResults = if use_distributed_collector {
        let collector = DistributedAggregationCollector::from_aggs(agg_req.clone(), None);

        let searcher = reader.searcher();
        let intermediate_agg_result = searcher.search(&AllQuery, &collector).unwrap();
        intermediate_agg_result
            .into_final_bucket_result(agg_req)
            .unwrap()
    } else {
        let collector = AggregationCollector::from_aggs(agg_req, None);

        let searcher = reader.searcher();
        searcher.search(&AllQuery, &collector).unwrap()
    };

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

    assert_eq!(res["bucketsL1"]["buckets"][0]["doc_count"], 3);
    assert_eq!(
        res["bucketsL1"]["buckets"][0]["bucketsL2"]["buckets"][0]["doc_count"],
        3
    );
    assert_eq!(res["bucketsL1"]["buckets"][1]["key"], "3-70");
    assert_eq!(res["bucketsL1"]["buckets"][1]["doc_count"], 70 - 3);
    assert_eq!(
        res["bucketsL1"]["buckets"][1]["bucketsL2"]["buckets"][0]["doc_count"],
        27
    );
    assert_eq!(
        res["bucketsL1"]["buckets"][1]["bucketsL2"]["buckets"][1]["doc_count"],
        40
    );
    assert_eq!(
        res["bucketsL1"]["buckets"][1]["bucketsL2"]["buckets"][2]["doc_count"],
        0
    );
    assert_eq!(
        res["bucketsL1"]["buckets"][2]["bucketsL2"]["buckets"][2]["doc_count"],
        80 - 70
    );
    assert_eq!(res["bucketsL1"]["buckets"][2]["doc_count"], 80 - 70);

    assert_eq!(
        res["term_agg_test"],
        json!(
        {
            "buckets": [
              {
                "bucketsL2": {
                  "buckets": [
                    {
                      "doc_count": 70,
                      "key": 0.0
                    },
                    {
                      "doc_count": 9,
                      "key": 70.0
                    }
                  ]
                },
                "doc_count": 79,
                "key": "terma"
              },
              {
                "bucketsL2": {
                  "buckets": [
                    {
                      "doc_count": 1,
                      "key": 70.0
                    }
                  ]
                },
                "doc_count": 1,
                "key": "termb"
              }
            ],
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0
          }
        )
    );

    Ok(())
}

#[test]
fn test_aggregation_flushing_variants() {
    test_aggregation_flushing(false, false).unwrap();
    test_aggregation_flushing(false, true).unwrap();
    test_aggregation_flushing(true, false).unwrap();
    test_aggregation_flushing(true, true).unwrap();
}

#[test]
fn test_aggregation_level1() -> crate::Result<()> {
    let index = get_test_index_2_segments(true)?;

    let reader = index.reader()?;
    let text_field = reader.searcher().schema().get_field("text").unwrap();

    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );

    let agg_req_1: Aggregations = vec![
        ("average_i64".to_string(), get_avg_req("score_i64")),
        ("average_f64".to_string(), get_avg_req("score_f64")),
        ("average".to_string(), get_avg_req("score")),
        (
            "range".to_string(),
            Aggregation::Bucket(Box::new(BucketAggregation {
                bucket_agg: BucketAggregationType::Range(RangeAggregation {
                    field: "score".to_string(),
                    ranges: vec![(3f64..7f64).into(), (7f64..20f64).into()],
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            })),
        ),
        (
            "rangef64".to_string(),
            Aggregation::Bucket(Box::new(BucketAggregation {
                bucket_agg: BucketAggregationType::Range(RangeAggregation {
                    field: "score_f64".to_string(),
                    ranges: vec![(3f64..7f64).into(), (7f64..20f64).into()],
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            })),
        ),
        (
            "rangei64".to_string(),
            Aggregation::Bucket(Box::new(BucketAggregation {
                bucket_agg: BucketAggregationType::Range(RangeAggregation {
                    field: "score_i64".to_string(),
                    ranges: vec![(3f64..7f64).into(), (7f64..20f64).into()],
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            })),
        ),
    ]
    .into_iter()
    .collect();

    let collector = AggregationCollector::from_aggs(agg_req_1, None);

    let searcher = reader.searcher();
    let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
    assert_eq!(res["average"]["value"], 12.142857142857142);
    assert_eq!(res["average_f64"]["value"], 12.214285714285714);
    assert_eq!(res["average_i64"]["value"], 12.142857142857142);
    assert_eq!(
        res["range"]["buckets"],
        json!(
        [
        {
          "key": "*-3",
          "doc_count": 1,
          "to": 3.0
        },
        {
          "key": "3-7",
          "doc_count": 2,
          "from": 3.0,
          "to": 7.0
        },
        {
          "key": "7-20",
          "doc_count": 3,
          "from": 7.0,
          "to": 20.0
        },
        {
          "key": "20-*",
          "doc_count": 1,
          "from": 20.0
        }
        ])
    );

    Ok(())
}

fn test_aggregation_level2(
    merge_segments: bool,
    use_distributed_collector: bool,
    use_elastic_json_req: bool,
) -> crate::Result<()> {
    let index = get_test_index_2_segments(merge_segments)?;

    let reader = index.reader()?;
    let text_field = reader.searcher().schema().get_field("text").unwrap();

    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );

    let query_with_no_hits = TermQuery::new(
        Term::from_field_text(text_field, "thistermdoesnotexist"),
        IndexRecordOption::Basic,
    );

    let sub_agg_req: Aggregations = vec![
        ("average_in_range".to_string(), get_avg_req("score")),
        (
            "term_agg".to_string(),
            Aggregation::Bucket(Box::new(BucketAggregation {
                bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                    field: "text".to_string(),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            })),
        ),
    ]
    .into_iter()
    .collect();
    let agg_req: Aggregations = if use_elastic_json_req {
        let elasticsearch_compatible_json_req = r#"
{
  "rangef64": {
    "range": {
      "field": "score_f64",
      "ranges": [
        { "to": 3.0 },
        { "from": 3.0, "to": 7.0 },
        { "from": 7.0, "to": 19.0 },
        { "from": 19.0, "to": 20.0 },
        { "from": 20.0 }
      ]
    },
    "aggs": {
      "average_in_range": { "avg": { "field": "score" } },
      "term_agg": { "terms": { "field": "text" } }
    }
  },
  "rangei64": {
    "range": {
      "field": "score_i64",
      "ranges": [
        { "to": 3.0 },
        { "from": 3.0, "to": 7.0 },
        { "from": 7.0, "to": 19.0 },
        { "from": 19.0, "to": 20.0 },
        { "from": 20.0 }
      ]
    },
    "aggs": {
      "average_in_range": { "avg": { "field": "score" } },
      "term_agg": { "terms": { "field": "text" } }
    }
  },
  "average": {
    "avg": { "field": "score" }
  },
  "range": {
    "range": {
      "field": "score",
      "ranges": [
        { "to": 3.0 },
        { "from": 3.0, "to": 7.0 },
        { "from": 7.0, "to": 19.0 },
        { "from": 19.0, "to": 20.0 },
        { "from": 20.0 }
      ]
    },
    "aggs": {
      "average_in_range": { "avg": { "field": "score" } },
      "term_agg": { "terms": { "field": "text" } }
    }
  }
}
"#;
        let value: Aggregations = serde_json::from_str(elasticsearch_compatible_json_req).unwrap();
        value
    } else {
        let agg_req: Aggregations = vec![
            ("average".to_string(), get_avg_req("score")),
            (
                "range".to_string(),
                Aggregation::Bucket(Box::new(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field: "score".to_string(),
                        ranges: vec![
                            (3f64..7f64).into(),
                            (7f64..19f64).into(),
                            (19f64..20f64).into(),
                        ],
                        ..Default::default()
                    }),
                    sub_aggregation: sub_agg_req.clone(),
                })),
            ),
            (
                "rangef64".to_string(),
                Aggregation::Bucket(Box::new(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field: "score_f64".to_string(),
                        ranges: vec![
                            (3f64..7f64).into(),
                            (7f64..19f64).into(),
                            (19f64..20f64).into(),
                        ],
                        ..Default::default()
                    }),
                    sub_aggregation: sub_agg_req.clone(),
                })),
            ),
            (
                "rangei64".to_string(),
                Aggregation::Bucket(Box::new(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field: "score_i64".to_string(),
                        ranges: vec![
                            (3f64..7f64).into(),
                            (7f64..19f64).into(),
                            (19f64..20f64).into(),
                        ],
                        ..Default::default()
                    }),
                    sub_aggregation: sub_agg_req,
                })),
            ),
        ]
        .into_iter()
        .collect();
        agg_req
    };

    let agg_res: AggregationResults = if use_distributed_collector {
        let collector = DistributedAggregationCollector::from_aggs(agg_req.clone(), None);

        let searcher = reader.searcher();
        let res = searcher.search(&term_query, &collector).unwrap();
        // Test de/serialization roundtrip on intermediate_agg_result
        let res: IntermediateAggregationResults =
            serde_json::from_str(&serde_json::to_string(&res).unwrap()).unwrap();
        res.into_final_bucket_result(agg_req.clone()).unwrap()
    } else {
        let collector = AggregationCollector::from_aggs(agg_req.clone(), None);

        let searcher = reader.searcher();
        searcher.search(&term_query, &collector).unwrap()
    };

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

    assert_eq!(res["range"]["buckets"][1]["key"], "3-7");
    assert_eq!(res["range"]["buckets"][1]["doc_count"], 2u64);
    assert_eq!(res["rangef64"]["buckets"][1]["doc_count"], 2u64);
    assert_eq!(res["rangei64"]["buckets"][1]["doc_count"], 2u64);

    assert_eq!(res["average"]["value"], 12.142857142857142f64);
    assert_eq!(res["range"]["buckets"][2]["key"], "7-19");
    assert_eq!(res["range"]["buckets"][2]["doc_count"], 3u64);
    assert_eq!(res["rangef64"]["buckets"][2]["doc_count"], 3u64);
    assert_eq!(res["rangei64"]["buckets"][2]["doc_count"], 3u64);
    assert_eq!(res["rangei64"]["buckets"][5], serde_json::Value::Null);

    assert_eq!(res["range"]["buckets"][4]["key"], "20-*");
    assert_eq!(res["range"]["buckets"][4]["doc_count"], 1u64);
    assert_eq!(res["rangef64"]["buckets"][4]["doc_count"], 1u64);
    assert_eq!(res["rangei64"]["buckets"][4]["doc_count"], 1u64);

    assert_eq!(res["range"]["buckets"][3]["key"], "19-20");
    assert_eq!(res["range"]["buckets"][3]["doc_count"], 0u64);
    assert_eq!(res["rangef64"]["buckets"][3]["doc_count"], 0u64);
    assert_eq!(res["rangei64"]["buckets"][3]["doc_count"], 0u64);

    assert_eq!(
        res["range"]["buckets"][3]["average_in_range"]["value"],
        serde_json::Value::Null
    );

    assert_eq!(
        res["range"]["buckets"][4]["average_in_range"]["value"],
        44.0f64
    );
    assert_eq!(
        res["rangef64"]["buckets"][4]["average_in_range"]["value"],
        44.0f64
    );
    assert_eq!(
        res["rangei64"]["buckets"][4]["average_in_range"]["value"],
        44.0f64
    );

    assert_eq!(
        res["range"]["7-19"]["average_in_range"]["value"],
        res["rangef64"]["7-19"]["average_in_range"]["value"]
    );
    assert_eq!(
        res["range"]["7-19"]["average_in_range"]["value"],
        res["rangei64"]["7-19"]["average_in_range"]["value"]
    );

    // Test empty result set
    let collector = AggregationCollector::from_aggs(agg_req, None);
    let searcher = reader.searcher();
    searcher.search(&query_with_no_hits, &collector).unwrap();

    Ok(())
}

#[test]
fn test_aggregation_level2_multi_segments() -> crate::Result<()> {
    test_aggregation_level2(false, false, false)
}

#[test]
fn test_aggregation_level2_single_segment() -> crate::Result<()> {
    test_aggregation_level2(true, false, false)
}

#[test]
fn test_aggregation_level2_multi_segments_distributed_collector() -> crate::Result<()> {
    test_aggregation_level2(false, true, false)
}

#[test]
fn test_aggregation_level2_single_segment_distributed_collector() -> crate::Result<()> {
    test_aggregation_level2(true, true, false)
}

#[test]
fn test_aggregation_level2_multi_segments_use_json() -> crate::Result<()> {
    test_aggregation_level2(false, false, true)
}

#[test]
fn test_aggregation_level2_single_segment_use_json() -> crate::Result<()> {
    test_aggregation_level2(true, false, true)
}

#[test]
fn test_aggregation_level2_multi_segments_distributed_collector_use_json() -> crate::Result<()> {
    test_aggregation_level2(false, true, true)
}

#[test]
fn test_aggregation_level2_single_segment_distributed_collector_use_json() -> crate::Result<()> {
    test_aggregation_level2(true, true, true)
}

#[test]
fn test_aggregation_invalid_requests() -> crate::Result<()> {
    let index = get_test_index_2_segments(false)?;

    let reader = index.reader()?;

    let avg_on_field = |field_name: &str| {
        let agg_req_1: Aggregations = vec![(
            "average".to_string(),
            Aggregation::Metric(MetricAggregation::Average(
                AverageAggregation::from_field_name(field_name.to_string()),
            )),
        )]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(agg_req_1, None);

        let searcher = reader.searcher();

        searcher.search(&AllQuery, &collector).unwrap_err()
    };

    let agg_res = avg_on_field("dummy_text");
    assert_eq!(
        format!("{:?}", agg_res),
        r#"InvalidArgument("No fast field found for field: dummy_text")"#
    );

    let agg_res = avg_on_field("not_exist_field");
    assert_eq!(
        format!("{:?}", agg_res),
        r#"InvalidArgument("No fast field found for field: not_exist_field")"#
    );

    let agg_res = avg_on_field("ip_addr");
    assert_eq!(
        format!("{:?}", agg_res),
        r#"InvalidArgument("No fast field found for field: ip_addr")"#
    );

    Ok(())
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use columnar::Cardinality;
    use rand::prelude::SliceRandom;
    use rand::{thread_rng, Rng};
    use test::{self, Bencher};

    use super::*;
    use crate::aggregation::bucket::{
        CustomOrder, HistogramAggregation, HistogramBounds, Order, OrderTarget, TermsAggregation,
    };
    use crate::aggregation::metric::StatsAggregation;
    use crate::query::AllQuery;
    use crate::schema::{Schema, TextFieldIndexing, FAST, STRING};
    use crate::Index;

    fn get_test_index_bench(cardinality: Cardinality) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_fieldtype = crate::schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let text_field_many_terms = schema_builder.add_text_field("text_many_terms", STRING | FAST);
        let text_field_few_terms = schema_builder.add_text_field("text_few_terms", STRING | FAST);
        let score_fieldtype = crate::schema::NumericOptions::default().set_fast();
        let score_field = schema_builder.add_u64_field("score", score_fieldtype.clone());
        let score_field_f64 = schema_builder.add_f64_field("score_f64", score_fieldtype.clone());
        let score_field_i64 = schema_builder.add_i64_field("score_i64", score_fieldtype);
        let index = Index::create_from_tempdir(schema_builder.build())?;
        let few_terms_data = vec!["INFO", "ERROR", "WARN", "DEBUG"];
        let many_terms_data = (0..150_000)
            .map(|num| format!("author{}", num))
            .collect::<Vec<_>>();
        {
            let mut rng = thread_rng();
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000)?;
            // To make the different test cases comparable we just change one doc to force the
            // cardinality
            if cardinality == Cardinality::Optional {
                index_writer.add_document(doc!())?;
            }
            if cardinality == Cardinality::Multivalued {
                index_writer.add_document(doc!(
                    text_field => "cool",
                    text_field => "cool",
                    text_field_many_terms => "cool",
                    text_field_many_terms => "cool",
                    text_field_few_terms => "cool",
                    text_field_few_terms => "cool",
                    score_field => 1u64,
                    score_field => 1u64,
                    score_field_f64 => 1.0,
                    score_field_f64 => 1.0,
                    score_field_i64 => 1i64,
                    score_field_i64 => 1i64,
                ))?;
            }
            for _ in 0..1_000_000 {
                let val: f64 = rng.gen_range(0.0..1_000_000.0);
                index_writer.add_document(doc!(
                    text_field => "cool",
                    text_field_many_terms => many_terms_data.choose(&mut rng).unwrap().to_string(),
                    text_field_few_terms => few_terms_data.choose(&mut rng).unwrap().to_string(),
                    score_field => val as u64,
                    score_field_f64 => val,
                    score_field_i64 => val as i64,
                ))?;
            }
            // writing the segment
            index_writer.commit()?;
        }

        Ok(index)
    }

    use paste::paste;
    #[macro_export]
    macro_rules! bench_all_cardinalities {
        (  $x:ident ) => {
            paste! {
                #[bench]
                fn $x(b: &mut Bencher) {
                    [<$x _card>](b, Cardinality::Full)
                }

                #[bench]
                fn [<$x _opt>](b: &mut Bencher) {
                    [<$x _card>](b, Cardinality::Optional)
                }

                #[bench]
                fn [<$x _multi>](b: &mut Bencher) {
                    [<$x _card>](b, Cardinality::Multivalued)
                }
            }
        };
    }

    bench_all_cardinalities!(bench_aggregation_average_u64);

    fn bench_aggregation_average_u64_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let agg_req_1: Aggregations = vec![(
                "average".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_stats_f64);

    fn bench_aggregation_stats_f64_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let agg_req_1: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Stats(StatsAggregation::from_field_name(
                    "score_f64".to_string(),
                ))),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_average_f64);

    fn bench_aggregation_average_f64_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let agg_req_1: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score_f64".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_average_u64_and_f64);

    fn bench_aggregation_average_u64_and_f64_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let agg_req_1: Aggregations = vec![
                (
                    "average_f64".to_string(),
                    Aggregation::Metric(MetricAggregation::Average(
                        AverageAggregation::from_field_name("score_f64".to_string()),
                    )),
                ),
                (
                    "average".to_string(),
                    Aggregation::Metric(MetricAggregation::Average(
                        AverageAggregation::from_field_name("score".to_string()),
                    )),
                ),
            ]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_terms_few);

    fn bench_aggregation_terms_few_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req: Aggregations = vec![(
                "my_texts".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                            field: "text_few_terms".to_string(),
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req, None);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_terms_many_with_sub_agg);

    fn bench_aggregation_terms_many_with_sub_agg_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let sub_agg_req: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score_f64".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let agg_req: Aggregations = vec![(
                "my_texts".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                            field: "text_many_terms".to_string(),
                            ..Default::default()
                        }),
                        sub_aggregation: sub_agg_req,
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req, None);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_terms_many2);

    fn bench_aggregation_terms_many2_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req: Aggregations = vec![(
                "my_texts".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                            field: "text_many_terms".to_string(),
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req, None);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_terms_many_order_by_term);

    fn bench_aggregation_terms_many_order_by_term_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req: Aggregations = vec![(
                "my_texts".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Terms(TermsAggregation {
                            field: "text_many_terms".to_string(),
                            order: Some(CustomOrder {
                                order: Order::Desc,
                                target: OrderTarget::Key,
                            }),
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req, None);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_range_only);

    fn bench_aggregation_range_only_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Range(RangeAggregation {
                            field: "score_f64".to_string(),
                            ranges: vec![
                                (3f64..7000f64).into(),
                                (7000f64..20000f64).into(),
                                (20000f64..30000f64).into(),
                                (30000f64..40000f64).into(),
                                (40000f64..50000f64).into(),
                                (50000f64..60000f64).into(),
                            ],
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_range_with_avg);

    fn bench_aggregation_range_with_avg_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let sub_agg_req: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score_f64".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Range(RangeAggregation {
                            field: "score_f64".to_string(),
                            ranges: vec![
                                (3f64..7000f64).into(),
                                (7000f64..20000f64).into(),
                                (20000f64..30000f64).into(),
                                (30000f64..40000f64).into(),
                                (40000f64..50000f64).into(),
                                (50000f64..60000f64).into(),
                            ],
                            ..Default::default()
                        }),
                        sub_aggregation: sub_agg_req,
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    // hard bounds has a different algorithm, because it actually limits collection range
    //
    bench_all_cardinalities!(bench_aggregation_histogram_only_hard_bounds);

    fn bench_aggregation_histogram_only_hard_bounds_card(
        b: &mut Bencher,
        cardinality: Cardinality,
    ) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                            field: "score_f64".to_string(),
                            interval: 100f64,
                            hard_bounds: Some(HistogramBounds {
                                min: 1000.0,
                                max: 300_000.0,
                            }),
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);
            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_histogram_with_avg);

    fn bench_aggregation_histogram_with_avg_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let sub_agg_req: Aggregations = vec![(
                "average_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score_f64".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                            field: "score_f64".to_string(),
                            interval: 100f64, // 1000 buckets
                            ..Default::default()
                        }),
                        sub_aggregation: sub_agg_req,
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_histogram_only);

    fn bench_aggregation_histogram_only_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();

        b.iter(|| {
            let agg_req_1: Aggregations = vec![(
                "rangef64".to_string(),
                Aggregation::Bucket(
                    BucketAggregation {
                        bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                            field: "score_f64".to_string(),
                            interval: 100f64, // 1000 buckets
                            ..Default::default()
                        }),
                        sub_aggregation: Default::default(),
                    }
                    .into(),
                ),
            )]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);

            let searcher = reader.searcher();
            searcher.search(&AllQuery, &collector).unwrap()
        });
    }

    bench_all_cardinalities!(bench_aggregation_avg_and_range_with_avg);

    fn bench_aggregation_avg_and_range_with_avg_card(b: &mut Bencher, cardinality: Cardinality) {
        let index = get_test_index_bench(cardinality).unwrap();
        let reader = index.reader().unwrap();
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        b.iter(|| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, "cool"),
                IndexRecordOption::Basic,
            );

            let sub_agg_req_1: Aggregations = vec![(
                "average_in_range".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("score".to_string()),
                )),
            )]
            .into_iter()
            .collect();

            let agg_req_1: Aggregations = vec![
                (
                    "average".to_string(),
                    Aggregation::Metric(MetricAggregation::Average(
                        AverageAggregation::from_field_name("score".to_string()),
                    )),
                ),
                (
                    "rangef64".to_string(),
                    Aggregation::Bucket(
                        BucketAggregation {
                            bucket_agg: BucketAggregationType::Range(RangeAggregation {
                                field: "score_f64".to_string(),
                                ranges: vec![
                                    (3f64..7000f64).into(),
                                    (7000f64..20000f64).into(),
                                    (20000f64..60000f64).into(),
                                ],
                                ..Default::default()
                            }),
                            sub_aggregation: sub_agg_req_1,
                        }
                        .into(),
                    ),
                ),
            ]
            .into_iter()
            .collect();

            let collector = AggregationCollector::from_aggs(agg_req_1, None);

            let searcher = reader.searcher();
            searcher.search(&term_query, &collector).unwrap()
        });
    }
}
