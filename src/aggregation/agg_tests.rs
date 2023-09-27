use serde_json::Value;

use crate::aggregation::agg_req::{Aggregation, Aggregations};
use crate::aggregation::agg_result::AggregationResults;
use crate::aggregation::buf_collector::DOC_BLOCK_SIZE;
use crate::aggregation::collector::AggregationCollector;
use crate::aggregation::segment_agg_result::AggregationLimits;
use crate::aggregation::tests::{get_test_index_2_segments, get_test_index_from_values_and_terms};
use crate::aggregation::DistributedAggregationCollector;
use crate::query::{AllQuery, TermQuery};
use crate::schema::{IndexRecordOption, Schema, FAST};
use crate::{Index, Term};

fn get_avg_req(field_name: &str) -> Aggregation {
    serde_json::from_value(json!({
        "avg": {
            "field": field_name,
        }
    }))
    .unwrap()
}

fn get_collector(agg_req: Aggregations) -> AggregationCollector {
    AggregationCollector::from_aggs(agg_req, Default::default())
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
        let collector = DistributedAggregationCollector::from_aggs(
            agg_req.clone(),
            AggregationLimits::default(),
        );

        let searcher = reader.searcher();
        let intermediate_agg_result = searcher.search(&AllQuery, &collector).unwrap();
        intermediate_agg_result
            .into_final_result(agg_req, &Default::default())
            .unwrap()
    } else {
        let collector = get_collector(agg_req);

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
fn test_aggregation_level1_simple() -> crate::Result<()> {
    let index = get_test_index_2_segments(true)?;

    let reader = index.reader()?;
    let text_field = reader.searcher().schema().get_field("text").unwrap();

    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );

    let range_agg = |field_name: &str| -> Aggregation {
        serde_json::from_value(json!({
            "range": {
                "field": field_name,
                "ranges": [ { "from": 3.0f64, "to": 7.0f64 }, { "from": 7.0f64, "to": 20.0f64 } ]
            }
        }))
        .unwrap()
    };

    let agg_req_1: Aggregations = vec![
        ("average".to_string(), get_avg_req("score")),
        ("range".to_string(), range_agg("score")),
    ]
    .into_iter()
    .collect();

    let collector = get_collector(agg_req_1);

    let searcher = reader.searcher();
    let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
    assert_eq!(res["average"]["value"], 12.142857142857142);
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

#[test]
fn test_aggregation_level1() -> crate::Result<()> {
    let index = get_test_index_2_segments(true)?;

    let reader = index.reader()?;
    let text_field = reader.searcher().schema().get_field("text").unwrap();

    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );

    let range_agg = |field_name: &str| -> Aggregation {
        serde_json::from_value(json!({
            "range": {
                "field": field_name,
                "ranges": [ { "from": 3.0f64, "to": 7.0f64 }, { "from": 7.0f64, "to": 20.0f64 } ]
            }
        }))
        .unwrap()
    };

    let agg_req_1: Aggregations = vec![
        ("average_i64".to_string(), get_avg_req("score_i64")),
        ("average_f64".to_string(), get_avg_req("score_f64")),
        ("average".to_string(), get_avg_req("score")),
        ("range".to_string(), range_agg("score")),
        ("rangef64".to_string(), range_agg("score_f64")),
        ("rangei64".to_string(), range_agg("score_i64")),
    ]
    .into_iter()
    .collect();

    let collector = get_collector(agg_req_1);

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
    let agg_req: Aggregations = serde_json::from_str(elasticsearch_compatible_json_req).unwrap();

    let agg_res: AggregationResults = if use_distributed_collector {
        let collector =
            DistributedAggregationCollector::from_aggs(agg_req.clone(), Default::default());

        let searcher = reader.searcher();
        let res = searcher.search(&term_query, &collector).unwrap();
        res.into_final_result(agg_req.clone(), &Default::default())
            .unwrap()
    } else {
        let collector = get_collector(agg_req.clone());

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
    let collector = get_collector(agg_req);
    let searcher = reader.searcher();
    searcher.search(&query_with_no_hits, &collector).unwrap();

    Ok(())
}

#[test]
fn test_aggregation_level2_multi_segments() -> crate::Result<()> {
    test_aggregation_level2(false, false)
}

#[test]
fn test_aggregation_level2_single_segment() -> crate::Result<()> {
    test_aggregation_level2(true, false)
}

#[test]
fn test_aggregation_level2_multi_segments_distributed_collector() -> crate::Result<()> {
    test_aggregation_level2(false, true)
}

#[test]
fn test_aggregation_level2_single_segment_distributed_collector() -> crate::Result<()> {
    test_aggregation_level2(true, true)
}

#[test]
fn test_aggregation_invalid_requests() -> crate::Result<()> {
    let index = get_test_index_2_segments(false)?;

    let reader = index.reader()?;

    let avg_on_field = |field_name: &str| {
        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "average": {
                "avg": {
                    "field": field_name,
                },
            }
        }))
        .unwrap();

        let collector = get_collector(agg_req_1);

        let searcher = reader.searcher();

        searcher.search(&AllQuery, &collector)
    };

    let agg_res = avg_on_field("dummy_text").unwrap_err();
    assert_eq!(
        format!("{agg_res:?}"),
        r#"InvalidArgument("Field \"dummy_text\" is not configured as fast field")"#
    );

    let agg_req_1: Result<Aggregations, serde_json::Error> = serde_json::from_value(json!({
        "average": {
            "avg": {
                "fieldd": "a",
            },
        }
    }));

    assert_eq!(agg_req_1.is_err(), true);
    assert_eq!(agg_req_1.unwrap_err().to_string(), "missing field `field`");

    let agg_req_1: Result<Aggregations, serde_json::Error> = serde_json::from_value(json!({
        "average": {
            "doesnotmatchanyagg": {
                "field": "a",
            },
        }
    }));

    assert_eq!(agg_req_1.is_err(), true);
    // TODO: This should list valid values
    assert!(agg_req_1
        .unwrap_err()
        .to_string()
        .contains("unknown variant `doesnotmatchanyagg`, expected one of"));

    // TODO: This should return an error
    // let agg_res = avg_on_field("not_exist_field").unwrap_err();
    // assert_eq!(
    // format!("{:?}", agg_res),
    // r#"InvalidArgument("No fast field found for field: not_exist_field")"#
    //);

    // TODO: This should return an error
    // let agg_res = avg_on_field("ip_addr").unwrap_err();
    // assert_eq!(
    // format!("{:?}", agg_res),
    // r#"InvalidArgument("No fast field found for field: ip_addr")"#
    //);

    Ok(())
}

#[test]
fn test_aggregation_on_json_object() {
    let mut schema_builder = Schema::builder();
    let json = schema_builder.add_json_field("json", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut index_writer = index.writer_for_tests().unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "red"})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "blue"})))
        .unwrap();
    index_writer.commit().unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let agg: Aggregations = serde_json::from_value(json!({
        "jsonagg": {
            "terms": {
                "field": "json.color",
            }
        }
    }))
    .unwrap();

    let aggregation_collector = get_collector(agg);
    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!({
            "jsonagg": {
                "buckets": [
                    {"doc_count": 1, "key": "blue"},
                    {"doc_count": 1, "key": "red"}
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0
            }
        })
    );
}

#[test]
fn test_aggregation_on_json_object_empty_columns() {
    let mut schema_builder = Schema::builder();
    let json = schema_builder.add_json_field("json", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut index_writer = index.writer_for_tests().unwrap();
    // => Empty column when accessing color
    index_writer
        .add_document(doc!(json => json!({"price": 10.0})))
        .unwrap();
    index_writer.commit().unwrap();
    // => Empty column when accessing price
    index_writer
        .add_document(doc!(json => json!({"color": "blue"})))
        .unwrap();
    index_writer.commit().unwrap();

    // => Non Empty columns
    index_writer
        .add_document(doc!(json => json!({"color": "red", "price": 10.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "red", "price": 10.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "green", "price": 20.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "green", "price": 20.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "green", "price": 20.0})))
        .unwrap();

    index_writer.commit().unwrap();

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let agg: Aggregations = serde_json::from_value(json!({
        "jsonagg": {
            "terms": {
                "field": "json.color",
            }
        }
    }))
    .unwrap();

    let aggregation_collector = get_collector(agg);
    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!({
            "jsonagg": {
                "buckets": [
                    {"doc_count": 3, "key": "green"},
                    {"doc_count": 2, "key": "red"},
                    {"doc_count": 1, "key": "blue"}
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0
            }
        })
    );

    let agg_req_str = r#"
    {
      "jsonagg": {
        "aggs": {
          "min_price": { "min": { "field": "json.price" } }
        },
        "terms": {
          "field": "json.color",
          "order": { "min_price": "desc" }
        }
      }
    } "#;
    let agg: Aggregations = serde_json::from_str(agg_req_str).unwrap();
    let aggregation_collector = get_collector(agg);
    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!(
            {
              "jsonagg": {
                "buckets": [
                  {
                    "key": "green",
                    "doc_count": 3,
                    "min_price": {
                      "value": 20.0
                    }
                  },
                  {
                    "key": "red",
                    "doc_count": 2,
                    "min_price": {
                      "value": 10.0
                    }
                  },
                  {
                    "key": "blue",
                    "doc_count": 1,
                    "min_price": {
                      "value": null
                    }
                  }
                ],
                "sum_other_doc_count": 0
              }
            }
        )
    );
}

#[test]
fn test_aggregation_on_json_object_mixed_types() {
    let mut schema_builder = Schema::builder();
    let json = schema_builder.add_json_field("json", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut index_writer = index.writer_for_tests().unwrap();
    // => Segment with all values numeric
    index_writer
        .add_document(doc!(json => json!({"mixed_type": 10.0})))
        .unwrap();
    index_writer.commit().unwrap();
    // => Segment with all values text
    index_writer
        .add_document(doc!(json => json!({"mixed_type": "blue"})))
        .unwrap();
    index_writer.commit().unwrap();
    // => Segment with all boolen
    index_writer
        .add_document(doc!(json => json!({"mixed_type": true})))
        .unwrap();
    index_writer.commit().unwrap();

    // => Segment with mixed values
    index_writer
        .add_document(doc!(json => json!({"mixed_type": "red"})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"mixed_type": -20.5})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"mixed_type": true})))
        .unwrap();

    index_writer.commit().unwrap();

    // All bucket types
    let agg_req_str = r#"
    {
        "termagg": {
            "terms": {
                "field": "json.mixed_type",
                "order": { "min_price": "desc" }
            },
            "aggs": {
                "min_price": { "min": { "field": "json.mixed_type" } }
            }
        },
        "rangeagg": {
            "range": {
                "field": "json.mixed_type",
                "ranges": [
                    { "to": 3.0 },
                    { "from": 19.0, "to": 20.0 },
                    { "from": 20.0 }
                ]
            },
            "aggs": {
                "average_in_range": { "avg": { "field": "json.mixed_type" } }
            }
        }
    } "#;
    let agg: Aggregations = serde_json::from_str(agg_req_str).unwrap();
    let aggregation_collector = get_collector(agg);
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!({
          "rangeagg": {
            "buckets": [
              { "average_in_range": { "value": -20.5 }, "doc_count": 1, "key": "*-3", "to": 3.0 },
              { "average_in_range": { "value": 10.0 }, "doc_count": 1, "from": 3.0, "key": "3-19", "to": 19.0 },
              { "average_in_range": { "value": null }, "doc_count": 0, "from": 19.0, "key": "19-20", "to": 20.0 },
              { "average_in_range": { "value": null }, "doc_count": 0, "from": 20.0, "key": "20-*" }
            ]
          },
          "termagg": {
            "buckets": [
              { "doc_count": 1, "key": 10.0, "min_price": { "value": 10.0 } },
              { "doc_count": 1, "key": -20.5, "min_price": { "value": -20.5 } },
              // TODO bool is also not yet handled in aggregation
              { "doc_count": 1, "key": "blue", "min_price": { "value": null } },
              { "doc_count": 1, "key": "red", "min_price": { "value": null } },
            ],
            "sum_other_doc_count": 0
          }
        }
        )
    );
}
