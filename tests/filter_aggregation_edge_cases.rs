use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, IndexWriter};

fn create_edge_case_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text_field", TEXT | FAST);
    let u64_field = schema_builder.add_u64_field("u64_field", FAST | INDEXED);
    let f64_field = schema_builder.add_f64_field("f64_field", FAST | INDEXED);
    let bool_field = schema_builder.add_bool_field("bool_field", FAST | INDEXED);
    let date_field = schema_builder.add_date_field("date_field", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // Edge case values
    let documents = vec![
        doc!(
            text_field => "normal",
            u64_field => 100u64,
            f64_field => 1.5f64,
            bool_field => true,
            date_field => tantivy::DateTime::from_timestamp_secs(1640995200)
        ),
        doc!(
            text_field => "extreme",
            u64_field => u64::MAX,
            f64_field => f64::MAX,
            bool_field => false,
            date_field => tantivy::DateTime::from_timestamp_secs(0)
        ),
        doc!(
            text_field => "minimum",
            u64_field => 0u64,
            f64_field => f64::MIN_POSITIVE,
            bool_field => true,
            date_field => tantivy::DateTime::from_timestamp_secs(1000000)
        ),
        doc!(
            text_field => "",
            u64_field => 1u64,
            f64_field => 0.0f64,
            bool_field => true,
            date_field => tantivy::DateTime::from_timestamp_secs(1640995200)
        ),
    ];

    for doc in documents {
        writer.add_document(doc)?;
    }
    writer.commit()?;
    Ok((index, schema))
}

#[test]
fn test_extreme_numeric_values() -> tantivy::Result<()> {
    let (index, _) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "max_u64": {
            "filter": { "query_string": format!("u64_field:{}", u64::MAX) },
            "aggs": { "stats": { "stats": { "field": "u64_field" } } }
        },
        "zero_u64": {
            "filter": { "query_string": "u64_field:0" },
            "aggs": { "avg": { "avg": { "field": "f64_field" } } }
        },
        "max_f64": {
            "filter": { "query_string": "f64_field:[1e308 TO *]" },
            "aggs": { "count": { "value_count": { "field": "f64_field" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("max_u64"));
    assert!(result.0.contains_key("zero_u64"));
    assert!(result.0.contains_key("max_f64"));
    Ok(())
}

#[test]
fn test_empty_and_special_strings() -> tantivy::Result<()> {
    let (index, _) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "empty_string": {
            "filter": { "query_string": "text_field:\"\"" },
            "aggs": { "count": { "value_count": { "field": "text_field" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("empty_string"));
    Ok(())
}

#[test]
fn test_date_edge_cases() -> tantivy::Result<()> {
    let (index, _) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "epoch_time": {
            "filter": { "query_string": "date_field:\"1970-01-01T00:00:00Z\"" },
            "aggs": { "count": { "value_count": { "field": "date_field" } } }
        },
        "far_future": {
            "filter": { "query_string": "date_field:[2038-01-19T03:14:07Z TO *]" },
            "aggs": { "avg": { "avg": { "field": "u64_field" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("epoch_time"));
    assert!(result.0.contains_key("far_future"));
    Ok(())
}

#[test]
fn test_malformed_queries() -> tantivy::Result<()> {
    let (index, _) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test invalid query types
    let invalid_cases = vec![
        json!({
            "invalid": {
                "filter": { "invalid_query": { "field": "value" } },
                "aggs": { "count": { "value_count": { "field": "u64_field" } } }
            }
        }),
        json!({
            "empty": {
                "filter": { "query_string": "" },
                "aggs": { "count": { "value_count": { "field": "u64_field" } } }
            }
        }),
        json!({
            "nonexistent": {
                "filter": { "query_string": "nonexistent_field:value" },
                "aggs": { "count": { "value_count": { "field": "u64_field" } } }
            }
        }),
    ];

    for test_case in invalid_cases {
        let result = serde_json::from_value::<Aggregations>(test_case)
            .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))
            .and_then(|agg| {
                let collector = AggregationCollector::from_aggs(agg, Default::default());
                searcher.search(&AllQuery, &collector)
            });

        // These should either fail or handle gracefully
        match result {
            Ok(_) => println!("Query handled gracefully"),
            Err(e) => println!("Query failed as expected: {}", e),
        }
    }

    Ok(())
}

#[test]
fn test_field_type_compatibility() -> tantivy::Result<()> {
    let (index, _) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "field_types": {
            "filter": { "query_string": "*" },
            "aggs": {
                "text_count": { "value_count": { "field": "text_field" } },
                "numeric_terms": { "terms": { "field": "u64_field" } },
                "bool_aggs": {
                    "terms": { "field": "bool_field" },
                    "aggs": {
                        "bool_count": { "value_count": { "field": "bool_field" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("field_types"));
    Ok(())
}

#[test]
fn test_deeply_nested_bool_queries() -> tantivy::Result<()> {
    let (index, _) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "complex_bool": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    "text_field:normal",
                                    "text_field:extreme"
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must_not": ["text_field:nonexistent"]
                            }
                        }
                    ]
                }
            },
            "aggs": { "stats": { "stats": { "field": "u64_field" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("complex_bool"));
    Ok(())
}

#[test]
fn test_concurrent_access() -> tantivy::Result<()> {
    let (index, _) = create_edge_case_index()?;
    let reader = index.reader()?;

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let reader = reader.clone();
            std::thread::spawn(move || -> tantivy::Result<()> {
                let searcher = reader.searcher();
                let agg = json!({
                    format!("concurrent_{}", i): {
                        "filter": { "query_string": "text_field:normal" },
                        "aggs": { "avg": { "avg": { "field": "u64_field" } } }
                    }
                });

                let aggregations: Aggregations = serde_json::from_value(agg)?;
                let collector = AggregationCollector::from_aggs(aggregations, Default::default());
                let _result = searcher.search(&AllQuery, &collector)?;
                Ok(())
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap()?;
    }

    Ok(())
}
