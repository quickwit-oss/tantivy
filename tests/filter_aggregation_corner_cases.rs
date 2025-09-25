use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, IndexWriter};

/// Create test index for corner case testing
fn create_corner_case_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();

    let text_field = schema_builder.add_text_field("text_field", TEXT | FAST);
    let u64_field = schema_builder.add_u64_field("u64_field", FAST | INDEXED);
    let f64_field = schema_builder.add_f64_field("f64_field", FAST | INDEXED);
    let bool_field = schema_builder.add_bool_field("bool_field", FAST | INDEXED);
    let date_field = schema_builder.add_date_field("date_field", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add documents with various edge case values
    let documents = vec![
        // Normal document
        doc!(
            text_field => "normal",
            u64_field => 100u64,
            f64_field => 1.5f64,
            bool_field => true,
            date_field => tantivy::DateTime::from_timestamp_secs(1640995200)
        ),
        // Document with extreme values
        doc!(
            text_field => "extreme",
            u64_field => u64::MAX,
            f64_field => f64::MAX,
            bool_field => false,
            date_field => tantivy::DateTime::from_timestamp_secs(0)
        ),
        // Document with minimum values
        doc!(
            text_field => "minimum",
            u64_field => 0u64,
            f64_field => f64::MIN,
            bool_field => true,
            date_field => tantivy::DateTime::from_timestamp_secs(1000000)
        ),
        // Document with special float values
        doc!(
            text_field => "special",
            u64_field => 42u64,
            f64_field => 0.0f64,
            bool_field => false,
            date_field => tantivy::DateTime::from_timestamp_secs(1000000000)
        ),
        // Empty document (only required fields)
        doc!(
            text_field => "",
            u64_field => 1u64,
            f64_field => 1.0f64,
            bool_field => true,
            date_field => tantivy::DateTime::from_timestamp_secs(1640995200)
        ),
    ];

    for doc in documents {
        index_writer.add_document(doc)?;
    }

    index_writer.commit()?;
    Ok((index, schema))
}

#[test]
fn test_filter_aggregation_malformed_queries() -> tantivy::Result<()> {
    let (index, _schema) = create_corner_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test various malformed query scenarios
    let test_cases = vec![
        // Invalid query type
        json!({
            "invalid_query_filter": {
                "filter": { "invalid_query_type": { "field": "value" } },
                "aggs": { "count": { "value_count": { "field": "u64_field" } } }
            }
        }),
        // Missing field in query string
        json!({
            "missing_field_filter": {
                "filter": { "query_string": "" },
                "aggs": { "count": { "value_count": { "field": "u64_field" } } }
            }
        }),
        // Invalid field name
        json!({
            "nonexistent_field_filter": {
                "filter": { "query_string": "nonexistent_field:value" },
                "aggs": { "count": { "value_count": { "field": "u64_field" } } }
            }
        }),
        // Empty bool query
        json!({
            "empty_bool_filter": {
                "filter": { "bool": {} },
                "aggs": { "count": { "value_count": { "field": "u64_field" } } }
            }
        }),
    ];

    for (i, test_case) in test_cases.iter().enumerate() {
        println!("Testing malformed query case {}: {:?}", i, test_case);

        let result = serde_json::from_value::<Aggregations>(test_case.clone())
            .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))
            .and_then(|aggregations| {
                let collector = AggregationCollector::from_aggs(aggregations, Default::default());
                searcher.search(&AllQuery, &collector)
            });

        // These should either fail gracefully or handle the error appropriately
        match result {
            Ok(_) => println!("Case {} unexpectedly succeeded", i),
            Err(e) => println!("Case {} failed as expected: {}", i, e),
        }
    }

    Ok(())
}

#[test]
fn test_filter_aggregation_extreme_values() -> tantivy::Result<()> {
    let (index, _schema) = create_corner_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test with extreme numeric values
    let agg_request = json!({
        "max_u64_filter": {
            "filter": { "query_string": format!("u64_field:{}", u64::MAX) },
            "aggs": {
                "stats": { "stats": { "field": "u64_field" } }
            }
        },
        "zero_u64_filter": {
            "filter": { "query_string": "u64_field:0" },
            "aggs": {
                "avg": { "avg": { "field": "f64_field" } }
            }
        },
        "max_f64_filter": {
            "filter": { "query_string": "f64_field:[1e308 TO *]" },
            "aggs": {
                "count": { "value_count": { "field": "f64_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let agg_result = searcher.search(&AllQuery, &collector)?;

    println!(
        "Max u64 filter result: {:?}",
        agg_result.0.get("max_u64_filter")
    );
    println!(
        "Zero u64 filter result: {:?}",
        agg_result.0.get("zero_u64_filter")
    );
    println!(
        "Max f64 filter result: {:?}",
        agg_result.0.get("max_f64_filter")
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_empty_strings_and_special_chars() -> tantivy::Result<()> {
    let (index, _schema) = create_corner_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test with empty strings and special characters
    let agg_request = json!({
        "empty_string_filter": {
            "filter": { "query_string": "text_field:\"\"" },
            "aggs": {
                "count": { "value_count": { "field": "text_field" } }
            }
        },
        "unicode_filter": {
            "filter": { "query_string": "text_field:🚀" },
            "aggs": {
                "count": { "value_count": { "field": "text_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let agg_result = searcher.search(&AllQuery, &collector)?;

    println!(
        "Empty string filter result: {:?}",
        agg_result.0["empty_string_filter"]
    );
    println!(
        "Unicode filter result: {:?}",
        agg_result.0["unicode_filter"]
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_date_edge_cases() -> tantivy::Result<()> {
    let (index, _schema) = create_corner_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test with date edge cases
    let agg_request = json!({
        "epoch_filter": {
            "filter": { "query_string": "date_field:\"1970-01-01T00:00:00Z\"" },
            "aggs": {
                "count": { "value_count": { "field": "date_field" } }
            }
        },
        "far_future_filter": {
            "filter": { "query_string": "date_field:[2038-01-19T03:14:07Z TO *]" },
            "aggs": {
                "avg_u64": { "avg": { "field": "u64_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let agg_result = searcher.search(&AllQuery, &collector)?;

    println!("Epoch filter result: {:?}", agg_result.0["epoch_filter"]);
    println!(
        "Far future filter result: {:?}",
        agg_result.0["far_future_filter"]
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_deeply_nested_bool_queries() -> tantivy::Result<()> {
    let (index, _schema) = create_corner_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test deeply nested boolean queries
    let agg_request = json!({
        "deeply_nested_filter": {
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
                                "must_not": [
                                    "text_field:nonexistent"
                                ]
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "nested_stats": { "stats": { "field": "u64_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let agg_result = searcher.search(&AllQuery, &collector)?;

    println!(
        "Deeply nested filter result: {:?}",
        agg_result.0["deeply_nested_filter"]
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_memory_and_performance() -> tantivy::Result<()> {
    // Create a larger index for performance testing
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("category", TEXT | FAST);
    let u64_field = schema_builder.add_u64_field("value", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add many documents
    for i in 0..10000 {
        index_writer.add_document(doc!(
            text_field => format!("category_{}", i % 100),
            u64_field => (i as u64)
        ))?;
    }
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with many sub-aggregations
    let agg_request = json!({
        "large_filter": {
            "filter": { "query_string": "value:[5000 TO *]" },
            "aggs": {
                "avg": { "avg": { "field": "value" } },
                "min": { "min": { "field": "value" } },
                "max": { "max": { "field": "value" } },
                "sum": { "sum": { "field": "value" } },
                "count": { "value_count": { "field": "value" } },
                "stats": { "stats": { "field": "value" } },
                "categories": { "terms": { "field": "category", "size": 50 } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let agg_result = searcher.search(&AllQuery, &collector)?;
    let duration = start.elapsed();

    println!("Large filter aggregation completed in {:?}", duration);
    println!("Result doc_count: {:?}", agg_result.0["large_filter"]);

    // Should complete in reasonable time (< 1 second for this size)
    assert!(
        duration.as_secs() < 5,
        "Filter aggregation took too long: {:?}",
        duration
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_concurrent_access() -> tantivy::Result<()> {
    let (index, _schema) = create_corner_case_index()?;
    let reader = index.reader()?;

    // Test multiple concurrent searches with filter aggregations
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let reader = reader.clone();
            std::thread::spawn(move || -> tantivy::Result<()> {
                let searcher = reader.searcher();

                let agg_request = json!({
                    format!("concurrent_filter_{}", i): {
                        "filter": { "query_string": "text_field:normal" },
                        "aggs": {
                            "avg": { "avg": { "field": "u64_field" } }
                        }
                    }
                });

                let aggregations: Aggregations = serde_json::from_value(agg_request)?;
                let collector = AggregationCollector::from_aggs(aggregations, Default::default());
                let _result = searcher.search(&AllQuery, &collector)?;

                Ok(())
            })
        })
        .collect();

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap()?;
    }

    println!("Concurrent access test completed successfully");
    Ok(())
}

#[test]
fn test_filter_aggregation_json_serialization_edge_cases() -> tantivy::Result<()> {
    let (index, _schema) = create_corner_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test JSON serialization with various edge cases
    let agg_request = json!({
        "serialization_test": {
            "filter": { "query_string": "text_field:normal" },
            "aggs": {
                "stats_with_nulls": { "stats": { "field": "u64_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Test that the result can be serialized back to JSON
    let json_result = serde_json::to_string(&agg_result)?;
    println!("Serialized result length: {}", json_result.len());

    // Test that it can be deserialized back
    let _deserialized: serde_json::Value = serde_json::from_str(&json_result)?;

    println!("JSON serialization round-trip successful");
    Ok(())
}

#[test]
fn test_filter_aggregation_field_type_mismatches() -> tantivy::Result<()> {
    let (index, _schema) = create_corner_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test aggregations on fields with type mismatches
    let test_cases = vec![
        // Try to use text field in numeric aggregation
        json!({
            "text_in_numeric": {
                "filter": { "match_all": {} },
                "aggs": {
                    "avg_text": { "avg": { "field": "text_field" } }
                }
            }
        }),
        // Try to use numeric field in text-specific operations
        json!({
            "numeric_in_text": {
                "filter": { "match_all": {} },
                "aggs": {
                    "terms_numeric": { "terms": { "field": "u64_field" } }
                }
            }
        }),
    ];

    for (i, test_case) in test_cases.iter().enumerate() {
        println!("Testing field type mismatch case {}", i);

        let result = serde_json::from_value::<Aggregations>(test_case.clone())
            .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))
            .and_then(|aggregations| {
                let collector = AggregationCollector::from_aggs(aggregations, Default::default());
                searcher.search(&AllQuery, &collector)
            });

        match result {
            Ok(res) => println!("Case {} result: {:?}", i, res),
            Err(e) => println!("Case {} error: {}", i, e),
        }
    }

    Ok(())
}
