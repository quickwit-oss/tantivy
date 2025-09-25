use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, IndexWriter};

/// Create test index for edge case testing
fn create_edge_case_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();

    let text_field = schema_builder.add_text_field("text_field", TEXT | FAST);
    let u64_field = schema_builder.add_u64_field("u64_field", FAST);
    let f64_field = schema_builder.add_f64_field("f64_field", FAST);
    let bool_field = schema_builder.add_bool_field("bool_field", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add documents with edge case values
    let documents = vec![
        // Normal document
        doc!(
            text_field => "normal",
            u64_field => 100u64,
            f64_field => 1.5f64,
            bool_field => true
        ),
        // Document with extreme numeric values
        doc!(
            text_field => "extreme",
            u64_field => u64::MAX,
            f64_field => 1e308f64, // Close to f64::MAX but safe
            bool_field => false
        ),
        // Document with minimum values
        doc!(
            text_field => "minimum",
            u64_field => 0u64,
            f64_field => f64::MIN_POSITIVE,
            bool_field => true
        ),
        // Document with special float values
        doc!(
            text_field => "special",
            u64_field => 42u64,
            f64_field => 0.0f64,
            bool_field => false
        ),
        // Document with empty string
        doc!(
            text_field => "",
            u64_field => 1u64,
            f64_field => 1.0f64,
            bool_field => true
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
    let (index, _schema) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test various malformed query scenarios
    let test_cases = vec![
        // Invalid query type
        (
            "invalid_query_type",
            json!({
                "invalid_query_filter": {
                    "filter": { "invalid_query_type": { "field": "value" } },
                    "aggs": { "count": { "value_count": { "field": "u64_field" } } }
                }
            }),
        ),
        // Empty term query
        (
            "empty_term_query",
            json!({
                "empty_term_filter": {
                    "filter": { "term": {} },
                    "aggs": { "count": { "value_count": { "field": "u64_field" } } }
                }
            }),
        ),
        // Nonexistent field
        (
            "nonexistent_field",
            json!({
                "nonexistent_field_filter": {
                    "filter": { "term": { "nonexistent_field": "value" } },
                    "aggs": { "count": { "value_count": { "field": "u64_field" } } }
                }
            }),
        ),
    ];

    for (name, test_case) in test_cases.iter() {
        println!("Testing malformed query case: {}", name);

        let result = serde_json::from_value::<Aggregations>(test_case.clone())
            .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))
            .and_then(|aggregations| {
                let collector = AggregationCollector::from_aggs(aggregations, Default::default());
                searcher.search(&AllQuery, &collector)
            });

        match result {
            Ok(res) => println!("Case '{}' unexpectedly succeeded: {:?}", name, res),
            Err(e) => println!("Case '{}' failed as expected: {}", name, e),
        }
    }

    Ok(())
}

#[test]
fn test_filter_aggregation_empty_and_special_strings() -> tantivy::Result<()> {
    let (index, _schema) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test with empty strings and special cases
    let agg_request = json!({
        "empty_string_filter": {
            "filter": { "term": { "text_field": "" } },
            "aggs": {
                "count": { "value_count": { "field": "text_field" } }
            }
        },
        "whitespace_filter": {
            "filter": { "term": { "text_field": " " } },
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
        "Whitespace filter result: {:?}",
        agg_result.0["whitespace_filter"]
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_complex_nested_bool_queries() -> tantivy::Result<()> {
    let (index, _schema) = create_edge_case_index()?;
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
                                    { "term": { "text_field": "normal" } },
                                    { "term": { "text_field": "extreme" } }
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must_not": [
                                    { "term": { "text_field": "nonexistent" } }
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
fn test_filter_aggregation_field_type_edge_cases() -> tantivy::Result<()> {
    let (index, _schema) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test various field type scenarios
    let test_cases = vec![
        // Text field with numeric aggregation (should work with value_count)
        (
            "text_value_count",
            json!({
                "text_count": {
                    "filter": { "match_all": {} },
                    "aggs": {
                        "count_text": { "value_count": { "field": "text_field" } }
                    }
                }
            }),
        ),
        // Numeric field with terms aggregation (should work)
        (
            "numeric_terms",
            json!({
                "numeric_terms": {
                    "filter": { "match_all": {} },
                    "aggs": {
                        "terms_numeric": { "terms": { "field": "u64_field" } }
                    }
                }
            }),
        ),
        // Bool field with various aggregations
        (
            "bool_aggregations",
            json!({
                "bool_aggs": {
                    "filter": { "match_all": {} },
                    "aggs": {
                        "bool_count": { "value_count": { "field": "bool_field" } },
                        "bool_terms": { "terms": { "field": "bool_field" } }
                    }
                }
            }),
        ),
    ];

    for (name, test_case) in test_cases.iter() {
        println!("Testing field type case: {}", name);

        let result = serde_json::from_value::<Aggregations>(test_case.clone())
            .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))
            .and_then(|aggregations| {
                let collector = AggregationCollector::from_aggs(aggregations, Default::default());
                searcher.search(&AllQuery, &collector)
            });

        match result {
            Ok(res) => println!("Case '{}' succeeded: {:?}", name, res),
            Err(e) => println!("Case '{}' failed: {}", name, e),
        }
    }

    Ok(())
}

#[test]
fn test_filter_aggregation_json_serialization() -> tantivy::Result<()> {
    let (index, _schema) = create_edge_case_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test JSON serialization with various result types
    let agg_request = json!({
        "serialization_test": {
            "filter": { "term": { "text_field": "normal" } },
            "aggs": {
                "stats_with_nulls": { "stats": { "field": "u64_field" } },
                "terms_result": { "terms": { "field": "text_field" } }
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
fn test_filter_aggregation_memory_stress() -> tantivy::Result<()> {
    // Create a larger index for memory testing
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("category", TEXT | FAST);
    let u64_field = schema_builder.add_u64_field("value", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add many documents to test memory usage
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
        "memory_test": {
            "filter": { "range": { "value": { "gte": "5000" } } },
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

    println!("Memory stress test completed in {:?}", duration);

    // Verify results are reasonable
    if let Some(filter_result) = agg_result.0.get("memory_test") {
        println!("Memory test result exists: {:?}", filter_result);
    }

    // Should complete in reasonable time
    assert!(
        duration.as_secs() < 10,
        "Filter aggregation took too long: {:?}",
        duration
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_concurrent_queries() -> tantivy::Result<()> {
    let (index, _schema) = create_edge_case_index()?;
    let reader = index.reader()?;

    // Test multiple concurrent searches with filter aggregations
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let reader = reader.clone();
            std::thread::spawn(move || -> tantivy::Result<()> {
                let searcher = reader.searcher();

                let agg_request = json!({
                    format!("concurrent_filter_{}", i): {
                        "filter": { "term": { "text_field": "normal" } },
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

    println!("Concurrent queries test completed successfully");
    Ok(())
}
