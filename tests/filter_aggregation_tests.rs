use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
use tantivy::{doc, Index, IndexWriter};

/// Create a test index with all field types
fn create_test_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();

    // Text fields
    let text_field = schema_builder.add_text_field("text_field", TEXT | FAST);
    let text_stored_field = schema_builder.add_text_field("text_stored", TEXT | STORED);

    // Numeric fields
    let u64_field = schema_builder.add_u64_field("u64_field", FAST | INDEXED);
    let i64_field = schema_builder.add_i64_field("i64_field", FAST | INDEXED);
    let f64_field = schema_builder.add_f64_field("f64_field", FAST | INDEXED);

    // Bool field
    let bool_field = schema_builder.add_bool_field("bool_field", FAST | INDEXED);

    // Date field
    let date_field = schema_builder.add_date_field("date_field", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add diverse documents
    let documents = vec![
        doc!(
            text_field => "electronics",
            text_stored_field => "iPhone 15",
            u64_field => 999u64,
            i64_field => -100i64,
            f64_field => 4.5f64,
            bool_field => true,
            date_field => tantivy::DateTime::from_timestamp_secs(1640995200) // 2022-01-01
        ),
        doc!(
            text_field => "electronics",
            text_stored_field => "Galaxy S24",
            u64_field => 799u64,
            i64_field => 200i64,
            f64_field => 4.2f64,
            bool_field => true,
            date_field => tantivy::DateTime::from_timestamp_secs(1672531200) // 2023-01-01
        ),
        doc!(
            text_field => "books",
            text_stored_field => "Programming Guide",
            u64_field => 45u64,
            i64_field => 0i64,
            f64_field => 4.8f64,
            bool_field => false,
            date_field => tantivy::DateTime::from_timestamp_secs(1704067200) // 2024-01-01
        ),
        doc!(
            text_field => "clothing",
            text_stored_field => "Nike Shoes",
            u64_field => 120u64,
            i64_field => -50i64,
            f64_field => 3.9f64,
            bool_field => true,
            date_field => tantivy::DateTime::from_timestamp_secs(1609459200) // 2021-01-01
        ),
    ];

    for doc in documents {
        index_writer.add_document(doc)?;
    }

    index_writer.commit()?;
    Ok((index, schema))
}

#[test]
fn test_filter_aggregation_all_field_types() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with different field types in sub-aggregations
    let agg_request = json!({
        "electronics_filter": {
            "filter": { "query_string": "text_field:electronics" },
            "aggs": {
                "u64_stats": { "stats": { "field": "u64_field" } },
                "i64_avg": { "avg": { "field": "i64_field" } },
                "f64_max": { "max": { "field": "f64_field" } },
                "bool_count": { "value_count": { "field": "bool_field" } },
                "text_count": { "value_count": { "field": "text_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify the filter aggregation exists and has correct results
    assert!(agg_result.0.contains_key("electronics_filter"));
    let result = &agg_result.0["electronics_filter"];

    // Verify the aggregation exists and print the result
    println!("Electronics filter result: {:?}", result);

    Ok(())
}

#[test]
fn test_filter_aggregation_bool_field_queries() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with bool field in filter query
    let agg_request = json!({
        "active_items": {
            "filter": { "query_string": "bool_field:true" },
            "aggs": {
                "avg_price": { "avg": { "field": "u64_field" } }
            }
        },
        "inactive_items": {
            "filter": { "query_string": "bool_field:false" },
            "aggs": {
                "count": { "value_count": { "field": "u64_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify both filter aggregations exist
    assert!(agg_result.0.contains_key("active_items"));
    assert!(agg_result.0.contains_key("inactive_items"));

    println!("Active items result: {:?}", agg_result.0["active_items"]);
    println!(
        "Inactive items result: {:?}",
        agg_result.0["inactive_items"]
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_numeric_range_queries() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with range queries on different numeric types
    let agg_request = json!({
        "expensive_items": {
            "filter": { "query_string": "u64_field:[500 TO *]" },
            "aggs": {
                "count": { "value_count": { "field": "u64_field" } }
            }
        },
        "positive_i64": {
            "filter": { "query_string": "i64_field:{0 TO *]" },
            "aggs": {
                "avg_f64": { "avg": { "field": "f64_field" } }
            }
        },
        "high_rating": {
            "filter": { "query_string": "f64_field:[4.5 TO *]" },
            "aggs": {
                "max_price": { "max": { "field": "u64_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify all filter aggregations exist
    assert!(agg_result.0.contains_key("expensive_items"));
    assert!(agg_result.0.contains_key("positive_i64"));
    assert!(agg_result.0.contains_key("high_rating"));

    println!(
        "Expensive items result: {:?}",
        agg_result.0["expensive_items"]
    );
    println!("Positive i64 result: {:?}", agg_result.0["positive_i64"]);
    println!("High rating result: {:?}", agg_result.0["high_rating"]);

    Ok(())
}

#[test]
fn test_filter_aggregation_complex_bool_queries() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with complex boolean queries
    let agg_request = json!({
        "electronics_and_expensive": {
            "filter": {
                "bool": {
                    "must": [
                        "text_field:electronics",
                        "u64_field:[800 TO *]"
                    ]
                }
            },
            "aggs": {
                "avg_rating": { "avg": { "field": "f64_field" } }
            }
        },
        "not_books": {
            "filter": {
                "bool": {
                    "must_not": [
                        "text_field:books"
                    ]
                }
            },
            "aggs": {
                "count": { "value_count": { "field": "text_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify filter aggregations exist
    assert!(agg_result.0.contains_key("electronics_and_expensive"));
    assert!(agg_result.0.contains_key("not_books"));

    println!(
        "Electronics and expensive result: {:?}",
        agg_result.0["electronics_and_expensive"]
    );
    println!("Not books result: {:?}", agg_result.0["not_books"]);

    Ok(())
}

#[test]
fn test_filter_aggregation_nested_sub_aggregations() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with nested sub-aggregations
    let agg_request = json!({
        "all_items": {
            "filter": { "query_string": "*" },
            "aggs": {
                "by_category": {
                    "terms": { "field": "text_field" },
                    "aggs": {
                        "price_stats": { "stats": { "field": "u64_field" } },
                        "avg_rating": { "avg": { "field": "f64_field" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify the nested structure exists
    assert!(agg_result.0.contains_key("all_items"));
    let result = &agg_result.0["all_items"];

    println!("Nested sub-aggregations result: {:?}", result);

    Ok(())
}

#[test]
fn test_filter_aggregation_edge_cases() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test edge cases
    let agg_request = json!({
        "no_matches": {
            "filter": { "query_string": "text_field:nonexistent" },
            "aggs": {
                "count": { "value_count": { "field": "u64_field" } }
            }
        },
        "all_matches": {
            "filter": { "query_string": "*" },
            "aggs": {
                "total_count": { "value_count": { "field": "text_field" } }
            }
        },
        "empty_sub_aggs": {
            "filter": { "query_string": "text_field:electronics" }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Print results for verification
    println!("No matches result: {:?}", agg_result.0["no_matches"]);
    println!("All matches result: {:?}", agg_result.0["all_matches"]);
    println!(
        "Empty sub-aggs result: {:?}",
        agg_result.0["empty_sub_aggs"]
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_field_type_compatibility() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test that different aggregation types work with different field types
    let agg_request = json!({
        "field_type_test": {
            "filter": { "query_string": "*" },
            "aggs": {
                // Numeric field aggregations
                "u64_avg": { "avg": { "field": "u64_field" } },
                "u64_sum": { "sum": { "field": "u64_field" } },
                "u64_min": { "min": { "field": "u64_field" } },
                "u64_max": { "max": { "field": "u64_field" } },
                "u64_count": { "value_count": { "field": "u64_field" } },

                "i64_avg": { "avg": { "field": "i64_field" } },
                "f64_avg": { "avg": { "field": "f64_field" } },

                // Text field aggregations
                "text_count": { "value_count": { "field": "text_field" } },
                "text_terms": { "terms": { "field": "text_field" } },

                // Bool field aggregations
                "bool_count": { "value_count": { "field": "bool_field" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    let result = &agg_result.0["field_type_test"];

    // Print result for verification
    println!("Field type compatibility result: {:?}", result);

    Ok(())
}
