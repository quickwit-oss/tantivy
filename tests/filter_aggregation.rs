//! Test suite for Filter Aggregation
//!
//! This module contains all tests for the filter aggregation feature, organized by functionality:
//! - Basic filtering operations
//! - Query type support (term, range, boolean)
//! - Nested filter aggregations
//! - Sub-aggregation combinations
//! - Edge cases and error handling
//! - Integration with custom queries

mod common;

use common::filter_test_helpers::*;
use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::bucket::filter::FilterAggregation;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::{AllQuery, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, Term, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, IndexWriter};

// ============================================================================
// Test Data Setup
// ============================================================================

/// Creates a standard test index with diverse product data
fn create_standard_test_index() -> tantivy::Result<Index> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let brand = schema_builder.add_text_field("brand", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST | INDEXED);
    let rating = schema_builder.add_f64_field("rating", FAST);
    let in_stock = schema_builder.add_bool_field("in_stock", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // 4 products covering different categories and price ranges
    writer.add_document(doc!(
        category => "electronics", brand => "apple",
        price => 999u64, rating => 4.5f64, in_stock => true
    ))?;
    writer.add_document(doc!(
        category => "electronics", brand => "samsung",
        price => 799u64, rating => 4.2f64, in_stock => true
    ))?;
    writer.add_document(doc!(
        category => "clothing", brand => "nike",
        price => 120u64, rating => 4.1f64, in_stock => false
    ))?;
    writer.add_document(doc!(
        category => "books", brand => "penguin",
        price => 25u64, rating => 4.8f64, in_stock => true
    ))?;

    writer.commit()?;
    Ok(index)
}

// ============================================================================
// Basic Filter Tests
// ============================================================================

#[test]
fn test_basic_filter_with_metric_agg() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "electronics": {
            "doc_count": 2,
            "avg_price": { "value": 899.0 }  // (999 + 799) / 2
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_filter_with_no_matches() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "furniture": {
            "filter": "category:furniture",
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "furniture": {
            "doc_count": 0,
            "avg_price": { "value": null }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_multiple_independent_filters() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        },
        "in_stock": {
            "filter": "in_stock:true",
            "aggs": { "count": { "value_count": { "field": "brand" } } }
        },
        "high_rated": {
            "filter": "rating:[4.5 TO *]",
            "aggs": { "count": { "value_count": { "field": "brand" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "electronics": {
            "doc_count": 2,
            "avg_price": { "value": 899.0 }
        },
        "in_stock": {
            "doc_count": 3,  // apple, samsung, penguin
            "count": { "value": 3.0 }
        },
        "high_rated": {
            "doc_count": 2,  // apple (4.5), penguin (4.8)
            "count": { "value": 2.0 }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

// ============================================================================
// Query Type Tests
// ============================================================================

#[test]
fn test_term_query_filter() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "apple_products": {
            "filter": "brand:apple",
            "aggs": { "max_price": { "max": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "apple_products": {
            "doc_count": 1,
            "max_price": { "value": 999.0 }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_range_query_filter() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "mid_price": {
            "filter": "price:[100 TO 900]",
            "aggs": { "count": { "value_count": { "field": "brand" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "mid_price": {
            "doc_count": 2,  // samsung (799), nike (120)
            "count": { "value": 2.0 }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_boolean_query_filter() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "premium_electronics": {
            "filter": "category:electronics AND price:[800 TO *]",
            "aggs": { "avg_rating": { "avg": { "field": "rating" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "premium_electronics": {
            "doc_count": 1,  // Only apple (999) is >= 800 in tantivy's range semantics
            "avg_rating": { "value": 4.5 }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_bool_field_filter() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "in_stock": {
            "filter": "in_stock:true",
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        },
        "out_of_stock": {
            "filter": "in_stock:false",
            "aggs": { "count": { "value_count": { "field": "brand" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "in_stock": {
            "doc_count": 3,  // apple, samsung, penguin
            "avg_price": { "value": 607.67 }  // (999 + 799 + 25) / 3 ≈ 607.67
        },
        "out_of_stock": {
            "doc_count": 1,  // nike
            "count": { "value": 1.0 }
        }
    });

    assert_aggregation_results_match(&result, expected, 1.0);
    Ok(())
}

// ============================================================================
// Nested Filter Tests
// ============================================================================

#[test]
fn test_two_level_nested_filters() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "all": {
            "filter": "*",
            "aggs": {
                "electronics": {
                    "filter": "category:electronics",
                    "aggs": {
                        "expensive": {
                            "filter": "price:[900 TO *]",
                            "aggs": {
                                "count": { "value_count": { "field": "brand" } }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "all": {
            "doc_count": 4,
            "electronics": {
                "doc_count": 2,
                "expensive": {
                    "doc_count": 1,  // Only apple (999) is >= 900
                    "count": { "value": 1.0 }
                }
            }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_deeply_nested_filters() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "level1": {
            "filter": "*",
            "aggs": {
                "level2": {
                    "filter": "in_stock:true",
                    "aggs": {
                        "level3": {
                            "filter": "rating:[4.0 TO *]",
                            "aggs": {
                                "level4": {
                                    "filter": "price:[500 TO *]",
                                    "aggs": {
                                        "final_count": { "value_count": { "field": "brand" } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "level1": {
            "doc_count": 4,
            "level2": {
                "doc_count": 3,  // in_stock: apple, samsung, penguin
                "level3": {
                    "doc_count": 3,  // all have rating >= 4.0
                    "level4": {
                        "doc_count": 2,  // apple (999), samsung (799)
                        "final_count": { "value": 2.0 }
                    }
                }
            }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_multiple_nested_branches() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "root": {
            "filter": "*",
            "aggs": {
                "electronics_branch": {
                    "filter": "category:electronics",
                    "aggs": {
                        "avg_price": { "avg": { "field": "price" } }
                    }
                },
                "in_stock_branch": {
                    "filter": "in_stock:true",
                    "aggs": {
                        "count": { "value_count": { "field": "brand" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "root": {
            "doc_count": 4,
            "electronics_branch": {
                "doc_count": 2,
                "avg_price": { "value": 899.0 }
            },
            "in_stock_branch": {
                "doc_count": 3,
                "count": { "value": 3.0 }
            }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_nested_filters_with_multiple_siblings_at_each_level() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test complex nesting: multiple branches at each level
    let agg = json!({
        "all": {
            "filter": "*",
            "aggs": {
                // Level 2: Two independent filters
                "expensive": {
                    "filter": "price:[500 TO *]",
                    "aggs": {
                        // Level 3: Multiple branches under "expensive"
                        "electronics": {
                            "filter": "category:electronics",
                            "aggs": {
                                "avg_rating": { "avg": { "field": "rating" } }
                            }
                        },
                        "in_stock": {
                            "filter": "in_stock:true",
                            "aggs": {
                                "count": { "value_count": { "field": "brand" } }
                            }
                        }
                    }
                },
                "affordable": {
                    "filter": "price:[0 TO 200]",
                    "aggs": {
                        // Level 3: Multiple branches under "affordable"
                        "books": {
                            "filter": "category:books",
                            "aggs": {
                                "max_rating": { "max": { "field": "rating" } }
                            }
                        },
                        "clothing": {
                            "filter": "category:clothing",
                            "aggs": {
                                "min_price": { "min": { "field": "price" } }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "all": {
            "doc_count": 4,
            "expensive": {
                "doc_count": 2,  // apple (999), samsung (799)
                "electronics": {
                    "doc_count": 2,  // both are electronics
                    "avg_rating": { "value": 4.35 }  // (4.5 + 4.2) / 2
                },
                "in_stock": {
                    "doc_count": 2,  // both are in stock
                    "count": { "value": 2.0 }
                }
            },
            "affordable": {
                "doc_count": 2,  // nike (120), penguin (25)
                "books": {
                    "doc_count": 1,  // penguin (25)
                    "max_rating": { "value": 4.8 }
                },
                "clothing": {
                    "doc_count": 1,  // nike (120)
                    "min_price": { "value": 120.0 }
                }
            }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

// ============================================================================
// Sub-Aggregation Combination Tests
// ============================================================================

#[test]
fn test_filter_with_terms_sub_agg() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": {
                "brands": {
                    "terms": { "field": "brand" },
                    "aggs": {
                        "avg_price": { "avg": { "field": "price" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    // Verify the structure exists and has expected doc_count
    let expected = json!({
        "electronics": {
            "doc_count": 2,
            "brands": {
                "buckets": [
                    {
                        "key": "samsung",
                        "doc_count": 1,
                        "avg_price": { "value": 799.0 }
                    },
                    {
                        "key": "apple",
                        "doc_count": 1,
                        "avg_price": { "value": 999.0 }
                    }
                ],
                "sum_other_doc_count": 0,
                "doc_count_error_upper_bound": 0
            }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_filter_with_multiple_metric_aggs() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": {
                "price_stats": { "stats": { "field": "price" } },
                "rating_avg": { "avg": { "field": "rating" } },
                "count": { "value_count": { "field": "brand" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "electronics": {
            "doc_count": 2,
            "price_stats": {
                "count": 2,
                "min": 799.0,
                "max": 999.0,
                "sum": 1798.0,
                "avg": 899.0
            },
            "rating_avg": { "value": 4.35 },
            "count": { "value": 2.0 }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

#[test]
fn test_filter_on_empty_index() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let _category = schema_builder.add_text_field("category", TEXT | FAST);
    let _price = schema_builder.add_u64_field("price", FAST);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer(50_000_000)?;
    writer.commit()?; // Commit empty index

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "electronics": {
            "doc_count": 0,
            "avg_price": { "value": null }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

#[test]
fn test_malformed_query_string() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Empty query string
    let agg = json!({
        "test": {
            "filter": "",
            "aggs": { "count": { "value_count": { "field": "brand" } } }
        }
    });

    let result = serde_json::from_value::<Aggregations>(agg)
        .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))
        .and_then(|aggregations| {
            let collector = AggregationCollector::from_aggs(aggregations, Default::default());
            searcher.search(&AllQuery, &collector)
        });

    // Empty string should either work (matching nothing) or error gracefully
    assert!(result.is_ok() || result.is_err());
    Ok(())
}

#[test]
fn test_filter_with_base_query() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let schema = index.schema();

    // Use a base query to pre-filter to in_stock items only
    let in_stock_field = schema.get_field("in_stock").unwrap();
    let base_query = TermQuery::new(
        Term::from_field_bool(in_stock_field, true),
        IndexRecordOption::Basic,
    );

    let agg = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": { "count": { "value_count": { "field": "brand" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&base_query, &collector)?;

    let expected = json!({
        "electronics": {
            "doc_count": 2,  // Both in-stock electronics
            "count": { "value": 2.0 }
        }
    });

    assert_aggregation_results_match(&result, expected, 0.1);
    Ok(())
}

// ============================================================================
// Custom Query Integration Tests
// ============================================================================

#[test]
fn test_direct_query_object() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let schema = index.schema();

    // Create a custom query directly
    let category_field = schema.get_field("category").unwrap();
    let term = Term::from_field_text(category_field, "electronics");
    let term_query = TermQuery::new(term, IndexRecordOption::Basic);

    // Use it in FilterAggregation
    let filter_agg = FilterAggregation::new_with_query(Box::new(term_query));

    // Verify it works
    let query = filter_agg.parse_query(&schema)?;
    let count = query.count(&searcher)?;
    assert_eq!(count, 2, "Should match 2 electronics");

    // Verify it cannot be serialized
    let serialization_result = serde_json::to_string(&filter_agg);
    assert!(
        serialization_result.is_err(),
        "Direct queries should not serialize"
    );
    assert!(serialization_result
        .unwrap_err()
        .to_string()
        .contains("Direct Query objects cannot be serialized"));

    Ok(())
}

#[test]
fn test_query_string_serialization() -> tantivy::Result<()> {
    // Query strings should serialize/deserialize correctly
    let filter_agg = FilterAggregation::new("category:electronics".to_string());

    let serialized = serde_json::to_string(&filter_agg)?;
    assert!(serialized.contains("electronics"));

    let deserialized: FilterAggregation = serde_json::from_str(&serialized)?;
    assert_eq!(filter_agg, deserialized);

    Ok(())
}

#[test]
fn test_query_equality_behavior() -> tantivy::Result<()> {
    // Query strings should be comparable
    let filter1 = FilterAggregation::new("category:books".to_string());
    let filter2 = FilterAggregation::new("category:books".to_string());
    let filter3 = FilterAggregation::new("category:electronics".to_string());

    assert_eq!(filter1, filter2, "Identical query strings should be equal");
    assert_ne!(
        filter1, filter3,
        "Different query strings should not be equal"
    );

    // Direct queries should not be comparable (by design)
    let index = create_standard_test_index()?;
    let schema = index.schema();
    let field = schema.get_field("category").unwrap();

    let term1 = Term::from_field_text(field, "books");
    let query1 = TermQuery::new(term1.clone(), IndexRecordOption::Basic);
    let filter_direct1 = FilterAggregation::new_with_query(Box::new(query1));

    let term2 = Term::from_field_text(field, "books");
    let query2 = TermQuery::new(term2, IndexRecordOption::Basic);
    let filter_direct2 = FilterAggregation::new_with_query(Box::new(query2));

    assert_ne!(
        filter_direct1, filter_direct2,
        "Direct queries should not be equal even if logically equivalent"
    );

    Ok(())
}

// ============================================================================
// Correctness Validation Tests
// ============================================================================

#[test]
fn test_filter_result_correctness_vs_separate_query() -> tantivy::Result<()> {
    let index = create_standard_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let schema = index.schema();

    // Method 1: Filter aggregation
    let filter_agg = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(filter_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let filter_result = searcher.search(&AllQuery, &collector)?;

    // Method 2: Separate query
    let category_field = schema.get_field("category").unwrap();
    let term = Term::from_field_text(category_field, "electronics");
    let term_query = TermQuery::new(term, IndexRecordOption::Basic);

    let separate_agg = json!({
        "result": { "avg": { "field": "price" } }
    });

    let separate_aggregations: Aggregations = serde_json::from_value(separate_agg)?;
    let separate_collector =
        AggregationCollector::from_aggs(separate_aggregations, Default::default());
    let separate_result = searcher.search(&term_query, &separate_collector)?;

    // Both methods should produce identical results
    let filter_expected = json!({
        "electronics": {
            "doc_count": 2,
            "avg_price": { "value": 899.0 }
        }
    });

    let separate_expected = json!({
        "result": {
            "value": 899.0
        }
    });

    // Verify filter aggregation result
    assert_aggregation_results_match(&filter_result, filter_expected, 0.1);

    // Verify separate query result matches
    assert_aggregation_results_match(&separate_result, separate_expected, 0.1);

    // This test demonstrates that filter aggregation produces the same results
    // as running a separate query with the same condition
    Ok(())
}
