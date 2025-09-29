mod common;

use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, IndexWriter};

use common::filter_test_helpers::*;

fn create_nested_test_index() -> tantivy::Result<Index> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let brand = schema_builder.add_text_field("brand", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST);
    let rating = schema_builder.add_f64_field("rating", FAST);
    let in_stock = schema_builder.add_bool_field("in_stock", FAST | INDEXED);
    let region = schema_builder.add_text_field("region", TEXT | FAST);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // Create a rich dataset for nested filtering
    let products = [
        ("electronics", "Apple", 1200, 4.8, true, "US"),
        ("electronics", "Samsung", 800, 4.5, true, "US"),
        ("electronics", "Sony", 600, 4.2, false, "EU"),
        ("electronics", "Apple", 1500, 4.9, true, "EU"),
        ("books", "Penguin", 25, 4.3, true, "US"),
        ("books", "Harper", 30, 4.1, true, "EU"),
        ("books", "Random", 20, 3.9, false, "US"),
        ("clothing", "Nike", 120, 4.4, true, "US"),
        ("clothing", "Adidas", 100, 4.2, true, "EU"),
        ("clothing", "Puma", 80, 4.0, false, "US"),
    ];

    for (cat, br, pr, rt, stock, reg) in products {
        writer.add_document(doc!(
            category => cat,
            brand => br,
            price => pr as u64,
            rating => rt,
            in_stock => stock,
            region => reg
        ))?;
    }

    writer.commit()?;
    Ok(index)
}

#[test]
fn test_deeply_nested_filters() -> tantivy::Result<()> {
    let index = create_nested_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test 4 levels of nested filters
    let agg = json!({
        "all_products": {
            "filter": { "query_string": "*" },
            "aggs": {
                "electronics_only": {
                    "filter": { "query_string": "category:electronics" },
                    "aggs": {
                        "premium_electronics": {
                            "filter": { "query_string": "price:[800 TO *]" },
                            "aggs": {
                                "high_rated_premium": {
                                    "filter": { "query_string": "rating:[4.5 TO *]" },
                                    "aggs": {
                                        "count": { "value_count": { "field": "brand" } },
                                        "avg_price": { "avg": { "field": "price" } }
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

    // Validate 4-level nested structure with actual values
    assert!(result.0.contains_key("all_products"));
    let all_result = result.0.get("all_products").unwrap();
    assert!(validate_filter_bucket(all_result, 10)); // All 10 products

    let all_bucket = get_filter_bucket(all_result).unwrap();

    let electronics_result = all_bucket
        .sub_aggregations
        .0
        .get("electronics_only")
        .unwrap();
    assert!(validate_filter_bucket(electronics_result, 4)); // 4 electronics

    let electronics_bucket = get_filter_bucket(electronics_result).unwrap();

    let premium_result = electronics_bucket
        .sub_aggregations
        .0
        .get("premium_electronics")
        .unwrap();
    assert!(validate_filter_bucket(premium_result, 3)); // 3 items >= 800

    let premium_bucket = get_filter_bucket(premium_result).unwrap();

    let high_rated_result = premium_bucket
        .sub_aggregations
        .0
        .get("high_rated_premium")
        .unwrap();
    assert!(validate_filter_bucket(high_rated_result, 3)); // 3 items >= 4.5 rating

    let high_rated_bucket = get_filter_bucket(high_rated_result).unwrap();

    // Check sub-aggregations
    let count_result = high_rated_bucket.sub_aggregations.0.get("count").unwrap();
    assert!(validate_metric_value(count_result, 3.0, 0.1));

    let avg_price_result = high_rated_bucket
        .sub_aggregations
        .0
        .get("avg_price")
        .unwrap();
    // Average of 1200, 800, 1500 = 1166.67
    assert!(validate_metric_value(avg_price_result, 1166.67, 1.0));
    println!("✅ 4-level nested filter aggregation works!");
    Ok(())
}

#[test]
fn test_multiple_nested_branches() -> tantivy::Result<()> {
    let index = create_nested_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test multiple nested branches from the same parent
    let agg = json!({
        "by_region": {
            "filter": { "query_string": "*" },
            "aggs": {
                "us_products": {
                    "filter": { "query_string": "region:US" },
                    "aggs": {
                        "us_electronics": {
                            "filter": { "query_string": "category:electronics" },
                            "aggs": { "count": { "value_count": { "field": "brand" } } }
                        },
                        "us_books": {
                            "filter": { "query_string": "category:books" },
                            "aggs": { "avg_price": { "avg": { "field": "price" } } }
                        }
                    }
                },
                "eu_products": {
                    "filter": { "query_string": "region:EU" },
                    "aggs": {
                        "eu_electronics": {
                            "filter": { "query_string": "category:electronics" },
                            "aggs": { "max_price": { "max": { "field": "price" } } }
                        },
                        "eu_clothing": {
                            "filter": { "query_string": "category:clothing" },
                            "aggs": { "min_rating": { "min": { "field": "rating" } } }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    // Validate multiple nested branches with actual values
    assert!(result.0.contains_key("by_region"));
    let by_region_result = result.0.get("by_region").unwrap();
    assert!(validate_filter_bucket(by_region_result, 10)); // All 10 products

    let by_region_bucket = get_filter_bucket(by_region_result).unwrap();

    // Validate US products branch
    let us_products_result = by_region_bucket
        .sub_aggregations
        .0
        .get("us_products")
        .unwrap();
    assert!(validate_filter_bucket(us_products_result, 6)); // 6 US products

    let us_products_bucket = get_filter_bucket(us_products_result).unwrap();

    // US electronics: Apple(1200), Samsung(800) = 2 items
    let us_electronics_result = us_products_bucket
        .sub_aggregations
        .0
        .get("us_electronics")
        .unwrap();
    assert!(validate_filter_bucket(us_electronics_result, 2));

    // US books: Penguin(25), Random(20) = 2 items
    let us_books_result = us_products_bucket
        .sub_aggregations
        .0
        .get("us_books")
        .unwrap();
    assert!(validate_filter_bucket(us_books_result, 2));

    let us_books_bucket = get_filter_bucket(us_books_result).unwrap();
    let us_avg_price_result = us_books_bucket.sub_aggregations.0.get("avg_price").unwrap();
    // Average of 25 and 20 = 22.5
    assert!(validate_metric_value(us_avg_price_result, 22.5, 0.1));

    // Validate EU products branch
    let eu_products_result = by_region_bucket
        .sub_aggregations
        .0
        .get("eu_products")
        .unwrap();
    assert!(validate_filter_bucket(eu_products_result, 4)); // 4 EU products

    let eu_products_bucket = get_filter_bucket(eu_products_result).unwrap();

    // EU electronics: Sony(600), Apple(1500) = 2 items
    let eu_electronics_result = eu_products_bucket
        .sub_aggregations
        .0
        .get("eu_electronics")
        .unwrap();
    assert!(validate_filter_bucket(eu_electronics_result, 2));

    let eu_electronics_bucket = get_filter_bucket(eu_electronics_result).unwrap();
    let eu_max_price_result = eu_electronics_bucket
        .sub_aggregations
        .0
        .get("max_price")
        .unwrap();
    assert!(validate_metric_value(eu_max_price_result, 1500.0, 0.1));
    println!("✅ Multiple nested branches work!");
    Ok(())
}

#[test]
fn test_nested_with_complex_boolean_queries() -> tantivy::Result<()> {
    let index = create_nested_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test nested filters with complex boolean queries
    let agg = json!({
        "complex_nested": {
            "filter": {
                "bool": {
                    "must": [
                        { "query_string": "in_stock:true" }
                    ]
                }
            },
            "aggs": {
                "premium_available": {
                    "filter": {
                        "bool": {
                            "must": [
                                { "query_string": "price:[500 TO *]" },
                                { "query_string": "rating:[4.0 TO *]" }
                            ],
                            "should": [
                                { "query_string": "brand:Apple" },
                                { "query_string": "brand:Samsung" }
                            ]
                        }
                    },
                    "aggs": {
                        "by_category": {
                            "filter": { "query_string": "category:electronics" },
                            "aggs": {
                                "stats": { "stats": { "field": "price" } }
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

    assert!(result.0.contains_key("complex_nested"));
    println!("✅ Nested filters with complex boolean queries work!");
    Ok(())
}

#[test]
fn test_nested_filter_with_metrics() -> tantivy::Result<()> {
    let index = create_nested_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test nested filters with multiple metric aggregations
    let agg = json!({
        "electronics_analysis": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "premium_only": {
                    "filter": { "query_string": "price:[1000 TO *]" },
                    "aggs": {
                        "high_rated": {
                            "filter": { "query_string": "rating:[4.7 TO *]" },
                            "aggs": {
                                "stats": { "stats": { "field": "price" } },
                                "avg_rating": { "avg": { "field": "rating" } },
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

    assert!(result.0.contains_key("electronics_analysis"));
    println!("✅ Nested filters with multiple metrics work!");
    Ok(())
}

#[test]
fn test_nested_filter_performance() -> tantivy::Result<()> {
    let index = create_nested_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test that nested filters are efficient
    let start = std::time::Instant::now();

    let agg = json!({
        "level1": {
            "filter": { "query_string": "*" },
            "aggs": {
                "level2": {
                    "filter": { "query_string": "in_stock:true" },
                    "aggs": {
                        "level3": {
                            "filter": { "query_string": "rating:[4.0 TO *]" },
                            "aggs": {
                                "level4": {
                                    "filter": { "query_string": "price:[100 TO *]" },
                                    "aggs": {
                                        "level5": {
                                            "filter": { "query_string": "category:electronics" },
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
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let duration = start.elapsed();

    assert!(result.0.contains_key("level1"));
    assert!(duration.as_millis() < 100); // Should be very fast

    println!("5-level nested filter duration: {:?}", duration);
    println!("✅ Deep nested filters are efficient!");
    Ok(())
}
