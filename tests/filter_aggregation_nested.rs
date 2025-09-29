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

    // Compare entire 4-level nested result with expected JSON structure
    let expected = json!({
        "all_products": {
            "doc_count": 10,  // All 10 products
            "electronics_only": {
                "doc_count": 4,  // 4 electronics
                "premium_electronics": {
                    "doc_count": 3,  // 3 items >= 800 (Apple:1200, Samsung:800, Apple:1500)
                    "high_rated_premium": {
                        "doc_count": 3,  // 3 items >= 4.5 rating (Apple:4.8, Samsung:4.5, Apple:4.9)
                        "count": {
                            "value": 3.0
                        },
                        "avg_price": {
                            "value": 1166.67  // Average of 1200, 800, 1500 = 1166.67
                        }
                    }
                }
            }
        }
    });

    assert_aggregation_results_match(&result.0, expected, 1.0);
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

    // Compare entire multiple nested branches result with expected JSON structure
    let expected = json!({
        "by_region": {
            "doc_count": 10,  // All 10 products
            "us_products": {
                "doc_count": 6,  // 6 US products
                "us_electronics": {
                    "doc_count": 2,  // Apple(1200), Samsung(800)
                    "count": {
                        "value": 2.0
                    }
                },
                "us_books": {
                    "doc_count": 2,  // Penguin(25), Random(20)
                    "avg_price": {
                        "value": 22.5  // Average of 25 and 20
                    }
                }
            },
            "eu_products": {
                "doc_count": 4,  // 4 EU products
                "eu_electronics": {
                    "doc_count": 2,  // Sony(600), Apple(1500)
                    "max_price": {
                        "value": 1500.0
                    }
                },
                "eu_clothing": {
                    "doc_count": 1,  // Adidas(100)
                    "min_rating": {
                        "value": 4.2
                    }
                }
            }
        }
    });

    assert_aggregation_results_match(&result.0, expected, 0.1);
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

    // Compare complex boolean nested result with expected JSON structure
    let expected = json!({
        "complex_nested": {
            "doc_count": 7,  // 7 in_stock items
            "premium_available": {
                "doc_count": 3,  // Apple(1200, 4.8), Samsung(800, 4.5), Apple(1500, 4.9) match criteria
                "by_category": {
                    "doc_count": 3,  // All 3 are electronics
                    "stats": {
                        "count": 3,
                        "min": 800.0,
                        "max": 1500.0,
                        "sum": 3500.0,  // 1200 + 800 + 1500
                        "avg": 1166.67  // 3500 / 3
                    }
                }
            }
        }
    });

    assert_aggregation_results_match(&result.0, expected, 0.1);
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

    // Compare nested metrics result with expected JSON structure
    let expected = json!({
        "electronics_analysis": {
            "doc_count": 4,  // 4 electronics
            "premium_only": {
                "doc_count": 2,  // Apple(1200), Apple(1500) >= 1000
                "high_rated": {
                    "doc_count": 2,  // Both Apple items >= 4.7 rating (4.8, 4.9)
                    "stats": {
                        "count": 2,
                        "min": 1200.0,
                        "max": 1500.0,
                        "sum": 2700.0,
                        "avg": 1350.0
                    },
                    "avg_rating": {
                        "value": 4.85  // (4.8 + 4.9) / 2
                    },
                    "count": {
                        "value": 2.0
                    }
                }
            }
        }
    });

    assert_aggregation_results_match(&result.0, expected, 0.1);
    println!("✅ Nested filters with multiple metrics work!");
    Ok(())
}

#[test]
fn test_nested_filter_performance() -> tantivy::Result<()> {
    let index = create_nested_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

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

    // Compare 5-level nested result with expected JSON structure
    let expected = json!({
        "level1": {
            "doc_count": 10,  // All products
            "level2": {
                "doc_count": 7,  // in_stock items
                "level3": {
                    "doc_count": 7,  // rating >= 4.0 (all in_stock items have rating >= 4.0)
                    "level4": {
                        "doc_count": 5,  // price >= 100 (excludes books: Penguin:25, Random:20)
                        "level5": {
                            "doc_count": 3,  // electronics only (Apple:1200, Samsung:800, Apple:1500)
                            "final_count": {
                                "value": 3.0
                            }
                        }
                    }
                }
            }
        }
    });

    assert_aggregation_results_match(&result.0, expected, 0.1);
    println!("✅ Deep nested filters work correctly!");
    Ok(())
}
