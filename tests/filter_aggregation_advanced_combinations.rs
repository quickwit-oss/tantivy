use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
use tantivy::{doc, Index, IndexWriter};

fn create_advanced_test_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();

    // Multi-dimensional fields for advanced testing
    let category_field = schema_builder.add_text_field("category", TEXT | FAST);
    let subcategory_field = schema_builder.add_text_field("subcategory", TEXT | FAST);
    let brand_field = schema_builder.add_text_field("brand", TEXT | FAST);
    let region_field = schema_builder.add_text_field("region", TEXT | FAST);
    let status_field = schema_builder.add_text_field("status", TEXT | FAST);

    // Numeric fields for complex calculations
    let price_field = schema_builder.add_u64_field("price", FAST);
    let cost_field = schema_builder.add_u64_field("cost", FAST);
    let rating_field = schema_builder.add_f64_field("rating", FAST);
    let weight_field = schema_builder.add_f64_field("weight", FAST);
    let quantity_field = schema_builder.add_i64_field("quantity", FAST);
    let sales_field = schema_builder.add_u64_field("sales", FAST);
    let views_field = schema_builder.add_u64_field("views", FAST);

    // Boolean fields
    let premium_field = schema_builder.add_bool_field("premium", FAST | INDEXED);
    let featured_field = schema_builder.add_bool_field("featured", FAST | INDEXED);
    let available_field = schema_builder.add_bool_field("available", FAST | INDEXED);

    // Date field for time-based analysis
    let launch_date_field = schema_builder.add_date_field("launch_date", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Rich dataset for advanced testing
    let documents = vec![
        // Electronics - Smartphones
        doc!(
            category_field => "electronics", subcategory_field => "smartphones",
            brand_field => "Apple", region_field => "North America", status_field => "active",
            price_field => 1199u64, cost_field => 600u64, rating_field => 4.8f64,
            weight_field => 0.2f64, quantity_field => 100i64, sales_field => 5000u64, views_field => 50000u64,
            premium_field => true, featured_field => true, available_field => true,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1672531200)
        ),
        doc!(
            category_field => "electronics", subcategory_field => "smartphones",
            brand_field => "Samsung", region_field => "Europe", status_field => "active",
            price_field => 999u64, cost_field => 500u64, rating_field => 4.6f64,
            weight_field => 0.19f64, quantity_field => 150i64, sales_field => 4200u64, views_field => 42000u64,
            premium_field => true, featured_field => false, available_field => true,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1672617600)
        ),
        doc!(
            category_field => "electronics", subcategory_field => "smartphones",
            brand_field => "Xiaomi", region_field => "Asia", status_field => "active",
            price_field => 299u64, cost_field => 150u64, rating_field => 4.3f64,
            weight_field => 0.18f64, quantity_field => 500i64, sales_field => 8000u64, views_field => 60000u64,
            premium_field => false, featured_field => false, available_field => true,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1672704000)
        ),
        // Electronics - Laptops
        doc!(
            category_field => "electronics", subcategory_field => "laptops",
            brand_field => "Apple", region_field => "North America", status_field => "active",
            price_field => 2499u64, cost_field => 1200u64, rating_field => 4.9f64,
            weight_field => 1.4f64, quantity_field => 50i64, sales_field => 1200u64, views_field => 25000u64,
            premium_field => true, featured_field => true, available_field => false,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1672790400)
        ),
        doc!(
            category_field => "electronics", subcategory_field => "laptops",
            brand_field => "Dell", region_field => "Europe", status_field => "active",
            price_field => 899u64, cost_field => 450u64, rating_field => 4.2f64,
            weight_field => 1.8f64, quantity_field => 80i64, sales_field => 2100u64, views_field => 18000u64,
            premium_field => false, featured_field => false, available_field => true,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1672876800)
        ),
        // Clothing - Shoes
        doc!(
            category_field => "clothing", subcategory_field => "shoes",
            brand_field => "Nike", region_field => "North America", status_field => "active",
            price_field => 180u64, cost_field => 60u64, rating_field => 4.5f64,
            weight_field => 0.8f64, quantity_field => 200i64, sales_field => 3500u64, views_field => 35000u64,
            premium_field => false, featured_field => true, available_field => true,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1672963200)
        ),
        doc!(
            category_field => "clothing", subcategory_field => "shoes",
            brand_field => "Adidas", region_field => "Europe", status_field => "discontinued",
            price_field => 160u64, cost_field => 55u64, rating_field => 4.4f64,
            weight_field => 0.75f64, quantity_field => 50i64, sales_field => 2800u64, views_field => 28000u64,
            premium_field => false, featured_field => false, available_field => false,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1673049600)
        ),
        // Clothing - Apparel
        doc!(
            category_field => "clothing", subcategory_field => "apparel",
            brand_field => "Zara", region_field => "Europe", status_field => "active",
            price_field => 89u64, cost_field => 25u64, rating_field => 4.1f64,
            weight_field => 0.3f64, quantity_field => 300i64, sales_field => 6200u64, views_field => 45000u64,
            premium_field => false, featured_field => false, available_field => true,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1673136000)
        ),
        // Books - Technical
        doc!(
            category_field => "books", subcategory_field => "technical",
            brand_field => "O'Reilly", region_field => "Global", status_field => "active",
            price_field => 59u64, cost_field => 15u64, rating_field => 4.7f64,
            weight_field => 0.6f64, quantity_field => 120i64, sales_field => 1800u64, views_field => 12000u64,
            premium_field => false, featured_field => false, available_field => true,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1673222400)
        ),
        // Books - Fiction
        doc!(
            category_field => "books", subcategory_field => "fiction",
            brand_field => "Penguin", region_field => "Global", status_field => "active",
            price_field => 25u64, cost_field => 8u64, rating_field => 4.6f64,
            weight_field => 0.4f64, quantity_field => 400i64, sales_field => 12000u64, views_field => 80000u64,
            premium_field => false, featured_field => true, available_field => true,
            launch_date_field => tantivy::DateTime::from_timestamp_secs(1673308800)
        ),
    ];

    for doc in documents {
        index_writer.add_document(doc)?;
    }
    index_writer.commit()?;

    Ok((index, schema))
}

#[test]
fn test_multi_level_nested_aggregations() -> tantivy::Result<()> {
    let (index, _schema) = create_advanced_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("🎯 Test 1: Multi-Level Nested Aggregations (4+ levels deep)");

    // Test deeply nested: Filter → Terms → Range → Terms → Metrics
    let agg_request = json!({
        "active_products": {
            "filter": { "term": { "status": "active" } },
            "aggs": {
                "by_category": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "price_segments": {
                            "range": {
                                "field": "price",
                                "ranges": [
                                    { "key": "budget", "to": "200" },
                                    { "key": "mid_range", "from": "200", "to": "1000" },
                                    { "key": "premium", "from": "1000" }
                                ]
                            },
                            "aggs": {
                                "by_region": {
                                    "terms": { "field": "region" },
                                    "aggs": {
                                        "performance_metrics": {
                                            "stats": { "field": "rating" }
                                        },
                                        "financial_metrics": {
                                            "stats": { "field": "price" }
                                        },
                                        "inventory_metrics": {
                                            "sum": { "field": "quantity" }
                                        },
                                        "conversion_rate": {
                                            "avg": { "field": "sales" }
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

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("active_products"));
    println!(
        "Multi-level nested result: {:#?}\n",
        result.0["active_products"]
    );

    Ok(())
}

#[test]
fn test_complex_business_kpis() -> tantivy::Result<()> {
    let (index, _schema) = create_advanced_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("📊 Test 2: Complex Business KPIs with Multiple Filters");

    // Test complex business metrics calculation
    let agg_request = json!({
        "premium_segment_analysis": {
            "filter": { "term": { "premium": true } },
            "aggs": {
                "profitability_by_subcategory": {
                    "terms": { "field": "subcategory" },
                    "aggs": {
                        "revenue": { "sum": { "field": "price" } },
                        "cost": { "sum": { "field": "cost" } },
                        "avg_margin_per_unit": { "avg": { "field": "price" } },
                        "total_units": { "sum": { "field": "quantity" } },
                        "performance_rating": { "avg": { "field": "rating" } },
                        "market_reach": { "sum": { "field": "views" } }
                    }
                },
                "regional_performance": {
                    "terms": { "field": "region" },
                    "aggs": {
                        "featured_vs_regular": {
                            "terms": { "field": "featured" },
                            "aggs": {
                                "avg_sales_performance": { "avg": { "field": "sales" } },
                                "price_positioning": { "stats": { "field": "price" } }
                            }
                        }
                    }
                }
            }
        },

        "mass_market_analysis": {
            "filter": { "term": { "premium": false } },
            "aggs": {
                "volume_leaders": {
                    "terms": { "field": "brand", "size": 5 },
                    "aggs": {
                        "category_spread": {
                            "terms": { "field": "category" },
                            "aggs": {
                                "efficiency_metrics": {
                                    "range": {
                                        "field": "sales",
                                        "ranges": [
                                            { "key": "low_volume", "to": "3000" },
                                            { "key": "medium_volume", "from": "3000", "to": "6000" },
                                            { "key": "high_volume", "from": "6000" }
                                        ]
                                    },
                                    "aggs": {
                                        "avg_rating": { "avg": { "field": "rating" } },
                                        "total_inventory": { "sum": { "field": "quantity" } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("premium_segment_analysis"));
    assert!(result.0.contains_key("mass_market_analysis"));

    println!(
        "Premium segment analysis: {:#?}",
        result.0["premium_segment_analysis"]
    );
    println!(
        "Mass market analysis: {:#?}\n",
        result.0["mass_market_analysis"]
    );

    Ok(())
}

#[test]
fn test_advanced_filter_combinations_with_bool_queries() -> tantivy::Result<()> {
    let (index, _schema) = create_advanced_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("🔍 Test 3: Advanced Filter Combinations with Bool Queries");

    // Test complex boolean queries in filters
    let agg_request = json!({
        "high_value_available_products": {
            "filter": {
                "bool": {
                    "must": [
                        { "range": { "price": { "gte": "500" } } },
                        { "term": { "available": true } },
                        { "range": { "rating": { "gte": "4.0" } } }
                    ],
                    "should": [
                        { "term": { "featured": true } },
                        { "term": { "premium": true } }
                    ],
                    "must_not": [
                        { "term": { "status": "discontinued" } }
                    ]
                }
            },
            "aggs": {
                "market_positioning": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "brand_competition": {
                            "terms": { "field": "brand" },
                            "aggs": {
                                "price_vs_performance": {
                                    "range": {
                                        "field": "rating",
                                        "ranges": [
                                            { "key": "good", "from": "4.0", "to": "4.5" },
                                            { "key": "excellent", "from": "4.5" }
                                        ]
                                    },
                                    "aggs": {
                                        "avg_price": { "avg": { "field": "price" } },
                                        "market_share": { "sum": { "field": "sales" } },
                                        "inventory_depth": { "avg": { "field": "quantity" } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },

        "budget_high_volume_products": {
            "filter": {
                "bool": {
                    "must": [
                        { "range": { "price": { "lt": "300" } } },
                        { "range": { "sales": { "gte": "5000" } } }
                    ]
                }
            },
            "aggs": {
                "success_factors": {
                    "terms": { "field": "subcategory" },
                    "aggs": {
                        "regional_success": {
                            "terms": { "field": "region" },
                            "aggs": {
                                "engagement_metrics": {
                                    "stats": { "field": "views" }
                                },
                                "conversion_efficiency": {
                                    "avg": { "field": "sales" }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("high_value_available_products"));
    assert!(result.0.contains_key("budget_high_volume_products"));

    println!(
        "High value available products: {:#?}",
        result.0["high_value_available_products"]
    );
    println!(
        "Budget high volume products: {:#?}\n",
        result.0["budget_high_volume_products"]
    );

    Ok(())
}

#[test]
fn test_mixed_aggregation_types_comprehensive() -> tantivy::Result<()> {
    let (index, _schema) = create_advanced_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("🎪 Test 4: Mixed Aggregation Types - Comprehensive Combination");

    // Test all aggregation types together in complex combinations
    let agg_request = json!({
        "comprehensive_product_analysis": {
            "filter": { "match_all": {} },
            "aggs": {
                // Metric aggregations on different fields
                "overall_stats": {
                    "stats": { "field": "price" }
                },
                "total_inventory_value": {
                    "sum": { "field": "price" }
                },
                "avg_customer_satisfaction": {
                    "avg": { "field": "rating" }
                },

                // Terms aggregation with nested metrics and ranges
                "category_deep_dive": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "subcategory_breakdown": {
                            "terms": { "field": "subcategory" },
                            "aggs": {
                                "price_distribution": {
                                    "range": {
                                        "field": "price",
                                        "ranges": [
                                            { "key": "entry", "to": "100" },
                                            { "key": "standard", "from": "100", "to": "500" },
                                            { "key": "premium", "from": "500", "to": "1500" },
                                            { "key": "luxury", "from": "1500" }
                                        ]
                                    },
                                    "aggs": {
                                        "segment_performance": {
                                            "stats": { "field": "sales" }
                                        },
                                        "quality_metrics": {
                                            "stats": { "field": "rating" }
                                        },
                                        "availability_analysis": {
                                            "terms": { "field": "available" },
                                            "aggs": {
                                                "inventory_levels": {
                                                    "sum": { "field": "quantity" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },

                        // Range aggregation with nested terms and metrics
                        "weight_categories": {
                            "range": {
                                "field": "weight",
                                "ranges": [
                                    { "key": "lightweight", "to": "0.5" },
                                    { "key": "medium", "from": "0.5", "to": "1.0" },
                                    { "key": "heavy", "from": "1.0" }
                                ]
                            },
                            "aggs": {
                                "brand_distribution": {
                                    "terms": { "field": "brand" },
                                    "aggs": {
                                        "avg_price": { "avg": { "field": "price" } },
                                        "total_sales": { "sum": { "field": "sales" } }
                                    }
                                }
                            }
                        }
                    }
                },

                // Multiple range aggregations
                "market_segmentation": {
                    "range": {
                        "field": "views",
                        "ranges": [
                            { "key": "niche", "to": "20000" },
                            { "key": "popular", "from": "20000", "to": "50000" },
                            { "key": "viral", "from": "50000" }
                        ]
                    },
                    "aggs": {
                        "engagement_to_sales": {
                            "avg": { "field": "sales" }
                        },
                        "premium_presence": {
                            "terms": { "field": "premium" },
                            "aggs": {
                                "avg_rating": { "avg": { "field": "rating" } }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("comprehensive_product_analysis"));
    println!(
        "Comprehensive product analysis: {:#?}\n",
        result.0["comprehensive_product_analysis"]
    );

    Ok(())
}

#[test]
fn test_filter_aggregation_performance_scenarios() -> tantivy::Result<()> {
    let (index, _schema) = create_advanced_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("⚡ Test 5: Filter Aggregation Performance Scenarios");

    // Test scenarios that stress different aspects of the aggregation system
    let agg_request = json!({
        "high_cardinality_analysis": {
            "filter": { "term": { "available": true } },
            "aggs": {
                "brand_performance": {
                    "terms": { "field": "brand", "size": 100 },
                    "aggs": {
                        "all_metrics": { "stats": { "field": "price" } },
                        "rating_stats": { "stats": { "field": "rating" } },
                        "sales_stats": { "stats": { "field": "sales" } },
                        "quantity_stats": { "stats": { "field": "quantity" } },
                        "views_stats": { "stats": { "field": "views" } },
                        "weight_stats": { "stats": { "field": "weight" } }
                    }
                }
            }
        },

        "multi_dimensional_slicing": {
            "filter": { "range": { "rating": { "gte": "4.0" } } },
            "aggs": {
                "category_region_matrix": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "by_region": {
                            "terms": { "field": "region" },
                            "aggs": {
                                "by_subcategory": {
                                    "terms": { "field": "subcategory" },
                                    "aggs": {
                                        "by_status": {
                                            "terms": { "field": "status" },
                                            "aggs": {
                                                "financial_summary": {
                                                    "stats": { "field": "price" }
                                                },
                                                "operational_summary": {
                                                    "stats": { "field": "quantity" }
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
        },

        "range_intensive_analysis": {
            "filter": { "term": { "status": "active" } },
            "aggs": {
                "price_ranges": {
                    "range": {
                        "field": "price",
                        "ranges": [
                            { "key": "under_50", "to": "50" },
                            { "key": "50_to_100", "from": "50", "to": "100" },
                            { "key": "100_to_200", "from": "100", "to": "200" },
                            { "key": "200_to_500", "from": "200", "to": "500" },
                            { "key": "500_to_1000", "from": "500", "to": "1000" },
                            { "key": "1000_to_2000", "from": "1000", "to": "2000" },
                            { "key": "over_2000", "from": "2000" }
                        ]
                    },
                    "aggs": {
                        "rating_ranges": {
                            "range": {
                                "field": "rating",
                                "ranges": [
                                    { "key": "poor", "to": "3.0" },
                                    { "key": "fair", "from": "3.0", "to": "4.0" },
                                    { "key": "good", "from": "4.0", "to": "4.5" },
                                    { "key": "excellent", "from": "4.5" }
                                ]
                            },
                            "aggs": {
                                "sales_ranges": {
                                    "range": {
                                        "field": "sales",
                                        "ranges": [
                                            { "key": "low", "to": "2000" },
                                            { "key": "medium", "from": "2000", "to": "5000" },
                                            { "key": "high", "from": "5000" }
                                        ]
                                    },
                                    "aggs": {
                                        "segment_count": { "value_count": { "field": "brand" } },
                                        "avg_views": { "avg": { "field": "views" } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("high_cardinality_analysis"));
    assert!(result.0.contains_key("multi_dimensional_slicing"));
    assert!(result.0.contains_key("range_intensive_analysis"));

    println!("Performance test results completed successfully!");
    println!(
        "High cardinality analysis: {} aggregations",
        if let Some(_) = result.0.get("high_cardinality_analysis") {
            "✓"
        } else {
            "✗"
        }
    );
    println!(
        "Multi-dimensional slicing: {} aggregations",
        if let Some(_) = result.0.get("multi_dimensional_slicing") {
            "✓"
        } else {
            "✗"
        }
    );
    println!(
        "Range intensive analysis: {} aggregations\n",
        if let Some(_) = result.0.get("range_intensive_analysis") {
            "✓"
        } else {
            "✗"
        }
    );

    Ok(())
}
