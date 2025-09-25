use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
use tantivy::{doc, Index, IndexWriter};

fn create_ecommerce_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();

    // Product fields
    let category_field = schema_builder.add_text_field("category", TEXT | FAST);
    let brand_field = schema_builder.add_text_field("brand", TEXT | FAST);
    let product_name_field = schema_builder.add_text_field("product_name", TEXT | FAST | STORED);
    let tags_field = schema_builder.add_text_field("tags", TEXT | FAST);

    // Numeric fields
    let price_field = schema_builder.add_u64_field("price", FAST);
    let rating_field = schema_builder.add_f64_field("rating", FAST);
    let quantity_field = schema_builder.add_i64_field("quantity", FAST);
    let discount_pct_field = schema_builder.add_f64_field("discount_pct", FAST);
    let sales_count_field = schema_builder.add_u64_field("sales_count", FAST);

    // Boolean and date fields
    let in_stock_field = schema_builder.add_bool_field("in_stock", FAST | INDEXED);
    let is_featured_field = schema_builder.add_bool_field("is_featured", FAST | INDEXED);
    let created_date_field = schema_builder.add_date_field("created_date", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add comprehensive sample data
    let documents = vec![
        // Electronics - High-end
        doc!(
            category_field => "electronics",
            brand_field => "Apple",
            product_name_field => "iPhone 15 Pro",
            tags_field => "smartphone",
            tags_field => "premium",
            price_field => 1199u64,
            rating_field => 4.8f64,
            quantity_field => 50i64,
            discount_pct_field => 0.0f64,
            sales_count_field => 1250u64,
            in_stock_field => true,
            is_featured_field => true,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1672531200)
        ),
        doc!(
            category_field => "electronics",
            brand_field => "Samsung",
            product_name_field => "Galaxy S24 Ultra",
            tags_field => "smartphone",
            tags_field => "android",
            price_field => 1099u64,
            rating_field => 4.7f64,
            quantity_field => 30i64,
            discount_pct_field => 5.0f64,
            sales_count_field => 980u64,
            in_stock_field => true,
            is_featured_field => true,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1672617600)
        ),
        doc!(
            category_field => "electronics",
            brand_field => "Apple",
            product_name_field => "MacBook Air M3",
            tags_field => "laptop",
            tags_field => "premium",
            price_field => 1299u64,
            rating_field => 4.9f64,
            quantity_field => 15i64,
            discount_pct_field => 0.0f64,
            sales_count_field => 650u64,
            in_stock_field => false,
            is_featured_field => true,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1672704000)
        ),
        // Electronics - Budget
        doc!(
            category_field => "electronics",
            brand_field => "Xiaomi",
            product_name_field => "Redmi Note 13",
            tags_field => "smartphone",
            tags_field => "budget",
            price_field => 299u64,
            rating_field => 4.3f64,
            quantity_field => 100i64,
            discount_pct_field => 15.0f64,
            sales_count_field => 2100u64,
            in_stock_field => true,
            is_featured_field => false,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1672790400)
        ),
        doc!(
            category_field => "electronics",
            brand_field => "Lenovo",
            product_name_field => "ThinkPad E15",
            tags_field => "laptop",
            tags_field => "business",
            price_field => 699u64,
            rating_field => 4.4f64,
            quantity_field => 25i64,
            discount_pct_field => 10.0f64,
            sales_count_field => 450u64,
            in_stock_field => true,
            is_featured_field => false,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1672876800)
        ),
        // Clothing
        doc!(
            category_field => "clothing",
            brand_field => "Nike",
            product_name_field => "Air Max 270",
            tags_field => "shoes",
            tags_field => "sports",
            price_field => 150u64,
            rating_field => 4.5f64,
            quantity_field => 80i64,
            discount_pct_field => 20.0f64,
            sales_count_field => 1800u64,
            in_stock_field => true,
            is_featured_field => true,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1672963200)
        ),
        doc!(
            category_field => "clothing",
            brand_field => "Adidas",
            product_name_field => "Ultraboost 23",
            tags_field => "shoes",
            tags_field => "running",
            price_field => 180u64,
            rating_field => 4.6f64,
            quantity_field => 60i64,
            discount_pct_field => 0.0f64,
            sales_count_field => 1200u64,
            in_stock_field => true,
            is_featured_field => false,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1673049600)
        ),
        doc!(
            category_field => "clothing",
            brand_field => "Levi's",
            product_name_field => "501 Original Jeans",
            tags_field => "jeans",
            tags_field => "classic",
            price_field => 89u64,
            rating_field => 4.2f64,
            quantity_field => 120i64,
            discount_pct_field => 25.0f64,
            sales_count_field => 3200u64,
            in_stock_field => true,
            is_featured_field => false,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1673136000)
        ),
        // Books
        doc!(
            category_field => "books",
            brand_field => "Penguin",
            product_name_field => "1984 by George Orwell",
            tags_field => "fiction",
            tags_field => "classic",
            price_field => 15u64,
            rating_field => 4.7f64,
            quantity_field => 200i64,
            discount_pct_field => 0.0f64,
            sales_count_field => 5600u64,
            in_stock_field => true,
            is_featured_field => true,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1673222400)
        ),
        doc!(
            category_field => "books",
            brand_field => "O'Reilly",
            product_name_field => "Rust Programming Language",
            tags_field => "programming",
            tags_field => "technical",
            price_field => 45u64,
            rating_field => 4.8f64,
            quantity_field => 75i64,
            discount_pct_field => 10.0f64,
            sales_count_field => 890u64,
            in_stock_field => true,
            is_featured_field => false,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1673308800)
        ),
        doc!(
            category_field => "books",
            brand_field => "Manning",
            product_name_field => "Microservices Patterns",
            tags_field => "programming",
            tags_field => "architecture",
            price_field => 55u64,
            rating_field => 4.6f64,
            quantity_field => 40i64,
            discount_pct_field => 15.0f64,
            sales_count_field => 320u64,
            in_stock_field => false,
            is_featured_field => false,
            created_date_field => tantivy::DateTime::from_timestamp_secs(1673395200)
        ),
    ];

    for doc in documents {
        index_writer.add_document(doc)?;
    }
    index_writer.commit()?;

    Ok((index, schema))
}

#[test]
fn test_filter_with_nested_bucket_aggregations() -> tantivy::Result<()> {
    let (index, _schema) = create_ecommerce_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("🔍 Test 1: Filter → Terms → Metrics (Nested Bucket Aggregations)");

    // Test filter aggregation with nested terms aggregation and metrics
    let agg_request = json!({
        "electronics_analysis": {
            "filter": { "term": { "category": "electronics" } },
            "aggs": {
                "brands": {
                    "terms": { "field": "brand" },
                    "aggs": {
                        "avg_price": { "avg": { "field": "price" } },
                        "total_sales": { "sum": { "field": "sales_count" } },
                        "avg_rating": { "avg": { "field": "rating" } },
                        "price_stats": { "stats": { "field": "price" } }
                    }
                },
                "price_ranges": {
                    "range": {
                        "field": "price",
                        "ranges": [
                            { "to": "500" },
                            { "from": "500", "to": "1000" },
                            { "from": "1000" }
                        ]
                    },
                    "aggs": {
                        "avg_rating": { "avg": { "field": "rating" } },
                        "total_quantity": { "sum": { "field": "quantity" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("electronics_analysis"));
    println!(
        "Electronics nested aggregation result: {:#?}\n",
        result.0["electronics_analysis"]
    );

    Ok(())
}

#[test]
fn test_multiple_filters_with_different_aggregation_combinations() -> tantivy::Result<()> {
    let (index, _schema) = create_ecommerce_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("🏪 Test 2: Multiple Filters with Different Aggregation Combinations");

    let agg_request = json!({
        "premium_products": {
            "filter": { "range": { "price": { "gte": "1000" } } },
            "aggs": {
                "categories": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "avg_price": { "avg": { "field": "price" } },
                        "max_rating": { "max": { "field": "rating" } }
                    }
                },
                "overall_stats": { "stats": { "field": "price" } }
            }
        },
        "budget_products": {
            "filter": { "range": { "price": { "lt": "200" } } },
            "aggs": {
                "brand_performance": {
                    "terms": { "field": "brand" },
                    "aggs": {
                        "total_sales": { "sum": { "field": "sales_count" } },
                        "avg_discount": { "avg": { "field": "discount_pct" } }
                    }
                },
                "tag_analysis": {
                    "terms": { "field": "tags" },
                    "aggs": {
                        "avg_rating": { "avg": { "field": "rating" } }
                    }
                }
            }
        },
        "featured_products": {
            "filter": { "term": { "is_featured": true } },
            "aggs": {
                "price_distribution": {
                    "range": {
                        "field": "price",
                        "ranges": [
                            { "to": "100" },
                            { "from": "100", "to": "500" },
                            { "from": "500", "to": "1000" },
                            { "from": "1000" }
                        ]
                    },
                    "aggs": {
                        "count": { "value_count": { "field": "product_name" } },
                        "avg_sales": { "avg": { "field": "sales_count" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("premium_products"));
    assert!(result.0.contains_key("budget_products"));
    assert!(result.0.contains_key("featured_products"));

    println!(
        "Premium products analysis: {:#?}",
        result.0["premium_products"]
    );
    println!(
        "Budget products analysis: {:#?}",
        result.0["budget_products"]
    );
    println!(
        "Featured products analysis: {:#?}\n",
        result.0["featured_products"]
    );

    Ok(())
}

#[test]
fn test_complex_ecommerce_analytics_scenario() -> tantivy::Result<()> {
    let (index, _schema) = create_ecommerce_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("📊 Test 3: Complex E-commerce Analytics Scenario");

    // Real-world e-commerce dashboard scenario
    let agg_request = json!({
        "in_stock_analysis": {
            "filter": { "term": { "in_stock": true } },
            "aggs": {
                "category_breakdown": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "revenue_potential": { "sum": { "field": "price" } },
                        "avg_price": { "avg": { "field": "price" } },
                        "inventory_value": {
                            "sum": { "field": "quantity" }
                        },
                        "brand_diversity": {
                            "terms": { "field": "brand" },
                            "aggs": {
                                "avg_rating": { "avg": { "field": "rating" } },
                                "total_sales_history": { "sum": { "field": "sales_count" } }
                            }
                        }
                    }
                },
                "discount_impact": {
                    "range": {
                        "field": "discount_pct",
                        "ranges": [
                            { "key": "no_discount", "to": "1" },
                            { "key": "low_discount", "from": "1", "to": "15" },
                            { "key": "high_discount", "from": "15" }
                        ]
                    },
                    "aggs": {
                        "avg_sales": { "avg": { "field": "sales_count" } },
                        "avg_rating": { "avg": { "field": "rating" } }
                    }
                }
            }
        },
        "high_performers": {
            "filter": {
                "bool": {
                    "must": [
                        { "range": { "rating": { "gte": "4.5" } } },
                        { "range": { "sales_count": { "gte": "1000" } } }
                    ]
                }
            },
            "aggs": {
                "performance_by_category": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "price_stats": { "stats": { "field": "price" } },
                        "top_brands": {
                            "terms": { "field": "brand", "size": 3 },
                            "aggs": {
                                "avg_discount": { "avg": { "field": "discount_pct" } }
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

    assert!(result.0.contains_key("in_stock_analysis"));
    assert!(result.0.contains_key("high_performers"));

    println!("In-stock analysis: {:#?}", result.0["in_stock_analysis"]);
    println!(
        "High performers analysis: {:#?}\n",
        result.0["high_performers"]
    );

    Ok(())
}

#[test]
fn test_deeply_nested_filter_combinations() -> tantivy::Result<()> {
    let (index, _schema) = create_ecommerce_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("🎯 Test 4: Deeply Nested Filter Combinations");

    // Test filter within filter (nested filters)
    let agg_request = json!({
        "electronics_deep_dive": {
            "filter": { "term": { "category": "electronics" } },
            "aggs": {
                "smartphone_analysis": {
                    "filter": { "term": { "tags": "smartphone" } },
                    "aggs": {
                        "brand_comparison": {
                            "terms": { "field": "brand" },
                            "aggs": {
                                "price_tiers": {
                                    "range": {
                                        "field": "price",
                                        "ranges": [
                                            { "key": "budget", "to": "500" },
                                            { "key": "premium", "from": "500" }
                                        ]
                                    },
                                    "aggs": {
                                        "performance_metrics": {
                                            "stats": { "field": "rating" }
                                        },
                                        "sales_performance": {
                                            "stats": { "field": "sales_count" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "laptop_analysis": {
                    "filter": { "term": { "tags": "laptop" } },
                    "aggs": {
                        "availability_impact": {
                            "terms": { "field": "in_stock" },
                            "aggs": {
                                "avg_price": { "avg": { "field": "price" } },
                                "total_potential_sales": { "sum": { "field": "quantity" } }
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

    assert!(result.0.contains_key("electronics_deep_dive"));
    println!(
        "Electronics deep dive analysis: {:#?}\n",
        result.0["electronics_deep_dive"]
    );

    Ok(())
}

#[test]
fn test_all_metric_types_with_filter_combinations() -> tantivy::Result<()> {
    let (index, _schema) = create_ecommerce_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("📈 Test 5: All Metric Types with Filter Combinations");

    // Comprehensive test of all metric aggregation types
    let agg_request = json!({
        "comprehensive_metrics": {
            "filter": { "match_all": {} },
            "aggs": {
                // All metric types on different fields
                "price_stats": { "stats": { "field": "price" } },
                "price_avg": { "avg": { "field": "price" } },
                "price_sum": { "sum": { "field": "price" } },
                "price_min": { "min": { "field": "price" } },
                "price_max": { "max": { "field": "price" } },
                "price_count": { "value_count": { "field": "price" } },

                "rating_stats": { "stats": { "field": "rating" } },
                "quantity_sum": { "sum": { "field": "quantity" } },
                "sales_avg": { "avg": { "field": "sales_count" } },
                "discount_max": { "max": { "field": "discount_pct" } },

                // Bucket aggregations with metrics
                "category_metrics": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "all_metrics": { "stats": { "field": "price" } },
                        "inventory": { "sum": { "field": "quantity" } },
                        "performance": { "avg": { "field": "rating" } }
                    }
                },

                // Range with metrics
                "price_ranges_with_metrics": {
                    "range": {
                        "field": "price",
                        "ranges": [
                            { "key": "budget", "to": "100" },
                            { "key": "mid_range", "from": "100", "to": "500" },
                            { "key": "premium", "from": "500" }
                        ]
                    },
                    "aggs": {
                        "segment_stats": { "stats": { "field": "rating" } },
                        "total_sales": { "sum": { "field": "sales_count" } },
                        "avg_discount": { "avg": { "field": "discount_pct" } }
                    }
                }
            }
        },

        "filtered_comprehensive": {
            "filter": { "term": { "in_stock": true } },
            "aggs": {
                "available_inventory_metrics": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "financial_metrics": { "stats": { "field": "price" } },
                        "operational_metrics": { "stats": { "field": "quantity" } },
                        "quality_metrics": { "stats": { "field": "rating" } },
                        "sales_metrics": { "stats": { "field": "sales_count" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("comprehensive_metrics"));
    assert!(result.0.contains_key("filtered_comprehensive"));

    println!(
        "Comprehensive metrics: {:#?}",
        result.0["comprehensive_metrics"]
    );
    println!(
        "Filtered comprehensive: {:#?}\n",
        result.0["filtered_comprehensive"]
    );

    Ok(())
}

#[test]
fn test_real_world_business_scenarios() -> tantivy::Result<()> {
    let (index, _schema) = create_ecommerce_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    println!("🏢 Test 6: Real-World Business Scenarios");

    // Scenario 1: Marketing Campaign Analysis
    let marketing_analysis = json!({
        "discounted_products_performance": {
            "filter": { "range": { "discount_pct": { "gt": "0" } } },
            "aggs": {
                "discount_tiers": {
                    "range": {
                        "field": "discount_pct",
                        "ranges": [
                            { "key": "light_discount", "from": "0", "to": "10" },
                            { "key": "medium_discount", "from": "10", "to": "20" },
                            { "key": "heavy_discount", "from": "20" }
                        ]
                    },
                    "aggs": {
                        "sales_impact": { "avg": { "field": "sales_count" } },
                        "rating_impact": { "avg": { "field": "rating" } },
                        "category_breakdown": {
                            "terms": { "field": "category" },
                            "aggs": {
                                "revenue_per_category": { "sum": { "field": "price" } }
                            }
                        }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(marketing_analysis)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    println!(
        "Marketing campaign analysis: {:#?}",
        result.0["discounted_products_performance"]
    );

    // Scenario 2: Inventory Management
    let inventory_analysis = json!({
        "low_stock_alert": {
            "filter": { "range": { "quantity": { "lt": "50" } } },
            "aggs": {
                "critical_categories": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "avg_sales_velocity": { "avg": { "field": "sales_count" } },
                        "reorder_priority": { "min": { "field": "quantity" } },
                        "revenue_at_risk": { "sum": { "field": "price" } }
                    }
                }
            }
        },
        "out_of_stock_impact": {
            "filter": { "term": { "in_stock": false } },
            "aggs": {
                "lost_opportunities": {
                    "terms": { "field": "category" },
                    "aggs": {
                        "potential_revenue": { "sum": { "field": "price" } },
                        "historical_performance": { "avg": { "field": "sales_count" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(inventory_analysis)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    println!("Inventory management analysis: {:#?}\n", result.0);

    Ok(())
}
