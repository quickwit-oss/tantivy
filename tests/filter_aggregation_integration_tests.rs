use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, STORED, TEXT};
use tantivy::{doc, Index, IndexWriter};

/// Create a test index with sample e-commerce data
fn create_test_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();

    // Add fields for e-commerce data
    let category_field = schema_builder.add_text_field("category", TEXT | FAST);
    let brand_field = schema_builder.add_text_field("brand", TEXT | FAST);
    let price_field = schema_builder.add_u64_field("price", FAST);
    let rating_field = schema_builder.add_f64_field("rating", FAST);
    let in_stock_field = schema_builder.add_bool_field("in_stock", FAST);
    let name_field = schema_builder.add_text_field("name", TEXT | STORED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add sample documents
    let documents = vec![
        // Electronics
        doc!(
            category_field => "electronics",
            brand_field => "apple",
            price_field => 999u64,
            rating_field => 4.5f64,
            in_stock_field => true,
            name_field => "iPhone 15"
        ),
        doc!(
            category_field => "electronics",
            brand_field => "samsung",
            price_field => 799u64,
            rating_field => 4.2f64,
            in_stock_field => true,
            name_field => "Galaxy S24"
        ),
        doc!(
            category_field => "electronics",
            brand_field => "apple",
            price_field => 1299u64,
            rating_field => 4.7f64,
            in_stock_field => false,
            name_field => "MacBook Pro"
        ),
        // Clothing
        doc!(
            category_field => "clothing",
            brand_field => "nike",
            price_field => 120u64,
            rating_field => 4.1f64,
            in_stock_field => true,
            name_field => "Air Max Sneakers"
        ),
        doc!(
            category_field => "clothing",
            brand_field => "adidas",
            price_field => 85u64,
            rating_field => 3.9f64,
            in_stock_field => true,
            name_field => "Running Shoes"
        ),
        doc!(
            category_field => "clothing",
            brand_field => "nike",
            price_field => 200u64,
            rating_field => 4.6f64,
            in_stock_field => false,
            name_field => "Premium Jacket"
        ),
        // Books
        doc!(
            category_field => "books",
            brand_field => "penguin",
            price_field => 15u64,
            rating_field => 4.8f64,
            in_stock_field => true,
            name_field => "Classic Literature"
        ),
        doc!(
            category_field => "books",
            brand_field => "oreilly",
            price_field => 45u64,
            rating_field => 4.3f64,
            in_stock_field => true,
            name_field => "Programming Guide"
        ),
    ];

    for doc in documents {
        index_writer.add_document(doc)?;
    }

    index_writer.commit()?;

    Ok((index, schema))
}

#[test]
fn test_filter_aggregation_basic_term_filter() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test basic filter aggregation with term query
    let agg_request = json!({
        "electronics_only": {
            "filter": { "term": { "category": "electronics" } },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } },
                "max_price": { "max": { "field": "price" } },
                "min_price": { "min": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify the filter aggregation exists
    assert!(agg_result.0.contains_key("electronics_only"));

    let electronics_result = &agg_result.0["electronics_only"];
    println!("Electronics aggregation result: {:?}", electronics_result);

    // The aggregation should have processed only electronics items (3 items: iPhone, Galaxy, MacBook)
    // Expected average price: (999 + 799 + 1299) / 3 = 1032.33
    // Expected max price: 1299
    // Expected min price: 799

    Ok(())
}

#[test]
fn test_filter_aggregation_range_filter() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with range query
    let agg_request = json!({
        "expensive_items": {
            "filter": { "range": { "price": { "gte": "500" } } },
            "aggs": {
                "avg_rating": { "avg": { "field": "rating" } },
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify the filter aggregation exists
    assert!(agg_result.0.contains_key("expensive_items"));

    let expensive_result = &agg_result.0["expensive_items"];
    println!("Expensive items aggregation result: {:?}", expensive_result);

    // Should include: iPhone (999), Galaxy (799), MacBook (1299) = 3 items
    // Expected average rating: (4.5 + 4.2 + 4.7) / 3 = 4.47

    Ok(())
}

#[test]
fn test_filter_aggregation_bool_query() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with boolean query
    let agg_request = json!({
        "premium_electronics": {
            "filter": {
                "bool": {
                    "must": [
                        { "term": { "category": "electronics" } },
                        { "range": { "price": { "gte": "1000" } } }
                    ]
                }
            },
            "aggs": {
                "avg_rating": { "avg": { "field": "rating" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify the filter aggregation exists
    assert!(agg_result.0.contains_key("premium_electronics"));

    let premium_result = &agg_result.0["premium_electronics"];
    println!(
        "Premium electronics aggregation result: {:?}",
        premium_result
    );

    // Should include: iPhone (999 - no, < 1000), MacBook (1299 - yes) = 1 item
    // Expected average rating: 4.7

    Ok(())
}

#[test]
fn test_filter_aggregation_with_main_query() -> tantivy::Result<()> {
    let (index, schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Create a main query that filters to electronics category
    let category_field = schema.get_field("category").unwrap();
    use tantivy::schema::Term;
    let main_query = tantivy::query::TermQuery::new(
        Term::from_field_text(category_field, "electronics"),
        tantivy::schema::IndexRecordOption::Basic,
    );

    // Test filter aggregation on top of main query
    // Main query filters to electronics, filter aggregation also filters to electronics
    let agg_request = json!({
        "electronics_subset": {
            "filter": { "term": { "brand": "apple" } },
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&main_query, &collector)?;

    // Verify the filter aggregation exists
    assert!(agg_result.0.contains_key("electronics_subset"));

    let result = &agg_result.0["electronics_subset"];
    println!("Electronics subset aggregation result: {:?}", result);

    // Main query filters to electronics: iPhone, Galaxy, MacBook (3 items)
    // Filter aggregation further filters to Apple brand: iPhone, MacBook (2 items)

    Ok(())
}

#[test]
fn test_filter_aggregation_empty_result() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation that matches no documents
    let agg_request = json!({
        "nonexistent_category": {
            "filter": { "term": { "category": "furniture" } },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify the filter aggregation exists even with no matches
    assert!(agg_result.0.contains_key("nonexistent_category"));

    let result = &agg_result.0["nonexistent_category"];
    println!("Empty filter aggregation result: {:?}", result);

    // Should have 0 documents and null/empty aggregation results

    Ok(())
}

#[test]
fn test_multiple_filter_aggregations() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test multiple filter aggregations in one request
    let agg_request = json!({
        "electronics": {
            "filter": { "term": { "category": "electronics" } },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        },
        "clothing": {
            "filter": { "term": { "category": "clothing" } },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        },
        "books": {
            "filter": { "term": { "category": "books" } },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify all filter aggregations exist
    assert!(agg_result.0.contains_key("electronics"));
    assert!(agg_result.0.contains_key("clothing"));
    assert!(agg_result.0.contains_key("books"));

    println!("Multiple filter aggregations result: {:?}", agg_result);

    // Each should have different average prices:
    // Electronics: (999 + 799 + 1299) / 3 = 1032.33
    // Clothing: (120 + 85 + 200) / 3 = 135
    // Books: (15 + 45) / 2 = 30

    Ok(())
}

#[test]
fn test_filter_aggregation_performance() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test that filter aggregations are efficient
    let start = std::time::Instant::now();

    let agg_request = json!({
        "filter1": {
            "filter": { "term": { "category": "electronics" } },
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        },
        "filter2": {
            "filter": { "term": { "category": "clothing" } },
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        },
        "filter3": {
            "filter": { "range": { "price": { "gte": "100" } } },
            "aggs": { "avg_rating": { "avg": { "field": "rating" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let _agg_result = searcher.search(&AllQuery, &collector)?;

    let duration = start.elapsed();
    println!("Filter aggregation performance test took: {:?}", duration);

    // Should complete quickly (< 100ms for this small dataset)
    assert!(duration.as_millis() < 100);

    Ok(())
}

#[test]
fn test_filter_aggregation_with_nested_sub_aggregations() -> tantivy::Result<()> {
    let (index, _schema) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with multiple levels of sub-aggregations
    let agg_request = json!({
        "electronics_analysis": {
            "filter": { "term": { "category": "electronics" } },
            "aggs": {
                "price_stats": { "stats": { "field": "price" } },
                "rating_stats": { "stats": { "field": "rating" } },
                "brand_breakdown": {
                    "terms": { "field": "brand" },
                    "aggs": {
                        "avg_price_per_brand": { "avg": { "field": "price" } }
                    }
                }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify the filter aggregation exists
    assert!(agg_result.0.contains_key("electronics_analysis"));

    let result = &agg_result.0["electronics_analysis"];
    println!("Nested sub-aggregations result: {:?}", result);

    Ok(())
}
