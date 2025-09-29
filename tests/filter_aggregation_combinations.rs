mod common;

use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, IndexWriter};

use common::filter_test_helpers::*;

fn create_rich_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let brand = schema_builder.add_text_field("brand", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST | INDEXED);
    let rating = schema_builder.add_f64_field("rating", FAST);
    let in_stock = schema_builder.add_bool_field("in_stock", FAST | INDEXED);
    let sales = schema_builder.add_u64_field("sales", FAST);
    let discount = schema_builder.add_f64_field("discount", FAST);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    let products = vec![
        ("electronics", "Apple", 1199, 4.8, true, 1250, 0.0),
        ("electronics", "Samsung", 999, 4.6, true, 980, 5.0),
        ("electronics", "Xiaomi", 299, 4.3, true, 2100, 15.0),
        ("clothing", "Nike", 150, 4.5, true, 1800, 20.0),
        ("clothing", "Adidas", 160, 4.4, false, 1200, 0.0),
        ("books", "Penguin", 25, 4.7, true, 5600, 0.0),
        ("books", "O'Reilly", 45, 4.8, true, 890, 10.0),
    ];

    for (cat, br, pr, rt, stock, sl, disc) in products {
        writer.add_document(doc!(
            category => cat, brand => br, price => pr as u64, rating => rt,
            in_stock => stock, sales => sl as u64, discount => disc
        ))?;
    }
    writer.commit()?;
    Ok((index, schema))
}

#[test]
fn test_all_metric_types() -> tantivy::Result<()> {
    let (index, _) = create_rich_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "all_metrics": {
            "filter": { "query_string": "*" },
            "aggs": {
                "price_stats": { "stats": { "field": "price" } },
                "price_avg": { "avg": { "field": "price" } },
                "price_sum": { "sum": { "field": "price" } },
                "price_min": { "min": { "field": "price" } },
                "price_max": { "max": { "field": "price" } },
                "price_count": { "value_count": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    // Compare entire result with expected JSON structure
    let expected = json!({
        "all_metrics": {
            "doc_count": 7,  // All 7 products
            "price_stats": {
                "count": 7,
                "min": 25.0,
                "max": 1199.0,
                "sum": 2877.0,  // 1199 + 999 + 299 + 150 + 160 + 25 + 45 = 2877
                "avg": 411.0    // 2877/7 ≈ 411.0
            },
            "price_avg": {
                "value": 411.0
            },
            "price_sum": {
                "value": 2877.0
            },
            "price_min": {
                "value": 25.0
            },
            "price_max": {
                "value": 1199.0
            },
            "price_count": {
                "value": 7.0
            }
        }
    });

    assert_aggregation_results_match(&result.0, expected, 1.0);
    Ok(())
}

#[test]
fn test_nested_bucket_aggregations() -> tantivy::Result<()> {
    let (index, _) = create_rich_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "by_brand": {
                    "terms": { "field": "brand" },
                    "aggs": {
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
                                "avg_rating": { "avg": { "field": "rating" } }
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

    assert!(result.0.contains_key("electronics"));
    Ok(())
}

#[test]
fn test_multiple_filter_types() -> tantivy::Result<()> {
    let (index, _) = create_rich_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "premium": {
            "filter": { "query_string": "price:[1000 TO *]" },
            "aggs": { "avg_rating": { "avg": { "field": "rating" } } }
        },
        "discounted": {
            "filter": { "query_string": "discount:[5.0 TO *]" },
            "aggs": { "total_sales": { "sum": { "field": "sales" } } }
        },
        "high_performers": {
            "filter": {
                "bool": {
                    "must": [
                        "rating:[4.5 TO *]",
                        "sales:[1000 TO *]"
                    ]
                }
            },
            "aggs": { "categories": { "terms": { "field": "category" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("premium"));
    assert!(result.0.contains_key("discounted"));
    assert!(result.0.contains_key("high_performers"));
    Ok(())
}

#[test]
fn test_performance_with_large_dataset() -> tantivy::Result<()> {
    // Create larger dataset for performance testing
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let value = schema_builder.add_u64_field("value", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // Add 1000 documents
    for i in 0..1000 {
        writer.add_document(doc!(
            category => format!("cat_{}", i % 10),
            value => (i as u64)
        ))?;
    }
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "large_filter": {
            "filter": { "query_string": "value:[500 TO *]" },
            "aggs": {
                "avg": { "avg": { "field": "value" } },
                "categories": { "terms": { "field": "category", "size": 20 } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("large_filter"));
    Ok(())
}
