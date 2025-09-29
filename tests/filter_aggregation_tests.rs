mod common;

use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::{AllQuery, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, Term, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, IndexWriter};

use common::filter_test_helpers::*;

fn setup_test_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let brand = schema_builder.add_text_field("brand", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST | INDEXED);
    let rating = schema_builder.add_f64_field("rating", FAST);
    let in_stock = schema_builder.add_bool_field("in_stock", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

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
    Ok((index, schema))
}

#[test]
fn basic_filter() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    // Validate the filter bucket exists and has correct values
    assert!(result.0.contains_key("electronics"));

    let electronics_result = result.0.get("electronics").unwrap();
    assert!(validate_filter_bucket(electronics_result, 2));

    let filter_bucket = get_filter_bucket(electronics_result).unwrap();
    let avg_price_result = filter_bucket.sub_aggregations.0.get("avg_price").unwrap();
    assert!(validate_metric_value(avg_price_result, 899.0, 0.1));
    Ok(())
}

#[test]
fn multiple_filters() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        },
        "in_stock": {
            "filter": { "query_string": "in_stock:true" },
            "aggs": { "count": { "value_count": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    // Validate electronics filter
    assert!(result.0.contains_key("electronics"));
    let electronics_result = result.0.get("electronics").unwrap();
    assert!(validate_filter_bucket(electronics_result, 2));

    let electronics_bucket = get_filter_bucket(electronics_result).unwrap();
    let avg_price_result = electronics_bucket
        .sub_aggregations
        .0
        .get("avg_price")
        .unwrap();
    assert!(validate_metric_value(avg_price_result, 899.0, 0.1)); // (999 + 799) / 2 = 899

    // Validate in_stock filter
    assert!(result.0.contains_key("in_stock"));
    let in_stock_result = result.0.get("in_stock").unwrap();
    // 3 documents are in_stock: electronics (2) + books (1)
    assert!(validate_filter_bucket(in_stock_result, 3));

    let in_stock_bucket = get_filter_bucket(in_stock_result).unwrap();
    let count_result = in_stock_bucket.sub_aggregations.0.get("count").unwrap();
    assert!(validate_metric_value(count_result, 3.0, 0.1));
    Ok(())
}

#[test]
fn nested_filters() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "all": {
            "filter": { "query_string": "*" },
            "aggs": {
                "electronics": {
                    "filter": { "query_string": "category:electronics" },
                    "aggs": {
                        "expensive": {
                            "filter": { "query_string": "price:[800 TO *]" },
                            "aggs": {
                                "count": { "value_count": { "field": "price" } }
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

    // Validate nested filter structure and values
    assert!(result.0.contains_key("all"));
    let all_result = result.0.get("all").unwrap();
    assert!(validate_filter_bucket(all_result, 4)); // All 4 documents should match "*"

    let all_bucket = get_filter_bucket(all_result).unwrap();

    // Check electronics sub-filter
    let electronics_result = all_bucket.sub_aggregations.0.get("electronics").unwrap();
    assert!(validate_filter_bucket(electronics_result, 2)); // 2 electronics

    let electronics_bucket = get_filter_bucket(electronics_result).unwrap();

    // Check expensive sub-filter (price >= 800)
    let expensive_result = electronics_bucket
        .sub_aggregations
        .0
        .get("expensive")
        .unwrap();
    // Only 2 electronics items: 999 and 799, but only 999 >= 800
    assert!(validate_filter_bucket(expensive_result, 1));

    let expensive_bucket = get_filter_bucket(expensive_result).unwrap();
    let count_result = expensive_bucket.sub_aggregations.0.get("count").unwrap();
    assert!(validate_metric_value(count_result, 1.0, 0.1));
    Ok(())
}

#[test]
fn filter_with_base_query() -> tantivy::Result<()> {
    let (index, schema) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let in_stock_field = schema.get_field("in_stock").unwrap();
    let base_query = TermQuery::new(
        Term::from_field_bool(in_stock_field, true),
        IndexRecordOption::Basic,
    );

    let agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": { "count": { "value_count": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&base_query, &collector)?;

    assert!(result.0.contains_key("electronics"));
    Ok(())
}

#[test]
fn bool_queries() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "premium_electronics": {
            "filter": {
                "bool": {
                    "must": ["category:electronics", "price:[800 TO *]"]
                }
            },
            "aggs": { "avg_rating": { "avg": { "field": "rating" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("premium_electronics"));
    Ok(())
}

#[test]
fn empty_results() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "nonexistent": {
            "filter": { "query_string": "category:furniture" },
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("nonexistent"));
    Ok(())
}

#[test]
fn mixed_aggregations() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "price_stats": { "stats": { "field": "price" } },
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

    assert!(result.0.contains_key("electronics"));
    Ok(())
}

#[test]
fn test_all_field_types() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text_field", TEXT | FAST);
    let u64_field = schema_builder.add_u64_field("u64_field", FAST | INDEXED);
    let i64_field = schema_builder.add_i64_field("i64_field", FAST | INDEXED);
    let f64_field = schema_builder.add_f64_field("f64_field", FAST | INDEXED);
    let bool_field = schema_builder.add_bool_field("bool_field", FAST | INDEXED);
    let date_field = schema_builder.add_date_field("date_field", FAST | INDEXED);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    writer.add_document(doc!(
        text_field => "electronics",
        u64_field => 999u64,
        i64_field => -100i64,
        f64_field => 4.5f64,
        bool_field => true,
        date_field => tantivy::DateTime::from_timestamp_secs(1640995200)
    ))?;
    writer.add_document(doc!(
        text_field => "books",
        u64_field => 45u64,
        i64_field => 0i64,
        f64_field => 4.8f64,
        bool_field => false,
        date_field => tantivy::DateTime::from_timestamp_secs(1704067200)
    ))?;
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
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

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("electronics_filter"));
    Ok(())
}

#[test]
fn test_range_queries() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "expensive": {
            "filter": { "query_string": "price:[500 TO *]" },
            "aggs": { "count": { "value_count": { "field": "price" } } }
        },
        "high_rated": {
            "filter": { "query_string": "rating:[4.5 TO *]" },
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("expensive"));
    assert!(result.0.contains_key("high_rated"));
    Ok(())
}

#[test]
fn test_bool_field_queries() -> tantivy::Result<()> {
    let (index, _) = setup_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "in_stock": {
            "filter": { "query_string": "in_stock:true" },
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        },
        "out_of_stock": {
            "filter": { "query_string": "in_stock:false" },
            "aggs": { "count": { "value_count": { "field": "price" } } }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("in_stock"));
    assert!(result.0.contains_key("out_of_stock"));
    Ok(())
}
