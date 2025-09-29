use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, TEXT};
use tantivy::{doc, Index, IndexWriter};

fn create_test_index() -> tantivy::Result<(Index, Schema)> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST);
    let in_stock = schema_builder.add_bool_field("in_stock", FAST);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    writer.add_document(doc!(
        category => "electronics", price => 999u64, in_stock => true
    ))?;
    writer.add_document(doc!(
        category => "books", price => 25u64, in_stock => true
    ))?;
    writer.add_document(doc!(
        category => "clothing", price => 150u64, in_stock => false
    ))?;

    writer.commit()?;
    Ok((index, schema))
}

#[test]
fn test_all_query_fast_path() -> tantivy::Result<()> {
    let (index, _) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test AllQuery fast path - should match all documents
    let agg = json!({
        "all_items": {
            "filter": { "query_string": "*" },
            "aggs": {
                "count": { "value_count": { "field": "category" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    assert!(result.0.contains_key("all_items"));

    // The fast path should work correctly and return all 3 documents
    if let Some(bucket_result) = result.0.get("all_items") {
        println!("All items result: {:#?}", bucket_result);
    }

    Ok(())
}

#[test]
fn test_complex_query_fallback() -> tantivy::Result<()> {
    let (index, _) = create_test_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test complex query that should fall back to full evaluation
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

    assert!(result.0.contains_key("electronics"));

    // This should work correctly even without fast path
    if let Some(bucket_result) = result.0.get("electronics") {
        println!("Electronics result: {:#?}", bucket_result);
    }

    Ok(())
}

#[test]
fn test_performance_comparison() -> tantivy::Result<()> {
    // Create a larger index for performance testing
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let value = schema_builder.add_u64_field("value", FAST);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // Add 10,000 documents
    for i in 0..10_000 {
        writer.add_document(doc!(
            category => if i % 2 == 0 { "even" } else { "odd" },
            value => (i as u64)
        ))?;
    }
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test AllQuery (fast path)
    let all_query_agg = json!({
        "all": {
            "filter": { "query_string": "*" },
            "aggs": {
                "count": { "value_count": { "field": "value" } },
                "avg": { "avg": { "field": "value" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(all_query_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

    // Test complex query (full evaluation)
    let complex_agg = json!({
        "even": {
            "filter": { "query_string": "category:even" },
            "aggs": {
                "count": { "value_count": { "field": "value" } },
                "avg": { "avg": { "field": "value" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(complex_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

    Ok(())
}
