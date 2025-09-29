mod common;

use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, TEXT};
use tantivy::{doc, Index};

use common::filter_test_helpers::*;

fn create_simple_index() -> tantivy::Result<Index> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut writer = index.writer(50_000_000)?;

    // Add some test documents
    writer.add_document(doc!(
        category => "electronics",
        price => 999u64
    ))?;
    writer.add_document(doc!(
        category => "electronics", 
        price => 799u64
    ))?;
    writer.add_document(doc!(
        category => "books",
        price => 25u64
    ))?;

    writer.commit()?;
    Ok(index)
}

#[test]
fn example_json_comparison() -> tantivy::Result<()> {
    let index = create_simple_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Define aggregation request
    let agg = json!({
        "electronics_filter": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } },
                "max_price": { "max": { "field": "price" } },
                "count": { "value_count": { "field": "price" } }
            }
        },
        "all_categories": {
            "filter": { "query_string": "*" },
            "aggs": {
                "total_count": { "value_count": { "field": "category" } },
                "price_stats": { "stats": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    // Define expected JSON structure - this is much cleaner than manual extraction!
    let expected = json!({
        "electronics_filter": {
            "doc_count": 2,
            "avg_price": {
                "value": 899.0  // (999 + 799) / 2
            },
            "max_price": {
                "value": 999.0
            },
            "count": {
                "value": 2.0
            }
        },
        "all_categories": {
            "doc_count": 3,  // All 3 documents
            "total_count": {
                "value": 3.0
            },
            "price_stats": {
                "count": 3,
                "min": 25.0,
                "max": 999.0,
                "sum": 1823.0,  // 999 + 799 + 25
                "avg": 607.67   // 1823 / 3
            }
        }
    });

    // Single assertion compares the entire result structure!
    assert_aggregation_results_match(&result.0, expected, 0.1);
    
    println!("✅ JSON comparison works perfectly!");
    Ok(())
}

#[test]
fn example_with_tolerance() -> tantivy::Result<()> {
    let index = create_simple_index()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg = json!({
        "price_analysis": {
            "filter": { "query_string": "*" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    // We can specify different tolerances for floating point comparisons
    let expected = json!({
        "price_analysis": {
            "doc_count": 3,
            "avg_price": {
                "value": 607.67  // This will match with tolerance
            }
        }
    });

    // Use a larger tolerance for this comparison
    assert_aggregation_results_match(&result.0, expected, 1.0);
    
    println!("✅ Tolerance-based comparison works!");
    Ok(())
}
