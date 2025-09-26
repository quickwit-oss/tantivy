use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, TEXT, INDEXED};
use tantivy::{doc, Index, IndexWriter};

#[test]
fn test_term_query_fast_path() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST | INDEXED);
    let in_stock = schema_builder.add_bool_field("in_stock", FAST | INDEXED);
    let rating = schema_builder.add_f64_field("rating", FAST | INDEXED);
    
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // Add test data
    for i in 0..1000 {
        writer.add_document(doc!(
            category => if i % 3 == 0 { "electronics" } else if i % 3 == 1 { "books" } else { "clothing" },
            price => (100 + i) as u64,
            in_stock => i % 2 == 0,
            rating => (i as f64) / 100.0
        ))?;
    }
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test TermQuery fast path for text field
    let text_agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(text_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let text_duration = start.elapsed();

    // Test TermQuery fast path for u64 field
    let u64_agg = json!({
        "price_150": {
            "filter": { "query_string": "price:150" },
            "aggs": {
                "count": { "value_count": { "field": "category" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(u64_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let u64_duration = start.elapsed();

    // Test TermQuery fast path for bool field
    let bool_agg = json!({
        "in_stock": {
            "filter": { "query_string": "in_stock:true" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(bool_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let bool_duration = start.elapsed();

    println!("Text TermQuery fast path duration: {:?}", text_duration);
    println!("U64 TermQuery fast path duration: {:?}", u64_duration);
    println!("Bool TermQuery fast path duration: {:?}", bool_duration);

    // All should complete quickly
    assert!(text_duration.as_millis() < 100);
    assert!(u64_duration.as_millis() < 100);
    assert!(bool_duration.as_millis() < 100);

    Ok(())
}

#[test]
fn test_range_query_fast_path() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let price = schema_builder.add_u64_field("price", FAST | INDEXED);
    let rating = schema_builder.add_f64_field("rating", FAST | INDEXED);
    let score = schema_builder.add_i64_field("score", FAST | INDEXED);
    
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // Add test data
    for i in 0..1000 {
        writer.add_document(doc!(
            price => (100 + i) as u64,
            rating => (i as f64) / 100.0,
            score => (i as i64) - 500
        ))?;
    }
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test RangeQuery fast path for u64 field
    let u64_range_agg = json!({
        "mid_price": {
            "filter": { "query_string": "price:[500 TO 600]" },
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(u64_range_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let u64_range_duration = start.elapsed();

    // Test RangeQuery fast path for f64 field
    let f64_range_agg = json!({
        "mid_rating": {
            "filter": { "query_string": "rating:[5.0 TO 7.0]" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(f64_range_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let f64_range_duration = start.elapsed();

    // Test RangeQuery fast path for i64 field
    let i64_range_agg = json!({
        "positive_scores": {
            "filter": { "query_string": "score:[0 TO *]" },
            "aggs": {
                "count": { "value_count": { "field": "score" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(i64_range_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let i64_range_duration = start.elapsed();

    println!("U64 RangeQuery fast path duration: {:?}", u64_range_duration);
    println!("F64 RangeQuery fast path duration: {:?}", f64_range_duration);
    println!("I64 RangeQuery fast path duration: {:?}", i64_range_duration);

    // All should complete quickly
    assert!(u64_range_duration.as_millis() < 100);
    assert!(f64_range_duration.as_millis() < 100);
    assert!(i64_range_duration.as_millis() < 100);

    Ok(())
}

#[test]
fn test_fast_path_vs_full_evaluation() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", TEXT | FAST);
    let price = schema_builder.add_u64_field("price", FAST | INDEXED);
    
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer(50_000_000)?;

    // Add 5000 documents for better performance comparison
    for i in 0..5000 {
        writer.add_document(doc!(
            category => if i % 2 == 0 { "electronics" } else { "books" },
            price => (100 + i) as u64
        ))?;
    }
    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test simple term query (should use fast path)
    let simple_agg = json!({
        "electronics": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(simple_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let fast_path_duration = start.elapsed();

    // Test complex boolean query (should fall back to full evaluation)
    let complex_agg = json!({
        "complex": {
            "filter": {
                "bool": {
                    "must": [
                        { "query_string": "category:electronics" },
                        { "query_string": "price:[200 TO 300]" }
                    ]
                }
            },
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let start = std::time::Instant::now();
    let aggregations: Aggregations = serde_json::from_value(complex_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;
    let full_eval_duration = start.elapsed();

    println!("Fast path (TermQuery) duration: {:?}", fast_path_duration);
    println!("Full evaluation (BoolQuery) duration: {:?}", full_eval_duration);

    // Fast path should be significantly faster
    let speedup = full_eval_duration.as_nanos() as f64 / fast_path_duration.as_nanos() as f64;
    println!("Fast path is {:.2}x faster", speedup);

    // Both should complete reasonably quickly
    assert!(fast_path_duration.as_millis() < 500);
    assert!(full_eval_duration.as_millis() < 1000);

    // Fast path should be at least 2x faster
    assert!(speedup >= 2.0);

    Ok(())
}
