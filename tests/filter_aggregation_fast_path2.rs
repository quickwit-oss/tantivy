use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
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
            "filter": "category:electronics",
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(text_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

    // Test TermQuery fast path for u64 field
    let u64_agg = json!({
        "price_150": {
            "filter": "price:150",
            "aggs": {
                "count": { "value_count": { "field": "category" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(u64_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

    // Test TermQuery fast path for bool field
    let bool_agg = json!({
        "in_stock": {
            "filter": "in_stock:true",
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(bool_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

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
            "filter": "price:[500 TO 600]",
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(u64_range_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

    // Test RangeQuery fast path for f64 field
    let f64_range_agg = json!({
        "mid_rating": {
            "filter": "rating:[5.0 TO 7.0]",
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(f64_range_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

    // Test RangeQuery fast path for i64 field
    let i64_range_agg = json!({
        "positive_scores": {
            "filter": "score:[0 TO *]",
            "aggs": {
                "count": { "value_count": { "field": "score" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(i64_range_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

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
            "filter": "category:electronics",
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(simple_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

    // Test complex boolean query (should fall back to full evaluation)
    let complex_agg = json!({
        "complex": {
            "filter": "category:electronics AND price:[200 TO 300]",
            "aggs": {
                "count": { "value_count": { "field": "price" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(complex_agg)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());
    let _result = searcher.search(&AllQuery, &collector)?;

    Ok(())
}
