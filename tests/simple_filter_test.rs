use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, TEXT};
use tantivy::{doc, Index, IndexWriter};

#[test]
fn test_simple_filter_aggregation() -> tantivy::Result<()> {
    // Create a simple index
    let mut schema_builder = Schema::builder();
    let category_field = schema_builder.add_text_field("category", TEXT | FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add some documents
    index_writer.add_document(doc!(category_field => "electronics"))?;
    index_writer.add_document(doc!(category_field => "electronics"))?;
    index_writer.add_document(doc!(category_field => "clothing"))?;
    index_writer.add_document(doc!(category_field => "books"))?;

    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test basic filter aggregation without sub-aggregations first
    let agg_request = json!({
        "electronics_only": {
            "filter": { "query_string": "category:electronics" }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify the filter aggregation exists
    assert!(agg_result.0.contains_key("electronics_only"));

    println!("Simple filter aggregation result: {:?}", agg_result);

    Ok(())
}

#[test]
fn test_filter_aggregation_with_count() -> tantivy::Result<()> {
    // Create a simple index
    let mut schema_builder = Schema::builder();
    let category_field = schema_builder.add_text_field("category", TEXT | FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Add some documents
    index_writer.add_document(doc!(category_field => "electronics"))?;
    index_writer.add_document(doc!(category_field => "electronics"))?;
    index_writer.add_document(doc!(category_field => "clothing"))?;
    index_writer.add_document(doc!(category_field => "books"))?;

    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Test filter aggregation with a simple count sub-aggregation
    let agg_request = json!({
        "electronics_only": {
            "filter": { "query_string": "category:electronics" },
            "aggs": {
                "doc_count": { "value_count": { "field": "category" } }
            }
        }
    });

    let aggregations: Aggregations = serde_json::from_value(agg_request)?;
    let collector = AggregationCollector::from_aggs(aggregations, Default::default());

    let agg_result = searcher.search(&AllQuery, &collector)?;

    // Verify the filter aggregation exists
    assert!(agg_result.0.contains_key("electronics_only"));

    println!("Filter aggregation with count result: {:?}", agg_result);

    // Should have 2 electronics documents

    Ok(())
}
