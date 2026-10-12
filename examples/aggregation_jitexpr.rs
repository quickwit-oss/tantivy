// # Jitexpr fields in aggregations
//
// This example shows how aggregations can use a jitexpr field: a field that is not in the
// index, but computed from the fast fields of each document with a jitexpr expression.
//
// - A jitexpr field is registered under a name in a `ValueSourceRegistry`
// - Aggregations use that name like the name of a fast field.
//
// Requires the `jitexpr` feature.
// ---

use std::sync::Arc;

use serde_json::{json, Value};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::{
    AggContextParams, AggregationCollector, JitExprValueSourceProvider, ValueSourceRegistry,
};
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, STRING};
use tantivy::{doc, Index, IndexWriter};

fn main() -> tantivy::Result<()> {
    // # Create Schema
    //
    // Aggregations read fast fields: the inputs of the jitexpr field must be fast.
    let mut schema_builder = Schema::builder();
    let category = schema_builder.add_text_field("category", STRING | FAST);
    let price = schema_builder.add_f64_field("price", FAST);
    let stock = schema_builder.add_f64_field("stock", FAST);
    let schema = schema_builder.build();

    // # Indexing documents
    let index = Index::create_in_ram(schema);
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;
    index_writer.add_document(doc!(category => "Womens Footwear", price => 99.0, stock => 5.0))?;
    index_writer.add_document(doc!(category => "Womens Footwear", price => 59.0, stock => 8.0))?;
    index_writer.add_document(doc!(category => "Mens Footwear", price => 120.0, stock => 2.0))?;
    index_writer.add_document(doc!(category => "Mens Footwear", price => 75.0, stock => 10.0))?;
    // A document without a price: the jitexpr field has no value for it.
    index_writer.add_document(doc!(category => "Mens Footwear", stock => 3.0))?;
    index_writer.commit()?;

    // # Registering the jitexpr field
    //
    // The expression is written in the jitexpr s-expression syntax. Variables (e.g. `price`) are
    // resolved as fast field names, for each segment.
    //
    // A jitexpr field shadows a fast field with the same name. Here, the name starts with `#`
    // so that it cannot collide with a fast field.
    let expression = tantivy::jitexpr::ast::deserialize("(MULTIPLY price stock)")
        .map_err(|err| tantivy::TantivyError::InvalidArgument(format!("{err:?}")))?;
    let provider = JitExprValueSourceProvider::new(expression)
        .map_err(|err| tantivy::TantivyError::InvalidArgument(format!("{err:?}")))?;
    let mut value_sources = ValueSourceRegistry::default();
    value_sources.register("#stock_value", Arc::new(provider));
    let context = AggContextParams::default().with_value_sources(Arc::new(value_sources));

    // # Aggregating over the jitexpr field
    //
    // The total stock value of each category.
    let aggs: Aggregations = serde_json::from_value(json!({
        "categories": {
            "terms": { "field": "category" },
            "aggs": {
                "total_stock_value": { "sum": { "field": "#stock_value" } }
            }
        }
    }))?;
    let collector = AggregationCollector::from_aggs(aggs, context);

    let searcher = index.reader()?.searcher();
    let agg_res = searcher.search(&AllQuery, &collector)?;
    let res: Value = serde_json::to_value(agg_res)?;
    println!("{}", serde_json::to_string_pretty(&res)?);

    let buckets = &res["categories"]["buckets"];
    assert_eq!(buckets[0]["key"], "Mens Footwear");
    // 120 * 2 + 75 * 10. The document without a price has no stock value.
    assert_eq!(buckets[0]["total_stock_value"]["value"], 990.0);
    assert_eq!(buckets[1]["key"], "Womens Footwear");
    // 99 * 5 + 59 * 8.
    assert_eq!(buckets[1]["total_stock_value"]["value"], 967.0);

    Ok(())
}
