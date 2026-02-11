use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Float64Type, Int64Type};
use datafusion::prelude::*;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::schema::{SchemaBuilder, FAST, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};
use tantivy_datafusion::{translate_aggregations, TantivyTableProvider};

fn create_test_index() -> Index {
    let mut builder = SchemaBuilder::new();
    let _id = builder.add_u64_field("id", FAST);
    let _price = builder.add_f64_field("price", FAST);
    let _category = builder.add_text_field("category", TEXT | FAST);
    let schema = builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();

    let id_f = schema.get_field("id").unwrap();
    let price_f = schema.get_field("price").unwrap();
    let cat_f = schema.get_field("category").unwrap();

    let data = [
        (1u64, 1.5, "electronics"),
        (2, 2.5, "books"),
        (3, 3.5, "electronics"),
        (4, 4.5, "books"),
        (5, 5.5, "clothing"),
    ];
    for (id, price, cat) in &data {
        let mut doc = TantivyDocument::default();
        doc.add_u64(id_f, *id);
        doc.add_f64(price_f, *price);
        doc.add_text(cat_f, *cat);
        writer.add_document(doc).unwrap();
    }
    writer.commit().unwrap();
    index
}

async fn setup() -> SessionContext {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index)))
        .unwrap();
    ctx
}

fn collect(batches: &[arrow::array::RecordBatch]) -> arrow::array::RecordBatch {
    arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
}

/// Read string value from a column that may be StringArray or DictionaryArray.
fn string_val(col: &dyn Array, idx: usize) -> String {
    // Try as plain string first
    if let Some(s) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        return s.value(idx).to_string();
    }
    // Try as Dictionary<Int32, Utf8>
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
    {
        let values = dict.values().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        let key = dict.keys().value(idx) as usize;
        return values.value(key).to_string();
    }
    // Cast to string as fallback
    let cast = arrow::compute::cast(col, &arrow::datatypes::DataType::Utf8).unwrap();
    cast.as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(idx)
        .to_string()
}

// --- Metric-only tests ---

#[tokio::test]
async fn test_agg_translate_avg() {
    let ctx = setup().await;
    let df = ctx.table("f").await.unwrap();

    let aggs: Aggregations =
        serde_json::from_value(serde_json::json!({
            "avg_price": { "avg": { "field": "price" } }
        }))
        .unwrap();

    let results = translate_aggregations(df, &aggs).unwrap();
    let batches = results["avg_price"].clone().collect().await.unwrap();
    let batch = collect(&batches);

    assert_eq!(batch.num_rows(), 1);
    let avg = batch.column(0).as_primitive::<Float64Type>().value(0);
    assert!((avg - 3.5).abs() < 1e-10, "avg should be 3.5, got {avg}");
}

#[tokio::test]
async fn test_agg_translate_stats() {
    let ctx = setup().await;
    let df = ctx.table("f").await.unwrap();

    let aggs: Aggregations =
        serde_json::from_value(serde_json::json!({
            "s": { "stats": { "field": "price" } }
        }))
        .unwrap();

    let results = translate_aggregations(df, &aggs).unwrap();
    let batches = results["s"].clone().collect().await.unwrap();
    let batch = collect(&batches);

    assert_eq!(batch.num_rows(), 1);

    let schema = batch.schema();
    let min_idx = schema.index_of("s_min").unwrap();
    let max_idx = schema.index_of("s_max").unwrap();
    let sum_idx = schema.index_of("s_sum").unwrap();
    let count_idx = schema.index_of("s_count").unwrap();
    let avg_idx = schema.index_of("s_avg").unwrap();

    let min_val = batch.column(min_idx).as_primitive::<Float64Type>().value(0);
    let max_val = batch.column(max_idx).as_primitive::<Float64Type>().value(0);
    let sum_val = batch.column(sum_idx).as_primitive::<Float64Type>().value(0);
    let count_val = batch.column(count_idx).as_primitive::<Int64Type>().value(0);
    let avg_val = batch.column(avg_idx).as_primitive::<Float64Type>().value(0);

    assert!((min_val - 1.5).abs() < 1e-10);
    assert!((max_val - 5.5).abs() < 1e-10);
    assert!((sum_val - 17.5).abs() < 1e-10);
    assert_eq!(count_val, 5);
    assert!((avg_val - 3.5).abs() < 1e-10);
}

// --- Bucket: terms ---

#[tokio::test]
async fn test_agg_translate_terms() {
    let ctx = setup().await;
    let df = ctx.table("f").await.unwrap();

    let aggs: Aggregations =
        serde_json::from_value(serde_json::json!({
            "cats": { "terms": { "field": "category" } }
        }))
        .unwrap();

    let results = translate_aggregations(df, &aggs).unwrap();
    let batches = results["cats"].clone().collect().await.unwrap();
    let batch = collect(&batches);

    assert_eq!(batch.num_rows(), 3);

    // Default order: doc_count DESC
    let counts = batch.column(1).as_primitive::<Int64Type>();
    let cat_col = batch.column(0);

    // electronics=2, books=2 (tied), clothing=1
    assert!(counts.value(0) >= counts.value(1));
    assert!(counts.value(1) >= counts.value(2));
    assert_eq!(counts.value(2), 1);
    // The single clothing doc should be last
    assert_eq!(string_val(cat_col.as_ref(), 2), "clothing");
}

#[tokio::test]
async fn test_agg_translate_terms_with_metrics() {
    let ctx = setup().await;
    let df = ctx.table("f").await.unwrap();

    let aggs: Aggregations = serde_json::from_value(serde_json::json!({
        "cats": {
            "terms": { "field": "category" },
            "aggs": {
                "avg_price": { "avg": { "field": "price" } },
                "max_price": { "max": { "field": "price" } }
            }
        }
    }))
    .unwrap();

    let results = translate_aggregations(df, &aggs).unwrap();
    let batches = results["cats"].clone().collect().await.unwrap();
    let batch = collect(&batches);

    assert_eq!(batch.num_rows(), 3);

    let schema = batch.schema();
    assert!(schema.index_of("category").is_ok());
    assert!(schema.index_of("doc_count").is_ok());
    assert!(schema.index_of("avg_price").is_ok());
    assert!(schema.index_of("max_price").is_ok());

    // Find the clothing row and verify its sub-agg values
    let cat_col = batch.column(schema.index_of("category").unwrap());
    let avg_prices = batch
        .column(schema.index_of("avg_price").unwrap())
        .as_primitive::<Float64Type>();
    let max_prices = batch
        .column(schema.index_of("max_price").unwrap())
        .as_primitive::<Float64Type>();

    for i in 0..batch.num_rows() {
        if string_val(cat_col.as_ref(), i) == "clothing" {
            assert!((avg_prices.value(i) - 5.5).abs() < 1e-10);
            assert!((max_prices.value(i) - 5.5).abs() < 1e-10);
        }
    }
}

// --- Bucket: histogram ---

#[tokio::test]
async fn test_agg_translate_histogram() {
    let ctx = setup().await;
    let df = ctx.table("f").await.unwrap();

    let aggs: Aggregations = serde_json::from_value(serde_json::json!({
        "price_hist": {
            "histogram": { "field": "price", "interval": 2.0 }
        }
    }))
    .unwrap();

    let results = translate_aggregations(df, &aggs).unwrap();
    let batches = results["price_hist"].clone().collect().await.unwrap();
    let batch = collect(&batches);

    // Prices: 1.5, 2.5, 3.5, 4.5, 5.5
    // Buckets: 0.0 (1.5), 2.0 (2.5, 3.5), 4.0 (4.5, 5.5)
    assert_eq!(batch.num_rows(), 3);

    let buckets = batch.column(0).as_primitive::<Float64Type>();
    let counts = batch.column(1).as_primitive::<Int64Type>();

    assert!((buckets.value(0) - 0.0).abs() < 1e-10);
    assert_eq!(counts.value(0), 1);

    assert!((buckets.value(1) - 2.0).abs() < 1e-10);
    assert_eq!(counts.value(1), 2);

    assert!((buckets.value(2) - 4.0).abs() < 1e-10);
    assert_eq!(counts.value(2), 2);
}

// --- Bucket: range ---

#[tokio::test]
async fn test_agg_translate_range() {
    let ctx = setup().await;
    let df = ctx.table("f").await.unwrap();

    let aggs: Aggregations = serde_json::from_value(serde_json::json!({
        "price_ranges": {
            "range": {
                "field": "price",
                "ranges": [
                    { "key": "cheap", "to": 3.0 },
                    { "key": "mid", "from": 3.0, "to": 5.0 },
                    { "key": "expensive", "from": 5.0 }
                ]
            }
        }
    }))
    .unwrap();

    let results = translate_aggregations(df, &aggs).unwrap();
    let batches = results["price_ranges"].clone().collect().await.unwrap();
    let batch = collect(&batches);

    assert_eq!(batch.num_rows(), 3);

    let bucket_col = batch.column(0);
    let counts = batch.column(1).as_primitive::<Int64Type>();

    // Sorted alphabetically: cheap, expensive, mid
    let mut rows: Vec<(String, i64)> = (0..3)
        .map(|i| (string_val(bucket_col.as_ref(), i), counts.value(i)))
        .collect();
    rows.sort_by_key(|(k, _)| k.clone());

    assert_eq!(rows[0], ("cheap".to_string(), 2));      // 1.5, 2.5
    assert_eq!(rows[1], ("expensive".to_string(), 1));  // 5.5
    assert_eq!(rows[2], ("mid".to_string(), 2));        // 3.5, 4.5
}

// --- Metric-only with pre-filtered DataFrame ---

#[tokio::test]
async fn test_agg_translate_with_filter() {
    let ctx = setup().await;
    let df = ctx
        .table("f")
        .await
        .unwrap()
        .filter(col("price").gt(lit(2.0)))
        .unwrap();

    let aggs: Aggregations =
        serde_json::from_value(serde_json::json!({
            "avg_price": { "avg": { "field": "price" } }
        }))
        .unwrap();

    let results = translate_aggregations(df, &aggs).unwrap();
    let batches = results["avg_price"].clone().collect().await.unwrap();
    let batch = collect(&batches);

    assert_eq!(batch.num_rows(), 1);
    // avg of {2.5, 3.5, 4.5, 5.5} = 4.0
    let avg = batch.column(0).as_primitive::<Float64Type>().value(0);
    assert!((avg - 4.0).abs() < 1e-10, "avg should be 4.0, got {avg}");
}

// --- Cardinality ---

#[tokio::test]
async fn test_agg_translate_cardinality() {
    let ctx = setup().await;
    let df = ctx.table("f").await.unwrap();

    let aggs: Aggregations =
        serde_json::from_value(serde_json::json!({
            "unique_cats": { "cardinality": { "field": "category" } }
        }))
        .unwrap();

    let results = translate_aggregations(df, &aggs).unwrap();
    let batches = results["unique_cats"].clone().collect().await.unwrap();
    let batch = collect(&batches);

    assert_eq!(batch.num_rows(), 1);
    // 3 distinct categories: electronics, books, clothing
    let col = batch.column(0);
    let distinct: u64 = if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        arr.value(0)
    } else {
        batch.column(0).as_primitive::<Int64Type>().value(0) as u64
    };
    assert_eq!(distinct, 3);
}
