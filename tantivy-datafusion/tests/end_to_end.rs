use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use datafusion::prelude::*;
use tantivy::schema::{SchemaBuilder, FAST, STORED, TEXT};
use tantivy::{doc, Index, IndexWriter};
use tantivy_datafusion::TantivyTableProvider;

fn create_test_index() -> Index {
    let mut builder = SchemaBuilder::new();
    let u64_field = builder.add_u64_field("id", FAST | STORED);
    let i64_field = builder.add_i64_field("score", FAST);
    let f64_field = builder.add_f64_field("price", FAST);
    let bool_field = builder.add_bool_field("active", FAST);
    let text_field = builder.add_text_field("category", TEXT | FAST | STORED);
    let schema = builder.build();

    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();

    writer
        .add_document(doc!(
            u64_field => 1u64,
            i64_field => 10i64,
            f64_field => 1.5f64,
            bool_field => true,
            text_field => "electronics",
        ))
        .unwrap();

    writer
        .add_document(doc!(
            u64_field => 2u64,
            i64_field => 20i64,
            f64_field => 2.5f64,
            bool_field => false,
            text_field => "books",
        ))
        .unwrap();

    writer
        .add_document(doc!(
            u64_field => 3u64,
            i64_field => 30i64,
            f64_field => 3.5f64,
            bool_field => true,
            text_field => "electronics",
        ))
        .unwrap();

    writer
        .add_document(doc!(
            u64_field => 4u64,
            i64_field => 40i64,
            f64_field => 4.5f64,
            bool_field => false,
            text_field => "books",
        ))
        .unwrap();

    writer
        .add_document(doc!(
            u64_field => 5u64,
            i64_field => 50i64,
            f64_field => 5.5f64,
            bool_field => true,
            text_field => "clothing",
        ))
        .unwrap();

    writer.commit().unwrap();
    index
}

fn collect_batches(batches: &[RecordBatch]) -> RecordBatch {
    arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
}

#[tokio::test]
async fn test_select_all() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    let df = ctx.sql("SELECT * FROM test_index").await.unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.num_columns(), 5); // id, score, price, active, category
}

#[tokio::test]
async fn test_projection_and_filter() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    let df = ctx
        .sql("SELECT id, category FROM test_index WHERE id > 2")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);

    // Check that the id values are > 2
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    let mut id_values: Vec<u64> = ids.iter().map(|v| v.unwrap()).collect();
    id_values.sort();
    assert_eq!(id_values, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_aggregation() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    let df = ctx
        .sql("SELECT COUNT(*) as cnt, AVG(price) as avg_price FROM test_index")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 1);

    let count = batch.column(0).as_primitive::<Int64Type>().value(0);
    assert_eq!(count, 5);

    let avg_price = batch.column(1).as_primitive::<Float64Type>().value(0);
    assert!((avg_price - 3.5).abs() < 1e-10); // (1.5+2.5+3.5+4.5+5.5)/5 = 3.5
}

#[tokio::test]
async fn test_group_by() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    let df = ctx
        .sql("SELECT category, COUNT(*) as cnt FROM test_index GROUP BY category ORDER BY category")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 3);

    let categories = batch.column(0).as_string::<i32>();
    let counts = batch.column(1).as_primitive::<Int64Type>();

    // Sorted: books(2), clothing(1), electronics(2)
    assert_eq!(categories.value(0), "books");
    assert_eq!(counts.value(0), 2);
    assert_eq!(categories.value(1), "clothing");
    assert_eq!(counts.value(1), 1);
    assert_eq!(categories.value(2), "electronics");
    assert_eq!(counts.value(2), 2);
}
