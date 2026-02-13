use std::net::Ipv4Addr;
use std::sync::Arc;

use arrow::array::{AsArray, ListArray, RecordBatch};
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use tantivy::query::TermQuery;
use tantivy::schema::{Field, IndexRecordOption, SchemaBuilder, Term, FAST, STORED, TEXT};
use tantivy::{doc, DateTime, Index, IndexWriter, TantivyDocument};
use tantivy_datafusion::{
    full_text_udf, FastFieldFilterPushdown, TantivyDocumentProvider,
    TantivyInvertedIndexProvider, TantivyTableProvider, TopKPushdown,
    UnifiedTantivyTableProvider,
};

fn plan_to_string(batches: &[RecordBatch]) -> String {
    let batch = collect_batches(batches);
    let plan_col = batch.column(1).as_string::<i32>();
    (0..batch.num_rows())
        .map(|i| plan_col.value(i))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Builds the shared test schema with all supported field types.
fn build_test_schema() -> (
    tantivy::schema::Schema,
    Field, // id (u64)
    Field, // score (i64)
    Field, // price (f64)
    Field, // active (bool)
    Field, // category (text)
    Field, // created_at (date)
    Field, // ip_address (ip)
    Field, // data (bytes)
) {
    let mut builder = SchemaBuilder::new();
    let u64_field = builder.add_u64_field("id", FAST | STORED);
    let i64_field = builder.add_i64_field("score", FAST);
    let f64_field = builder.add_f64_field("price", FAST);
    let bool_field = builder.add_bool_field("active", FAST);
    let text_field = builder.add_text_field("category", TEXT | FAST | STORED);
    let date_field = builder.add_date_field("created_at", FAST);
    let ip_field = builder.add_ip_addr_field("ip_address", FAST);
    let bytes_field = builder.add_bytes_field("data", FAST);
    let schema = builder.build();
    (
        schema,
        u64_field,
        i64_field,
        f64_field,
        bool_field,
        text_field,
        date_field,
        ip_field,
        bytes_field,
    )
}

/// Adds the 5 standard test documents to the writer.
fn add_test_documents(
    writer: &IndexWriter,
    fields: (Field, Field, Field, Field, Field, Field, Field, Field),
) {
    let (u64_field, i64_field, f64_field, bool_field, text_field, date_field, ip_field, bytes_field) =
        fields;

    // Timestamps: 1_000_000, 2_000_000, ... microseconds
    let timestamps = [1_000_000i64, 2_000_000, 3_000_000, 4_000_000, 5_000_000];
    let ips: [Ipv4Addr; 5] = [
        Ipv4Addr::new(192, 168, 1, 1),
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 168, 1, 2),
        Ipv4Addr::new(10, 0, 0, 2),
        Ipv4Addr::new(172, 16, 0, 1),
    ];
    let data_payloads: [&[u8]; 5] = [b"aaa", b"bbb", b"ccc", b"ddd", b"eee"];

    let ids = [1u64, 2, 3, 4, 5];
    let scores = [10i64, 20, 30, 40, 50];
    let prices = [1.5f64, 2.5, 3.5, 4.5, 5.5];
    let actives = [true, false, true, false, true];
    let categories = ["electronics", "books", "electronics", "books", "clothing"];

    for i in 0..5 {
        let mut doc = TantivyDocument::default();
        doc.add_u64(u64_field, ids[i]);
        doc.add_i64(i64_field, scores[i]);
        doc.add_f64(f64_field, prices[i]);
        doc.add_bool(bool_field, actives[i]);
        doc.add_text(text_field, categories[i]);
        doc.add_date(date_field, DateTime::from_timestamp_micros(timestamps[i]));
        doc.add_ip_addr(ip_field, ips[i].to_ipv6_mapped());
        doc.add_bytes(bytes_field, data_payloads[i]);
        writer.add_document(doc).unwrap();
    }
}

fn create_test_index() -> Index {
    let (schema, u64_f, i64_f, f64_f, bool_f, text_f, date_f, ip_f, bytes_f) =
        build_test_schema();

    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();

    add_test_documents(
        &writer,
        (u64_f, i64_f, f64_f, bool_f, text_f, date_f, ip_f, bytes_f),
    );
    writer.commit().unwrap();
    index
}

/// Creates an index with 2 segments (commit after first 3 docs, then remaining 2).
fn create_multi_segment_test_index() -> Index {
    let (schema, u64_f, i64_f, f64_f, bool_f, text_f, date_f, ip_f, bytes_f) =
        build_test_schema();

    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();

    let timestamps = [1_000_000i64, 2_000_000, 3_000_000, 4_000_000, 5_000_000];
    let ips: [Ipv4Addr; 5] = [
        Ipv4Addr::new(192, 168, 1, 1),
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 168, 1, 2),
        Ipv4Addr::new(10, 0, 0, 2),
        Ipv4Addr::new(172, 16, 0, 1),
    ];
    let data_payloads: [&[u8]; 5] = [b"aaa", b"bbb", b"ccc", b"ddd", b"eee"];
    let ids = [1u64, 2, 3, 4, 5];
    let scores = [10i64, 20, 30, 40, 50];
    let prices = [1.5f64, 2.5, 3.5, 4.5, 5.5];
    let actives = [true, false, true, false, true];
    let categories = ["electronics", "books", "electronics", "books", "clothing"];

    // Segment 1: docs 0..3
    for i in 0..3 {
        let mut doc = TantivyDocument::default();
        doc.add_u64(u64_f, ids[i]);
        doc.add_i64(i64_f, scores[i]);
        doc.add_f64(f64_f, prices[i]);
        doc.add_bool(bool_f, actives[i]);
        doc.add_text(text_f, categories[i]);
        doc.add_date(date_f, DateTime::from_timestamp_micros(timestamps[i]));
        doc.add_ip_addr(ip_f, ips[i].to_ipv6_mapped());
        doc.add_bytes(bytes_f, data_payloads[i]);
        writer.add_document(doc).unwrap();
    }
    writer.commit().unwrap();

    // Segment 2: docs 3..5
    for i in 3..5 {
        let mut doc = TantivyDocument::default();
        doc.add_u64(u64_f, ids[i]);
        doc.add_i64(i64_f, scores[i]);
        doc.add_f64(f64_f, prices[i]);
        doc.add_bool(bool_f, actives[i]);
        doc.add_text(text_f, categories[i]);
        doc.add_date(date_f, DateTime::from_timestamp_micros(timestamps[i]));
        doc.add_ip_addr(ip_f, ips[i].to_ipv6_mapped());
        doc.add_bytes(bytes_f, data_payloads[i]);
        writer.add_document(doc).unwrap();
    }
    writer.commit().unwrap();

    index
}

/// Creates an index where docs with id=2 and id=4 are deleted.
fn create_test_index_with_deletes() -> Index {
    let (schema, u64_f, i64_f, f64_f, bool_f, text_f, date_f, ip_f, bytes_f) =
        build_test_schema();

    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();

    add_test_documents(
        &writer,
        (u64_f, i64_f, f64_f, bool_f, text_f, date_f, ip_f, bytes_f),
    );
    writer.commit().unwrap();

    // Delete docs with id=2 and id=4
    writer.delete_term(Term::from_field_u64(u64_f, 2u64));
    writer.delete_term(Term::from_field_u64(u64_f, 4u64));
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
    // _doc_id, _segment_ord, id, score, price, active, category, created_at, ip_address, data
    assert_eq!(batch.num_columns(), 10);
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

    // Category column may be Dictionary<Int32, Utf8> or Utf8 depending on
    // DataFusion's aggregate output. Cast to Utf8 to read uniformly.
    let cat_col = arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Utf8).unwrap();
    let categories = cat_col.as_string::<i32>();
    let counts = batch.column(1).as_primitive::<Int64Type>();

    // Sorted: books(2), clothing(1), electronics(2)
    assert_eq!(categories.value(0), "books");
    assert_eq!(counts.value(0), 2);
    assert_eq!(categories.value(1), "clothing");
    assert_eq!(counts.value(1), 1);
    assert_eq!(categories.value(2), "electronics");
    assert_eq!(counts.value(2), 2);
}

// --- Filter pushdown tests ---

fn get_test_field(index: &Index, name: &str) -> Field {
    index.schema().get_field(name).unwrap()
}

#[tokio::test]
async fn test_filter_pushdown_sql() {
    // SQL WHERE clause should be pushed down to tantivy via supports_filters_pushdown
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    // id > 2 should push down as a RangeQuery
    let df = ctx
        .sql("SELECT id, score FROM test_index WHERE id > 2")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 3);
    let mut ids: Vec<u64> = batch
        .column(0)
        .as_primitive::<UInt64Type>()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_filter_pushdown_equality() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    // active = true should push down as a TermQuery
    let df = ctx
        .sql("SELECT id FROM test_index WHERE active = true")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 3); // ids 1, 3, 5
    let mut ids: Vec<u64> = batch
        .column(0)
        .as_primitive::<UInt64Type>()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3, 5]);
}

#[tokio::test]
async fn test_direct_tantivy_query() {
    // Use new_with_query to pass a tantivy query directly — no Expr conversion
    let index = create_test_index();
    let category_field = get_test_field(&index, "category");

    // TermQuery for "electronics" in the category field
    let query = TermQuery::new(
        Term::from_field_text(category_field, "electronics"),
        IndexRecordOption::Basic,
    );
    let provider = TantivyTableProvider::new_with_query(index, Box::new(query));

    let ctx = SessionContext::new();
    let df = ctx.read_table(Arc::new(provider)).unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2); // ids 1 and 3
    let id_col_idx = batch.schema().index_of("id").unwrap();
    let mut ids: Vec<u64> = batch
        .column(id_col_idx)
        .as_primitive::<UInt64Type>()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}

#[tokio::test]
async fn test_combined_tantivy_query_and_sql_filter() {
    // Direct tantivy query (category = "electronics") + SQL filter (id > 1)
    let index = create_test_index();
    let category_field = get_test_field(&index, "category");

    let query = TermQuery::new(
        Term::from_field_text(category_field, "electronics"),
        IndexRecordOption::Basic,
    );
    let provider = TantivyTableProvider::new_with_query(index, Box::new(query));

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    // category = "electronics" gives ids {1, 3}, then id > 1 gives {3}
    let df = ctx
        .sql("SELECT id FROM test_index WHERE id > 1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 3);
}

#[tokio::test]
async fn test_filter_pushdown_compound() {
    // Test AND/OR pushdown: id > 2 AND score < 45
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    let df = ctx
        .sql("SELECT id FROM test_index WHERE id > 2 AND score < 45")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // id > 2 → {3, 4, 5}, score < 45 → {1(10), 2(20), 3(30), 4(40)}
    // Intersection: {3, 4}
    assert_eq!(batch.num_rows(), 2);
    let mut ids: Vec<u64> = batch
        .column(0)
        .as_primitive::<UInt64Type>()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, vec![3, 4]);
}

// --- Multi-valued field tests ---

fn create_multivalued_test_index() -> Index {
    let mut builder = SchemaBuilder::new();
    let id_field = builder.add_u64_field("id", FAST);
    let tags_field = builder.add_u64_field("tags", FAST);
    let schema = builder.build();

    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();

    // Doc 0: tags [10, 20]
    writer
        .add_document(doc!(id_field => 1u64, tags_field => 10u64, tags_field => 20u64))
        .unwrap();
    // Doc 1: tags [30]
    writer
        .add_document(doc!(id_field => 2u64, tags_field => 30u64))
        .unwrap();
    // Doc 2: tags [10, 30, 40]
    writer
        .add_document(doc!(
            id_field => 3u64,
            tags_field => 10u64,
            tags_field => 30u64,
            tags_field => 40u64
        ))
        .unwrap();

    writer.commit().unwrap();
    index
}

#[tokio::test]
async fn test_multivalued_field_schema() {
    use arrow::datatypes::DataType;

    let index = create_multivalued_test_index();
    let provider = TantivyTableProvider::new(index);

    let schema = provider.schema();
    let tags_field = schema.field_with_name("tags").unwrap();
    assert!(
        matches!(tags_field.data_type(), DataType::List(_)),
        "Expected List type for multi-valued field, got {:?}",
        tags_field.data_type()
    );

    // id should remain scalar
    let id_field = schema.field_with_name("id").unwrap();
    assert_eq!(id_field.data_type(), &DataType::UInt64);
}

#[tokio::test]
async fn test_multivalued_field_values() {
    let index = create_multivalued_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_mv", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT id, tags FROM test_mv ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 3);

    // id column is scalar u64
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);

    // tags column is List<UInt64>
    let tags_list = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("tags should be a ListArray");

    // Doc 1: tags [10, 20]
    let row0 = tags_list.value(0);
    let row0_vals: Vec<u64> = row0.as_primitive::<UInt64Type>().iter().map(|v| v.unwrap()).collect();
    assert_eq!(row0_vals, vec![10, 20]);

    // Doc 2: tags [30]
    let row1 = tags_list.value(1);
    let row1_vals: Vec<u64> = row1.as_primitive::<UInt64Type>().iter().map(|v| v.unwrap()).collect();
    assert_eq!(row1_vals, vec![30]);

    // Doc 3: tags [10, 30, 40]
    let row2 = tags_list.value(2);
    let row2_vals: Vec<u64> = row2.as_primitive::<UInt64Type>().iter().map(|v| v.unwrap()).collect();
    assert_eq!(row2_vals, vec![10, 30, 40]);
}

// --- Limit pushdown tests ---

#[tokio::test]
async fn test_limit_pushdown() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    let df = ctx
        .sql("SELECT id FROM test_index LIMIT 2")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
}

#[tokio::test]
async fn test_limit_with_filter() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    // id > 2 gives {3,4,5}, LIMIT 1 should give exactly 1 row
    let df = ctx
        .sql("SELECT id FROM test_index WHERE id > 2 LIMIT 1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert!(id > 2);
}

// --- full_text() UDF + inverted index join tests ---

#[tokio::test]
async fn test_full_text_join_basic() {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    let df = ctx
        .sql(
            "SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 3);
}

#[tokio::test]
async fn test_full_text_join_with_sql_filter() {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    let df = ctx
        .sql(
            "SELECT f.id, f.price \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0 \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics = ids {1, 3}, price > 2.0 = ids {2, 3, 4, 5} → intersection = {3}
    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 3);
}

#[tokio::test]
async fn test_full_text_join_no_matches() {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    let df = ctx
        .sql(
            "SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'nonexistent_xyz')",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_full_text_multiple_predicates() {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    // "electronics" and "books" are disjoint sets → intersection = 0 rows
    let df = ctx
        .sql(
            "SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND full_text(inv.category, 'books')",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_dynamic_filter_in_plan() {
    let index = create_test_index();

    // Set target_partitions = num_segments (1) so the optimizer doesn't
    // add RepartitionExec nodes to increase parallelism beyond segments.
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    // EXPLAIN the join query — inv listed first so optimizer places the
    // inverted index (small, filtered) on the build side and fast fields
    // (large) on the probe side, enabling dynamic filter pushdown.
    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics')",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    // Full physical plan showing:
    //
    // - No RepartitionExec: both sides declare Hash([_doc_id, _segment_ord], 1)
    //   so the optimizer recognises them as co-partitioned by segment.
    //   Each segment is joined locally — no shuffle needed.
    //
    // - inv (InvertedIndexDataSource) on the BUILD side (left child of HashJoinExec)
    // - f (FastFieldDataSource) on the PROBE side (right child of HashJoinExec)
    //
    // - DynamicFilter pushed into FastFieldDataSource from the hash join.
    //   At runtime, once the build side completes, the DynamicFilter is
    //   populated with min/max bounds on (_doc_id, _segment_ord) from the
    //   hash table, pruning non-matching rows on the probe side.
    let expected_physical_plan = "\
HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[id@4]
  CooperativeExec
    DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=None)
  CooperativeExec
    DataSourceExec: FastFieldDataSource(partitions=1, query=false, limit=None, pushed_filters=[DynamicFilter [ empty ]])";

    let physical_plan: String = plan
        .lines()
        .skip_while(|line| !line.starts_with("HashJoinExec"))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        physical_plan, expected_physical_plan,
        "Physical plan mismatch.\n\nActual:\n{physical_plan}\n\nExpected:\n{expected_physical_plan}"
    );
}

// --- Document provider tests ---

#[tokio::test]
async fn test_document_fetch_basic() {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table(
        "inv",
        Arc::new(TantivyInvertedIndexProvider::new(index.clone())),
    )
    .unwrap();
    ctx.register_table(
        "d",
        Arc::new(TantivyDocumentProvider::new(index)),
    )
    .unwrap();

    let df = ctx
        .sql(
            "SELECT d._document \
             FROM inv \
             JOIN d ON d._doc_id = inv._doc_id AND d._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') \
             ORDER BY d._doc_id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);

    let docs = batch.column(0).as_string::<i32>();
    for i in 0..2 {
        let json: serde_json::Value = serde_json::from_str(docs.value(i)).unwrap();
        assert!(json.get("id").is_some(), "document should contain 'id'");
        assert!(
            json.get("category").is_some(),
            "document should contain 'category'"
        );
    }
}

#[tokio::test]
async fn test_document_three_way_join() {
    let index = create_test_index();

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table(
        "inv",
        Arc::new(TantivyInvertedIndexProvider::new(index.clone())),
    )
    .unwrap();
    ctx.register_table(
        "f",
        Arc::new(TantivyTableProvider::new(index.clone())),
    )
    .unwrap();
    ctx.register_table(
        "d",
        Arc::new(TantivyDocumentProvider::new(index)),
    )
    .unwrap();

    let df = ctx
        .sql(
            "SELECT f.id, f.price, d._document \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             JOIN d ON d._doc_id = f._doc_id AND d._segment_ord = f._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0 \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics = ids {1, 3}, price > 2.0 = ids {2, 3, 4, 5} → intersection = {3}
    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 3);

    let price = batch.column(1).as_primitive::<Float64Type>().value(0);
    assert!((price - 3.5).abs() < 1e-10);

    let doc_json: serde_json::Value =
        serde_json::from_str(batch.column(2).as_string::<i32>().value(0)).unwrap();
    assert_eq!(doc_json["id"][0], 3);
    assert_eq!(doc_json["category"][0], "electronics");
}

#[tokio::test]
async fn test_document_three_way_join_plan() {
    let index = create_test_index();

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table(
        "inv",
        Arc::new(TantivyInvertedIndexProvider::new(index.clone())),
    )
    .unwrap();
    ctx.register_table(
        "f",
        Arc::new(TantivyTableProvider::new(index.clone())),
    )
    .unwrap();
    ctx.register_table(
        "d",
        Arc::new(TantivyDocumentProvider::new(index)),
    )
    .unwrap();

    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT f.id, f.price, d._document \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             JOIN d ON d._doc_id = f._doc_id AND d._segment_ord = f._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0 \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    // Full physical plan for the three-way join showing:
    //
    // - No RepartitionExec: all three providers declare
    //   Hash([_doc_id, _segment_ord], 1) so the optimizer recognises them as
    //   co-partitioned by segment — no shuffle needed.
    //
    // - Two CollectLeft HashJoinExecs chained:
    //   1. inv (build) ⋈ f (probe) — DynamicFilter + price pushdown into f
    //   2. result (build) ⋈ d (probe) — document provider is the probe side
    //
    // - DynamicFilter pushed into FastFieldDataSource from the first hash
    //   join; DynamicFilter also pushed into DocumentDataSource from the
    //   second hash join, pruning stored-field reads at scan time.
    let expected_physical_plan = "\
SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[false]
  HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[id@2, price@3, _document@6]
    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[_doc_id@2, _segment_ord@3, id@4, price@5]
      CooperativeExec
        DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=None)
      CooperativeExec
        DataSourceExec: FastFieldDataSource(partitions=1, query=false, limit=None, pushed_filters=[price@3 > 2, DynamicFilter [ empty ]])
    CooperativeExec
      DataSourceExec: DocumentDataSource(segments=1, pushed_filters=[DynamicFilter [ empty ]])";

    let physical_plan: String = plan
        .lines()
        .skip_while(|line| !line.starts_with("SortExec"))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        physical_plan, expected_physical_plan,
        "Physical plan mismatch.\n\nActual:\n{physical_plan}\n\nExpected:\n{expected_physical_plan}"
    );
}

#[tokio::test]
async fn test_document_standalone() {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_table(
        "docs",
        Arc::new(TantivyDocumentProvider::new(index)),
    )
    .unwrap();

    let df = ctx
        .sql("SELECT _document FROM docs ORDER BY _doc_id LIMIT 3")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 3);

    let docs = batch.column(0).as_string::<i32>();
    for i in 0..3 {
        let json: serde_json::Value = serde_json::from_str(docs.value(i)).unwrap();
        assert!(json.is_object(), "each _document should be a JSON object");
    }
}

// --- _score column tests ---

#[tokio::test]
async fn test_score_column_in_schema() {
    use arrow::datatypes::DataType;

    let index = create_test_index();
    let provider = TantivyInvertedIndexProvider::new(index);
    let schema = provider.schema();

    let score_field = schema.field_with_name("_score").unwrap();
    assert_eq!(score_field.data_type(), &DataType::Float32);
    assert!(score_field.is_nullable());
}

#[tokio::test]
async fn test_score_with_full_text() {
    use arrow::datatypes::Float32Type;

    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    // Query for "electronics" — 2 matching docs, should have BM25 scores
    let df = ctx
        .sql(
            "SELECT inv._doc_id, inv._score \
             FROM inv \
             WHERE full_text(inv.category, 'electronics') \
             ORDER BY inv._score DESC",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);

    let scores = batch.column(1).as_primitive::<Float32Type>();
    // Both scores should be > 0
    assert!(scores.value(0) > 0.0, "score should be positive");
    assert!(scores.value(1) > 0.0, "score should be positive");
    // Sorted descending
    assert!(
        scores.value(0) >= scores.value(1),
        "scores should be descending"
    );
}

#[tokio::test]
async fn test_score_not_projected() {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    // _score not in SELECT — should still work, no scoring overhead
    let df = ctx
        .sql(
            "SELECT inv._doc_id \
             FROM inv \
             WHERE full_text(inv.category, 'electronics')",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 1); // Only _doc_id
}

#[tokio::test]
async fn test_score_null_without_query() {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    // No query — _score should be null for all rows
    let df = ctx
        .sql("SELECT inv._doc_id, inv._score FROM inv ORDER BY inv._doc_id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 5);
    let scores = batch.column(1);
    // All scores should be null
    for i in 0..5 {
        assert!(scores.is_null(i), "score should be null without query");
    }
}

// --- TopK pushdown tests ---

fn create_topk_session(index: &Index) -> SessionContext {
    use datafusion::execution::SessionStateBuilder;

    let config = SessionConfig::new().with_target_partitions(1);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(TopKPushdown::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(full_text_udf());
    ctx.register_table(
        "inv",
        Arc::new(TantivyInvertedIndexProvider::new(index.clone())),
    )
    .unwrap();
    ctx
}

#[tokio::test]
async fn test_topk_pushdown_plan() {
    let index = create_test_index();
    let ctx = create_topk_session(&index);

    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT inv._doc_id, inv._score \
             FROM inv \
             WHERE full_text(inv.category, 'electronics') \
             ORDER BY inv._score DESC LIMIT 1",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    // The plan should show topk=Some(1) in the InvertedIndexDataSource
    assert!(
        plan.contains("topk=Some(1)"),
        "Plan should contain topk=Some(1).\n\nActual plan:\n{plan}"
    );
}

#[tokio::test]
async fn test_topk_result_correctness() {
    use arrow::datatypes::Float32Type;

    let index = create_test_index();
    let ctx = create_topk_session(&index);

    // TopK=1 should return the single highest-scoring doc
    let df = ctx
        .sql(
            "SELECT inv._doc_id, inv._score \
             FROM inv \
             WHERE full_text(inv.category, 'electronics') \
             ORDER BY inv._score DESC LIMIT 1",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 1);
    let score = batch.column(1).as_primitive::<Float32Type>().value(0);
    assert!(score > 0.0, "top-1 score should be positive");
}

#[tokio::test]
async fn test_topk_not_applied_with_join_filter() {
    use arrow::datatypes::Float32Type;

    let index = create_test_index();

    let config = SessionConfig::new().with_target_partitions(1);
    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(TopKPushdown::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table(
        "inv",
        Arc::new(TantivyInvertedIndexProvider::new(index)),
    )
    .unwrap();

    // With a join + filter, topK should NOT be pushed down (safety)
    // but the query should still return correct results
    let df = ctx
        .sql(
            "SELECT f.id, inv._score \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0 \
             ORDER BY inv._score DESC LIMIT 1",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics = ids {1, 3}, price > 2.0 = ids {2, 3, 4, 5} → intersection = {3}
    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 3);
    let score = batch.column(1).as_primitive::<Float32Type>().value(0);
    assert!(score > 0.0, "score should be positive");

    // Verify the plan does NOT have topk pushed down
    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT f.id, inv._score \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0 \
             ORDER BY inv._score DESC LIMIT 1",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    assert!(
        !plan.contains("topk=Some"),
        "topK should NOT be pushed through join+filter.\n\nActual plan:\n{plan}"
    );
}

// --- Fast field filter pushdown tests ---

fn create_filter_pushdown_session(index: &Index) -> SessionContext {
    use datafusion::execution::SessionStateBuilder;

    let config = SessionConfig::new().with_target_partitions(1);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(FastFieldFilterPushdown::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table(
        "inv",
        Arc::new(TantivyInvertedIndexProvider::new(index.clone())),
    )
    .unwrap();
    ctx
}

#[tokio::test]
async fn test_filter_pushdown_into_inverted_index_plan() {
    let index = create_test_index();
    let ctx = create_filter_pushdown_session(&index);

    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT f.id, f.price \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0 \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    // Full physical plan showing FastFieldFilterPushdown in action:
    //
    // - The `f.price > 2.0` predicate has been moved from FastFieldDataSource's
    //   pushed_filters into the InvertedIndexDataSource's tantivy query
    //   (combined with full_text('electronics') via BooleanQuery::intersection).
    //
    // - FastFieldDataSource retains only the DynamicFilter (join-internal bounds
    //   from the hash table build side — evaluates to true until populated).
    //
    // - The join is now purely 1:1 enrichment: every doc emitted by the inverted
    //   index already satisfies the price filter.
    let expected_physical_plan = "\
SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[false]
  HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[id@4, price@5]
    CooperativeExec
      DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=None)
    CooperativeExec
      DataSourceExec: FastFieldDataSource(partitions=1, query=false, limit=None, pushed_filters=[DynamicFilter [ empty ]])";

    let physical_plan: String = plan
        .lines()
        .skip_while(|line| !line.starts_with("SortExec"))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        physical_plan, expected_physical_plan,
        "Physical plan mismatch.\n\nActual:\n{physical_plan}\n\nExpected:\n{expected_physical_plan}"
    );
}

#[tokio::test]
async fn test_filter_pushdown_result_correctness() {
    let index = create_test_index();
    let ctx = create_filter_pushdown_session(&index);

    let df = ctx
        .sql(
            "SELECT f.id, f.price \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0 \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics = ids {1, 3}, price > 2.0 = ids {2, 3, 4, 5} → intersection = {3}
    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 3);
    let price = batch.column(1).as_primitive::<Float64Type>().value(0);
    assert!((price - 3.5).abs() < 1e-10);
}

#[tokio::test]
async fn test_topk_through_join_with_filter_pushdown() {
    use arrow::datatypes::Float32Type;
    use datafusion::execution::SessionStateBuilder;

    let index = create_test_index();

    // Register BOTH rules: filter pushdown first, then topK
    let config = SessionConfig::new().with_target_partitions(1);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(FastFieldFilterPushdown::new()))
        .with_physical_optimizer_rule(Arc::new(TopKPushdown::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table(
        "inv",
        Arc::new(TantivyInvertedIndexProvider::new(index)),
    )
    .unwrap();

    // With filter pushdown + topK, the price filter moves into inverted index,
    // then topK can traverse the join into the build side.
    let df = ctx
        .sql(
            "SELECT f.id, inv._score \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0 \
             ORDER BY inv._score DESC LIMIT 1",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics AND price > 2.0 → only id=3
    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 3);
    let score = batch.column(1).as_primitive::<Float32Type>().value(0);
    assert!(score > 0.0, "score should be positive");

    // Full physical plan showing both rules working together:
    //
    // 1. FastFieldFilterPushdown moved `price > 2.0` into the inverted index query
    // 2. TopKPushdown traversed the HashJoinExec (now filter-free on probe side)
    //    and injected topk=Some(1) into InvertedIndexDataSource
    // 3. Block-WAND pruning in tantivy produces at most 1 doc per segment
    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT f.id, inv._score \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0 \
             ORDER BY inv._score DESC LIMIT 1",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    let expected_physical_plan = "\
SortExec: TopK(fetch=1), expr=[_score@1 DESC], preserve_partitioning=[false]
  ProjectionExec: expr=[id@1 as id, _score@0 as _score]
    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[_score@2, id@5]
      CooperativeExec
        DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=Some(1))
      ProjectionExec: expr=[_doc_id@0 as _doc_id, _segment_ord@1 as _segment_ord, id@2 as id]
        CooperativeExec
          DataSourceExec: FastFieldDataSource(partitions=1, query=false, limit=None, pushed_filters=[DynamicFilter [ empty ]])";

    let physical_plan: String = plan
        .lines()
        .skip_while(|line| !line.starts_with("SortExec"))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        physical_plan, expected_physical_plan,
        "Physical plan mismatch.\n\nActual:\n{physical_plan}\n\nExpected:\n{expected_physical_plan}"
    );
}

#[tokio::test]
async fn test_filter_pushdown_preserves_dynamic_filter() {
    let index = create_test_index();
    let ctx = create_filter_pushdown_session(&index);

    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') AND f.price > 2.0",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    // Physical plan without ORDER BY — same filter pushdown behavior:
    //
    // - price > 2.0 moved into InvertedIndexDataSource query
    // - DynamicFilter preserved on FastFieldDataSource
    // - ProjectionExec trims fast field columns after the join
    let expected_physical_plan = "\
HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[id@4]
  CooperativeExec
    DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=None)
  ProjectionExec: expr=[_doc_id@0 as _doc_id, _segment_ord@1 as _segment_ord, id@2 as id]
    CooperativeExec
      DataSourceExec: FastFieldDataSource(partitions=1, query=false, limit=None, pushed_filters=[DynamicFilter [ empty ]])";

    let physical_plan: String = plan
        .lines()
        .skip_while(|line| !line.starts_with("HashJoinExec"))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        physical_plan, expected_physical_plan,
        "Physical plan mismatch.\n\nActual:\n{physical_plan}\n\nExpected:\n{expected_physical_plan}"
    );
}

#[tokio::test]
async fn test_no_pushdown_without_inverted_index_query() {
    // When there's no full_text() query, no filter pushdown should occur
    let index = create_test_index();
    let ctx = create_filter_pushdown_session(&index);

    let df = ctx
        .sql(
            "SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE f.price > 2.0 \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // price > 2.0 → ids {2, 3, 4, 5} (4 of 5 total docs)
    // All docs appear on both sides since inv has no filter
    assert_eq!(batch.num_rows(), 4);
    let mut ids: Vec<u64> = batch
        .column(0)
        .as_primitive::<UInt64Type>()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 3, 4, 5]);
}

// --- Date, IpAddr, Bytes field tests ---

#[tokio::test]
async fn test_date_field_filter() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    // created_at > 3_000_000 microseconds → docs 4 and 5
    let df = ctx
        .sql(
            "SELECT id FROM test_index \
             WHERE created_at > TIMESTAMP '1970-01-01T00:00:03' \
             ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 4);
    assert_eq!(ids.value(1), 5);
}

#[tokio::test]
async fn test_ip_address_field_filter() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    // ip_address = '192.168.1.1' → doc 1 only
    let df = ctx
        .sql("SELECT id FROM test_index WHERE ip_address = '192.168.1.1' ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 1);
}

#[tokio::test]
async fn test_bytes_field_roundtrip() {
    let index = create_test_index();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    let df = ctx
        .sql("SELECT id, data FROM test_index ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 5);

    let data_col = batch.column(1).as_binary::<i32>();
    assert_eq!(data_col.value(0), b"aaa");
    assert_eq!(data_col.value(1), b"bbb");
    assert_eq!(data_col.value(2), b"ccc");
    assert_eq!(data_col.value(3), b"ddd");
    assert_eq!(data_col.value(4), b"eee");
}

// --- Multi-segment tests ---

#[tokio::test]
async fn test_multi_segment_join() {
    let index = create_multi_segment_test_index();
    let num_segments = index.searchable_segments().unwrap().len();
    assert_eq!(num_segments, 2);

    let config = SessionConfig::new().with_target_partitions(num_segments);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    let df = ctx
        .sql(
            "SELECT f.id, f.price \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics = ids {1, 3}, both in segment 0 (first 3 docs)
    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 3);
}

/// Three-way join across 2 segments: inverted index + fast fields + document store.
///
/// Segment 0 has docs {id=1, id=2, id=3} and segment 1 has docs {id=4, id=5}.
/// The query searches for 'books' (ids {2, 4}) which span both segments,
/// then joins fast fields for price/score and documents for stored JSON.
/// Every column value is verified per row.
#[tokio::test]
async fn test_multi_segment_three_way_join() {
    let index = create_multi_segment_test_index();
    let num_segments = index.searchable_segments().unwrap().len();
    assert_eq!(num_segments, 2);

    // Disable join dynamic filter pushdown to work around a DF 52 bug where
    // Partitioned hash joins with dynamic filters use CaseHash routing instead
    // of PartitionIndex routing, causing cross-partition mismatches.
    // Upstream fix: https://github.com/apache/datafusion/pull/20246
    let mut config = SessionConfig::new().with_target_partitions(num_segments);
    config
        .options_mut()
        .optimizer
        .enable_join_dynamic_filter_pushdown = false;
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table(
        "inv",
        Arc::new(TantivyInvertedIndexProvider::new(index.clone())),
    )
    .unwrap();
    ctx.register_table(
        "f",
        Arc::new(TantivyTableProvider::new(index.clone())),
    )
    .unwrap();
    ctx.register_table(
        "d",
        Arc::new(TantivyDocumentProvider::new(index)),
    )
    .unwrap();

    let df = ctx
        .sql(
            "SELECT f.id, f.score, f.price, f.active, d._document \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             JOIN d ON d._doc_id = f._doc_id AND d._segment_ord = f._segment_ord \
             WHERE full_text(inv.category, 'books') \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // 'books' matches ids {2, 4} — id=2 in segment 0, id=4 in segment 1.
    assert_eq!(batch.num_rows(), 2);

    let ids = batch.column(0).as_primitive::<UInt64Type>();
    let scores = batch.column(1).as_primitive::<Int64Type>();
    let prices = batch.column(2).as_primitive::<Float64Type>();
    let actives = batch.column(3).as_boolean();
    let docs = batch.column(4).as_string::<i32>();

    // Row 0: id=2, score=20, price=2.5, active=false
    assert_eq!(ids.value(0), 2);
    assert_eq!(scores.value(0), 20);
    assert!((prices.value(0) - 2.5).abs() < 1e-10);
    assert!(!actives.value(0));

    let doc0: serde_json::Value = serde_json::from_str(docs.value(0)).unwrap();
    assert_eq!(doc0["id"][0], 2);
    assert_eq!(doc0["category"][0], "books");

    // Row 1: id=4, score=40, price=4.5, active=false
    assert_eq!(ids.value(1), 4);
    assert_eq!(scores.value(1), 40);
    assert!((prices.value(1) - 4.5).abs() < 1e-10);
    assert!(!actives.value(1));

    let doc1: serde_json::Value = serde_json::from_str(docs.value(1)).unwrap();
    assert_eq!(doc1["id"][0], 4);
    assert_eq!(doc1["category"][0], "books");
}

/// Fast-field-only query across 2 segments with a price filter.
/// Verifies that all columns come back correct when results span both partitions.
#[tokio::test]
async fn test_multi_segment_fast_field_filter() {
    let index = create_multi_segment_test_index();
    let num_segments = index.searchable_segments().unwrap().len();
    assert_eq!(num_segments, 2);

    let config = SessionConfig::new().with_target_partitions(num_segments);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index)))
        .unwrap();

    // price > 3.0 → ids {3, 4, 5} — id=3 in segment 0, ids {4, 5} in segment 1
    let df = ctx
        .sql(
            "SELECT id, score, price, active \
             FROM f \
             WHERE price > 3.0 \
             ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 3);

    let ids = batch.column(0).as_primitive::<UInt64Type>();
    let scores = batch.column(1).as_primitive::<Int64Type>();
    let prices = batch.column(2).as_primitive::<Float64Type>();
    let actives = batch.column(3).as_boolean();

    // Row 0: id=3, score=30, price=3.5, active=true
    assert_eq!(ids.value(0), 3);
    assert_eq!(scores.value(0), 30);
    assert!((prices.value(0) - 3.5).abs() < 1e-10);
    assert!(actives.value(0));

    // Row 1: id=4, score=40, price=4.5, active=false
    assert_eq!(ids.value(1), 4);
    assert_eq!(scores.value(1), 40);
    assert!((prices.value(1) - 4.5).abs() < 1e-10);
    assert!(!actives.value(1));

    // Row 2: id=5, score=50, price=5.5, active=true
    assert_eq!(ids.value(2), 5);
    assert_eq!(scores.value(2), 50);
    assert!((prices.value(2) - 5.5).abs() < 1e-10);
    assert!(actives.value(2));
}

#[tokio::test]
async fn test_multi_segment_plan() {
    let index = create_multi_segment_test_index();
    let num_segments = index.searchable_segments().unwrap().len();
    assert_eq!(num_segments, 2);

    let config = SessionConfig::new().with_target_partitions(num_segments);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone())))
        .unwrap();
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics')",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    // Plan should show segments=2 for both providers
    assert!(
        plan.contains("segments=2"),
        "Plan should show segments=2.\n\nActual plan:\n{plan}"
    );
}

// --- Deleted docs tests ---

#[tokio::test]
async fn test_deleted_docs_excluded() {
    let index = create_test_index_with_deletes();
    let provider = TantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("test_index", Arc::new(provider))
        .unwrap();

    let df = ctx
        .sql("SELECT id FROM test_index ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // Docs 2 and 4 deleted → only ids {1, 3, 5} remain
    assert_eq!(batch.num_rows(), 3);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 3);
    assert_eq!(ids.value(2), 5);
}

#[tokio::test]
async fn test_deleted_docs_excluded_inverted_index() {
    let index = create_test_index_with_deletes();
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index)))
        .unwrap();

    // "books" matched ids {2, 4} originally, both deleted → 0 rows
    let df = ctx
        .sql(
            "SELECT inv._doc_id \
             FROM inv \
             WHERE full_text(inv.category, 'books')",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_deleted_docs_excluded_document_provider() {
    let index = create_test_index_with_deletes();
    let ctx = SessionContext::new();
    ctx.register_table("docs", Arc::new(TantivyDocumentProvider::new(index)))
        .unwrap();

    let df = ctx
        .sql("SELECT _document FROM docs ORDER BY _doc_id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // Only 3 alive docs
    assert_eq!(batch.num_rows(), 3);
}

// --- Date filter pushdown into inverted index ---

#[tokio::test]
async fn test_date_filter_pushdown_into_inverted_index() {
    let index = create_test_index();
    let ctx = create_filter_pushdown_session(&index);

    // full_text for 'electronics' (ids {1, 3}) AND created_at > 1_000_000 micros (ids {2..5})
    // → intersection = {3}
    let df = ctx
        .sql(
            "SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') \
               AND f.created_at > TIMESTAMP '1970-01-01T00:00:01' \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 3);
}

#[tokio::test]
async fn test_date_filter_pushdown_into_inverted_index_plan() {
    let index = create_test_index();
    let ctx = create_filter_pushdown_session(&index);

    let df = ctx
        .sql(
            "EXPLAIN \
             SELECT f.id \
             FROM inv \
             JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord \
             WHERE full_text(inv.category, 'electronics') \
               AND f.created_at > TIMESTAMP '1970-01-01T00:00:01'",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    // Extract physical plan only (skip logical plan lines)
    let physical_plan: String = plan
        .lines()
        .skip_while(|line| !line.starts_with("HashJoinExec"))
        .collect::<Vec<_>>()
        .join("\n");

    // The date predicate should have been pushed into the InvertedIndexDataSource,
    // leaving only the DynamicFilter on the FastFieldDataSource
    assert!(
        physical_plan.contains("pushed_filters=[DynamicFilter"),
        "Date filter should be pushed into inverted index, only DynamicFilter on fast field.\n\nPhysical plan:\n{physical_plan}"
    );
    // The FastFieldDataSource should NOT have the created_at filter
    assert!(
        !physical_plan.contains("created_at"),
        "created_at filter should not remain on FastFieldDataSource.\n\nPhysical plan:\n{physical_plan}"
    );
}

// --- Unified provider tests ---

#[tokio::test]
async fn test_unified_fast_fields_only() {
    let index = create_test_index();
    let provider = UnifiedTantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT id, price FROM t WHERE price > 2.0 ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // price > 2.0 → ids {2, 3, 4, 5}
    assert_eq!(batch.num_rows(), 4);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 2);
    assert_eq!(ids.value(1), 3);
    assert_eq!(ids.value(2), 4);
    assert_eq!(ids.value(3), 5);
}

#[tokio::test]
async fn test_unified_full_text_with_score() {
    use arrow::datatypes::Float32Type;

    let index = create_test_index();
    let provider = UnifiedTantivyTableProvider::new(index);

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT id, _score FROM t WHERE full_text(category, 'electronics') ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics → ids {1, 3}
    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 3);

    // Scores should be positive
    let scores = batch.column(1).as_primitive::<Float32Type>();
    assert!(scores.value(0) > 0.0);
    assert!(scores.value(1) > 0.0);
}

#[tokio::test]
async fn test_unified_full_text_with_filter() {
    let index = create_test_index();
    let provider = UnifiedTantivyTableProvider::new(index);

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql(
            "SELECT id, price FROM t \
             WHERE full_text(category, 'electronics') AND price > 2.0 \
             ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics AND price > 2.0 → only id=3
    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 3);
}

#[tokio::test]
async fn test_unified_with_document() {
    let index = create_test_index();
    let provider = UnifiedTantivyTableProvider::new(index);

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql(
            "SELECT id, _document FROM t \
             WHERE full_text(category, 'electronics') \
             ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<UInt64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 3);

    // _document should be valid JSON
    let docs = batch.column(1).as_string::<i32>();
    let doc0: serde_json::Value = serde_json::from_str(docs.value(0)).unwrap();
    assert_eq!(doc0["id"][0], 1);
    let doc1: serde_json::Value = serde_json::from_str(docs.value(1)).unwrap();
    assert_eq!(doc1["id"][0], 3);
}

#[tokio::test]
async fn test_unified_three_way() {
    use arrow::datatypes::Float32Type;

    let index = create_test_index();
    let provider = UnifiedTantivyTableProvider::new(index);

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql(
            "SELECT id, price, _score, _document FROM t \
             WHERE full_text(category, 'electronics') AND price > 2.0 \
             ORDER BY _score DESC LIMIT 10",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    // electronics AND price > 2.0 → only id=3
    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 3);
    let price = batch.column(1).as_primitive::<Float64Type>().value(0);
    assert!((price - 3.5).abs() < 1e-10);
    let score = batch.column(2).as_primitive::<Float32Type>().value(0);
    assert!(score > 0.0);
    let doc: serde_json::Value =
        serde_json::from_str(batch.column(3).as_string::<i32>().value(0)).unwrap();
    assert_eq!(doc["id"][0], 3);
}

#[tokio::test]
async fn test_unified_score_null_without_query() {
    let index = create_test_index();
    let provider = UnifiedTantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT id, _score FROM t ORDER BY id LIMIT 3")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 3);
    // _score should be null when no full_text() filter
    let scores = batch.column(1);
    assert!(scores.is_null(0), "_score should be null without a query");
}

#[tokio::test]
async fn test_unified_document_without_inverted_index() {
    let index = create_test_index();
    let provider = UnifiedTantivyTableProvider::new(index);

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT id, _document FROM t WHERE id = 1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 1);
    let id = batch.column(0).as_primitive::<UInt64Type>().value(0);
    assert_eq!(id, 1);

    let doc: serde_json::Value =
        serde_json::from_str(batch.column(1).as_string::<i32>().value(0)).unwrap();
    assert_eq!(doc["id"][0], 1);
}

#[tokio::test]
async fn test_unified_plan_fast_fields_only() {
    let index = create_test_index();
    let provider = UnifiedTantivyTableProvider::new(index);

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("EXPLAIN SELECT id, price FROM t WHERE price > 2.0")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    // Should NOT contain HashJoinExec — fast fields only
    assert!(
        !plan.contains("HashJoinExec"),
        "Fast-fields-only query should not have joins.\n\nPlan:\n{plan}"
    );
    assert!(
        plan.contains("FastFieldDataSource"),
        "Plan should contain FastFieldDataSource.\n\nPlan:\n{plan}"
    );
}

#[tokio::test]
async fn test_unified_plan_with_join() {
    let index = create_test_index();
    let provider = UnifiedTantivyTableProvider::new(index);

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(full_text_udf());
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql(
            "EXPLAIN SELECT id, _score FROM t \
             WHERE full_text(category, 'electronics')",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let plan = plan_to_string(&batches);

    assert!(
        plan.contains("HashJoinExec"),
        "Full-text query should produce a join plan.\n\nPlan:\n{plan}"
    );
    assert!(
        plan.contains("InvertedIndexDataSource"),
        "Plan should contain InvertedIndexDataSource.\n\nPlan:\n{plan}"
    );
    assert!(
        plan.contains("FastFieldDataSource"),
        "Plan should contain FastFieldDataSource.\n\nPlan:\n{plan}"
    );
}
