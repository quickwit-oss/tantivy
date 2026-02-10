use std::sync::Arc;

use arrow::array::{AsArray, ListArray, RecordBatch};
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use tantivy::query::TermQuery;
use tantivy::schema::{Field, IndexRecordOption, SchemaBuilder, Term, FAST, STORED, TEXT};
use tantivy::{doc, Index, IndexWriter};
use tantivy_datafusion::{TantivySearchFunction, TantivyTableProvider};

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

// --- Inverted index provider tests ---

#[tokio::test]
async fn test_inverted_index_basic() {
    use arrow::datatypes::Float32Type;

    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udtf("tantivy_search", Arc::new(TantivySearchFunction::new(index)));

    let df = ctx
        .sql("SELECT * FROM tantivy_search('category', 'electronics')")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 3);

    // Verify column names
    let schema = batch.schema();
    assert_eq!(schema.field(0).name(), "_doc_id");
    assert_eq!(schema.field(1).name(), "_segment_ord");
    assert_eq!(schema.field(2).name(), "_score");

    // Verify types
    let _doc_ids = batch.column(0).as_primitive::<arrow::datatypes::UInt32Type>();
    let _seg_ords = batch.column(1).as_primitive::<arrow::datatypes::UInt32Type>();
    let _scores = batch.column(2).as_primitive::<Float32Type>();
}

#[tokio::test]
async fn test_inverted_index_no_matches() {
    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udtf("tantivy_search", Arc::new(TantivySearchFunction::new(index)));

    let df = ctx
        .sql("SELECT * FROM tantivy_search('category', 'nonexistent_term_xyz')")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_inverted_index_scores_are_positive() {
    use arrow::datatypes::Float32Type;

    let index = create_test_index();
    let ctx = SessionContext::new();
    ctx.register_udtf("tantivy_search", Arc::new(TantivySearchFunction::new(index)));

    let df = ctx
        .sql("SELECT _score FROM tantivy_search('category', 'electronics')")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = collect_batches(&batches);

    let scores = batch.column(0).as_primitive::<Float32Type>();
    for i in 0..scores.len() {
        assert!(
            scores.value(i) > 0.0,
            "score at row {} should be positive, got {}",
            i,
            scores.value(i)
        );
    }
}
