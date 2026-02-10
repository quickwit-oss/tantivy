use std::sync::Arc;

use arrow::array::{AsArray, ListArray, RecordBatch};
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use tantivy::query::TermQuery;
use tantivy::schema::{Field, IndexRecordOption, SchemaBuilder, Term, FAST, STORED, TEXT};
use tantivy::{doc, Index, IndexWriter};
use tantivy_datafusion::{
    full_text_udf, FastFieldFilterPushdown, TantivyDocumentProvider,
    TantivyInvertedIndexProvider, TantivyTableProvider, TopKPushdown,
};

fn plan_to_string(batches: &[RecordBatch]) -> String {
    let batch = collect_batches(batches);
    let plan_col = batch.column(1).as_string::<i32>();
    (0..batch.num_rows())
        .map(|i| plan_col.value(i))
        .collect::<Vec<_>>()
        .join("\n")
}

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
    assert_eq!(batch.num_columns(), 7); // _doc_id, _segment_ord, id, score, price, active, category
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
CoalesceBatchesExec: target_batch_size=8192
  HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[id@4]
    CooperativeExec
      DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=None)
    CooperativeExec
      DataSourceExec: FastFieldDataSource(segments=1, query=false, limit=None, pushed_filters=[DynamicFilter [ empty ]])";

    let physical_plan: String = plan
        .lines()
        .skip_while(|line| !line.starts_with("CoalesceBatchesExec"))
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
    //   join; DocumentDataSource declines dynamic filters (PushedDown::No)
    //   since it's typically joined after filtering is done.
    let expected_physical_plan = "\
SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[false]
  CoalesceBatchesExec: target_batch_size=8192
    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[id@2, price@3, _document@6]
      CoalesceBatchesExec: target_batch_size=8192
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[_doc_id@2, _segment_ord@3, id@4, price@5]
          CooperativeExec
            DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=None)
          CooperativeExec
            DataSourceExec: FastFieldDataSource(segments=1, query=false, limit=None, pushed_filters=[price@3 > 2, DynamicFilter [ empty ]])
      CooperativeExec
        DataSourceExec: DocumentDataSource(segments=1)";

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
  CoalesceBatchesExec: target_batch_size=8192
    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[id@4, price@5]
      CooperativeExec
        DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=None)
      CooperativeExec
        DataSourceExec: FastFieldDataSource(segments=1, query=false, limit=None, pushed_filters=[DynamicFilter [ empty ]])";

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
    CoalesceBatchesExec: target_batch_size=8192
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[_score@2, id@5]
        CooperativeExec
          DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=Some(1))
        ProjectionExec: expr=[_doc_id@0 as _doc_id, _segment_ord@1 as _segment_ord, id@2 as id]
          CooperativeExec
            DataSourceExec: FastFieldDataSource(segments=1, query=false, limit=None, pushed_filters=[DynamicFilter [ empty ]])";

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
CoalesceBatchesExec: target_batch_size=8192
  HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(_doc_id@0, _doc_id@0), (_segment_ord@1, _segment_ord@1)], projection=[id@4]
    CooperativeExec
      DataSourceExec: InvertedIndexDataSource(segments=1, query=true, topk=None)
    ProjectionExec: expr=[_doc_id@0 as _doc_id, _segment_ord@1 as _segment_ord, id@2 as id]
      CooperativeExec
        DataSourceExec: FastFieldDataSource(segments=1, query=false, limit=None, pushed_filters=[DynamicFilter [ empty ]])";

    let physical_plan: String = plan
        .lines()
        .skip_while(|line| !line.starts_with("CoalesceBatchesExec"))
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
