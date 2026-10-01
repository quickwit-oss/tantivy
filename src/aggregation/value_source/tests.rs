use std::sync::Arc;

use columnar::ColumnType;

use super::*;
use crate::SegmentReader;

#[derive(Debug)]
pub(crate) struct Constant(u64);

impl ValueSource for Constant {
    fn column_type(&self) -> ColumnType {
        ColumnType::U64
    }

    fn load_block(
        &self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        _docids: &mut Vec<DocId>,
        _row_ids: &mut Vec<columnar::RowId>,
    ) -> Cardinality {
        values.clear();
        values.resize(docs.len(), self.0);
        Cardinality::Full
    }
}

pub(crate) struct ConstantProvider(pub u64);

impl ValueSourceProvider for ConstantProvider {
    fn for_segment(&self, _reader: &SegmentReader) -> crate::Result<Arc<dyn ValueSource>> {
        Ok(Arc::new(Constant(self.0)))
    }
}
fn index_with_scores(scores: &[u64]) -> crate::Index {
    use crate::schema::{Schema, FAST};
    let mut builder = Schema::builder();
    let score = builder.add_u64_field("score", FAST);
    let index = crate::Index::create_in_ram(builder.build());
    let mut writer = index.writer_for_tests().unwrap();
    for &value in scores {
        writer.add_document(crate::doc!(score => value)).unwrap();
    }
    writer.commit().unwrap();
    index
}

fn run_agg(index: &crate::Index, aggs: serde_json::Value) -> serde_json::Value {
    let mut registry = ValueSourceRegistry::default();
    registry.register("computed", Arc::new(ConstantProvider(1u64)));
    run_agg_with_registry(index, aggs, registry)
}

fn run_agg_with_registry(
    index: &crate::Index,
    aggs: serde_json::Value,
    registry: ValueSourceRegistry,
) -> serde_json::Value {
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::{AggContextParams, AggregationCollector};
    use crate::query::AllQuery;

    let context = AggContextParams::default().with_value_sources(Arc::new(registry));
    let aggs: Aggregations = serde_json::from_value(aggs).unwrap();
    let collector = AggregationCollector::from_aggs(aggs, context);
    let searcher = index.reader().unwrap().searcher();
    let result = searcher.search(&AllQuery, &collector).unwrap();
    serde_json::to_value(result).unwrap()
}

#[test]
fn test_metric_over_registered_source() {
    let index = index_with_scores(&[10, 20, 30, 40]);
    let result = run_agg(
        &index,
        serde_json::json!({ "s": { "stats": { "field": "computed" } } }),
    );
    // Every document contributes exactly 1.
    assert_eq!(result["s"]["count"], 4);
    assert_eq!(result["s"]["sum"], 4.0);
    assert_eq!(result["s"]["avg"], 1.0);
    assert_eq!(result["s"]["min"], 1.0);
    assert_eq!(result["s"]["max"], 1.0);
}

#[test]
fn test_registered_source_as_sub_aggregation_of_terms() {
    let index = index_with_scores(&[7, 7, 7, 9]);
    let result = run_agg(
        &index,
        serde_json::json!({
            "by_score": {
                "terms": { "field": "score" },
                "aggs": { "s": { "sum": { "field": "computed" } } }
            }
        }),
    );
    let buckets = result["by_score"]["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0]["key"], 7.0);
    assert_eq!(buckets[0]["doc_count"], 3);
    assert_eq!(buckets[0]["s"]["value"], 3.0);
    assert_eq!(buckets[1]["key"], 9.0);
    assert_eq!(buckets[1]["doc_count"], 1);
    assert_eq!(buckets[1]["s"]["value"], 1.0);
}

#[test]
fn test_is_contiguous() {
    assert!(!is_contiguous(&[]));
    assert!(is_contiguous(&[5]));
    assert!(is_contiguous(&[5, 6, 7, 8]));
    assert!(is_contiguous(&[0, 1, 2]));
    assert!(!is_contiguous(&[5, 7, 8]));
    assert!(!is_contiguous(&[0, 1, 3]));
}
