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
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        _docids: &mut Vec<DocId>,
        _row_ids: &mut Vec<columnar::RowId>,
    ) -> Cardinality {
        values.clear();
        values.resize(docs.len(), self.0);
        Cardinality::Full
    }

    fn to_f64(self: Box<Self>) -> Box<dyn ValueSource> {
        coerce_to_f64(self)
    }
}

pub(crate) struct ConstantProvider(pub u64);

impl ValueSourceProvider for ConstantProvider {
    fn for_segment(
        &self,
        _reader: &SegmentReader,
        _allowed_column_types: Option<&[ColumnType]>,
    ) -> crate::Result<Option<Box<dyn ValueSource>>> {
        Ok(Some(Box::new(Constant(self.0))))
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

/// A constant source of an arbitrary type.
#[derive(Debug)]
struct TypedConstant {
    column_type: ColumnType,
    value: u64,
}

impl ValueSource for TypedConstant {
    fn column_type(&self) -> ColumnType {
        self.column_type
    }

    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        _docids: &mut Vec<DocId>,
        _row_ids: &mut Vec<columnar::RowId>,
    ) -> Cardinality {
        values.clear();
        values.resize(docs.len(), self.value);
        Cardinality::Full
    }

    fn to_f64(self: Box<Self>) -> Box<dyn ValueSource> {
        coerce_to_f64(self)
    }
}

struct TypedConstantProvider {
    column_type: ColumnType,
    value: u64,
}

impl ValueSourceProvider for TypedConstantProvider {
    fn for_segment(
        &self,
        _reader: &SegmentReader,
        _allowed_column_types: Option<&[ColumnType]>,
    ) -> crate::Result<Option<Box<dyn ValueSource>>> {
        Ok(Some(Box::new(TypedConstant {
            column_type: self.column_type,
            value: self.value,
        })))
    }
}

fn try_run_agg(
    index: &crate::Index,
    aggs: serde_json::Value,
    context: crate::aggregation::AggContextParams,
) -> crate::Result<serde_json::Value> {
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::AggregationCollector;
    use crate::query::AllQuery;

    let aggs: Aggregations = serde_json::from_value(aggs).unwrap();
    let collector = AggregationCollector::from_aggs(aggs, context);
    let searcher = index.reader().unwrap().searcher();
    let result = searcher.search(&AllQuery, &collector)?;
    Ok(serde_json::to_value(result).unwrap())
}

#[test]
fn test_composite_and_multi_terms_reject_registered_source() {
    let index = index_with_scores(&[10, 20]);
    let provider = TypedConstantProvider {
        column_type: ColumnType::U64,
        value: 1,
    };
    let mut registry = ValueSourceRegistry::default();
    registry.register("score", Arc::new(provider));
    let context =
        crate::aggregation::AggContextParams::default().with_value_sources(Arc::new(registry));
    let composite = serde_json::json!({
        "c": {
            "composite": { "size": 10, "sources": [{ "s": { "terms": { "field": "score" } } }] }
        }
    });
    let err = try_run_agg(&index, composite, context.clone()).unwrap_err();
    assert!(
        err.to_string().contains("composite does not support"),
        "{err}"
    );
    let multi_terms = serde_json::json!({
        "m": { "multi_terms": { "terms": [{ "field": "score" }] } }
    });
    let err = try_run_agg(&index, multi_terms, context).unwrap_err();
    assert!(
        err.to_string().contains("multi_terms does not support"),
        "{err}"
    );
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

/// A computed `Str` source without any value. It returns an empty dictionary, as required by
/// `ValueSource::term_dictionary`.
struct EmptyStrValueSource(columnar::Dictionary);

impl std::fmt::Debug for EmptyStrValueSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmptyStrValueSource").finish()
    }
}

impl ValueSource for EmptyStrValueSource {
    fn column_type(&self) -> ColumnType {
        ColumnType::Str
    }

    fn load_block(
        &mut self,
        _docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<columnar::RowId>,
    ) -> Cardinality {
        values.clear();
        docids.clear();
        row_ids.clear();
        Cardinality::Optional
    }

    fn to_f64(self: Box<Self>) -> Box<dyn ValueSource> {
        self
    }

    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        Some(&self.0)
    }
}

struct EmptyStrValueSourceProvider;

impl ValueSourceProvider for EmptyStrValueSourceProvider {
    fn for_segment(
        &self,
        _reader: &SegmentReader,
        _allowed_column_types: Option<&[ColumnType]>,
    ) -> crate::Result<Option<Box<dyn ValueSource>>> {
        Ok(Some(Box::new(EmptyStrValueSource(
            columnar::Dictionary::empty(),
        ))))
    }
}

#[test]
fn test_empty_computed_str_source() {
    let index = index_with_scores(&[1, 2, 3]);
    let mut registry = ValueSourceRegistry::default();
    registry.register("empty_str", Arc::new(EmptyStrValueSourceProvider));
    let result = run_agg_with_registry(
        &index,
        serde_json::json!({
            "card": { "cardinality": { "field": "empty_str" } },
            "by_term": { "terms": { "field": "empty_str" } }
        }),
        registry,
    );
    assert_eq!(result["card"]["value"], 0.0);
    assert_eq!(result["by_term"]["buckets"].as_array().unwrap().len(), 0);
}

#[test]
fn test_cardinality_on_absent_field_with_str_missing() {
    let index = index_with_scores(&[1, 2, 3]);
    let result = run_agg_with_registry(
        &index,
        serde_json::json!({
            "card": { "cardinality": { "field": "absent", "missing": "foo" } }
        }),
        ValueSourceRegistry::default(),
    );
    assert_eq!(result["card"]["value"], 1.0);
}

/// A full column with the given values, in their monotonic `u64` mapping.
fn full_column(values: &[u64]) -> Column<u64> {
    use columnar::column_index::ColumnIndex;
    use columnar::column_values::{
        serialize_and_load_u64_based_column_values, ALL_U64_CODEC_TYPES,
    };
    Column {
        index: ColumnIndex::Full,
        values: serialize_and_load_u64_based_column_values::<u64>(&values, &ALL_U64_CODEC_TYPES),
    }
}

#[test]
fn test_to_f64_on_physical_columns() {
    use columnar::MonotonicallyMappableToU64;

    let docs: Vec<DocId> = vec![0, 1, 2];
    let mut values = Vec::new();
    let mut docids = Vec::new();
    let mut row_ids = Vec::new();

    let mut u64_source: Box<dyn ValueSource> =
        Box::new((full_column(&[3u64, 0, 7]), ColumnType::U64)).to_f64();
    assert_eq!(u64_source.column_type(), ColumnType::F64);
    let cardinality = u64_source.load_block(&docs, &mut values, &mut docids, &mut row_ids);
    assert!(cardinality.is_full());
    let decoded: Vec<f64> = values.iter().map(|value| f64::from_u64(*value)).collect();
    assert_eq!(decoded, vec![3.0, 0.0, 7.0]);

    // A borrowed column works too.
    let i64_column = full_column(&[(-2i64).to_u64(), 5i64.to_u64(), 0i64.to_u64()]);
    let mut i64_source: Box<dyn ValueSource> = Box::new((&i64_column, ColumnType::I64)).to_f64();
    assert_eq!(i64_source.column_type(), ColumnType::F64);
    i64_source.load_block(&docs, &mut values, &mut docids, &mut row_ids);
    let decoded: Vec<f64> = values.iter().map(|value| f64::from_u64(*value)).collect();
    assert_eq!(decoded, vec![-2.0, 5.0, 0.0]);

    // Other types are unchanged.
    let f64_column = full_column(&[1.5f64.to_u64()]);
    let f64_source: Box<dyn ValueSource> = Box::new((f64_column, ColumnType::F64)).to_f64();
    assert_eq!(f64_source.column_type(), ColumnType::F64);
    assert!(f64_source.as_physical_column().is_some());
}
