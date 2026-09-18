//! Registration of named, computed value sources.
//!
//! A [`ValueSourceProvider`] is a cross-segment *definition*: it is shared by every segment of a
//! search, so it must be `Send + Sync`. [`ValueSourceProvider::for_segment`] binds it to one
//! segment, producing the handle the aggregation actually reads through. That handle only needs
//! to outlive the segment collector, which is `'static` but neither `Send` nor `Sync`.

use std::sync::Arc;

use columnar::ColumnType;
use rustc_hash::FxHashMap;

use super::block_accessor::BlockValueSource;
use crate::{SegmentReader, TantivyError};

/// Defines a computed column that aggregations can read by name.
///
/// Implementors are shared across segments and threads for the duration of a search.
pub trait ValueSourceProvider: Send + Sync + 'static {
    /// The type of the values produced, which fixes how the `u64` block values are interpreted.
    ///
    /// This must not vary between segments, including segments where the source yields nothing.
    fn column_type(&self) -> ColumnType;

    /// Binds this definition to a single segment.
    fn for_segment(&self, reader: &SegmentReader) -> crate::Result<Arc<dyn BlockValueSource>>;
}

/// Named computed sources available to an aggregation request.
///
/// Cloning is cheap: the map sits behind an [`Arc`], so a clone per segment costs one refcount
/// bump rather than a copy of the table.
#[derive(Clone, Default)]
pub struct ValueSourceRegistry {
    providers: Arc<FxHashMap<String, Arc<dyn ValueSourceProvider>>>,
}

impl ValueSourceRegistry {
    /// Registers `provider` under `name`, which aggregation requests then use as a field name.
    ///
    /// Returns an error if `name` is already registered. A name that also resolves to a real
    /// fast field is rejected later, when the request is bound to a segment — the schema is not
    /// known here.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        provider: Arc<dyn ValueSourceProvider>,
    ) -> crate::Result<()> {
        let name = name.into();
        let providers = Arc::make_mut(&mut self.providers);
        if providers.contains_key(&name) {
            return Err(TantivyError::InvalidArgument(format!(
                "Value source `{name}` is already registered"
            )));
        }
        providers.insert(name, provider);
        Ok(())
    }

    /// Returns true when nothing is registered, which lets resolution skip the lookup entirely.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    #[inline]
    pub(crate) fn get(&self, name: &str) -> Option<&Arc<dyn ValueSourceProvider>> {
        self.providers.get(name)
    }
}

#[cfg(test)]
mod tests {
    use columnar::Cardinality;

    use super::*;
    use crate::DocId;

    /// A stand-in source: every document has the value 1. Deliberately trivial — the point is to
    /// exercise registration and dispatch, not expression evaluation.
    #[derive(Debug)]
    pub(crate) struct ConstantOne;

    impl BlockValueSource for ConstantOne {
        fn load_block(
            &self,
            docs: &[DocId],
            values: &mut Vec<u64>,
            _docids: &mut Vec<DocId>,
            _row_ids: &mut Vec<columnar::RowId>,
        ) -> Cardinality {
            values.clear();
            values.resize(docs.len(), 1u64);
            Cardinality::Full
        }
    }

    pub(crate) struct ConstantOneProvider;

    impl ValueSourceProvider for ConstantOneProvider {
        fn column_type(&self) -> ColumnType {
            ColumnType::U64
        }

        fn for_segment(&self, _reader: &SegmentReader) -> crate::Result<Arc<dyn BlockValueSource>> {
            Ok(Arc::new(ConstantOne))
        }
    }

    #[test]
    fn test_register_then_get() {
        let mut registry = ValueSourceRegistry::default();
        assert!(registry.is_empty());
        registry
            .register("computed", Arc::new(ConstantOneProvider))
            .unwrap();
        assert!(!registry.is_empty());
        assert!(registry.get("computed").is_some());
        assert!(registry.get("absent").is_none());
    }

    #[test]
    fn test_register_duplicate_name_errors() {
        let mut registry = ValueSourceRegistry::default();
        registry
            .register("computed", Arc::new(ConstantOneProvider))
            .unwrap();
        let err = registry
            .register("computed", Arc::new(ConstantOneProvider))
            .expect_err("a duplicate registration must be rejected");
        assert!(err.to_string().contains("already registered"), "{err}");
    }

    #[test]
    fn test_clone_does_not_share_later_registrations() {
        let mut registry = ValueSourceRegistry::default();
        let snapshot = registry.clone();
        registry
            .register("computed", Arc::new(ConstantOneProvider))
            .unwrap();
        assert!(registry.get("computed").is_some());
        assert!(
            snapshot.get("computed").is_none(),
            "a clone taken before registration must not observe it"
        );
    }

    #[test]
    fn test_registry_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        // The outer `Collector` is `Sync + Send`, so the registry it carries must be too.
        assert_send_sync::<ValueSourceRegistry>();
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
        use crate::aggregation::agg_req::Aggregations;
        use crate::aggregation::{AggContextParams, AggregationCollector};
        use crate::query::AllQuery;

        let mut registry = ValueSourceRegistry::default();
        registry
            .register("computed", Arc::new(ConstantOneProvider))
            .unwrap();
        let context = AggContextParams::default().with_value_sources(registry);
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
        // The sub-aggregation reads the computed source out of the per-bucket doc buffer, which
        // is the path that matters: the parent drains the shared block accessor before the child
        // fetches into it.
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
    fn test_terms_over_registered_source() {
        // A constant source yields a single bucket holding every document.
        let index = index_with_scores(&[10, 20, 30]);
        let result = run_agg(
            &index,
            serde_json::json!({ "t": { "terms": { "field": "computed" } } }),
        );
        let buckets = result["t"]["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0]["key"], 1.0);
        assert_eq!(buckets[0]["doc_count"], 3);
    }

    #[test]
    fn test_registered_name_colliding_with_a_fast_field_is_rejected() {
        use crate::aggregation::agg_req::Aggregations;
        use crate::aggregation::{AggContextParams, AggregationCollector};
        use crate::query::AllQuery;

        let index = index_with_scores(&[1, 2, 3]);
        let mut registry = ValueSourceRegistry::default();
        // `score` is a real fast field on this index.
        registry
            .register("score", Arc::new(ConstantOneProvider))
            .unwrap();
        let context = AggContextParams::default().with_value_sources(registry);
        let aggs: Aggregations =
            serde_json::from_value(serde_json::json!({ "s": { "sum": { "field": "score" } } }))
                .unwrap();
        let collector = AggregationCollector::from_aggs(aggs, context);
        let searcher = index.reader().unwrap().searcher();
        let err = searcher
            .search(&AllQuery, &collector)
            .expect_err("a registered name that shadows a fast field must be rejected");
        assert!(err.to_string().contains("collides"), "{err}");
    }

    #[test]
    fn test_unregistered_names_still_resolve_physically() {
        // Registering something must not disturb ordinary fast-field resolution.
        let index = index_with_scores(&[10, 20, 30]);
        let result = run_agg(
            &index,
            serde_json::json!({ "s": { "sum": { "field": "score" } } }),
        );
        assert_eq!(result["s"]["value"], 60.0);
    }
}
