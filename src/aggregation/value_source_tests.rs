//! Synthetic producers exercise the same binding and collection path used by physical requests.

mod irregular_docs;

use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use columnar::{Column, ColumnType, MonotonicallyMappableToU64};
use serde_json::{json, Value};

use super::agg_data::build_aggregations_data_from_req_with_virtual_columns;
use super::agg_req::Aggregations;
use super::value_source::{VirtualColumn, VirtualColumnEvaluator, VirtualColumns};
use super::{
    AggContextParams, AggregationCollector, AggregationLimitsGuard, AggregationSegmentCollector,
    DistributedAggregationCollector,
};
use crate::collector::SegmentCollector;
use crate::query::{AllQuery, EmptyQuery};
use crate::schema::{Schema, FAST};
use crate::{DocId, Index, SegmentReader, TantivyDocument};

#[derive(Clone, Copy)]
enum Generation {
    Full,
    Optional,
    FailsAfterThree,
    BatchFailure,
}

impl Generation {
    fn value(self, input: u64) -> Option<f64> {
        match self {
            Self::Optional if input % 3 == 0 => None,
            Self::FailsAfterThree if input >= 3 => None,
            Self::BatchFailure => None,
            _ => Some(input as f64 * 2.0 - 5.0),
        }
    }
}

struct SyntheticColumn {
    generation: Generation,
    column_type: ColumnType,
    bindings: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}

struct SyntheticEvaluator {
    input: Column<u64>,
    generation: Generation,
    column_type: ColumnType,
    calls: Arc<AtomicUsize>,
    // Deliberately !Send + !Sync: only definitions, not segment evaluators, must be shareable.
    scratch: Rc<()>,
}

impl VirtualColumn for SyntheticColumn {
    fn column_type(&self) -> ColumnType {
        self.column_type
    }

    fn for_segment(
        &self,
        reader: &SegmentReader,
    ) -> crate::Result<Box<dyn VirtualColumnEvaluator>> {
        self.bindings.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(SyntheticEvaluator {
            input: reader.fast_fields().u64("input")?,
            generation: self.generation,
            column_type: self.column_type,
            calls: self.calls.clone(),
            scratch: Rc::new(()),
        }))
    }
}

impl VirtualColumnEvaluator for SyntheticEvaluator {
    fn evaluate(&mut self, docs: &[DocId], output: &mut [Option<u64>]) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        assert_eq!(Rc::strong_count(&self.scratch), 1);
        assert_eq!(docs.len(), output.len());
        assert!(
            output.iter().all(Option::is_none),
            "framework must reset reused output"
        );
        if matches!(self.generation, Generation::BatchFailure) {
            // A whole-batch failure emits nothing, without a Result or panic recovery.
            return;
        }
        for (&doc, slot) in docs.iter().zip(output) {
            let input = self.input.first(doc).unwrap();
            // All intermediate work is local; failure never commits a partial document value.
            let intermediate = input * 2;
            let Some(value) = self.generation.value(input) else {
                continue;
            };
            assert_eq!(value, intermediate as f64 - 5.0);
            *slot = Some(match self.column_type {
                ColumnType::F64 => value.to_u64(),
                ColumnType::I64 => (value as i64).to_u64(),
                ColumnType::U64 => input,
                _ => unreachable!(),
            });
        }
    }
}

fn registry(
    generation: Generation,
    column_type: ColumnType,
) -> (VirtualColumns, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let bindings = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = VirtualColumns::default();
    registry
        .register(
            "computed".to_owned(),
            Arc::new(SyntheticColumn {
                generation,
                column_type,
                bindings: bindings.clone(),
                calls: calls.clone(),
            }),
        )
        .unwrap();
    (registry, bindings, calls)
}

fn index(generation: Generation, num_docs: u64, segments: usize) -> crate::Result<Index> {
    let mut schema = Schema::builder();
    let input = schema.add_u64_field("input", FAST);
    let group = schema.add_u64_field("group", FAST);
    let materialized = schema.add_f64_field("materialized", FAST);
    schema.add_u64_field("unpopulated", FAST);
    schema.add_json_field("attributes", FAST);
    let index = Index::create_in_ram(schema.build());
    let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 15_000_000)?;
    writer.set_merge_policy(Box::new(crate::merge_policy::NoMergePolicy));
    for _segment in 0..segments {
        for input_value in 0..num_docs {
            let mut document = doc!(input => input_value, group => input_value % 2);
            if let Some(value) = generation.value(input_value) {
                document.add_f64(materialized, value);
            }
            writer.add_document(document)?;
        }
        writer.commit()?;
    }
    Ok(index)
}

fn replace_computed_field(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if key == "field" && value == "computed" {
                    *value = json!("materialized");
                } else {
                    replace_computed_field(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                replace_computed_field(value);
            }
        }
        _ => {}
    }
}

fn assert_materialized_parity(
    index: &Index,
    request: Value,
    generation: Generation,
) -> crate::Result<()> {
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let (registry, bindings, calls) = registry(generation, ColumnType::F64);
    let aggs: Aggregations = serde_json::from_value(request.clone()).unwrap();
    let collector = AggregationCollector::from_aggs(aggs.clone(), AggContextParams::default())
        .with_virtual_columns(registry.clone())?;
    let actual = serde_json::to_value(searcher.search(&AllQuery, &collector)?).unwrap();
    assert_eq!(
        bindings.load(Ordering::Relaxed),
        searcher.segment_readers().len()
    );
    assert!(calls.load(Ordering::Relaxed) > 0);
    let mut physical_request = request;
    replace_computed_field(&mut physical_request);
    let physical_aggs = serde_json::from_value(physical_request).unwrap();
    let expected = searcher.search(
        &AllQuery,
        &AggregationCollector::from_aggs(physical_aggs, AggContextParams::default()),
    )?;
    assert_eq!(actual, serde_json::to_value(expected).unwrap());
    let distributed =
        DistributedAggregationCollector::from_aggs(aggs.clone(), AggContextParams::default())
            .with_virtual_columns(registry)?;
    let intermediate = searcher.search(&AllQuery, &distributed)?;
    // Runtime identity never escapes into the distributed intermediate result.
    let encoded = postcard::to_allocvec(&intermediate).unwrap();
    let decoded: super::intermediate_agg_result::IntermediateAggregationResults =
        postcard::from_bytes(&encoded).unwrap();
    let distributed_result = decoded.into_final_result(aggs, AggregationLimitsGuard::default())?;
    assert_eq!(actual, serde_json::to_value(distributed_result).unwrap());
    Ok(())
}

#[test]
fn virtual_numeric_collectors_match_materialized_full_optional_and_failure() -> crate::Result<()> {
    for generation in [
        Generation::Full,
        Generation::Optional,
        Generation::FailsAfterThree,
        Generation::BatchFailure,
    ] {
        let index = index(generation, 8, 2)?;
        assert_materialized_parity(
            &index,
            json!({
                "stats": {"stats": {"field": "computed", "missing": 4.0}},
                "sum": {"sum": {"field": "computed"}},
                "min": {"min": {"field": "computed"}},
                "max": {"max": {"field": "computed"}},
                "avg": {"avg": {"field": "computed", "missing": 4.0}},
                "count": {"value_count": {"field": "computed"}},
                "extended": {"extended_stats": {"field": "computed", "missing": 4.0}},
                "percentiles": {"percentiles": {"field": "computed", "missing": 4.0}},
                "cardinality": {"cardinality": {"field": "computed", "missing": 4.0}},
                "terms": {"terms": {"field": "computed", "size": 20, "order": {"_key": "asc"}}},
                "histogram": {"histogram": {"field": "computed", "interval": 2.0, "hard_bounds": {"min": -2.0, "max": 4.0}}},
                "range": {"range": {"field": "computed", "ranges": [{"to": 0.0}, {"from": 0.0, "to": 8.0}, {"from": 8.0}]}}
            }),
            generation,
        )?;
    }
    Ok(())
}

#[test]
fn virtual_nested_sources_preserve_physical_operators_and_fast_path_guards() -> crate::Result<()> {
    let index = index(Generation::Full, 8, 1)?;
    assert_materialized_parity(
        &index,
        json!({
            "physical_parent": {"terms": {"field": "group", "order": {"_key": "asc"}}, "aggs": {
                "virtual_histogram": {"histogram": {"field": "computed", "interval": 1.0, "hard_bounds": {"min": 0.0, "max": 3.0}}}
            }},
            "virtual_parent": {"terms": {"field": "computed", "order": {"metric": "asc"}}, "aggs": {
                "metric": {"avg": {"field": "computed"}},
                "physical_multi": {"multi_terms": {"terms": [{"field": "group"}, {"field": "input"}]}},
                "physical_top": {"top_hits": {"size": 1, "sort": [{"input": "asc"}], "docvalue_fields": ["input"]}}
            }},
            "virtual_terms_physical_histogram": {"terms": {"field": "computed", "order": {"_key": "asc"}}, "aggs": {
                "histogram": {"histogram": {"field": "input", "interval": 2.0}}
            }},
            "filter": {"filter": "*",  "aggs": {"stats": {"stats": {"field": "computed"}}}},
            "composite": {"composite": {"size": 20, "sources": [{"group": {"terms": {"field": "group"}}}]}, "aggs": {
                "sum": {"sum": {"field": "computed"}}
            }},
            "range": {"range": {"field": "computed", "ranges": [{"to": 0.0}, {"from": 0.0}]}, "aggs": {
                "nested": {"terms": {"field": "computed", "order": {"_key": "asc"}}}
            }}
        }),
        Generation::Full,
    )
}

#[test]
fn virtual_buffer_reset_alignment_sparse_repeats_and_owned_identity() -> crate::Result<()> {
    let index = index(Generation::Optional, 8, 1)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let segment = &searcher.segment_readers()[0];
    let (mut registry, bindings, _) = registry(Generation::Optional, ColumnType::F64);
    registry.register(
        "empty".into(),
        Arc::new(SyntheticColumn {
            generation: Generation::BatchFailure,
            column_type: ColumnType::F64,
            bindings: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(AtomicUsize::new(0)),
        }),
    )?;
    let aggs = serde_json::from_value(json!({
        "first": {"stats": {"field": "computed"}},
        "second": {"sum": {"field": "computed"}},
        "empty": {"stats": {"field": "empty"}}
    }))
    .unwrap();
    let mut data = build_aggregations_data_from_req_with_virtual_columns(
        &aggs,
        segment,
        0,
        AggContextParams::default(),
        &registry,
    )?;
    let (source, _) = data
        .value_sources
        .resolve_virtual(segment, "computed")?
        .unwrap();
    assert_eq!(bindings.load(Ordering::Relaxed), 1);
    let (empty, _) = data
        .value_sources
        .resolve_virtual(segment, "empty")?
        .unwrap();
    for docs in [vec![4, 7], vec![0, 1, 3, 5], vec![1], vec![4, 7], vec![]] {
        data.column_block_accessor
            .fetch_source_block(&docs, &source, &mut data.value_sources);
        let actual: Vec<_> = data.column_block_accessor.iter_docid_vals(&docs).collect();
        let expected: Vec<_> = docs
            .iter()
            .filter_map(|&doc| {
                Generation::Optional
                    .value(doc as u64)
                    .map(|value| (doc, value.to_u64()))
            })
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(
            data.column_block_accessor.has_one_value_per_doc(&docs),
            expected.len() == docs.len()
        );
        data.column_block_accessor
            .fetch_source_block(&docs, &empty, &mut data.value_sources);
        assert!(data.column_block_accessor.values().is_empty());
        data.column_block_accessor.fetch_source_block_with_missing(
            &docs,
            &empty,
            &mut data.value_sources,
            Some(6.0f64.to_u64()),
        );
        assert_eq!(
            data.column_block_accessor
                .iter_docid_vals(&docs)
                .collect::<Vec<_>>(),
            docs.iter()
                .map(|&doc| (doc, 6.0f64.to_u64()))
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[test]
fn virtual_direct_constructor_flush_failure_and_framework_errors() -> crate::Result<()> {
    let index = index(Generation::FailsAfterThree, 1024, 1)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let segment = &searcher.segment_readers()[0];
    let (registry, _, calls) = registry(Generation::FailsAfterThree, ColumnType::F64);
    let aggs: Aggregations =
        serde_json::from_value(json!({"stats": {"stats": {"field": "computed", "missing": 10.0}}}))
            .unwrap();
    let mut collector = AggregationSegmentCollector::from_agg_req_and_reader_with_virtual_columns(
        &aggs,
        segment,
        0,
        &AggContextParams::default(),
        &registry,
    )?;
    collector.collect_block(&[0, 1]);
    collector.collect_block(&[4, 5]);
    collector.collect(6, 0.0); // failure during the final buffered flush also substitutes missing
    let result = collector
        .harvest()?
        .into_final_result(aggs.clone(), AggregationLimitsGuard::default())?;
    let result = serde_json::to_value(result).unwrap();
    assert_eq!(result["stats"]["count"], 5);
    assert_eq!(result["stats"]["sum"], 22.0);
    assert_eq!(calls.load(Ordering::Relaxed), 3);

    // Existing public lower-level constructor still forwards the ordinary physical path.
    let physical: Aggregations =
        serde_json::from_value(json!({"sum": {"sum": {"field": "input"}}})).unwrap();
    let mut collector = AggregationSegmentCollector::from_agg_req_and_reader(
        &physical,
        segment,
        0,
        &AggContextParams::default(),
    )?;
    collector.collect_block(&[0, 1, 2]);
    let result = collector
        .harvest()?
        .into_final_result(physical, AggregationLimitsGuard::default())?;
    assert_eq!(serde_json::to_value(result).unwrap()["sum"]["value"], 3.0);
    Ok(())
}

#[test]
fn virtual_unsupported_shapes_fail_before_empty_index_or_zero_hit_search() -> crate::Result<()> {
    let empty_index = index(Generation::Full, 0, 0)?;
    let populated_index = index(Generation::Full, 1, 1)?;
    let unsupported = [
        json!({"terms": {"field": "computed", "missing": 1}}),
        json!({"terms": {"field": "computed", "min_doc_count": 0}}),
        json!({"cardinality": {"field": "computed", "missing": "none"}}),
        json!({"date_histogram": {"field": "computed", "fixed_interval": "1s"}}),
        json!({"multi_terms": {"terms": [{"field": "input"}, {"field": "computed"}]}}),
        json!({"composite": {"size": 20, "sources": [{"source": {"terms": {"field": "computed"}}}]}}),
        json!({"top_hits": {"size": 1, "sort": [{"computed": "asc"}]}}),
        json!({"top_hits": {"size": 1, "sort": [{"input": "asc"}], "docvalue_fields": ["comp*"]}}),
    ];
    for index in [&empty_index, &populated_index] {
        let reader = index.reader()?;
        let searcher = reader.searcher();
        for request in &unsupported {
            let aggs: Aggregations = serde_json::from_value(
                json!({"parent": {"terms": {"field": "input"}, "aggs": {"bad": request}}}),
            )
            .unwrap();
            let (registry, bindings, _) = registry(Generation::Full, ColumnType::F64);
            let normal = AggregationCollector::from_aggs(aggs.clone(), AggContextParams::default())
                .with_virtual_columns(registry.clone());
            let error = normal.err().expect("must reject before search").to_string();
            assert!(
                error.contains("parent.bad") && error.contains("computed"),
                "{error}"
            );
            assert!(DistributedAggregationCollector::from_aggs(
                aggs.clone(),
                AggContextParams::default()
            )
            .with_virtual_columns(registry.clone())
            .is_err());
            for segment in searcher.segment_readers() {
                assert!(
                    AggregationSegmentCollector::from_agg_req_and_reader_with_virtual_columns(
                        &aggs,
                        segment,
                        0,
                        &AggContextParams::default(),
                        &registry
                    )
                    .is_err()
                );
            }
            assert_eq!(bindings.load(Ordering::Relaxed), 0);
        }
        let aggs: Aggregations =
            serde_json::from_value(json!({"stats": {"stats": {"field": "computed"}}})).unwrap();
        let (registry, _, calls) = registry(Generation::Full, ColumnType::F64);
        let collector = AggregationCollector::from_aggs(aggs, AggContextParams::default())
            .with_virtual_columns(registry)?;
        let result = serde_json::to_value(searcher.search(&EmptyQuery, &collector)?).unwrap();
        assert_eq!(result["stats"]["count"], 0);
        // Collection may issue an empty block to a bound runtime, which is part of the contract.
        assert!(calls.load(Ordering::Relaxed) <= searcher.segment_readers().len());
    }
    Ok(())
}

#[test]
fn virtual_registry_duplicates_types_collisions_and_unused_definitions() -> crate::Result<()> {
    let index = index(Generation::Full, 2, 1)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let aggs: Aggregations =
        serde_json::from_value(json!({"sum": {"sum": {"field": "computed"}}})).unwrap();
    for column_type in [
        ColumnType::Str,
        ColumnType::Bool,
        ColumnType::DateTime,
        ColumnType::IpAddr,
        ColumnType::Bytes,
    ] {
        let mut registry = VirtualColumns::default();
        assert!(registry
            .register(
                "computed".into(),
                Arc::new(SyntheticColumn {
                    generation: Generation::Full,
                    column_type,
                    bindings: Arc::new(AtomicUsize::new(0)),
                    calls: Arc::new(AtomicUsize::new(0)),
                })
            )
            .is_err());
    }
    for name in ["computed", "input", "unpopulated", "attributes.unpopulated"] {
        let (mut registry, _, _) = registry(Generation::Full, ColumnType::F64);
        let definition = Arc::new(SyntheticColumn {
            generation: Generation::Full,
            column_type: ColumnType::F64,
            bindings: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let registered = registry.register(name.to_owned(), definition);
        if name == "computed" {
            assert!(registered.is_err());
            continue;
        }
        registered?;
        let collector = AggregationCollector::from_aggs(aggs.clone(), AggContextParams::default())
            .with_virtual_columns(registry)?;
        assert!(
            searcher.search(&EmptyQuery, &collector).is_err(),
            "collision: {name}"
        );
    }
    let (registry, bindings, _) = registry(Generation::Full, ColumnType::F64);
    let physical_aggs =
        serde_json::from_value(json!({"sum": {"sum": {"field": "input"}}})).unwrap();
    let collector = AggregationCollector::from_aggs(physical_aggs, AggContextParams::default())
        .with_virtual_columns(registry)?;
    searcher.search(&AllQuery, &collector)?;
    assert_eq!(bindings.load(Ordering::Relaxed), 0);
    Ok(())
}

#[test]
fn virtual_integer_terms_have_unknown_bounds_and_numeric_order() -> crate::Result<()> {
    let index = index(Generation::Full, 8, 1)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    for column_type in [ColumnType::I64, ColumnType::U64] {
        let (registry, _, _) = registry(Generation::Full, column_type);
        let aggs: Aggregations = serde_json::from_value(json!({
            "terms": {"terms": {"field": "computed", "size": 20, "order": {"_key": "asc"}}},
            "histogram": {"histogram": {"field": "computed", "interval": 2.0, "hard_bounds": {"min": 0.0, "max": 4.0}}}
        })).unwrap();
        let collector = AggregationCollector::from_aggs(aggs, AggContextParams::default())
            .with_virtual_columns(registry)?;
        let result = serde_json::to_value(searcher.search(&AllQuery, &collector)?).unwrap();
        let keys: Vec<f64> = result["terms"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| bucket["key"].as_f64().unwrap())
            .collect();
        let expected: Vec<f64> = (0..8)
            .map(|input| {
                if column_type == ColumnType::I64 {
                    input as f64 * 2.0 - 5.0
                } else {
                    input as f64
                }
            })
            .collect();
        assert_eq!(keys, expected);
        assert!(result["histogram"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .all(|bucket| (0.0..=4.0).contains(&bucket["key"].as_f64().unwrap())));
    }
    Ok(())
}

#[test]
fn virtual_binding_errors_and_changed_declared_types_fail_preparation() -> crate::Result<()> {
    struct InvalidDefinition {
        change_type: bool,
        type_reads: AtomicUsize,
    }
    impl VirtualColumn for InvalidDefinition {
        fn column_type(&self) -> ColumnType {
            if self.type_reads.fetch_add(1, Ordering::Relaxed) > 0 && self.change_type {
                return ColumnType::U64;
            }
            ColumnType::F64
        }

        fn for_segment(
            &self,
            _reader: &SegmentReader,
        ) -> crate::Result<Box<dyn VirtualColumnEvaluator>> {
            Err(crate::TantivyError::InvalidArgument(
                "synthetic binding failure".into(),
            ))
        }
    }
    let index = index(Generation::Full, 1, 1)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    for change_type in [false, true] {
        let mut registry = VirtualColumns::default();
        registry.register(
            "computed".into(),
            Arc::new(InvalidDefinition {
                change_type,
                type_reads: AtomicUsize::new(0),
            }),
        )?;
        let aggs = serde_json::from_value(json!({"sum": {"sum": {"field": "computed"}}})).unwrap();
        let collector = AggregationCollector::from_aggs(aggs, AggContextParams::default())
            .with_virtual_columns(registry)?;
        let error = searcher
            .search(&EmptyQuery, &collector)
            .err()
            .expect("binding must fail even without hits")
            .to_string();
        if change_type {
            assert!(error.contains("Virtual type changed"), "{error}");
        } else {
            assert!(error.contains("synthetic binding failure"), "{error}");
        }
    }
    Ok(())
}
