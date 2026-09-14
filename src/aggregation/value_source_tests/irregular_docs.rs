//! Buffered bucket children can receive repeated or out-of-order document IDs.

use super::*;

#[test]
fn virtual_children_preserve_repeated_docs_from_physical_buckets() -> crate::Result<()> {
    let mut schema = Schema::builder();
    let input = schema.add_u64_field("input", FAST);
    let group = schema.add_u64_field("group", FAST);
    let materialized = schema.add_f64_field("materialized", FAST);
    let index = Index::create_in_ram(schema.build());
    let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 15_000_000)?;
    writer
        .add_document(doc!(input => 1u64, group => 1u64, group => 2u64, materialized => -3.0f64))?;
    // Keep the materialized child optional: its physical source accepts repeated document IDs.
    writer.add_document(doc!(input => 0u64, group => 11u64))?;
    writer.commit()?;

    for parent in [
        json!({"histogram": {"field": "group", "interval": 10.0}}),
        json!({"range": {"field": "group", "ranges": [{"to": 10.0}, {"from": 10.0}]}}),
    ] {
        for missing in [None, Some(10.0)] {
            let mut request = parent.clone();
            let mut child = json!({"field": "computed"});
            if let Some(missing) = missing {
                child["missing"] = json!(missing);
            }
            request["aggs"] = json!({"child": {"stats": child}});
            assert_materialized_parity(&index, json!({"parent": request}), Generation::Optional)?;
        }
    }
    Ok(())
}

#[test]
fn virtual_children_preserve_order_from_physical_terms_missing() -> crate::Result<()> {
    let mut schema = Schema::builder();
    let input = schema.add_u64_field("input", FAST);
    let group = schema.add_u64_field("group", FAST);
    let materialized = schema.add_f64_field("materialized", FAST);
    let index = Index::create_in_ram(schema.build());
    let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 15_000_000)?;
    writer.add_document(doc!(input => 1u64, materialized => -3.0f64))?;
    writer.add_document(doc!(input => 2u64, group => 1u64, materialized => -1.0f64))?;
    writer.add_document(doc!(input => 0u64, group => 2u64))?;
    writer.commit()?;

    for missing in [None, Some(10.0)] {
        let mut child = json!({"field": "computed"});
        if let Some(missing) = missing {
            child["missing"] = json!(missing);
        }
        // The physical parent appends missing doc 0 after present doc 1 in the same bucket.
        assert_materialized_parity(
            &index,
            json!({"parent": {
                "terms": {"field": "group", "missing": 1, "order": {"_key": "asc"}},
                "aggs": {"child": {"stats": child}}
            }}),
            Generation::Optional,
        )?;
    }
    Ok(())
}

#[test]
fn virtual_normalized_input_restores_occurrences_and_missing_slots() -> crate::Result<()> {
    use crate::aggregation::block_accessor::ColumnBlockAccessor;
    use crate::aggregation::value_source::SegmentValueSources;

    struct OrderedColumn;
    struct OrderedEvaluator;

    impl VirtualColumn for OrderedColumn {
        fn column_type(&self) -> ColumnType {
            ColumnType::F64
        }

        fn for_segment(
            &self,
            _reader: &SegmentReader,
        ) -> crate::Result<Box<dyn VirtualColumnEvaluator>> {
            Ok(Box::new(OrderedEvaluator))
        }
    }

    impl VirtualColumnEvaluator for OrderedEvaluator {
        fn evaluate(&mut self, docs: &[DocId], output: &mut [Option<u64>]) {
            assert!(docs.windows(2).all(|pair| pair[0] < pair[1]));
            assert_eq!(docs.len(), output.len());
            assert!(output.iter().all(Option::is_none));
            for (&doc, slot) in docs.iter().zip(output) {
                *slot = Generation::Optional.value(u64::from(doc)).map(f64::to_u64);
            }
        }
    }

    let index = index(Generation::Optional, 6, 1)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let segment = &searcher.segment_readers()[0];
    let mut registry = VirtualColumns::default();
    registry.register("computed".to_owned(), Arc::new(OrderedColumn))?;
    let mut sources = SegmentValueSources::new(registry, segment)?;
    let (source, _) = sources.resolve(segment, "computed", None)?;
    let mut accessor = ColumnBlockAccessor::default();
    let mut limits = AggregationLimitsGuard::default();

    for docs in [
        &[5, 0, 2, 2, 3, 1][..],
        &[0, 0, 3][..],
        &[1, 1][..],
        &[4, 2][..],
        &[0, 1, 2][..],
        &[][..],
    ] {
        for missing in [None, Some(10.0)] {
            let mut expected: Vec<(DocId, f64)> = Vec::with_capacity(docs.len());
            let mut expected_unordered: Vec<(DocId, f64)> = Vec::with_capacity(docs.len());
            for &doc in docs {
                let value = Generation::Optional.value(u64::from(doc));
                if let Some(value) = value.or(missing) {
                    expected.push((doc, value));
                }
                if let Some(value) = value {
                    expected_unordered.push((doc, value));
                }
            }
            if let Some(missing) = missing {
                for &doc in docs {
                    if Generation::Optional.value(u64::from(doc)).is_none() {
                        expected_unordered.push((doc, missing));
                    }
                }
            }
            let missing = missing.map(f64::to_u64);
            accessor.fetch_source_block_with_missing(
                docs,
                &source,
                &mut sources,
                &mut limits,
                missing,
            )?;
            let actual: Vec<(DocId, f64)> = accessor
                .iter_docid_vals(docs)
                .map(|(doc, value)| (doc, f64::from_u64(value)))
                .collect();
            assert_eq!(actual, expected_unordered);

            for ordered in [false, true] {
                accessor.fetch_source_block_with_missing_unique_per_doc(
                    docs,
                    &source,
                    &mut sources,
                    &mut limits,
                    missing,
                    ordered,
                )?;
                let actual: Vec<(DocId, f64)> = accessor
                    .iter_docid_vals(docs)
                    .map(|(doc, value)| (doc, f64::from_u64(value)))
                    .collect();
                if ordered {
                    assert_eq!(actual, expected);
                } else {
                    assert_eq!(actual, expected_unordered);
                }
            }
        }
    }
    Ok(())
}
