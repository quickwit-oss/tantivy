use super::agg_req::Aggregations;
use super::agg_result::AggregationResults;
use super::buffered_sub_aggs::LowCardBufferedSubAggs;
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::AggContextParams;
// group buffering strategy is chosen explicitly by callers; no need to hash-group on the fly.
use crate::aggregation::agg_data::{
    build_aggregations_data_from_req, build_segment_agg_collectors_root, AggregationsSegmentCtx,
};
use crate::collector::{default_collect_segment_impl, Collector, SegmentCollector};
use crate::index::SegmentReader;
use crate::query::{AllScorer, Weight};
use crate::{DocId, SegmentOrdinal, TantivyError};

/// The default max bucket count, before the aggregation fails.
pub const DEFAULT_BUCKET_LIMIT: u32 = 65000;

/// The default memory limit in bytes before the aggregation fails. 500MB
pub const DEFAULT_MEMORY_LIMIT: u64 = 500_000_000;

/// Collector for aggregations.
///
/// The collector collects all aggregations by the underlying aggregation request.
pub struct AggregationCollector {
    agg: Aggregations,
    context: AggContextParams,
}

impl AggregationCollector {
    /// Create collector from aggregation request.
    ///
    /// Aggregation fails when the limits in `AggregationLimits` is exceeded. (memory limit and
    /// bucket limit)
    pub fn from_aggs(agg: Aggregations, context: AggContextParams) -> Self {
        Self { agg, context }
    }
}

/// Collector for distributed aggregations.
///
/// The collector collects all aggregations by the underlying aggregation request.
///
/// # Purpose
/// AggregationCollector returns `IntermediateAggregationResults` and not the final
/// `AggregationResults`, so that results from different indices can be merged and then converted
/// into the final `AggregationResults` via the `into_final_result()` method.
pub struct DistributedAggregationCollector {
    agg: Aggregations,
    context: AggContextParams,
}

impl DistributedAggregationCollector {
    /// Create collector from aggregation request.
    ///
    /// Aggregation fails when the limits in `AggregationLimits` is exceeded. (memory limit and
    /// bucket limit)
    pub fn from_aggs(agg: Aggregations, context: AggContextParams) -> Self {
        Self { agg, context }
    }
}

impl Collector for DistributedAggregationCollector {
    type Fruit = IntermediateAggregationResults;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        AggregationSegmentCollector::from_agg_req_and_reader(
            &self.agg,
            reader,
            segment_local_id,
            &self.context,
        )
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        merge_fruits(segment_fruits)
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
    ) -> crate::Result<<Self::Child as SegmentCollector>::Fruit> {
        AggregationSegmentCollector::collect_segment(
            &self.agg,
            &self.context,
            weight,
            segment_ord,
            reader,
        )
    }
}

impl Collector for AggregationCollector {
    type Fruit = AggregationResults;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        AggregationSegmentCollector::from_agg_req_and_reader(
            &self.agg,
            reader,
            segment_local_id,
            &self.context,
        )
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let res = merge_fruits(segment_fruits)?;
        res.into_final_result(self.agg.clone(), self.context.limits.clone())
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
    ) -> crate::Result<<Self::Child as SegmentCollector>::Fruit> {
        AggregationSegmentCollector::collect_segment(
            &self.agg,
            &self.context,
            weight,
            segment_ord,
            reader,
        )
    }
}

fn merge_fruits(
    mut segment_fruits: Vec<crate::Result<IntermediateAggregationResults>>,
) -> crate::Result<IntermediateAggregationResults> {
    if let Some(fruit) = segment_fruits.pop() {
        let mut fruit = fruit?;
        for next_fruit in segment_fruits {
            fruit.merge_fruits(next_fruit?)?;
        }
        Ok(fruit)
    } else {
        Ok(IntermediateAggregationResults::default())
    }
}

/// `AggregationSegmentCollector` does the aggregation collection on a segment.
pub struct AggregationSegmentCollector {
    aggs_with_accessor: AggregationsSegmentCtx,
    agg_collector: LowCardBufferedSubAggs,
    error: Option<TantivyError>,
}

impl AggregationSegmentCollector {
    fn collect_segment(
        agg: &Aggregations,
        context: &AggContextParams,
        weight: &dyn Weight,
        segment_ordinal: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> crate::Result<<Self as SegmentCollector>::Fruit> {
        let agg_data =
            build_aggregations_data_from_req(agg, reader, segment_ordinal, context.clone())?;
        // Column statistics include deleted documents and require matching the whole segment.
        let collect_all = !reader.has_deletes() && weight.scorer(reader, 1.0)?.is::<AllScorer>();
        let mut collector = Self::from_agg_data(agg_data, collect_all)?;
        default_collect_segment_impl(&mut collector, weight, reader, false)?;
        Ok(collector.harvest())
    }

    /// Creates an `AggregationSegmentCollector from` an [`Aggregations`] request and a segment
    /// reader. Also includes validation, e.g. checking field types and existence.
    pub fn from_agg_req_and_reader(
        agg: &Aggregations,
        reader: &SegmentReader,
        segment_ordinal: SegmentOrdinal,
        context: &AggContextParams,
    ) -> crate::Result<Self> {
        let agg_data =
            build_aggregations_data_from_req(agg, reader, segment_ordinal, context.clone())?;
        Self::from_agg_data(agg_data, false)
    }

    fn from_agg_data(
        mut agg_data: AggregationsSegmentCtx,
        collect_all: bool,
    ) -> crate::Result<Self> {
        let mut result = LowCardBufferedSubAggs::new(build_segment_agg_collectors_root(
            &mut agg_data,
            collect_all,
        )?);
        result
            .get_sub_agg_collector()
            .prepare_max_bucket(0, &agg_data)?; // prepare for bucket zero

        Ok(AggregationSegmentCollector {
            aggs_with_accessor: agg_data,
            agg_collector: result,
            error: None,
        })
    }
}

impl SegmentCollector for AggregationSegmentCollector {
    type Fruit = crate::Result<IntermediateAggregationResults>;

    #[inline]
    fn collect(&mut self, doc: DocId, _score: crate::Score) {
        if self.error.is_some() {
            return;
        }
        self.agg_collector.push(0, doc);
        match self
            .agg_collector
            .check_flush_local(&mut self.aggs_with_accessor)
        {
            Ok(_) => {}
            Err(e) => {
                self.error = Some(e);
            }
        }
    }
    fn collect_block(&mut self, docs: &[DocId]) {
        if self.error.is_some() {
            return;
        }

        match self.agg_collector.get_sub_agg_collector().collect(
            0,
            docs,
            &mut self.aggs_with_accessor,
        ) {
            Ok(_) => {}
            Err(e) => {
                self.error = Some(e);
            }
        }
    }

    fn harvest(mut self) -> Self::Fruit {
        if let Some(err) = self.error {
            return Err(err);
        }
        self.agg_collector.flush(&mut self.aggs_with_accessor)?;

        let mut sub_aggregation_res = IntermediateAggregationResults::default();
        self.agg_collector
            .get_sub_agg_collector()
            .add_intermediate_aggregation_result(
                &self.aggs_with_accessor,
                &mut sub_aggregation_res,
                0,
            )?;

        Ok(sub_aggregation_res)
    }
}

#[test]
fn test_column_stats_and_collecting_aggregations() -> crate::Result<()> {
    use crate::aggregation::tests::get_test_index_from_values;
    use crate::collector::Count;
    use crate::query::AllQuery;

    for merge_segments in [false, true] {
        let index = get_test_index_from_values(merge_segments, &[-5.0, 2.0, 10.0])?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        for request in [
            json!({
                "min": {"min": {"field": "score_f64"}},
                "max": {"max": {"field": "score_f64"}},
                "count": {"value_count": {"field": "score_f64"}},
                "count_str": {"value_count": {"field": "string_id"}},
                "count_empty": {"value_count": {"field": "absent"}}
            }),
            json!({
                "min": {"min": {"field": "score_f64"}},
                "sum": {"sum": {"field": "score_f64"}},
                "count": {"value_count": {"field": "score_f64"}},
                "count_missing": {"value_count": {"field": "absent", "missing": 42.0}},
                "missing": {"max": {"field": "absent", "missing": 42.0}},
                "empty": {"min": {"field": "absent"}}
            }),
            json!({
                "max": {"max": {"field": "score_f64"}},
                "terms": {
                    "terms": {"field": "string_id"},
                    "aggs": {
                        "min": {"min": {"field": "score_f64"}},
                        "count": {"value_count": {"field": "score_f64"}}
                    }
                }
            }),
        ] {
            let collector = AggregationCollector::from_aggs(
                serde_json::from_value(request)?,
                Default::default(),
            );
            let optimized = searcher.search(&AllQuery, &collector)?;
            // Tuple collectors use for_segment, without the full-segment optimization.
            let (collected, _) = searcher.search(&AllQuery, &(collector, Count))?;
            assert_eq!(optimized, collected);
        }
    }
    Ok(())
}

#[test]
fn test_column_stats_filtered_and_deleted_docs() -> crate::Result<()> {
    use crate::aggregation::tests::get_test_index_from_values;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::IndexRecordOption;
    use crate::Term;

    let index = get_test_index_from_values(true, &[-5.0, 2.0, 10.0])?;
    let reader = index.reader()?;
    let field = index.schema().get_field("string_id")?;
    let collector = AggregationCollector::from_aggs(
        serde_json::from_value(json!({
            "min": {"min": {"field": "score_f64"}},
            "max": {"max": {"field": "score_f64"}},
            "count": {"value_count": {"field": "score_f64"}}
        }))?,
        Default::default(),
    );
    let query = TermQuery::new(Term::from_field_text(field, "2"), IndexRecordOption::Basic);
    assert_eq!(
        serde_json::to_value(reader.searcher().search(&query, &collector)?)?,
        json!({"min": {"value": 2.0}, "max": {"value": 2.0}, "count": {"value": 1.0}}),
    );
    let mut writer: crate::IndexWriter = index.writer_for_tests()?;
    writer.delete_term(Term::from_field_text(field, "-5"));
    writer.delete_term(Term::from_field_text(field, "10"));
    writer.commit()?;
    reader.reload()?;
    assert!(reader.searcher().segment_reader(0).has_deletes());
    assert_eq!(
        serde_json::to_value(reader.searcher().search(&AllQuery, &collector)?)?,
        json!({"min": {"value": 2.0}, "max": {"value": 2.0}, "count": {"value": 1.0}}),
    );
    Ok(())
}
