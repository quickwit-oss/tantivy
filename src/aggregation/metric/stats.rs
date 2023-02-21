use columnar::{Cardinality, Column, ColumnType};
use serde::{Deserialize, Serialize};

use super::*;
use crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::{f64_from_fastfield_u64, VecWithNames};
use crate::{DocId, TantivyError};

/// A multi-value metric aggregation that computes a collection of statistics on numeric values that
/// are extracted from the aggregated documents.
/// See [`Stats`] for returned statistics.
///
/// # JSON Format
/// ```json
/// {
///     "stats": {
///         "field": "score"
///     }
///  }
/// ```

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatsAggregation {
    /// The field name to compute the stats on.
    pub field: String,
}

impl StatsAggregation {
    /// Creates a new [`StatsAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        StatsAggregation { field: field_name }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Stats contains a collection of statistics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    /// The number of documents.
    pub count: u64,
    /// The sum of the fast field values.
    pub sum: f64,
    /// The min value of the fast field values.
    pub min: Option<f64>,
    /// The max value of the fast field values.
    pub max: Option<f64>,
    /// The average of the fast field values. `None` if count equals zero.
    pub avg: Option<f64>,
}

impl Stats {
    pub(crate) fn get_value(&self, agg_property: &str) -> crate::Result<Option<f64>> {
        match agg_property {
            "count" => Ok(Some(self.count as f64)),
            "sum" => Ok(Some(self.sum)),
            "min" => Ok(self.min),
            "max" => Ok(self.max),
            "avg" => Ok(self.avg),
            _ => Err(TantivyError::InvalidArgument(format!(
                "Unknown property {} on stats metric aggregation",
                agg_property
            ))),
        }
    }
}

/// Intermediate result of the stats aggregation that can be combined with other intermediate
/// results.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateStats {
    /// The number of extracted values.
    count: u64,
    /// The sum of the extracted values.
    sum: f64,
    /// The min value.
    min: f64,
    /// The max value.
    max: f64,
}

impl Default for IntermediateStats {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }
}

impl IntermediateStats {
    /// Merges the other stats intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateStats) {
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Computes the final stats value.
    pub fn finalize(&self) -> Stats {
        let min = if self.count == 0 {
            None
        } else {
            Some(self.min)
        };
        let max = if self.count == 0 {
            None
        } else {
            Some(self.max)
        };
        let avg = if self.count == 0 {
            None
        } else {
            Some(self.sum / (self.count as f64))
        };
        Stats {
            count: self.count,
            sum: self.sum,
            min,
            max,
            avg,
        }
    }

    #[inline]
    fn collect(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentStatsType {
    Average,
    Count,
    Max,
    Min,
    Stats,
    Sum,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SegmentStatsCollector {
    field_type: ColumnType,
    pub(crate) collecting_for: SegmentStatsType,
    pub(crate) stats: IntermediateStats,
    pub(crate) accessor_idx: usize,
}

impl SegmentStatsCollector {
    pub fn from_req(
        field_type: ColumnType,
        collecting_for: SegmentStatsType,
        accessor_idx: usize,
    ) -> Self {
        Self {
            field_type,
            collecting_for,
            stats: IntermediateStats::default(),
            accessor_idx,
        }
    }
    #[inline]
    pub(crate) fn collect_block_with_field(&mut self, docs: &[DocId], field: &Column<u64>) {
        if field.get_cardinality() == Cardinality::Full {
            for doc in docs {
                let val = field.values.get_val(*doc);
                let val1 = f64_from_fastfield_u64(val, &self.field_type);
                self.stats.collect(val1);
            }
        } else {
            for doc in docs {
                for val in field.values(*doc) {
                    let val1 = f64_from_fastfield_u64(val, &self.field_type);
                    self.stats.collect(val1);
                }
            }
        }
    }
}

impl SegmentAggregationCollector for SegmentStatsCollector {
    fn into_intermediate_aggregations_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<IntermediateAggregationResults> {
        let name = agg_with_accessor.metrics.keys[self.accessor_idx].to_string();

        let intermediate_metric_result = match self.collecting_for {
            SegmentStatsType::Average => {
                IntermediateMetricResult::Average(IntermediateAverage::from_collector(*self))
            }
            SegmentStatsType::Count => {
                IntermediateMetricResult::Count(IntermediateCount::from_collector(*self))
            }
            SegmentStatsType::Max => {
                IntermediateMetricResult::Max(IntermediateMax::from_collector(*self))
            }
            SegmentStatsType::Min => {
                IntermediateMetricResult::Min(IntermediateMin::from_collector(*self))
            }
            SegmentStatsType::Stats => IntermediateMetricResult::Stats(self.stats),
            SegmentStatsType::Sum => {
                IntermediateMetricResult::Sum(IntermediateSum::from_collector(*self))
            }
        };

        let metrics = Some(VecWithNames::from_entries(vec![(
            name,
            intermediate_metric_result,
        )]));

        Ok(IntermediateAggregationResults {
            metrics,
            buckets: None,
        })
    }

    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let field = &agg_with_accessor.metrics.values[self.accessor_idx].accessor;

        for val in field.values(doc) {
            let val1 = f64_from_fastfield_u64(val, &self.field_type);
            self.stats.collect(val1);
        }

        Ok(())
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let field = &agg_with_accessor.metrics.values[self.accessor_idx].accessor;
        self.collect_block_with_field(docs, field);
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::iter;

    use serde_json::Value;

    use crate::aggregation::agg_req::{
        Aggregation, Aggregations, BucketAggregation, BucketAggregationType, MetricAggregation,
        RangeAggregation,
    };
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::metric::StatsAggregation;
    use crate::aggregation::tests::{get_test_index_2_segments, get_test_index_from_values};
    use crate::aggregation::AggregationCollector;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::IndexRecordOption;
    use crate::Term;

    #[test]
    fn test_aggregation_stats_empty_index() -> crate::Result<()> {
        // test index without segments
        let values = vec![];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = vec![(
            "stats".to_string(),
            Aggregation::Metric(MetricAggregation::Stats(StatsAggregation::from_field_name(
                "score".to_string(),
            ))),
        )]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(agg_req_1, None);

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["stats"],
            json!({
                "avg": Value::Null,
                "count": 0,
                "max": Value::Null,
                "min": Value::Null,
                "sum": 0.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_stats_simple() -> crate::Result<()> {
        // test index without segments
        let values = vec![10.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = vec![(
            "stats".to_string(),
            Aggregation::Metric(MetricAggregation::Stats(StatsAggregation::from_field_name(
                "score".to_string(),
            ))),
        )]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(agg_req_1, None);

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["stats"],
            json!({
                "avg": 10.0,
                "count": 1,
                "max": 10.0,
                "min": 10.0,
                "sum": 10.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_stats() -> crate::Result<()> {
        let index = get_test_index_2_segments(false)?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "cool"),
            IndexRecordOption::Basic,
        );

        let agg_req_1: Aggregations = vec![
            (
                "stats_i64".to_string(),
                Aggregation::Metric(MetricAggregation::Stats(StatsAggregation::from_field_name(
                    "score_i64".to_string(),
                ))),
            ),
            (
                "stats_f64".to_string(),
                Aggregation::Metric(MetricAggregation::Stats(StatsAggregation::from_field_name(
                    "score_f64".to_string(),
                ))),
            ),
            (
                "stats".to_string(),
                Aggregation::Metric(MetricAggregation::Stats(StatsAggregation::from_field_name(
                    "score".to_string(),
                ))),
            ),
            (
                "range".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field: "score".to_string(),
                        ranges: vec![
                            (3f64..7f64).into(),
                            (7f64..19f64).into(),
                            (19f64..20f64).into(),
                        ],
                        ..Default::default()
                    }),
                    sub_aggregation: iter::once((
                        "stats".to_string(),
                        Aggregation::Metric(MetricAggregation::Stats(
                            StatsAggregation::from_field_name("score".to_string()),
                        )),
                    ))
                    .collect(),
                }),
            ),
        ]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(agg_req_1, None);

        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        assert_eq!(
            res["stats"],
            json!({
                "avg": 12.142857142857142,
                "count": 7,
                "max": 44.0,
                "min": 1.0,
                "sum": 85.0
            })
        );

        assert_eq!(
            res["stats_i64"],
            json!({
                "avg": 12.142857142857142,
                "count": 7,
                "max": 44.0,
                "min": 1.0,
                "sum": 85.0
            })
        );

        assert_eq!(
            res["stats_f64"],
            json!({
                "avg":  12.214285714285714,
                "count": 7,
                "max": 44.5,
                "min": 1.0,
                "sum": 85.5
            })
        );

        assert_eq!(
            res["range"]["buckets"][2]["stats"],
            json!({
                "avg": 10.666666666666666,
                "count": 3,
                "max": 14.0,
                "min": 7.0,
                "sum": 32.0
            })
        );

        assert_eq!(
            res["range"]["buckets"][3]["stats"],
            json!({
                "avg": serde_json::Value::Null,
                "count": 0,
                "max": serde_json::Value::Null,
                "min": serde_json::Value::Null,
                "sum": 0.0,
            })
        );

        Ok(())
    }
}
