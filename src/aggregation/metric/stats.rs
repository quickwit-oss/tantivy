use serde::{Deserialize, Serialize};

use crate::aggregation::f64_from_fastfield_u64;
use crate::fastfield::{DynamicFastFieldReader, FastFieldReader};
use crate::schema::Type;
use crate::DocId;

/// A multi-value metric aggregation that computes stats of numeric values that are
/// extracted from the aggregated documents.
/// Supported field types are u64, i64, and f64.
/// See [Stats] for returned statistics.
///
/// # JSON Format
/// ```json
/// {
///     "stats": {
///         "field": "score",
///     }
///  }
///  ```

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatsAggregation {
    /// The field name to compute the stats on.
    pub field: String,
}

impl StatsAggregation {
    /// Create new StatsAggregation from a field.
    pub fn from_field_name(field_name: String) -> Self {
        StatsAggregation { field: field_name }
    }
    /// Return the field name.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Stats contains a collection of statistics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    /// The number of documents.
    pub count: usize,
    /// The sum of the fast field values.
    pub sum: f64,
    /// The standard deviation of the fast field values. None for count == 0.
    pub standard_deviation: Option<f64>,
    /// The min value of the fast field values.
    pub min: Option<f64>,
    /// The max value of the fast field values.
    pub max: Option<f64>,
    /// The average of the values. None for count == 0.
    pub avg: Option<f64>,
}

/// IntermediateStats contains the mergeable version for stats.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateStats {
    count: usize,
    sum: f64,
    squared_sum: f64,
    min: f64,
    max: f64,
}

impl IntermediateStats {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            squared_sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }

    pub(crate) fn avg(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum / (self.count as f64))
        }
    }

    fn square_mean(&self) -> f64 {
        self.squared_sum / (self.count as f64)
    }

    pub(crate) fn standard_deviation(&self) -> Option<f64> {
        self.avg()
            .map(|average| (self.square_mean() - average * average).sqrt())
    }

    /// Merge data from other stats into this instance.
    pub fn merge_fruits(&mut self, other: &IntermediateStats) {
        self.count += other.count;
        self.sum += other.sum;
        self.squared_sum += other.squared_sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// compute final resultimprove_docs
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
        Stats {
            count: self.count,
            sum: self.sum,
            standard_deviation: self.standard_deviation(),
            min,
            max,
            avg: self.avg(),
        }
    }

    #[inline]
    fn collect(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.squared_sum += value * value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SegmentStatsCollector {
    pub(crate) stats: IntermediateStats,
    field_type: Type,
}

impl SegmentStatsCollector {
    pub fn from_req(field_type: Type) -> Self {
        Self {
            field_type,
            stats: IntermediateStats::new(),
        }
    }
    pub(crate) fn collect_block(&mut self, doc: &[DocId], field: &DynamicFastFieldReader<u64>) {
        let mut iter = doc.chunks_exact(4);
        for docs in iter.by_ref() {
            let val1 = field.get(docs[0]);
            let val2 = field.get(docs[1]);
            let val3 = field.get(docs[2]);
            let val4 = field.get(docs[3]);
            let val1 = f64_from_fastfield_u64(val1, &self.field_type);
            let val2 = f64_from_fastfield_u64(val2, &self.field_type);
            let val3 = f64_from_fastfield_u64(val3, &self.field_type);
            let val4 = f64_from_fastfield_u64(val4, &self.field_type);
            self.stats.collect(val1);
            self.stats.collect(val2);
            self.stats.collect(val3);
            self.stats.collect(val4);
        }
        for doc in iter.remainder() {
            let val = field.get(*doc);
            let val = f64_from_fastfield_u64(val, &self.field_type);
            self.stats.collect(val);
        }
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
    use crate::aggregation::tests::get_test_index_2_segments;
    use crate::aggregation::AggregationCollector;
    use crate::query::TermQuery;
    use crate::schema::IndexRecordOption;
    use crate::Term;

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

        let collector = AggregationCollector::from_aggs(agg_req_1);

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
                "standard_deviation": 13.65313748796613,
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
                "standard_deviation": 13.65313748796613,
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
                "standard_deviation": 13.819905785437443,
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
                "standard_deviation": 2.867441755680877,
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
                "standard_deviation": serde_json::Value::Null,
                "sum": 0.0,
            })
        );

        Ok(())
    }
}
