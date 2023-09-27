use columnar::ColumnType;
use serde::{Deserialize, Serialize};

use super::*;
use crate::aggregation::agg_req_with_accessor::{
    AggregationWithAccessor, AggregationsWithAccessor,
};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::{f64_from_fastfield_u64, f64_to_fastfield_u64};
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
    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "field": "my_numbers", "missing": "10.0" }
    #[serde(default)]
    pub missing: Option<f64>,
}

impl StatsAggregation {
    /// Creates a new [`StatsAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        StatsAggregation {
            field: field_name,
            missing: None,
        }
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
                "Unknown property {agg_property} on stats metric aggregation"
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
    missing: Option<u64>,
    field_type: ColumnType,
    pub(crate) collecting_for: SegmentStatsType,
    pub(crate) stats: IntermediateStats,
    pub(crate) accessor_idx: usize,
    val_cache: Vec<u64>,
}

impl SegmentStatsCollector {
    pub fn from_req(
        field_type: ColumnType,
        collecting_for: SegmentStatsType,
        accessor_idx: usize,
        missing: Option<f64>,
    ) -> Self {
        let missing = missing.and_then(|val| f64_to_fastfield_u64(val, &field_type));
        Self {
            field_type,
            collecting_for,
            stats: IntermediateStats::default(),
            accessor_idx,
            missing,
            val_cache: Default::default(),
        }
    }
    #[inline]
    pub(crate) fn collect_block_with_field(
        &mut self,
        docs: &[DocId],
        agg_accessor: &mut AggregationWithAccessor,
    ) {
        if let Some(missing) = self.missing.as_ref() {
            agg_accessor.column_block_accessor.fetch_block_with_missing(
                docs,
                &agg_accessor.accessor,
                *missing,
            );
        } else {
            agg_accessor
                .column_block_accessor
                .fetch_block(docs, &agg_accessor.accessor);
        }
        for val in agg_accessor.column_block_accessor.iter_vals() {
            let val1 = f64_from_fastfield_u64(val, &self.field_type);
            self.stats.collect(val1);
        }
    }
}

impl SegmentAggregationCollector for SegmentStatsCollector {
    #[inline]
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();

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

        results.push(
            name,
            IntermediateAggregationResult::Metric(intermediate_metric_result),
        )?;

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let field = &agg_with_accessor.aggs.values[self.accessor_idx].accessor;
        if let Some(missing) = self.missing {
            let mut has_val = false;
            for val in field.values_for_doc(doc) {
                let val1 = f64_from_fastfield_u64(val, &self.field_type);
                self.stats.collect(val1);
                has_val = true;
            }
            if !has_val {
                self.stats
                    .collect(f64_from_fastfield_u64(missing, &self.field_type));
            }
        } else {
            for val in field.values_for_doc(doc) {
                let val1 = f64_from_fastfield_u64(val, &self.field_type);
                self.stats.collect(val1);
            }
        }

        Ok(())
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let field = &mut agg_with_accessor.aggs.values[self.accessor_idx];
        self.collect_block_with_field(docs, field);
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use serde_json::Value;

    use crate::aggregation::agg_req::{Aggregation, Aggregations};
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::tests::{
        exec_request_with_query, get_test_index_2_segments, get_test_index_from_values,
    };
    use crate::aggregation::AggregationCollector;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, FAST};
    use crate::{Index, Term};

    #[test]
    fn test_aggregation_stats_empty_index() -> crate::Result<()> {
        // test index without segments
        let values = vec![];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "stats": {
                "stats": {
                    "field": "score",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

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
        let values = vec![10.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "stats": {
                "stats": {
                    "field": "score",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

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

        let range_agg: Aggregation = {
            serde_json::from_value(json!({
                "range": {
                    "field": "score",
                    "ranges": [ { "from": 3.0f64, "to": 7.0f64 }, { "from": 7.0f64, "to": 19.0f64 }, { "from": 19.0f64, "to": 20.0f64 }  ]
                },
                "aggs": {
                    "stats": {
                        "stats": {
                            "field": "score"
                        }
                    }
                }
            }))
            .unwrap()
        };

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "stats_i64": {
                "stats": {
                    "field": "score_i64",
                },
            },
            "stats_f64": {
                "stats": {
                    "field": "score_f64",
                },
            },
            "stats": {
                "stats": {
                    "field": "score",
                },
            },
            "range": range_agg
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());

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

    #[test]
    fn test_stats_json() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        // => Segment with empty json
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        // => Segment with json, but no field partially_empty
        index_writer
            .add_document(doc!(json => json!({"different_field": "blue"})))
            .unwrap();
        index_writer.commit().unwrap();
        //// => Segment with field partially_empty
        index_writer
            .add_document(doc!(json => json!({"partially_empty": 10.0})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "stats": {
                    "field": "json.partially_empty"
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["my_stats"],
            json!({
                "avg":  10.0,
                "count": 1,
                "max": 10.0,
                "min": 10.0,
                "sum": 10.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_stats_json_missing() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        // => Segment with empty json
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        // => Segment with json, but no field partially_empty
        index_writer
            .add_document(doc!(json => json!({"different_field": "blue"})))
            .unwrap();
        index_writer.commit().unwrap();
        //// => Segment with field partially_empty
        index_writer
            .add_document(doc!(json => json!({"partially_empty": 10.0})))
            .unwrap();
        index_writer.add_document(doc!())?;
        index_writer.commit().unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "stats": {
                    "field": "json.partially_empty",
                    "missing": 0.0
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["my_stats"],
            json!({
                "avg":  2.5,
                "count": 4,
                "max": 10.0,
                "min": 0.0,
                "sum": 10.0
            })
        );

        Ok(())
    }

    #[test]
    fn test_stats_json_missing_sub_agg() -> crate::Result<()> {
        // This test verifies the `collect` method (in contrast to `collect_block`), which is
        // called when the sub-aggregations are flushed.
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("texts", FAST);
        let score_field_f64 = schema_builder.add_f64_field("score", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                score_field_f64 => 10.0f64,
                text_field => "a"
            ))?;

            index_writer.add_document(doc!(text_field => "a"))?;

            index_writer.commit()?;
        }

        let agg_req: Aggregations = {
            serde_json::from_value(json!({
                "range_with_stats": {
                    "terms": {
                        "field": "texts"
                    },
                    "aggs": {
                        "my_stats": {
                            "stats": {
                                "field": "score",
                                "missing": 0.0
                            }
                        }
                    }
                }
            }))
            .unwrap()
        };

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res["range_with_stats"]["buckets"][0]["my_stats"]["count"],
            2
        );
        assert_eq!(
            res["range_with_stats"]["buckets"][0]["my_stats"]["min"],
            0.0
        );
        assert_eq!(
            res["range_with_stats"]["buckets"][0]["my_stats"]["avg"],
            5.0
        );

        Ok(())
    }
}
