use std::fmt::Debug;

use columnar::{Column, ColumnType};
use serde::{Deserialize, Serialize};

use super::*;
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::*;
use crate::TantivyError;

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
    #[serde(default, deserialize_with = "deserialize_option_f64")]
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
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateStats {
    /// The number of extracted values.
    pub(crate) count: u64,
    /// The sum of the extracted values.
    pub(crate) sum: f64,
    /// delta for sum needed for [Kahan algorithm for summation](https://en.wikipedia.org/wiki/Kahan_summation_algorithm)
    pub(crate) delta: f64,
    /// The min value.
    pub(crate) min: f64,
    /// The max value.
    pub(crate) max: f64,
}

impl Default for IntermediateStats {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            delta: 0.0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }
}

impl IntermediateStats {
    /// Returns the number of values collected.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns the sum of all values collected.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Merges the other stats intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateStats) {
        self.count += other.count;

        // kahan algorithm for sum
        let y = other.sum - (self.delta + other.delta);
        let t = self.sum + y;
        self.delta = (t - self.sum) - y;
        self.sum = t;

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
    pub(in crate::aggregation::metric) fn collect(&mut self, value: f64) {
        self.count += 1;

        // kahan algorithm for sum
        let y = value - self.delta;
        let t = self.sum + y;
        self.delta = (t - self.sum) - y;
        self.sum = t;

        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// Collects a block of f64 values with SIMD-friendly batching.
    ///
    /// Processes values in chunks, maintaining Kahan summation precision
    /// while enabling the compiler to vectorize min/max/sum operations.
    /// On x86_64 with AVX2 support, uses explicit SIMD intrinsics for
    /// 4-lane parallel processing.
    #[inline]
    pub(in crate::aggregation::metric) fn collect_block_f64(&mut self, values: &[f64]) {
        if values.is_empty() {
            return;
        }

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                // SAFETY: Runtime feature detection guarantees AVX2 is available
                unsafe {
                    self.collect_block_avx2(values);
                }
                return;
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            // NEON is always available on aarch64
            unsafe {
                self.collect_block_neon(values);
            }
            return;
        }

        #[allow(unreachable_code)]
        self.collect_block_scalar(values);
    }

    /// Scalar implementation optimized for autovectorization.
    ///
    /// Uses 4-lane parallel accumulators to help the compiler
    /// vectorize the inner loop.
    #[inline]
    fn collect_block_scalar(&mut self, values: &[f64]) {
        // Process in chunks of 4 for autovectorization-friendly pattern
        const LANES: usize = 4;
        let mut local_min = [f64::MAX; LANES];
        let mut local_max = [f64::MIN; LANES];
        let mut local_sum = [0.0f64; LANES];
        let mut local_delta = [0.0f64; LANES];

        let chunks = values.len() / LANES;
        let remainder = values.len() % LANES;

        for chunk_idx in 0..chunks {
            let base = chunk_idx * LANES;
            for lane in 0..LANES {
                let val = values[base + lane];
                local_min[lane] = local_min[lane].min(val);
                local_max[lane] = local_max[lane].max(val);
                // Kahan summation per lane
                let y = val - local_delta[lane];
                let t = local_sum[lane] + y;
                local_delta[lane] = (t - local_sum[lane]) - y;
                local_sum[lane] = t;
            }
        }

        // Handle remainder
        for i in 0..remainder {
            let val = values[chunks * LANES + i];
            local_min[0] = local_min[0].min(val);
            local_max[0] = local_max[0].max(val);
            let y = val - local_delta[0];
            let t = local_sum[0] + y;
            local_delta[0] = (t - local_sum[0]) - y;
            local_sum[0] = t;
        }

        // Reduce lanes into self
        self.count += values.len() as u64;
        for lane in 0..LANES {
            self.min = self.min.min(local_min[lane]);
            self.max = self.max.max(local_max[lane]);
            // Merge lane sum into self using Kahan
            let y = local_sum[lane] - (self.delta + local_delta[lane]);
            let t = self.sum + y;
            self.delta = (t - self.sum) - y;
            self.sum = t;
        }
    }

    /// AVX2 SIMD implementation for x86_64.
    ///
    /// Processes 4 f64 values per cycle using 256-bit AVX registers.
    /// Maintains per-lane Kahan summation for numerical stability.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn collect_block_avx2(&mut self, values: &[f64]) {
        use std::arch::x86_64::*;

        let mut v_min = _mm256_set1_pd(f64::MAX);
        let mut v_max = _mm256_set1_pd(f64::MIN);
        let mut v_sum = _mm256_setzero_pd();
        let mut v_delta = _mm256_setzero_pd();

        let chunks = values.len() / 4;
        let ptr = values.as_ptr();

        for i in 0..chunks {
            let v_val = _mm256_loadu_pd(ptr.add(i * 4));
            v_min = _mm256_min_pd(v_min, v_val);
            v_max = _mm256_max_pd(v_max, v_val);
            // Kahan summation: y = val - delta; t = sum + y; delta = (t - sum) - y; sum = t
            let v_y = _mm256_sub_pd(v_val, v_delta);
            let v_t = _mm256_add_pd(v_sum, v_y);
            v_delta = _mm256_sub_pd(_mm256_sub_pd(v_t, v_sum), v_y);
            v_sum = v_t;
        }

        // Extract lanes
        let mut lane_min = [0.0f64; 4];
        let mut lane_max = [0.0f64; 4];
        let mut lane_sum = [0.0f64; 4];
        let mut lane_delta = [0.0f64; 4];
        _mm256_storeu_pd(lane_min.as_mut_ptr(), v_min);
        _mm256_storeu_pd(lane_max.as_mut_ptr(), v_max);
        _mm256_storeu_pd(lane_sum.as_mut_ptr(), v_sum);
        _mm256_storeu_pd(lane_delta.as_mut_ptr(), v_delta);

        // Handle remainder with scalar
        for i in (chunks * 4)..values.len() {
            let val = values[i];
            lane_min[0] = lane_min[0].min(val);
            lane_max[0] = lane_max[0].max(val);
            let y = val - lane_delta[0];
            let t = lane_sum[0] + y;
            lane_delta[0] = (t - lane_sum[0]) - y;
            lane_sum[0] = t;
        }

        // Reduce into self
        self.count += values.len() as u64;
        for lane in 0..4 {
            self.min = self.min.min(lane_min[lane]);
            self.max = self.max.max(lane_max[lane]);
            let y = lane_sum[lane] - (self.delta + lane_delta[lane]);
            let t = self.sum + y;
            self.delta = (t - self.sum) - y;
            self.sum = t;
        }
    }

    /// NEON SIMD implementation for aarch64.
    ///
    /// Processes 2 f64 values per cycle using 128-bit NEON registers.
    /// Maintains per-lane Kahan summation for numerical stability.
    ///
    /// Note: NEON is always available on aarch64, so no `#[target_feature]` is needed.
    #[cfg(target_arch = "aarch64")]
    unsafe fn collect_block_neon(&mut self, values: &[f64]) {
        use std::arch::aarch64::*;

        let mut v_min = vdupq_n_f64(f64::MAX);
        let mut v_max = vdupq_n_f64(f64::MIN);
        let mut v_sum = vdupq_n_f64(0.0);
        let mut v_delta = vdupq_n_f64(0.0);

        let chunks = values.len() / 2;
        let ptr = values.as_ptr();

        for i in 0..chunks {
            let v_val = vld1q_f64(ptr.add(i * 2));
            v_min = vminq_f64(v_min, v_val);
            v_max = vmaxq_f64(v_max, v_val);
            // Kahan summation
            let v_y = vsubq_f64(v_val, v_delta);
            let v_t = vaddq_f64(v_sum, v_y);
            v_delta = vsubq_f64(vsubq_f64(v_t, v_sum), v_y);
            v_sum = v_t;
        }

        // Extract lanes
        let mut lane_min = [0.0f64; 2];
        let mut lane_max = [0.0f64; 2];
        let mut lane_sum = [0.0f64; 2];
        let mut lane_delta = [0.0f64; 2];
        vst1q_f64(lane_min.as_mut_ptr(), v_min);
        vst1q_f64(lane_max.as_mut_ptr(), v_max);
        vst1q_f64(lane_sum.as_mut_ptr(), v_sum);
        vst1q_f64(lane_delta.as_mut_ptr(), v_delta);

        // Handle remainder
        if values.len() % 2 == 1 {
            let val = *values.last().unwrap();
            lane_min[0] = lane_min[0].min(val);
            lane_max[0] = lane_max[0].max(val);
            let y = val - lane_delta[0];
            let t = lane_sum[0] + y;
            lane_delta[0] = (t - lane_sum[0]) - y;
            lane_sum[0] = t;
        }

        // Reduce into self
        self.count += values.len() as u64;
        for lane in 0..2 {
            self.min = self.min.min(lane_min[lane]);
            self.max = self.max.max(lane_max[lane]);
            let y = lane_sum[lane] - (self.delta + lane_delta[lane]);
            let t = self.sum + y;
            self.delta = (t - self.sum) - y;
            self.sum = t;
        }
    }
}

/// The type of stats aggregation to perform.
/// Note that not all stats types are supported in the stats aggregation.
#[derive(Clone, Copy, Debug)]
pub enum StatsType {
    /// The average of the values.
    Average,
    /// The count of the values.
    Count,
    /// The maximum value.
    Max,
    /// The minimum value.
    Min,
    /// The stats (count, sum, min, max, avg) of the values.
    Stats,
    /// The extended stats (count, sum, min, max, avg, sum_of_squares, variance, std_deviation,
    ExtendedStats(Option<f64>), // sigma
    /// The sum of the values.
    Sum,
    /// The percentiles of the values.
    Percentiles,
}

fn create_collector<const TYPE_ID: u8>(
    req: &MetricAggReqData,
) -> Box<dyn SegmentAggregationCollector> {
    Box::new(SegmentStatsCollector::<TYPE_ID> {
        name: req.name.clone(),
        collecting_for: req.collecting_for,
        is_number_or_date_type: req.is_number_or_date_type,
        missing_u64: req.missing_u64,
        accessor: req.accessor.clone(),
        buckets: vec![IntermediateStats::default()],
    })
}

/// Build a concrete `SegmentStatsCollector` depending on the column type.
pub(crate) fn build_segment_stats_collector(
    req: &MetricAggReqData,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    match req.field_type {
        ColumnType::I64 => Ok(create_collector::<{ ColumnType::I64 as u8 }>(req)),
        ColumnType::U64 => Ok(create_collector::<{ ColumnType::U64 as u8 }>(req)),
        ColumnType::F64 => Ok(create_collector::<{ ColumnType::F64 as u8 }>(req)),
        ColumnType::Bool => Ok(create_collector::<{ ColumnType::Bool as u8 }>(req)),
        ColumnType::DateTime => Ok(create_collector::<{ ColumnType::DateTime as u8 }>(req)),
        ColumnType::Bytes => Ok(create_collector::<{ ColumnType::Bytes as u8 }>(req)),
        ColumnType::Str => Ok(create_collector::<{ ColumnType::Str as u8 }>(req)),
        ColumnType::IpAddr => Ok(create_collector::<{ ColumnType::IpAddr as u8 }>(req)),
    }
}

#[repr(C)]
#[derive(Clone, Debug)]
pub(crate) struct SegmentStatsCollector<const COLUMN_TYPE_ID: u8> {
    pub(crate) missing_u64: Option<u64>,
    pub(crate) accessor: Column<u64>,
    pub(crate) is_number_or_date_type: bool,
    pub(crate) buckets: Vec<IntermediateStats>,
    pub(crate) name: String,
    pub(crate) collecting_for: StatsType,
}

impl<const COLUMN_TYPE_ID: u8> SegmentAggregationCollector
    for SegmentStatsCollector<COLUMN_TYPE_ID>
{
    #[inline]
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()> {
        let name = self.name.clone();

        self.prepare_max_bucket(parent_bucket_id, agg_data)?;
        let stats = self.buckets[parent_bucket_id as usize];
        let intermediate_metric_result = match self.collecting_for {
            StatsType::Average => {
                IntermediateMetricResult::Average(IntermediateAverage::from_stats(stats))
            }
            StatsType::Count => {
                IntermediateMetricResult::Count(IntermediateCount::from_stats(stats))
            }
            StatsType::Max => IntermediateMetricResult::Max(IntermediateMax::from_stats(stats)),
            StatsType::Min => IntermediateMetricResult::Min(IntermediateMin::from_stats(stats)),
            StatsType::Stats => IntermediateMetricResult::Stats(stats),
            StatsType::Sum => IntermediateMetricResult::Sum(IntermediateSum::from_stats(stats)),
            _ => {
                return Err(TantivyError::InvalidArgument(format!(
                    "Unsupported stats type for stats aggregation: {:?}",
                    self.collecting_for
                )))
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
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        // TODO: remove once we fetch all values for all bucket ids in one go
        if docs.len() == 1 && self.missing_u64.is_none() {
            collect_stats::<COLUMN_TYPE_ID>(
                &mut self.buckets[parent_bucket_id as usize],
                self.accessor.values_for_doc(docs[0]),
                self.is_number_or_date_type,
            )?;

            return Ok(());
        }
        agg_data.column_block_accessor.fetch_block_with_missing(
            docs,
            &self.accessor,
            self.missing_u64,
        );
        collect_stats::<COLUMN_TYPE_ID>(
            &mut self.buckets[parent_bucket_id as usize],
            agg_data.column_block_accessor.iter_vals(),
            self.is_number_or_date_type,
        )?;

        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        _agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        let required_buckets = (max_bucket as usize) + 1;
        if self.buckets.len() < required_buckets {
            self.buckets
                .resize_with(required_buckets, IntermediateStats::default);
        }
        Ok(())
    }
}

#[inline]
fn collect_stats<const COLUMN_TYPE_ID: u8>(
    stats: &mut IntermediateStats,
    vals: impl Iterator<Item = u64>,
    is_number_or_date_type: bool,
) -> crate::Result<()> {
    if is_number_or_date_type {
        // Stream through values with a fixed-size stack buffer for SIMD-friendly processing.
        // O(1) memory regardless of input size.
        const CHUNK_SIZE: usize = 256;
        let mut buf = [0.0f64; CHUNK_SIZE];
        let mut buf_len = 0;
        for val in vals {
            buf[buf_len] = convert_to_f64::<COLUMN_TYPE_ID>(val);
            buf_len += 1;
            if buf_len == CHUNK_SIZE {
                stats.collect_block_f64(&buf[..buf_len]);
                buf_len = 0;
            }
        }
        if buf_len > 0 {
            stats.collect_block_f64(&buf[..buf_len]);
        }
    } else {
        // For non-numeric types, just count occurrences
        let mut count = 0u64;
        for _val in vals {
            count += 1;
        }
        stats.count += count;
    }

    Ok(())
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
    use crate::{Index, IndexWriter, Term};

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
            "count_str": {
                "value_count": {
                    "field": "text",
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

        assert_eq!(
            res["count_str"],
            json!({
                "value": 7.0,
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
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
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
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
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

        // From string
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_stats": {
                "stats": {
                    "field": "json.partially_empty",
                    "missing": "0.0"
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

    // ─── Unit tests for IntermediateStats::collect_block_f64 ───

    #[test]
    fn test_collect_block_f64_empty() {
        let mut stats = super::IntermediateStats::default();
        stats.collect_block_f64(&[]);
        assert_eq!(stats.count, 0);
        assert_eq!(stats.sum, 0.0);
    }

    #[test]
    fn test_collect_block_f64_single() {
        let mut stats = super::IntermediateStats::default();
        stats.collect_block_f64(&[42.0]);
        assert_eq!(stats.count, 1);
        assert_eq!(stats.sum, 42.0);
        assert_eq!(stats.min, 42.0);
        assert_eq!(stats.max, 42.0);
    }

    #[test]
    fn test_collect_block_f64_basic() {
        let mut stats = super::IntermediateStats::default();
        stats.collect_block_f64(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(stats.count, 5);
        assert!((stats.sum - 15.0).abs() < 1e-10);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 5.0);
    }

    #[test]
    fn test_collect_block_f64_matches_scalar_collect() {
        let values: Vec<f64> = (0..1000).map(|i| i as f64 * 0.1 + 0.01).collect();

        let mut stats_block = super::IntermediateStats::default();
        stats_block.collect_block_f64(&values);

        let mut stats_scalar = super::IntermediateStats::default();
        for &v in &values {
            stats_scalar.collect(v);
        }

        assert_eq!(stats_block.count, stats_scalar.count);
        assert!(
            (stats_block.sum - stats_scalar.sum).abs() < 1e-6,
            "sum mismatch: block={} scalar={}",
            stats_block.sum,
            stats_scalar.sum,
        );
        assert_eq!(stats_block.min, stats_scalar.min);
        assert_eq!(stats_block.max, stats_scalar.max);
    }

    #[test]
    fn test_collect_block_f64_large() {
        let values: Vec<f64> = (0..1024).map(|i| i as f64).collect();
        let mut stats = super::IntermediateStats::default();
        stats.collect_block_f64(&values);
        assert_eq!(stats.count, 1024);
        assert!((stats.sum - 1024.0 * 1023.0 / 2.0).abs() < 1e-6);
        assert_eq!(stats.min, 0.0);
        assert_eq!(stats.max, 1023.0);
    }

    #[test]
    fn test_collect_block_f64_negative_values() {
        let mut stats = super::IntermediateStats::default();
        stats.collect_block_f64(&[-10.0, -5.0, 0.0, 5.0, 10.0]);
        assert_eq!(stats.count, 5);
        assert!((stats.sum - 0.0).abs() < 1e-10);
        assert_eq!(stats.min, -10.0);
        assert_eq!(stats.max, 10.0);
    }

    #[test]
    fn test_collect_block_f64_incremental() {
        let mut stats = super::IntermediateStats::default();
        stats.collect_block_f64(&[1.0, 2.0]);
        stats.collect_block_f64(&[3.0, 4.0]);
        assert_eq!(stats.count, 4);
        assert!((stats.sum - 10.0).abs() < 1e-10);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 4.0);
    }
}
