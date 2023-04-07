//! Contains the intermediate aggregation tree, that can be merged.
//! Intermediate aggregation results can be used to merge results between segments or between
//! indices.

use std::cmp::Ordering;

use columnar::ColumnType;
use itertools::Itertools;
use rustc_hash::FxHashMap;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::agg_req::{
    Aggregations, AggregationsInternal, BucketAggregationInternal, BucketAggregationType,
    MetricAggregation,
};
use super::agg_result::{AggregationResult, BucketResult, MetricResult, RangeBucketEntry};
use super::bucket::{
    cut_off_buckets, get_agg_name_and_property, intermediate_histogram_buckets_to_final_buckets,
    GetDocCount, Order, OrderTarget, RangeAggregation, SegmentHistogramBucketEntry,
    TermsAggregation,
};
use super::metric::{
    IntermediateAverage, IntermediateCount, IntermediateMax, IntermediateMin, IntermediateStats,
    IntermediateSum, PercentilesCollector,
};
use super::segment_agg_result::AggregationLimits;
use super::{format_date, AggregationError, Key, SerializedKey, VecWithNames};
use crate::aggregation::agg_result::{AggregationResults, BucketEntries, BucketEntry};
use crate::aggregation::bucket::TermsAggregationInternal;
use crate::TantivyError;

/// Contains the intermediate aggregation result, which is optimized to be merged with other
/// intermediate results.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateAggregationResults {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics: Option<VecWithNames<IntermediateMetricResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) buckets: Option<VecWithNames<IntermediateBucketResult>>,
}

impl IntermediateAggregationResults {
    /// Convert intermediate result and its aggregation request to the final result.
    pub fn into_final_result(
        self,
        req: Aggregations,
        limits: &AggregationLimits,
    ) -> crate::Result<AggregationResults> {
        let res = self.into_final_result_internal(&(req.into()), limits)?;
        let bucket_count = res.get_bucket_count() as u32;
        if bucket_count > limits.get_bucket_limit() {
            return Err(TantivyError::AggregationError(
                AggregationError::BucketLimitExceeded {
                    limit: limits.get_bucket_limit(),
                    current: bucket_count,
                },
            ));
        }
        Ok(res)
    }

    /// Convert intermediate result and its aggregation request to the final result.
    ///
    /// Internal function, AggregationsInternal is used instead Aggregations, which is optimized
    /// for internal processing, by splitting metric and buckets into separate groups.
    pub(crate) fn into_final_result_internal(
        self,
        req: &AggregationsInternal,
        limits: &AggregationLimits,
    ) -> crate::Result<AggregationResults> {
        // Important assumption:
        // When the tree contains buckets/metric, we expect it to have all buckets/metrics from the
        // request
        let mut results: FxHashMap<String, AggregationResult> = FxHashMap::default();

        if let Some(buckets) = self.buckets {
            convert_and_add_final_buckets_to_result(&mut results, buckets, &req.buckets, limits)?
        } else {
            // When there are no buckets, we create empty buckets, so that the serialized json
            // format is constant
            add_empty_final_buckets_to_result(&mut results, &req.buckets, limits)?
        };

        if let Some(metrics) = self.metrics {
            convert_and_add_final_metrics_to_result(&mut results, metrics, &req.metrics);
        } else {
            // When there are no metrics, we create empty metric results, so that the serialized
            // json format is constant
            add_empty_final_metrics_to_result(&mut results, &req.metrics)?;
        }

        Ok(AggregationResults(results))
    }

    pub(crate) fn empty_from_req(req: &AggregationsInternal) -> Self {
        let metrics = if req.metrics.is_empty() {
            None
        } else {
            let metrics = req
                .metrics
                .iter()
                .map(|(key, req)| {
                    (
                        key.to_string(),
                        IntermediateMetricResult::empty_from_req(req),
                    )
                })
                .collect();
            Some(VecWithNames::from_entries(metrics))
        };

        let buckets = if req.buckets.is_empty() {
            None
        } else {
            let buckets = req
                .buckets
                .iter()
                .map(|(key, req)| {
                    (
                        key.to_string(),
                        IntermediateBucketResult::empty_from_req(&req.bucket_agg),
                    )
                })
                .collect();
            Some(VecWithNames::from_entries(buckets))
        };

        Self { metrics, buckets }
    }

    /// Merge another intermediate aggregation result into this result.
    ///
    /// The order of the values need to be the same on both results. This is ensured when the same
    /// (key values) are present on the underlying `VecWithNames` struct.
    pub fn merge_fruits(&mut self, other: IntermediateAggregationResults) -> crate::Result<()> {
        if let (Some(buckets_left), Some(buckets_right)) = (&mut self.buckets, other.buckets) {
            for (bucket_left, bucket_right) in
                buckets_left.values_mut().zip(buckets_right.into_values())
            {
                bucket_left.merge_fruits(bucket_right)?;
            }
        }

        if let (Some(metrics_left), Some(metrics_right)) = (&mut self.metrics, other.metrics) {
            for (metric_left, metric_right) in
                metrics_left.values_mut().zip(metrics_right.into_values())
            {
                metric_left.merge_fruits(metric_right)?;
            }
        }
        Ok(())
    }
}

fn convert_and_add_final_metrics_to_result(
    results: &mut FxHashMap<String, AggregationResult>,
    metrics: VecWithNames<IntermediateMetricResult>,
    metrics_req: &VecWithNames<MetricAggregation>,
) {
    let metric_result_with_request = metrics.into_iter().zip(metrics_req.values());
    results.extend(
        metric_result_with_request
            .into_iter()
            .map(|((key, metric), req)| {
                (
                    key,
                    AggregationResult::MetricResult(metric.into_final_metric_result(req)),
                )
            }),
    );
}

fn add_empty_final_metrics_to_result(
    results: &mut FxHashMap<String, AggregationResult>,
    req_metrics: &VecWithNames<MetricAggregation>,
) -> crate::Result<()> {
    results.extend(req_metrics.iter().map(|(key, req)| {
        let empty_bucket = IntermediateMetricResult::empty_from_req(req);
        (
            key.to_string(),
            AggregationResult::MetricResult(empty_bucket.into_final_metric_result(req)),
        )
    }));
    Ok(())
}

fn add_empty_final_buckets_to_result(
    results: &mut FxHashMap<String, AggregationResult>,
    req_buckets: &VecWithNames<BucketAggregationInternal>,
    limits: &AggregationLimits,
) -> crate::Result<()> {
    let requested_buckets = req_buckets.iter();
    for (key, req) in requested_buckets {
        let empty_bucket =
            AggregationResult::BucketResult(BucketResult::empty_from_req(req, limits)?);
        results.insert(key.to_string(), empty_bucket);
    }
    Ok(())
}

fn convert_and_add_final_buckets_to_result(
    results: &mut FxHashMap<String, AggregationResult>,
    buckets: VecWithNames<IntermediateBucketResult>,
    req_buckets: &VecWithNames<BucketAggregationInternal>,
    limits: &AggregationLimits,
) -> crate::Result<()> {
    assert_eq!(buckets.len(), req_buckets.len());

    let buckets_with_request = buckets.into_iter().zip(req_buckets.values());
    for ((key, bucket), req) in buckets_with_request {
        let result = AggregationResult::BucketResult(bucket.into_final_bucket_result(req, limits)?);
        results.insert(key, result);
    }
    Ok(())
}

/// An aggregation is either a bucket or a metric.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IntermediateAggregationResult {
    /// Bucket variant
    Bucket(IntermediateBucketResult),
    /// Metric variant
    Metric(IntermediateMetricResult),
}

/// Holds the intermediate data for metric results
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IntermediateMetricResult {
    /// Intermediate average result.
    Percentiles(PercentilesCollector),
    /// Intermediate average result.
    Average(IntermediateAverage),
    /// Intermediate count result.
    Count(IntermediateCount),
    /// Intermediate max result.
    Max(IntermediateMax),
    /// Intermediate min result.
    Min(IntermediateMin),
    /// Intermediate stats result.
    Stats(IntermediateStats),
    /// Intermediate sum result.
    Sum(IntermediateSum),
}

impl IntermediateMetricResult {
    fn into_final_metric_result(self, req: &MetricAggregation) -> MetricResult {
        match self {
            IntermediateMetricResult::Average(intermediate_avg) => {
                MetricResult::Average(intermediate_avg.finalize().into())
            }
            IntermediateMetricResult::Count(intermediate_count) => {
                MetricResult::Count(intermediate_count.finalize().into())
            }
            IntermediateMetricResult::Max(intermediate_max) => {
                MetricResult::Max(intermediate_max.finalize().into())
            }
            IntermediateMetricResult::Min(intermediate_min) => {
                MetricResult::Min(intermediate_min.finalize().into())
            }
            IntermediateMetricResult::Stats(intermediate_stats) => {
                MetricResult::Stats(intermediate_stats.finalize())
            }
            IntermediateMetricResult::Sum(intermediate_sum) => {
                MetricResult::Sum(intermediate_sum.finalize().into())
            }
            IntermediateMetricResult::Percentiles(percentiles) => MetricResult::Percentiles(
                percentiles.into_final_result(req.as_percentile().expect("unexpected metric type")),
            ),
        }
    }

    pub(crate) fn empty_from_req(req: &MetricAggregation) -> Self {
        match req {
            MetricAggregation::Average(_) => {
                IntermediateMetricResult::Average(IntermediateAverage::default())
            }
            MetricAggregation::Count(_) => {
                IntermediateMetricResult::Count(IntermediateCount::default())
            }
            MetricAggregation::Max(_) => IntermediateMetricResult::Max(IntermediateMax::default()),
            MetricAggregation::Min(_) => IntermediateMetricResult::Min(IntermediateMin::default()),
            MetricAggregation::Stats(_) => {
                IntermediateMetricResult::Stats(IntermediateStats::default())
            }
            MetricAggregation::Sum(_) => IntermediateMetricResult::Sum(IntermediateSum::default()),
            MetricAggregation::Percentiles(_) => {
                IntermediateMetricResult::Percentiles(PercentilesCollector::default())
            }
        }
    }
    fn merge_fruits(&mut self, other: IntermediateMetricResult) -> crate::Result<()> {
        match (self, other) {
            (
                IntermediateMetricResult::Average(avg_left),
                IntermediateMetricResult::Average(avg_right),
            ) => {
                avg_left.merge_fruits(avg_right);
            }
            (
                IntermediateMetricResult::Count(count_left),
                IntermediateMetricResult::Count(count_right),
            ) => {
                count_left.merge_fruits(count_right);
            }
            (IntermediateMetricResult::Max(max_left), IntermediateMetricResult::Max(max_right)) => {
                max_left.merge_fruits(max_right);
            }
            (IntermediateMetricResult::Min(min_left), IntermediateMetricResult::Min(min_right)) => {
                min_left.merge_fruits(min_right);
            }
            (
                IntermediateMetricResult::Stats(stats_left),
                IntermediateMetricResult::Stats(stats_right),
            ) => {
                stats_left.merge_fruits(stats_right);
            }
            (IntermediateMetricResult::Sum(sum_left), IntermediateMetricResult::Sum(sum_right)) => {
                sum_left.merge_fruits(sum_right);
            }
            (
                IntermediateMetricResult::Percentiles(left),
                IntermediateMetricResult::Percentiles(right),
            ) => {
                left.merge_fruits(right)?;
            }
            _ => {
                panic!("incompatible fruit types in tree or missing merge_fruits handler");
            }
        }

        Ok(())
    }
}

/// The intermediate bucket results. Internally they can be easily merged via the keys of the
/// buckets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IntermediateBucketResult {
    /// This is the range entry for a bucket, which contains a key, count, from, to, and optionally
    /// sub_aggregations.
    Range(IntermediateRangeBucketResult),
    /// This is the histogram entry for a bucket, which contains a key, count, and optionally
    /// sub_aggregations.
    Histogram {
        /// The column_type of the underlying `Column`
        column_type: Option<ColumnType>,
        /// The buckets
        buckets: Vec<IntermediateHistogramBucketEntry>,
    },
    /// Term aggregation
    Terms(IntermediateTermBucketResult),
}

impl IntermediateBucketResult {
    pub(crate) fn into_final_bucket_result(
        self,
        req: &BucketAggregationInternal,
        limits: &AggregationLimits,
    ) -> crate::Result<BucketResult> {
        match self {
            IntermediateBucketResult::Range(range_res) => {
                let mut buckets: Vec<RangeBucketEntry> = range_res
                    .buckets
                    .into_values()
                    .map(|bucket| {
                        bucket.into_final_bucket_entry(
                            &req.sub_aggregation,
                            req.as_range()
                                .expect("unexpected aggregation, expected histogram aggregation"),
                            range_res.column_type,
                            limits,
                        )
                    })
                    .collect::<crate::Result<Vec<_>>>()?;

                buckets.sort_by(|left, right| {
                    left.from
                        .unwrap_or(f64::MIN)
                        .total_cmp(&right.from.unwrap_or(f64::MIN))
                });

                let is_keyed = req
                    .as_range()
                    .expect("unexpected aggregation, expected range aggregation")
                    .keyed;
                let buckets = if is_keyed {
                    let mut bucket_map =
                        FxHashMap::with_capacity_and_hasher(buckets.len(), Default::default());
                    for bucket in buckets {
                        bucket_map.insert(bucket.key.to_string(), bucket);
                    }
                    BucketEntries::HashMap(bucket_map)
                } else {
                    BucketEntries::Vec(buckets)
                };
                Ok(BucketResult::Range { buckets })
            }
            IntermediateBucketResult::Histogram {
                column_type,
                buckets,
            } => {
                let histogram_req = &req
                    .as_histogram()?
                    .expect("unexpected aggregation, expected histogram aggregation");
                let buckets = intermediate_histogram_buckets_to_final_buckets(
                    buckets,
                    column_type,
                    histogram_req,
                    &req.sub_aggregation,
                    limits,
                )?;

                let buckets = if histogram_req.keyed {
                    let mut bucket_map =
                        FxHashMap::with_capacity_and_hasher(buckets.len(), Default::default());
                    for bucket in buckets {
                        bucket_map.insert(bucket.key.to_string(), bucket);
                    }
                    BucketEntries::HashMap(bucket_map)
                } else {
                    BucketEntries::Vec(buckets)
                };
                Ok(BucketResult::Histogram { buckets })
            }
            IntermediateBucketResult::Terms(terms) => terms.into_final_result(
                req.as_term()
                    .expect("unexpected aggregation, expected term aggregation"),
                &req.sub_aggregation,
                limits,
            ),
        }
    }

    pub(crate) fn empty_from_req(req: &BucketAggregationType) -> Self {
        match req {
            BucketAggregationType::Terms(_) => IntermediateBucketResult::Terms(Default::default()),
            BucketAggregationType::Range(_) => IntermediateBucketResult::Range(Default::default()),
            BucketAggregationType::Histogram(_) | BucketAggregationType::DateHistogram(_) => {
                IntermediateBucketResult::Histogram {
                    buckets: vec![],
                    column_type: None,
                }
            }
        }
    }
    fn merge_fruits(&mut self, other: IntermediateBucketResult) -> crate::Result<()> {
        match (self, other) {
            (
                IntermediateBucketResult::Terms(term_res_left),
                IntermediateBucketResult::Terms(term_res_right),
            ) => {
                merge_key_maps(&mut term_res_left.entries, term_res_right.entries)?;
                term_res_left.sum_other_doc_count += term_res_right.sum_other_doc_count;
                term_res_left.doc_count_error_upper_bound +=
                    term_res_right.doc_count_error_upper_bound;
            }

            (
                IntermediateBucketResult::Range(range_res_left),
                IntermediateBucketResult::Range(range_res_right),
            ) => {
                merge_serialized_key_maps(&mut range_res_left.buckets, range_res_right.buckets)?;
            }
            (
                IntermediateBucketResult::Histogram {
                    buckets: buckets_left,
                    ..
                },
                IntermediateBucketResult::Histogram {
                    buckets: buckets_right,
                    ..
                },
            ) => {
                let buckets: Result<Vec<IntermediateHistogramBucketEntry>, TantivyError> =
                    buckets_left
                        .drain(..)
                        .merge_join_by(buckets_right.into_iter(), |left, right| {
                            left.key.partial_cmp(&right.key).unwrap_or(Ordering::Equal)
                        })
                        .map(|either| match either {
                            itertools::EitherOrBoth::Both(mut left, right) => {
                                left.merge_fruits(right)?;
                                Ok(left)
                            }
                            itertools::EitherOrBoth::Left(left) => Ok(left),
                            itertools::EitherOrBoth::Right(right) => Ok(right),
                        })
                        .collect::<Result<_, _>>();

                *buckets_left = buckets?;
            }
            (IntermediateBucketResult::Range(_), _) => {
                panic!("try merge on different types")
            }
            (IntermediateBucketResult::Histogram { .. }, _) => {
                panic!("try merge on different types")
            }
            (IntermediateBucketResult::Terms { .. }, _) => {
                panic!("try merge on different types")
            }
        }
        Ok(())
    }
}

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Range aggregation including error counts
pub struct IntermediateRangeBucketResult {
    pub(crate) buckets: FxHashMap<SerializedKey, IntermediateRangeBucketEntry>,
    pub(crate) column_type: Option<ColumnType>,
}

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Term aggregation including error counts
pub struct IntermediateTermBucketResult {
    #[serde(
        serialize_with = "serialize_entries",
        deserialize_with = "deserialize_entries"
    )]
    pub(crate) entries: FxHashMap<Key, IntermediateTermBucketEntry>,
    pub(crate) sum_other_doc_count: u64,
    pub(crate) doc_count_error_upper_bound: u64,
}

// Serialize into a Vec to circument the JSON limitation, where keys can't be numbers
fn serialize_entries<S>(
    entries: &FxHashMap<Key, IntermediateTermBucketEntry>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(entries.len()))?;
    for (k, v) in entries {
        seq.serialize_element(&(k, v))?;
    }
    seq.end()
}

fn deserialize_entries<'de, D>(
    deserializer: D,
) -> Result<FxHashMap<Key, IntermediateTermBucketEntry>, D::Error>
where D: Deserializer<'de> {
    let vec_entries: Vec<(Key, IntermediateTermBucketEntry)> =
        Deserialize::deserialize(deserializer)?;
    Ok(vec_entries.into_iter().collect())
}

impl IntermediateTermBucketResult {
    pub(crate) fn into_final_result(
        self,
        req: &TermsAggregation,
        sub_aggregation_req: &AggregationsInternal,
        limits: &AggregationLimits,
    ) -> crate::Result<BucketResult> {
        let req = TermsAggregationInternal::from_req(req);
        let mut buckets: Vec<BucketEntry> = self
            .entries
            .into_iter()
            .filter(|bucket| bucket.1.doc_count >= req.min_doc_count)
            .map(|(key, entry)| {
                Ok(BucketEntry {
                    key_as_string: None,
                    key,
                    doc_count: entry.doc_count,
                    sub_aggregation: entry
                        .sub_aggregation
                        .into_final_result_internal(sub_aggregation_req, limits)?,
                })
            })
            .collect::<crate::Result<_>>()?;

        let order = req.order.order;
        match req.order.target {
            OrderTarget::Key => {
                buckets.sort_by(|left, right| {
                    if req.order.order == Order::Asc {
                        left.key.partial_cmp(&right.key)
                    } else {
                        right.key.partial_cmp(&left.key)
                    }
                    .expect("expected type string, which is always sortable")
                });
            }
            OrderTarget::Count => {
                if req.order.order == Order::Desc {
                    buckets.sort_unstable_by_key(|bucket| std::cmp::Reverse(bucket.doc_count()));
                } else {
                    buckets.sort_unstable_by_key(|bucket| bucket.doc_count());
                }
            }
            OrderTarget::SubAggregation(name) => {
                let (agg_name, agg_property) = get_agg_name_and_property(&name);
                let mut buckets_with_val = buckets
                    .into_iter()
                    .map(|bucket| {
                        let val = bucket
                            .sub_aggregation
                            .get_value_from_aggregation(agg_name, agg_property)?
                            .unwrap_or(f64::MIN);
                        Ok((bucket, val))
                    })
                    .collect::<crate::Result<Vec<_>>>()?;

                buckets_with_val.sort_by(|(_, val1), (_, val2)| match &order {
                    Order::Desc => val2.total_cmp(val1),
                    Order::Asc => val1.total_cmp(val2),
                });
                buckets = buckets_with_val
                    .into_iter()
                    .map(|(bucket, _val)| bucket)
                    .collect_vec();
            }
        }

        // We ignore _term_doc_count_before_cutoff here, because it increases the upperbound error
        // only for terms that didn't make it into the top N.
        //
        // This can be interesting, as a value of quality of the results, but not good to check the
        // actual error count for the returned terms.
        let (_term_doc_count_before_cutoff, sum_other_doc_count) =
            cut_off_buckets(&mut buckets, req.size as usize);

        let doc_count_error_upper_bound = if req.show_term_doc_count_error {
            Some(self.doc_count_error_upper_bound)
        } else {
            None
        };

        Ok(BucketResult::Terms {
            buckets,
            sum_other_doc_count: self.sum_other_doc_count + sum_other_doc_count,
            doc_count_error_upper_bound,
        })
    }
}

trait MergeFruits {
    fn merge_fruits(&mut self, other: Self) -> crate::Result<()>;
}

fn merge_serialized_key_maps<V: MergeFruits + Clone>(
    entries_left: &mut FxHashMap<SerializedKey, V>,
    mut entries_right: FxHashMap<SerializedKey, V>,
) -> crate::Result<()> {
    for (name, entry_left) in entries_left.iter_mut() {
        if let Some(entry_right) = entries_right.remove(name) {
            entry_left.merge_fruits(entry_right)?;
        }
    }

    for (key, res) in entries_right.into_iter() {
        entries_left.entry(key).or_insert(res);
    }
    Ok(())
}

fn merge_key_maps<V: MergeFruits + Clone>(
    entries_left: &mut FxHashMap<Key, V>,
    mut entries_right: FxHashMap<Key, V>,
) -> crate::Result<()> {
    for (name, entry_left) in entries_left.iter_mut() {
        if let Some(entry_right) = entries_right.remove(name) {
            entry_left.merge_fruits(entry_right)?;
        }
    }

    for (key, res) in entries_right.into_iter() {
        entries_left.entry(key).or_insert(res);
    }
    Ok(())
}

/// This is the histogram entry for a bucket, which contains a key, count, and optionally
/// sub_aggregations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateHistogramBucketEntry {
    /// The unique the bucket is identified.
    pub key: f64,
    /// The number of documents in the bucket.
    pub doc_count: u64,
    /// The sub_aggregation in this bucket.
    pub sub_aggregation: IntermediateAggregationResults,
}

impl IntermediateHistogramBucketEntry {
    pub(crate) fn into_final_bucket_entry(
        self,
        req: &AggregationsInternal,
        limits: &AggregationLimits,
    ) -> crate::Result<BucketEntry> {
        Ok(BucketEntry {
            key_as_string: None,
            key: Key::F64(self.key),
            doc_count: self.doc_count,
            sub_aggregation: self
                .sub_aggregation
                .into_final_result_internal(req, limits)?,
        })
    }
}

impl From<SegmentHistogramBucketEntry> for IntermediateHistogramBucketEntry {
    fn from(entry: SegmentHistogramBucketEntry) -> Self {
        IntermediateHistogramBucketEntry {
            key: entry.key,
            doc_count: entry.doc_count,
            sub_aggregation: Default::default(),
        }
    }
}

/// This is the range entry for a bucket, which contains a key, count, and optionally
/// sub_aggregations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateRangeBucketEntry {
    /// The unique the bucket is identified.
    pub key: Key,
    /// The number of documents in the bucket.
    pub doc_count: u64,
    /// The sub_aggregation in this bucket.
    pub sub_aggregation: IntermediateAggregationResults,
    /// The from range of the bucket. Equals `f64::MIN` when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<f64>,
    /// The to range of the bucket. Equals `f64::MAX` when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<f64>,
}

impl IntermediateRangeBucketEntry {
    pub(crate) fn into_final_bucket_entry(
        self,
        req: &AggregationsInternal,
        _range_req: &RangeAggregation,
        column_type: Option<ColumnType>,
        limits: &AggregationLimits,
    ) -> crate::Result<RangeBucketEntry> {
        let mut range_bucket_entry = RangeBucketEntry {
            key: self.key,
            doc_count: self.doc_count,
            sub_aggregation: self
                .sub_aggregation
                .into_final_result_internal(req, limits)?,
            to: self.to,
            from: self.from,
            to_as_string: None,
            from_as_string: None,
        };

        // If we have a date type on the histogram buckets, we add the `key_as_string` field as
        // rfc339
        if column_type == Some(ColumnType::DateTime) {
            if let Some(val) = range_bucket_entry.to {
                let key_as_string = format_date(val as i64)?;
                range_bucket_entry.to_as_string = Some(key_as_string);
            }
            if let Some(val) = range_bucket_entry.from {
                let key_as_string = format_date(val as i64)?;
                range_bucket_entry.from_as_string = Some(key_as_string);
            }
        }

        Ok(range_bucket_entry)
    }
}

/// This is the term entry for a bucket, which contains a count, and optionally
/// sub_aggregations.
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateTermBucketEntry {
    /// The number of documents in the bucket.
    pub doc_count: u64,
    /// The sub_aggregation in this bucket.
    pub sub_aggregation: IntermediateAggregationResults,
}

impl MergeFruits for IntermediateTermBucketEntry {
    fn merge_fruits(&mut self, other: IntermediateTermBucketEntry) -> crate::Result<()> {
        self.doc_count += other.doc_count;
        self.sub_aggregation.merge_fruits(other.sub_aggregation)?;
        Ok(())
    }
}

impl MergeFruits for IntermediateRangeBucketEntry {
    fn merge_fruits(&mut self, other: IntermediateRangeBucketEntry) -> crate::Result<()> {
        self.doc_count += other.doc_count;
        self.sub_aggregation.merge_fruits(other.sub_aggregation)?;
        Ok(())
    }
}

impl MergeFruits for IntermediateHistogramBucketEntry {
    fn merge_fruits(&mut self, other: IntermediateHistogramBucketEntry) -> crate::Result<()> {
        self.doc_count += other.doc_count;
        self.sub_aggregation.merge_fruits(other.sub_aggregation)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use pretty_assertions::assert_eq;

    use super::*;

    fn get_sub_test_tree(data: &[(String, u64)]) -> IntermediateAggregationResults {
        let mut map = HashMap::new();
        let mut buckets = FxHashMap::default();
        for (key, doc_count) in data {
            buckets.insert(
                key.to_string(),
                IntermediateRangeBucketEntry {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    sub_aggregation: Default::default(),
                    from: None,
                    to: None,
                },
            );
        }
        map.insert(
            "my_agg_level2".to_string(),
            IntermediateBucketResult::Range(IntermediateRangeBucketResult {
                buckets,
                column_type: None,
            }),
        );
        IntermediateAggregationResults {
            buckets: Some(VecWithNames::from_entries(map.into_iter().collect())),
            metrics: Default::default(),
        }
    }

    fn get_intermediat_tree_with_ranges(
        data: &[(String, u64, String, u64)],
    ) -> IntermediateAggregationResults {
        let mut map = HashMap::new();
        let mut buckets: FxHashMap<_, _> = Default::default();
        for (key, doc_count, sub_aggregation_key, sub_aggregation_count) in data {
            buckets.insert(
                key.to_string(),
                IntermediateRangeBucketEntry {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    from: None,
                    to: None,
                    sub_aggregation: get_sub_test_tree(&[(
                        sub_aggregation_key.to_string(),
                        *sub_aggregation_count,
                    )]),
                },
            );
        }
        map.insert(
            "my_agg_level1".to_string(),
            IntermediateBucketResult::Range(IntermediateRangeBucketResult {
                buckets,
                column_type: None,
            }),
        );
        IntermediateAggregationResults {
            buckets: Some(VecWithNames::from_entries(map.into_iter().collect())),
            metrics: Default::default(),
        }
    }

    #[test]
    fn test_merge_fruits_tree_1() {
        let mut tree_left = get_intermediat_tree_with_ranges(&[
            ("red".to_string(), 50, "1900".to_string(), 25),
            ("blue".to_string(), 30, "1900".to_string(), 30),
        ]);
        let tree_right = get_intermediat_tree_with_ranges(&[
            ("red".to_string(), 60, "1900".to_string(), 30),
            ("blue".to_string(), 25, "1900".to_string(), 50),
        ]);

        tree_left.merge_fruits(tree_right).unwrap();

        let tree_expected = get_intermediat_tree_with_ranges(&[
            ("red".to_string(), 110, "1900".to_string(), 55),
            ("blue".to_string(), 55, "1900".to_string(), 80),
        ]);

        assert_eq!(tree_left, tree_expected);
    }

    #[test]
    fn test_merge_fruits_tree_2() {
        let mut tree_left = get_intermediat_tree_with_ranges(&[
            ("red".to_string(), 50, "1900".to_string(), 25),
            ("blue".to_string(), 30, "1900".to_string(), 30),
        ]);
        let tree_right = get_intermediat_tree_with_ranges(&[
            ("red".to_string(), 60, "1900".to_string(), 30),
            ("green".to_string(), 25, "1900".to_string(), 50),
        ]);

        tree_left.merge_fruits(tree_right).unwrap();

        let tree_expected = get_intermediat_tree_with_ranges(&[
            ("red".to_string(), 110, "1900".to_string(), 55),
            ("blue".to_string(), 30, "1900".to_string(), 30),
            ("green".to_string(), 25, "1900".to_string(), 50),
        ]);

        assert_eq!(tree_left, tree_expected);
    }

    #[test]
    fn test_merge_fruits_tree_empty() {
        let mut tree_left = get_intermediat_tree_with_ranges(&[
            ("red".to_string(), 50, "1900".to_string(), 25),
            ("blue".to_string(), 30, "1900".to_string(), 30),
        ]);

        let orig = tree_left.clone();

        tree_left
            .merge_fruits(IntermediateAggregationResults::default())
            .unwrap();

        assert_eq!(tree_left, orig);
    }

    #[test]
    fn test_term_bucket_json_roundtrip() {
        let term_buckets = IntermediateTermBucketResult {
            entries: vec![(
                Key::F64(5.0),
                IntermediateTermBucketEntry {
                    doc_count: 10,
                    sub_aggregation: Default::default(),
                },
            )]
            .into_iter()
            .collect(),
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: 0,
        };

        let term_buckets_round: IntermediateTermBucketResult =
            serde_json::from_str(&serde_json::to_string(&term_buckets).unwrap()).unwrap();

        assert_eq!(term_buckets, term_buckets_round);
    }
}
