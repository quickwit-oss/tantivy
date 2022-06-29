//! Contains the intermediate aggregation tree, that can be merged.
//! Intermediate aggregation results can be used to merge results between segments or between
//! indices.

use std::cmp::Ordering;
use std::collections::HashMap;

use fnv::FnvHashMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::agg_req::{
    Aggregations, AggregationsInternal, BucketAggregationInternal, BucketAggregationType,
    MetricAggregation,
};
use super::agg_result::{AggregationResult, BucketResult, RangeBucketEntry};
use super::bucket::{
    cut_off_buckets, get_agg_name_and_property, intermediate_histogram_buckets_to_final_buckets,
    GetDocCount, Order, OrderTarget, SegmentHistogramBucketEntry, TermsAggregation,
};
use super::metric::{IntermediateAverage, IntermediateStats};
use super::segment_agg_result::SegmentMetricResultCollector;
use super::{Key, SerializedKey, VecWithNames};
use crate::aggregation::agg_result::{AggregationResults, BucketEntry};
use crate::aggregation::bucket::TermsAggregationInternal;

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
    pub fn into_final_bucket_result(self, req: Aggregations) -> crate::Result<AggregationResults> {
        self.into_final_bucket_result_internal(&(req.into()))
    }

    /// Convert intermediate result and its aggregation request to the final result.
    ///
    /// Internal function, AggregationsInternal is used instead Aggregations, which is optimized
    /// for internal processing, by splitting metric and buckets into seperate groups.
    pub(crate) fn into_final_bucket_result_internal(
        self,
        req: &AggregationsInternal,
    ) -> crate::Result<AggregationResults> {
        // Important assumption:
        // When the tree contains buckets/metric, we expect it to have all buckets/metrics from the
        // request
        let mut results: HashMap<String, AggregationResult> = HashMap::new();

        if let Some(buckets) = self.buckets {
            convert_and_add_final_buckets_to_result(&mut results, buckets, &req.buckets)?
        } else {
            // When there are no buckets, we create empty buckets, so that the serialized json
            // format is constant
            add_empty_final_buckets_to_result(&mut results, &req.buckets)?
        };

        if let Some(metrics) = self.metrics {
            convert_and_add_final_metrics_to_result(&mut results, metrics);
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

    /// Merge an other intermediate aggregation result into this result.
    ///
    /// The order of the values need to be the same on both results. This is ensured when the same
    /// (key values) are present on the underlying VecWithNames struct.
    pub fn merge_fruits(&mut self, other: IntermediateAggregationResults) {
        if let (Some(buckets_left), Some(buckets_right)) = (&mut self.buckets, other.buckets) {
            for (bucket_left, bucket_right) in
                buckets_left.values_mut().zip(buckets_right.into_values())
            {
                bucket_left.merge_fruits(bucket_right);
            }
        }

        if let (Some(metrics_left), Some(metrics_right)) = (&mut self.metrics, other.metrics) {
            for (metric_left, metric_right) in
                metrics_left.values_mut().zip(metrics_right.into_values())
            {
                metric_left.merge_fruits(metric_right);
            }
        }
    }
}

fn convert_and_add_final_metrics_to_result(
    results: &mut HashMap<String, AggregationResult>,
    metrics: VecWithNames<IntermediateMetricResult>,
) {
    results.extend(
        metrics
            .into_iter()
            .map(|(key, metric)| (key, AggregationResult::MetricResult(metric.into()))),
    );
}

fn add_empty_final_metrics_to_result(
    results: &mut HashMap<String, AggregationResult>,
    req_metrics: &VecWithNames<MetricAggregation>,
) -> crate::Result<()> {
    results.extend(req_metrics.iter().map(|(key, req)| {
        let empty_bucket = IntermediateMetricResult::empty_from_req(req);
        (
            key.to_string(),
            AggregationResult::MetricResult(empty_bucket.into()),
        )
    }));
    Ok(())
}

fn add_empty_final_buckets_to_result(
    results: &mut HashMap<String, AggregationResult>,
    req_buckets: &VecWithNames<BucketAggregationInternal>,
) -> crate::Result<()> {
    let requested_buckets = req_buckets.iter();
    for (key, req) in requested_buckets {
        let empty_bucket = AggregationResult::BucketResult(BucketResult::empty_from_req(req)?);
        results.insert(key.to_string(), empty_bucket);
    }
    Ok(())
}

fn convert_and_add_final_buckets_to_result(
    results: &mut HashMap<String, AggregationResult>,
    buckets: VecWithNames<IntermediateBucketResult>,
    req_buckets: &VecWithNames<BucketAggregationInternal>,
) -> crate::Result<()> {
    assert_eq!(buckets.len(), req_buckets.len());

    let buckets_with_request = buckets.into_iter().zip(req_buckets.values());
    for ((key, bucket), req) in buckets_with_request {
        let result = AggregationResult::BucketResult(bucket.into_final_bucket_result(req)?);
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
    /// Average containing intermediate average data result
    Average(IntermediateAverage),
    /// AverageData variant
    Stats(IntermediateStats),
}

impl From<SegmentMetricResultCollector> for IntermediateMetricResult {
    fn from(tree: SegmentMetricResultCollector) -> Self {
        match tree {
            SegmentMetricResultCollector::Average(collector) => {
                IntermediateMetricResult::Average(IntermediateAverage::from_collector(collector))
            }
            SegmentMetricResultCollector::Stats(collector) => {
                IntermediateMetricResult::Stats(collector.stats)
            }
        }
    }
}

impl IntermediateMetricResult {
    pub(crate) fn empty_from_req(req: &MetricAggregation) -> Self {
        match req {
            MetricAggregation::Average(_) => {
                IntermediateMetricResult::Average(IntermediateAverage::default())
            }
            MetricAggregation::Stats(_) => {
                IntermediateMetricResult::Stats(IntermediateStats::default())
            }
        }
    }
    fn merge_fruits(&mut self, other: IntermediateMetricResult) {
        match (self, other) {
            (
                IntermediateMetricResult::Average(avg_data_left),
                IntermediateMetricResult::Average(avg_data_right),
            ) => {
                avg_data_left.merge_fruits(avg_data_right);
            }
            (
                IntermediateMetricResult::Stats(stats_left),
                IntermediateMetricResult::Stats(stats_right),
            ) => {
                stats_left.merge_fruits(stats_right);
            }
            _ => {
                panic!("incompatible fruit types in tree");
            }
        }
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
    ) -> crate::Result<BucketResult> {
        match self {
            IntermediateBucketResult::Range(range_res) => {
                let mut buckets: Vec<RangeBucketEntry> = range_res
                    .buckets
                    .into_iter()
                    .map(|(_, bucket)| bucket.into_final_bucket_entry(&req.sub_aggregation))
                    .collect::<crate::Result<Vec<_>>>()?;

                buckets.sort_by(|left, right| {
                    // TODO use total_cmp next stable rust release
                    left.from
                        .unwrap_or(f64::MIN)
                        .partial_cmp(&right.from.unwrap_or(f64::MIN))
                        .unwrap_or(Ordering::Equal)
                });
                Ok(BucketResult::Range { buckets })
            }
            IntermediateBucketResult::Histogram { buckets } => {
                let buckets = intermediate_histogram_buckets_to_final_buckets(
                    buckets,
                    req.as_histogram()
                        .expect("unexpected aggregation, expected histogram aggregation"),
                    &req.sub_aggregation,
                )?;

                Ok(BucketResult::Histogram { buckets })
            }
            IntermediateBucketResult::Terms(terms) => terms.into_final_result(
                req.as_term()
                    .expect("unexpected aggregation, expected term aggregation"),
                &req.sub_aggregation,
            ),
        }
    }

    pub(crate) fn empty_from_req(req: &BucketAggregationType) -> Self {
        match req {
            BucketAggregationType::Terms(_) => IntermediateBucketResult::Terms(Default::default()),
            BucketAggregationType::Range(_) => IntermediateBucketResult::Range(Default::default()),
            BucketAggregationType::Histogram(_) => {
                IntermediateBucketResult::Histogram { buckets: vec![] }
            }
        }
    }
    fn merge_fruits(&mut self, other: IntermediateBucketResult) {
        match (self, other) {
            (
                IntermediateBucketResult::Terms(term_res_left),
                IntermediateBucketResult::Terms(term_res_right),
            ) => {
                merge_maps(&mut term_res_left.entries, term_res_right.entries);
                term_res_left.sum_other_doc_count += term_res_right.sum_other_doc_count;
                term_res_left.doc_count_error_upper_bound +=
                    term_res_right.doc_count_error_upper_bound;
            }

            (
                IntermediateBucketResult::Range(range_res_left),
                IntermediateBucketResult::Range(range_res_right),
            ) => {
                merge_maps(&mut range_res_left.buckets, range_res_right.buckets);
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
                let buckets = buckets_left
                    .drain(..)
                    .merge_join_by(buckets_right.into_iter(), |left, right| {
                        left.key.partial_cmp(&right.key).unwrap_or(Ordering::Equal)
                    })
                    .map(|either| match either {
                        itertools::EitherOrBoth::Both(mut left, right) => {
                            left.merge_fruits(right);
                            left
                        }
                        itertools::EitherOrBoth::Left(left) => left,
                        itertools::EitherOrBoth::Right(right) => right,
                    })
                    .collect();

                *buckets_left = buckets;
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
    }
}

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Range aggregation including error counts
pub struct IntermediateRangeBucketResult {
    pub(crate) buckets: FnvHashMap<SerializedKey, IntermediateRangeBucketEntry>,
}

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Term aggregation including error counts
pub struct IntermediateTermBucketResult {
    pub(crate) entries: FnvHashMap<String, IntermediateTermBucketEntry>,
    pub(crate) sum_other_doc_count: u64,
    pub(crate) doc_count_error_upper_bound: u64,
}

impl IntermediateTermBucketResult {
    pub(crate) fn into_final_result(
        self,
        req: &TermsAggregation,
        sub_aggregation_req: &AggregationsInternal,
    ) -> crate::Result<BucketResult> {
        let req = TermsAggregationInternal::from_req(req);
        let mut buckets: Vec<BucketEntry> = self
            .entries
            .into_iter()
            .filter(|bucket| bucket.1.doc_count >= req.min_doc_count)
            .map(|(key, entry)| {
                Ok(BucketEntry {
                    key: Key::Str(key),
                    doc_count: entry.doc_count,
                    sub_aggregation: entry
                        .sub_aggregation
                        .into_final_bucket_result_internal(sub_aggregation_req)?,
                })
            })
            .collect::<crate::Result<_>>()?;

        let order = req.order.order;
        match req.order.target {
            OrderTarget::Key => {
                buckets.sort_by(|left, right| {
                    if req.order.order == Order::Desc {
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
                            .unwrap_or(f64::NAN);
                        Ok((bucket, val))
                    })
                    .collect::<crate::Result<Vec<_>>>()?;

                buckets_with_val.sort_by(|(_, val1), (_, val2)| {
                    // TODO use total_cmp in next rust stable release
                    match &order {
                        Order::Desc => val2.partial_cmp(val1).unwrap_or(std::cmp::Ordering::Equal),
                        Order::Asc => val1.partial_cmp(val2).unwrap_or(std::cmp::Ordering::Equal),
                    }
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
    fn merge_fruits(&mut self, other: Self);
}

fn merge_maps<V: MergeFruits + Clone>(
    entries_left: &mut FnvHashMap<SerializedKey, V>,
    mut entries_right: FnvHashMap<SerializedKey, V>,
) {
    for (name, entry_left) in entries_left.iter_mut() {
        if let Some(entry_right) = entries_right.remove(name) {
            entry_left.merge_fruits(entry_right);
        }
    }

    for (key, res) in entries_right.into_iter() {
        entries_left.entry(key).or_insert(res);
    }
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
    ) -> crate::Result<BucketEntry> {
        Ok(BucketEntry {
            key: Key::F64(self.key),
            doc_count: self.doc_count,
            sub_aggregation: self
                .sub_aggregation
                .into_final_bucket_result_internal(req)?,
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
    /// The from range of the bucket. Equals f64::MIN when None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<f64>,
    /// The to range of the bucket. Equals f64::MAX when None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<f64>,
}

impl IntermediateRangeBucketEntry {
    pub(crate) fn into_final_bucket_entry(
        self,
        req: &AggregationsInternal,
    ) -> crate::Result<RangeBucketEntry> {
        Ok(RangeBucketEntry {
            key: self.key,
            doc_count: self.doc_count,
            sub_aggregation: self
                .sub_aggregation
                .into_final_bucket_result_internal(req)?,
            to: self.to,
            from: self.from,
        })
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
    fn merge_fruits(&mut self, other: IntermediateTermBucketEntry) {
        self.doc_count += other.doc_count;
        self.sub_aggregation.merge_fruits(other.sub_aggregation);
    }
}

impl MergeFruits for IntermediateRangeBucketEntry {
    fn merge_fruits(&mut self, other: IntermediateRangeBucketEntry) {
        self.doc_count += other.doc_count;
        self.sub_aggregation.merge_fruits(other.sub_aggregation);
    }
}

impl MergeFruits for IntermediateHistogramBucketEntry {
    fn merge_fruits(&mut self, other: IntermediateHistogramBucketEntry) {
        self.doc_count += other.doc_count;
        self.sub_aggregation.merge_fruits(other.sub_aggregation);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use pretty_assertions::assert_eq;

    use super::*;

    fn get_sub_test_tree(data: &[(String, u64)]) -> IntermediateAggregationResults {
        let mut map = HashMap::new();
        let mut buckets = FnvHashMap::default();
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
            IntermediateBucketResult::Range(IntermediateRangeBucketResult { buckets }),
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
        let mut buckets: FnvHashMap<_, _> = Default::default();
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
            IntermediateBucketResult::Range(IntermediateRangeBucketResult { buckets }),
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

        tree_left.merge_fruits(tree_right);

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

        tree_left.merge_fruits(tree_right);

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

        tree_left.merge_fruits(IntermediateAggregationResults::default());

        assert_eq!(tree_left, orig);
    }
}
