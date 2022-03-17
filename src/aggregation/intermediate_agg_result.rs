//! Contains the intermediate aggregation tree, that can be merged.
//! Intermediate aggregation results can be used to merge results between segments or between
//! indices.

use std::cmp::Ordering;

use fnv::FnvHashMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::agg_req::{BucketAggregationType, CollectorAggregations, MetricAggregation};
use super::metric::{IntermediateAverage, IntermediateStats};
use super::segment_agg_result::{
    SegmentAggregationResultsCollector, SegmentBucketResultCollector, SegmentHistogramBucketEntry,
    SegmentMetricResultCollector, SegmentRangeBucketEntry,
};
use super::{Key, SerializedKey, VecWithNames};

/// Contains the intermediate aggregation result, which is optimized to be merged with other
/// intermediate results.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateAggregationResults {
    pub(crate) metrics: Option<VecWithNames<IntermediateMetricResult>>,
    pub(crate) buckets: Option<VecWithNames<IntermediateBucketResult>>,
}

impl From<SegmentAggregationResultsCollector> for IntermediateAggregationResults {
    fn from(tree: SegmentAggregationResultsCollector) -> Self {
        let metrics = tree.metrics.map(VecWithNames::from_other);
        let buckets = tree.buckets.map(VecWithNames::from_other);

        Self { metrics, buckets }
    }
}

impl IntermediateAggregationResults {
    pub(crate) fn empty_from_req(req: &CollectorAggregations) -> Self {
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
    Range(FnvHashMap<SerializedKey, IntermediateRangeBucketEntry>),
    /// This is the histogram entry for a bucket, which contains a key, count, and optionally
    /// sub_aggregations.
    Histogram {
        /// The buckets
        buckets: Vec<IntermediateHistogramBucketEntry>,
    },
}

impl From<SegmentBucketResultCollector> for IntermediateBucketResult {
    fn from(collector: SegmentBucketResultCollector) -> Self {
        match collector {
            SegmentBucketResultCollector::Range(range) => range.into_intermediate_bucket_result(),
            SegmentBucketResultCollector::Histogram(histogram) => {
                histogram.into_intermediate_bucket_result()
            }
        }
    }
}

impl IntermediateBucketResult {
    pub(crate) fn empty_from_req(req: &BucketAggregationType) -> Self {
        match req {
            BucketAggregationType::Range(_) => IntermediateBucketResult::Range(Default::default()),
            BucketAggregationType::Histogram(_) => {
                IntermediateBucketResult::Histogram { buckets: vec![] }
            }
        }
    }
    fn merge_fruits(&mut self, other: IntermediateBucketResult) {
        match (self, other) {
            (
                IntermediateBucketResult::Range(entries_left),
                IntermediateBucketResult::Range(entries_right),
            ) => {
                merge_maps(entries_left, entries_right);
            }
            (
                IntermediateBucketResult::Histogram {
                    buckets: entries_left,
                    ..
                },
                IntermediateBucketResult::Histogram {
                    buckets: entries_right,
                    ..
                },
            ) => {
                let mut buckets = entries_left
                    .drain(..)
                    .merge_join_by(entries_right.into_iter(), |left, right| {
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

                std::mem::swap(entries_left, &mut buckets);
            }
            (IntermediateBucketResult::Range(_), _) => {
                panic!("try merge on different types")
            }
            (IntermediateBucketResult::Histogram { .. }, _) => {
                panic!("try merge on different types")
            }
        }
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

impl From<SegmentHistogramBucketEntry> for IntermediateHistogramBucketEntry {
    fn from(entry: SegmentHistogramBucketEntry) -> Self {
        IntermediateHistogramBucketEntry {
            key: entry.key,
            doc_count: entry.doc_count,
            sub_aggregation: Default::default(),
        }
    }
}

impl
    From<(
        SegmentHistogramBucketEntry,
        SegmentAggregationResultsCollector,
    )> for IntermediateHistogramBucketEntry
{
    fn from(
        entry: (
            SegmentHistogramBucketEntry,
            SegmentAggregationResultsCollector,
        ),
    ) -> Self {
        IntermediateHistogramBucketEntry {
            key: entry.0.key,
            doc_count: entry.0.doc_count,
            sub_aggregation: entry.1.into(),
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
    pub(crate) values: Option<Vec<u64>>,
    /// The sub_aggregation in this bucket.
    pub sub_aggregation: IntermediateAggregationResults,
    /// The from range of the bucket. Equals f64::MIN when None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<f64>,
    /// The to range of the bucket. Equals f64::MAX when None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<f64>,
}

impl From<SegmentRangeBucketEntry> for IntermediateRangeBucketEntry {
    fn from(entry: SegmentRangeBucketEntry) -> Self {
        let sub_aggregation = if let Some(sub_aggregation) = entry.sub_aggregation {
            sub_aggregation.into()
        } else {
            Default::default()
        };

        IntermediateRangeBucketEntry {
            key: entry.key,
            doc_count: entry.doc_count,
            values: None,
            sub_aggregation,
            to: entry.to,
            from: entry.from,
        }
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
                    values: None,
                    sub_aggregation: Default::default(),
                    from: None,
                    to: None,
                },
            );
        }
        map.insert(
            "my_agg_level2".to_string(),
            IntermediateBucketResult::Range(buckets),
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
                    values: None,
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
            IntermediateBucketResult::Range(buckets),
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
