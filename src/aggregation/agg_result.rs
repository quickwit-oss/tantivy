//! Contains the final aggregation tree.
//! This tree can be converted via the `into()` method from `IntermediateAggregationResults`.
//! This conversion computes the final result. For example: The intermediate result contains
//! intermediate average results, which is the sum and the number of values. The actual average is
//! calculated on the step from intermediate to final aggregation result tree.

use std::cmp::Ordering;
use std::collections::HashMap;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::bucket::generate_buckets;
use super::intermediate_agg_result::{
    IntermediateAggregationResults, IntermediateBucketResult, IntermediateHistogramBucketEntry,
    IntermediateMetricResult, IntermediateRangeBucketEntry,
};
use super::metric::{SingleMetricResult, Stats};
use super::Key;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
/// The final aggegation result.
pub struct AggregationResults(pub HashMap<String, AggregationResult>);

impl From<IntermediateAggregationResults> for AggregationResults {
    fn from(tree: IntermediateAggregationResults) -> Self {
        Self(
            tree.buckets
                .unwrap_or_default()
                .into_iter()
                .map(|(key, bucket)| (key, AggregationResult::BucketResult(bucket.into())))
                .chain(
                    tree.metrics
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(key, metric)| (key, AggregationResult::MetricResult(metric.into()))),
                )
                .collect(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// An aggregation is either a bucket or a metric.
pub enum AggregationResult {
    /// Bucket result variant.
    BucketResult(BucketResult),
    /// Metric result variant.
    MetricResult(MetricResult),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// MetricResult
pub enum MetricResult {
    /// Average metric result.
    Average(SingleMetricResult),
    /// Stats metric result.
    Stats(Stats),
}

impl From<IntermediateMetricResult> for MetricResult {
    fn from(metric: IntermediateMetricResult) -> Self {
        match metric {
            IntermediateMetricResult::Average(avg_data) => {
                MetricResult::Average(avg_data.finalize().into())
            }
            IntermediateMetricResult::Stats(intermediate_stats) => {
                MetricResult::Stats(intermediate_stats.finalize())
            }
        }
    }
}

/// BucketEntry holds bucket aggregation result types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BucketResult {
    /// This is the range entry for a bucket, which contains a key, count, from, to, and optionally
    /// sub_aggregations.
    Range {
        /// The range buckets sorted by range.
        buckets: Vec<RangeBucketEntry>,
    },
    /// This is the histogram entry for a bucket, which contains a key, count, and optionally
    /// sub_aggregations.
    Histogram {
        /// The buckets.
        ///
        /// If there are holes depends on the request, if min_doc_count is 0, then there are no
        /// holes between the first and last bucket.
        /// See [HistogramAggregation](super::bucket::HistogramAggregation)
        buckets: Vec<BucketEntry>,
    },
}

impl From<IntermediateBucketResult> for BucketResult {
    fn from(result: IntermediateBucketResult) -> Self {
        match result {
            IntermediateBucketResult::Range(range_map) => {
                let mut buckets: Vec<RangeBucketEntry> = range_map
                    .into_iter()
                    .map(|(_, bucket)| bucket.into())
                    .collect_vec();

                buckets.sort_by(|a, b| {
                    a.from
                        .unwrap_or(f64::MIN)
                        .partial_cmp(&b.from.unwrap_or(f64::MIN))
                        .unwrap_or(Ordering::Equal)
                });
                BucketResult::Range { buckets }
            }
            IntermediateBucketResult::Histogram { buckets, req } => {
                let buckets = if req.min_doc_count() == 0 {
                    // With min_doc_count != 0, we may need to add buckets, so that there are no
                    // gaps, since intermediate result does not contain empty buckets (filtered to
                    // reduce serialization size).
                    let fill_gaps_buckets = if buckets.len() > 1 {
                        // buckets are sorted
                        let min = buckets[0].key;
                        let max = buckets[buckets.len() - 1].key;
                        generate_buckets(&req, min, max)
                    } else {
                        vec![]
                    };

                    buckets
                        .into_iter()
                        .merge_join_by(
                            fill_gaps_buckets.into_iter(),
                            |existing_bucket, fill_gaps_bucket| {
                                existing_bucket
                                    .key
                                    .partial_cmp(fill_gaps_bucket)
                                    .unwrap_or(Ordering::Equal)
                            },
                        )
                        .map(|either| match either {
                            itertools::EitherOrBoth::Both(existing, _) => existing.into(),
                            itertools::EitherOrBoth::Left(existing) => existing.into(),
                            // Add missing bucket
                            itertools::EitherOrBoth::Right(bucket) => BucketEntry {
                                key: Key::F64(bucket),
                                doc_count: 0,
                                sub_aggregation: Default::default(),
                            },
                        })
                        .collect_vec()
                } else {
                    buckets
                        .into_iter()
                        .filter(|bucket| bucket.doc_count >= req.min_doc_count())
                        .map(|bucket| bucket.into())
                        .collect_vec()
                };

                BucketResult::Histogram { buckets }
            }
        }
    }
}

/// This is the default entry for a bucket, which contains a key, count, and optionally
/// sub_aggregations.
///
/// # JSON Format
/// ```json
/// {
///   ...
///     "my_histogram": {
///       "buckets": [
///         {
///           "key": "2.0",
///           "doc_count": 5
///         },
///         {
///           "key": "4.0",
///           "doc_count": 2
///         },
///         {
///           "key": "6.0",
///           "doc_count": 3
///         }
///       ]
///    }
///    ...
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BucketEntry {
    /// The identifier of the bucket.
    pub key: Key,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    #[serde(flatten)]
    /// sub-aggregations in this bucket.
    pub sub_aggregation: AggregationResults,
}

impl From<IntermediateHistogramBucketEntry> for BucketEntry {
    fn from(entry: IntermediateHistogramBucketEntry) -> Self {
        BucketEntry {
            key: Key::F64(entry.key),
            doc_count: entry.doc_count,
            sub_aggregation: entry.sub_aggregation.into(),
        }
    }
}

/// This is the range entry for a bucket, which contains a key, count, and optionally
/// sub_aggregations.
///
/// # JSON Format
/// ```json
/// {
///   ...
///     "my_ranges": {
///       "buckets": [
///         {
///           "key": "*-10",
///           "to": 10,
///           "doc_count": 5
///         },
///         {
///           "key": "10-20",
///           "from": 10,
///           "to": 20,
///           "doc_count": 2
///         },
///         {
///           "key": "20-*",
///           "from": 20,
///           "doc_count": 3
///         }
///       ]
///    }
///    ...
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangeBucketEntry {
    /// The identifier of the bucket.
    pub key: Key,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    #[serde(flatten)]
    /// sub-aggregations in this bucket.
    pub sub_aggregation: AggregationResults,
    /// The from range of the bucket. Equals f64::MIN when None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<f64>,
    /// The to range of the bucket. Equals f64::MAX when None.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<f64>,
}

impl From<IntermediateRangeBucketEntry> for RangeBucketEntry {
    fn from(entry: IntermediateRangeBucketEntry) -> Self {
        RangeBucketEntry {
            key: entry.key,
            doc_count: entry.doc_count,
            sub_aggregation: entry.sub_aggregation.into(),
            to: entry.to,
            from: entry.from,
        }
    }
}
