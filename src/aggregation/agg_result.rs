//! Contains the final aggregation tree.
//! This tree can be converted via the `into()` method from `IntermediateAggregationResults`.
//! This conversion computes the final result. For example: The intermediate result contains
//! intermediate average results, which is the sum and the number of values. The actual average is
//! calculated on the step from intermediate to final aggregation result tree.

use std::cmp::Ordering;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketEntry,
    IntermediateBucketEntryKeyCount, IntermediateBucketResult, IntermediateMetricResult,
};
use super::metric::{SingleMetricResult, Stats};
use super::Key;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// The final aggegation result.
pub struct AggregationResults(HashMap<String, AggregationResult>);

impl From<IntermediateAggregationResults> for AggregationResults {
    fn from(tree: IntermediateAggregationResults) -> Self {
        Self(
            tree.0
                .into_iter()
                .map(|(key, agg)| (key, agg.into()))
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
impl From<IntermediateAggregationResult> for AggregationResult {
    fn from(tree: IntermediateAggregationResult) -> Self {
        match tree {
            IntermediateAggregationResult::Bucket(bucket) => {
                AggregationResult::BucketResult(bucket.into())
            }
            IntermediateAggregationResult::Metric(metric) => {
                AggregationResult::MetricResult(metric.into())
            }
        }
    }
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Aggregation result for buckets.
pub struct BucketResult {
    buckets: Vec<BucketEntry>,
}

impl From<IntermediateBucketResult> for BucketResult {
    fn from(result: IntermediateBucketResult) -> Self {
        let mut buckets: Vec<BucketEntry> = result
            .buckets
            .into_iter()
            .filter(|(_, bucket)| match bucket {
                IntermediateBucketEntry::KeyCount(key_count) => key_count.doc_count != 0,
            })
            .map(|(_, bucket)| (bucket.into()))
            .collect();
        buckets.sort_by(
            |bucket1, bucket2| match bucket2.doc_count().cmp(&bucket1.doc_count()) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => bucket1.key().cmp(bucket2.key()),
                Ordering::Greater => Ordering::Greater,
            },
        );
        BucketResult { buckets }
    }
}

/// BucketEntry holds bucket aggregation result types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BucketEntry {
    /// This is the default entry for a bucket, which contains a key, count, and optionally
    /// sub_aggregations.
    KeyCount(BucketEntryKeyCount),
}

impl BucketEntry {
    fn doc_count(&self) -> u64 {
        match self {
            BucketEntry::KeyCount(key_count) => key_count.doc_count,
        }
    }

    fn key(&self) -> &Key {
        match self {
            BucketEntry::KeyCount(key_count) => &key_count.key,
        }
    }
}

impl From<IntermediateBucketEntry> for BucketEntry {
    fn from(entry: IntermediateBucketEntry) -> Self {
        match entry {
            IntermediateBucketEntry::KeyCount(key_count) => BucketEntry::KeyCount(key_count.into()),
        }
    }
}

/// This is the default entry for a bucket, which contains a key, count, and optionally
/// sub_aggregations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BucketEntryKeyCount {
    /// The identifier of the bucket.
    pub key: Key,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    #[serde(flatten)]
    /// sub-aggregations in this bucket.
    pub sub_aggregation: AggregationResults,
}

impl From<IntermediateBucketEntryKeyCount> for BucketEntryKeyCount {
    fn from(entry: IntermediateBucketEntryKeyCount) -> Self {
        BucketEntryKeyCount {
            key: entry.key,
            doc_count: entry.doc_count,
            sub_aggregation: entry.sub_aggregation.into(),
        }
    }
}
