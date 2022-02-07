//! Contains the final aggregation tree.
//! This tree will be used to compute intermediate trees.

use super::{
    intermediate_agg_result::{
        IntermediateAggregationResult, IntermediateAggregationResults,
        IntermediateBucketAggregationResult, IntermediateBucketDataEntry,
        IntermediateBucketDataEntryKeyCount, IntermediateMetricResult,
    },
    Key,
};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq)]
/// An aggregation is either a bucket or a metric.
pub enum AggregationResult {
    /// Bucket result variant.
    BucketResult(BucketAggregationResult),
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

#[derive(Clone, Debug, PartialEq)]
/// MetricResult
pub enum MetricResult {
    /// Average metric result.
    Average(f64),
}

impl From<IntermediateMetricResult> for MetricResult {
    fn from(metric: IntermediateMetricResult) -> Self {
        match metric {
            IntermediateMetricResult::Average(avg_data) => {
                MetricResult::Average(avg_data.finalize())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Aggregation result for buckets.
pub struct BucketAggregationResult {
    buckets: HashMap<Key, BucketDataEntry>,
}

impl From<IntermediateBucketAggregationResult> for BucketAggregationResult {
    fn from(result: IntermediateBucketAggregationResult) -> Self {
        BucketAggregationResult {
            buckets: result
                .buckets
                .into_iter()
                .map(|(key, bucket)| (key, bucket.into()))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// BucketDataEntry
pub enum BucketDataEntry {
    /// This is the default entry for a bucket, which contains a key, count, and optionally
    /// sub_aggregations.
    KeyCount(BucketDataEntryKeyCount),
}

impl From<IntermediateBucketDataEntry> for BucketDataEntry {
    fn from(entry: IntermediateBucketDataEntry) -> Self {
        match entry {
            IntermediateBucketDataEntry::KeyCount(key_count) => {
                BucketDataEntry::KeyCount(key_count.into())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// This is the default entry for a bucket, which contains a key, count, and optionally
/// sub_aggregations.
pub struct BucketDataEntryKeyCount {
    key: Key,
    doc_count: u64,
    values: Option<Vec<u64>>,
    sub_aggregation: AggregationResults,
}

impl From<IntermediateBucketDataEntryKeyCount> for BucketDataEntryKeyCount {
    fn from(entry: IntermediateBucketDataEntryKeyCount) -> Self {
        BucketDataEntryKeyCount {
            key: entry.key,
            doc_count: entry.doc_count,
            values: entry.values,
            sub_aggregation: entry.sub_aggregation.into(),
        }
    }
}
