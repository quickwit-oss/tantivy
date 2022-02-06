//! Contains the final aggregation tree.
//! This tree will be used to compute intermediate trees.

use super::{
    intermediate_agg_result::{
        IntermediateAggregationResult, IntermediateAggregationResults,
        IntermediateBucketAggregationResult, IntermediateBucketDataEntry,
        IntermediateBucketDataEntryKeyCount, IntermediateMetricResult,
    },
    metric::AverageData,
    BucketDataEntryKeyCount, Key,
};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
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
pub enum AggregationResult {
    BucketResult(BucketAggregationResult),
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
pub enum MetricResult {
    Average(f64),
}

impl From<IntermediateMetricResult> for MetricResult {
    fn from(metric: IntermediateMetricResult) -> Self {
        match metric {
            IntermediateMetricResult::Average(avg_data) => MetricResult::Average(avg_data.into()),
        }
    }
}

impl From<AverageData> for f64 {
    fn from(data: AverageData) -> Self {
        let avg = data.sum as f64 / data.num_vals as f64;
        avg
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BucketAggregationResult {
    pub buckets: HashMap<Key, BucketDataEntry>,
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
pub enum BucketDataEntry {
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
