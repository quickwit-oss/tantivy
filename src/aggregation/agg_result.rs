//! Contains the final aggregation tree.
//! This tree will be used to merge results between segments and between indices.

use super::Key;
use crate::DocId;
use std::collections::HashMap;

pub type AggregationResultTree = HashMap<String, AggregationResult>;

pub enum AggregationResult {
    BucketResult(BucketAggregationResult),
    MetricResult(MetricResult),
}

pub enum MetricResult {
    Average(i64),
}

pub struct BucketAggregationResult {
    buckets: Vec<BucketDataEntry>,
}

pub enum BucketDataEntry {
    KeyCount {
        key: Key,
        doc_count: u64,
        docs: Option<Vec<DocId>>,
        sub_aggregation: Option<HashMap<String, AggregationResult>>,
    },
}
