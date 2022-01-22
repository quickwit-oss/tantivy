use std::collections::HashMap;

use crate::DocId;

pub type AggregationResultTree = HashMap<String, AggregationResult>;

pub enum AggregationResult {
    BucketResult(BucketAggregationResult),
    MetricResult(MetricResult),
}

pub enum MetricResult {
    Average(i64),
}

pub struct BucketAggregationResult {
    buckets: Vec<Box<BucketDataEntry>>,
}

pub enum Key {
    String(String),
    ValueF64(f64),
    Valuei64(i64),
}

pub enum BucketDataEntry {
    KeyCount {
        key: Key,
        doc_count: u64,
        docs: Option<Vec<DocId>>,
        sub_aggregation: Option<HashMap<String, AggregationResult>>,
    },
}

//trait BucketEntry: Send {
//fn get_bucket_data(&self) -> BucketDataEntry;
//}
