mod agg_req;
mod agg_req_with_accessor;
mod agg_result;
mod bucket;
mod executor;
mod intermediate_agg_result;
mod metric;
mod segment_agg_result;

pub use agg_req::Aggregation;
pub use agg_req::BucketAggregationType;
pub use agg_req::MetricAggregation;

use crate::collector::Fruit;

use self::agg_result::BucketAggregationResult;
use self::intermediate_agg_result::IntermediateAggregationResultTree;

/// The `SubAggregationCollector` is the trait in charge of defining the
/// collect operation for sub aggreagations at the scale of the segment.
///
/// A `SubAggregationCollector` handles the result of a bucket collector.
///
pub trait SubAggregationCollector: 'static {
    /// `Fruit` is the type for the result of our collection.
    /// e.g. `usize` for the `Count` collector.
    type Fruit: Fruit;

    /// The query pushes the scored document to the collector via this method.
    fn collect(&mut self, result: BucketAggregationResult);

    /// Extract the fruit of the collection from the `SubAggregationCollector`.
    fn harvest(self) -> Self::Fruit;
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum Key {
    Str(String),
    U64(u64),
    I64(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BucketDataEntry {
    KeyCount(BucketDataEntryKeyCount),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketDataEntryKeyCount {
    key: Key,
    doc_count: u64,
    values: Option<Vec<u64>>,
    sub_aggregation: Option<Box<IntermediateAggregationResultTree>>,
}
