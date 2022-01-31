mod agg_result;
mod agg_tree;
mod bucket;
mod executor;
mod intermediate_agg_result;
mod metric;
mod segment_agg_result;

pub use agg_tree::Aggregation;
pub use agg_tree::BucketAggregationType;
pub use agg_tree::MetricAggregation;

use crate::collector::Fruit;

use self::agg_result::BucketAggregationResult;

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
