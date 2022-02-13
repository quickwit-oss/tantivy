//! Module for all bucket aggregations.
//!
//! Results of final buckets are [BucketDataEntry](super::agg_result::BucketDataEntry).
//! Results of intermediate buckets are
//! [IntermediateBucketDataEntry](super::intermediate_agg_result::IntermediateBucketDataEntry)

mod range;

pub use range::RangeAggregation;
pub(crate) use range::SegmentRangeCollector;
