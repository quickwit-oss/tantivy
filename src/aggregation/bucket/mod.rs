//! Module for all bucket aggregations.
//!
//! Results of final buckets are [BucketEntry](super::agg_result::BucketEntry).
//! Results of intermediate buckets are
//! [IntermediateBucketEntry](super::intermediate_agg_result::IntermediateBucketEntry)

mod range;

pub use range::RangeAggregation;
pub(crate) use range::SegmentRangeCollector;
