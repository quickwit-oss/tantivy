//! Module for all bucket aggregations.
//!
//! Results of final buckets are [BucketResult](super::agg_result::BucketResult).
//! Results of intermediate buckets are
//! [IntermediateBucketResult](super::intermediate_agg_result::IntermediateBucketResult)

mod range;

pub use range::RangeAggregation;
pub(crate) use range::SegmentRangeCollector;
