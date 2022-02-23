//! Module for all bucket aggregations.
//!
//! BucketAggregations create buckets of documents
//! [BucketAggregation](super::agg_req::BucketAggregation).
//!
//! Results of final buckets are [BucketResult](super::agg_result::BucketResult).
//! Results of intermediate buckets are
//! [IntermediateBucketResult](super::intermediate_agg_result::IntermediateBucketResult)

mod range;

pub(crate) use range::SegmentRangeCollector;
pub use range::*;
