//! Module for all bucket aggregations.
//!
//! BucketAggregations create buckets of documents
//! [BucketAggregation](super::agg_req::BucketAggregation).
//!
//! Results of final buckets are [BucketResult](super::agg_result::BucketResult).
//! Results of intermediate buckets are
//! [IntermediateBucketResult](super::intermediate_agg_result::IntermediateBucketResult)

mod histogram;
mod range;
mod term_agg;

pub(crate) use histogram::SegmentHistogramCollector;
pub use histogram::*;
pub(crate) use range::SegmentRangeCollector;
pub use range::*;
pub use term_agg::*;
