//! Module for all bucket aggregations.

mod range;

pub use range::RangeAggregation;
pub(crate) use range::SegmentRangeCollector;
