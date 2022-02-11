//! Contains the aggregation request tree. Used to build an
//! [AggregationCollector](super::AggregationCollector).
//!
//! [Aggregations] is the top level entry point to create a request, which is a `HashMap<String,
//! Aggregation>`.
//!
//! # Example
//!
//! ```verbatim
//! let agg_req_1: Aggregations = vec![
//!     (
//!         "range".to_string(),
//!         Aggregation::Bucket(BucketAggregation {
//!             bucket_agg: BucketAggregationType::RangeAggregation(RangeAggregationReq {                               
//!                 field_name: "score".to_string(),
//!                 buckets: vec![(3f64..7f64), (7f64..20f64)],
//!             }),
//!             sub_aggregation: Default::default(),
//!         }),
//!     )]
//!     .into_iter()
//!     .collect();
//! ```

use std::collections::HashMap;

pub use super::bucket::RangeAggregation;

/// The top-level aggregation request structure, which contains [Aggregation] and their user defined
/// names.
///
/// The key is the user defined name of the aggregation.
pub type Aggregations = HashMap<String, Aggregation>;

/// Aggregation request of [BucketAggregation] or [MetricAggregation].
///
/// An aggregation is either a bucket or a metric.
#[derive(Clone, Debug, PartialEq)]
pub enum Aggregation {
    /// Bucket aggregation, see [BucketAggregation] for details.
    Bucket(BucketAggregation),
    /// Metric aggregation, see [MetricAggregation] for details.
    Metric(MetricAggregation),
}

#[derive(Clone, Debug, PartialEq)]
/// BucketAggregations create buckets of documents. Each bucket is associated with a rule which
/// determines whether or not a document in the falls into it. In other words, the buckets
/// effectively define document sets. Buckets are not necessarily disjunct, therefore a document can
/// fall into multiple buckets. In addition to the buckets themselves, the bucket aggregations also
/// compute and return the number of documents for each bucket. Bucket aggregations, as opposed to
/// metric aggregations, can hold sub-aggregations. These sub-aggregations will be aggregated for
/// the buckets created by their "parent" bucket aggregation. There are different bucket
/// aggregators, each with a different "bucketing" strategy. Some define a single bucket, some
/// define fixed number of multiple buckets, and others dynamically create the buckets during the
/// aggregation process.
pub struct BucketAggregation {
    /// Bucket aggregation strategy to group documents.
    pub bucket_agg: BucketAggregationType,
    /// The sub_aggregations in the buckets. Each bucket will aggregate on the document set in the
    /// bucket.
    pub sub_aggregation: Aggregations,
}

#[derive(Clone, Debug, PartialEq)]
/// The bucket aggregation types.
pub enum BucketAggregationType {
    /// Put data into buckets of user-defined ranges.
    RangeAggregation(RangeAggregation),
}

/// The aggregations in this family compute metrics based on values extracted
/// from the documents that are being aggregated. Values are extracted from the fast field of
/// the document.

/// Some aggregations output a single numeric metric (e.g. Average) and are called
/// single-value numeric metrics aggregation, others generate multiple metrics (e.g. stats) and are
/// called multi-value numeric metrics aggregation.
#[derive(Clone, Debug, PartialEq)]
pub enum MetricAggregation {
    /// Calculates the average.
    Average {
        /// The field name to compute the average on.
        field_name: String,
    },
}
