//! Module for all bucket aggregations.
//!
//! BucketAggregations create buckets of documents.
//! Each bucket is associated with a rule which
//! determines whether or not a document in the falls into it. In other words, the buckets
//! effectively define document sets. Buckets are not necessarily disjunct, therefore a document can
//! fall into multiple buckets. In addition to the buckets themselves, the bucket aggregations also
//! compute and return the number of documents for each bucket. Bucket aggregations, as opposed to
//! metric aggregations, can hold sub-aggregations. These sub-aggregations will be aggregated for
//! the buckets created by their "parent" bucket aggregation. There are different bucket
//! aggregators, each with a different "bucketing" strategy. Some define a single bucket, some
//! define fixed number of multiple buckets, and others dynamically create the buckets during the
//! aggregation process.
//!
//! Results of final buckets are [`BucketResult`](super::agg_result::BucketResult).
//! Results of intermediate buckets are
//! [`IntermediateBucketResult`](super::intermediate_agg_result::IntermediateBucketResult)
//!
//! ## Supported Bucket Aggregations
//! - [Histogram](HistogramAggregation)
//! - [DateHistogram](DateHistogramAggregationReq)
//! - [Range](RangeAggregation)
//! - [Terms](TermsAggregation)

mod histogram;
mod range;
mod term_agg;
mod term_missing_agg;

use std::collections::HashMap;

pub use histogram::*;
pub use range::*;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
pub use term_agg::*;
pub use term_missing_agg::*;

/// Order for buckets in a bucket aggregation.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, Default)]
pub enum Order {
    /// Asc order
    #[serde(rename = "asc")]
    Asc,
    /// Desc order
    #[serde(rename = "desc")]
    #[default]
    Desc,
}

#[derive(Clone, Debug, PartialEq)]
/// Order property by which to apply the order
#[derive(Default)]
pub enum OrderTarget {
    /// The key of the bucket
    Key,
    /// The doc count of the bucket
    #[default]
    Count,
    /// Order by value of the sub aggregation metric with identified by given `String`.
    ///
    /// Only single value metrics are supported currently
    SubAggregation(String),
}

impl From<&str> for OrderTarget {
    fn from(val: &str) -> Self {
        match val {
            "_key" => OrderTarget::Key,
            "_count" => OrderTarget::Count,
            _ => OrderTarget::SubAggregation(val.to_string()),
        }
    }
}

impl ToString for OrderTarget {
    fn to_string(&self) -> String {
        match self {
            OrderTarget::Key => "_key".to_string(),
            OrderTarget::Count => "_count".to_string(),
            OrderTarget::SubAggregation(agg) => agg.to_string(),
        }
    }
}

/// Set the order. target is either "_count", "_key", or the name of
/// a metric sub_aggregation.
///
/// De/Serializes to elasticsearch compatible JSON.
///
/// Examples in JSON format:
/// { "_count": "asc" }
/// { "_key": "asc" }
/// { "average_price": "asc" }
#[derive(Clone, Default, Debug, PartialEq)]
pub struct CustomOrder {
    /// The target property by which to sort by
    pub target: OrderTarget,
    /// The order asc or desc
    pub order: Order,
}

impl Serialize for CustomOrder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let map: HashMap<String, Order> =
            std::iter::once((self.target.to_string(), self.order)).collect();
        map.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CustomOrder {
    fn deserialize<D>(deserializer: D) -> Result<CustomOrder, D::Error>
    where D: Deserializer<'de> {
        HashMap::<String, Order>::deserialize(deserializer).and_then(|map| {
            if let Some((key, value)) = map.into_iter().next() {
                Ok(CustomOrder {
                    target: key.as_str().into(),
                    order: value,
                })
            } else {
                Err(de::Error::custom(
                    "unexpected empty map in order".to_string(),
                ))
            }
        })
    }
}

#[test]
fn custom_order_serde_test() {
    let order = CustomOrder {
        target: OrderTarget::Key,
        order: Order::Desc,
    };

    let order_str = serde_json::to_string(&order).unwrap();
    assert_eq!(order_str, "{\"_key\":\"desc\"}");
    let order_deser = serde_json::from_str(&order_str).unwrap();

    assert_eq!(order, order_deser);

    let order_deser: serde_json::Result<CustomOrder> = serde_json::from_str("{}");
    assert!(order_deser.is_err());

    let order_deser: serde_json::Result<CustomOrder> = serde_json::from_str("[]");
    assert!(order_deser.is_err());
}
