//! Contains the aggregation request tree. Used to build an
//! [AggregationCollector](super::AggregationCollector).
//!
//! [Aggregations] is the top level entry point to create a request, which is a `HashMap<String,
//! Aggregation>`.
//!
//! Requests are compatible with the json format of elasticsearch.
//!
//! # Example
//!
//! ```
//! use tantivy::aggregation::bucket::RangeAggregation;
//! use tantivy::aggregation::agg_req::BucketAggregationType;
//! use tantivy::aggregation::agg_req::{Aggregation, Aggregations};
//! use tantivy::aggregation::agg_req::BucketAggregation;
//! let agg_req1: Aggregations = vec![
//!     (
//!         "range".to_string(),
//!         Aggregation::Bucket(BucketAggregation {
//!             bucket_agg: BucketAggregationType::Range(RangeAggregation{
//!                 field: "score".to_string(),
//!                 ranges: vec![(3f64..7f64).into(), (7f64..20f64).into()],
//!             }),
//!             sub_aggregation: Default::default(),
//!         }),
//!     ),
//! ]
//! .into_iter()
//! .collect();
//!
//! let elasticsearch_compatible_json_req = r#"
//! {
//!   "range": {
//!     "range": {
//!       "field": "score",
//!       "ranges": [
//!         { "from": 3.0, "to": 7.0 },
//!         { "from": 7.0, "to": 20.0 }
//!       ]
//!     }
//!   }
//! }"#;
//! let agg_req2: Aggregations = serde_json::from_str(elasticsearch_compatible_json_req).unwrap();
//! assert_eq!(agg_req1, agg_req2);
//! ```

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

pub use super::bucket::RangeAggregation;
use super::metric::{AverageAggregation, StatsAggregation};

/// The top-level aggregation request structure, which contains [Aggregation] and their user defined
/// names. It is also used in [buckets](BucketAggregation) to define sub-aggregations.
///
/// The key is the user defined name of the aggregation.
pub type Aggregations = HashMap<String, Aggregation>;

/// Extract all fast field names used in the tree.
pub fn get_fast_field_names(aggs: &Aggregations) -> HashSet<String> {
    let mut fast_field_names = Default::default();
    for el in aggs.values() {
        el.get_fast_field_names(&mut fast_field_names)
    }
    fast_field_names
}

/// Aggregation request of [BucketAggregation] or [MetricAggregation].
///
/// An aggregation is either a bucket or a metric.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Aggregation {
    /// Bucket aggregation, see [BucketAggregation] for details.
    Bucket(BucketAggregation),
    /// Metric aggregation, see [MetricAggregation] for details.
    Metric(MetricAggregation),
}

impl Aggregation {
    fn get_fast_field_names(&self, fast_field_names: &mut HashSet<String>) {
        match self {
            Aggregation::Bucket(bucket) => bucket.get_fast_field_names(fast_field_names),
            Aggregation::Metric(metric) => metric.get_fast_field_names(fast_field_names),
        }
    }
}

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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BucketAggregation {
    /// Bucket aggregation strategy to group documents.
    #[serde(flatten)]
    pub bucket_agg: BucketAggregationType,
    /// The sub_aggregations in the buckets. Each bucket will aggregate on the document set in the
    /// bucket.
    #[serde(rename = "aggs")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Aggregations::is_empty")]
    pub sub_aggregation: Aggregations,
}

impl BucketAggregation {
    fn get_fast_field_names(&self, fast_field_names: &mut HashSet<String>) {
        self.bucket_agg.get_fast_field_names(fast_field_names);
        fast_field_names.extend(get_fast_field_names(&self.sub_aggregation));
    }
}

/// The bucket aggregation types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BucketAggregationType {
    /// Put data into buckets of user-defined ranges.
    #[serde(rename = "range")]
    Range(RangeAggregation),
}

impl BucketAggregationType {
    fn get_fast_field_names(&self, fast_field_names: &mut HashSet<String>) {
        match self {
            BucketAggregationType::Range(range) => fast_field_names.insert(range.field.to_string()),
        };
    }
}

/// The aggregations in this family compute metrics based on values extracted
/// from the documents that are being aggregated. Values are extracted from the fast field of
/// the document.

/// Some aggregations output a single numeric metric (e.g. Average) and are called
/// single-value numeric metrics aggregation, others generate multiple metrics (e.g. Stats) and are
/// called multi-value numeric metrics aggregation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetricAggregation {
    /// Calculates the average.
    #[serde(rename = "avg")]
    Average(AverageAggregation),
    /// Calculates stats sum, average, min, max, standard_deviation on a field.
    #[serde(rename = "stats")]
    Stats(StatsAggregation),
}

impl MetricAggregation {
    fn get_fast_field_names(&self, fast_field_names: &mut HashSet<String>) {
        match self {
            MetricAggregation::Average(avg) => fast_field_names.insert(avg.field.to_string()),
            MetricAggregation::Stats(stats) => fast_field_names.insert(stats.field.to_string()),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_to_json_test() {
        let agg_req1: Aggregations = vec![(
            "range".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Range(RangeAggregation {
                    field: "score".to_string(),
                    ranges: vec![
                        (f64::MIN..3f64).into(),
                        (3f64..7f64).into(),
                        (7f64..20f64).into(),
                        (20f64..f64::MAX).into(),
                    ],
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let elasticsearch_compatible_json_req = r#"{
  "range": {
    "range": {
      "field": "score",
      "ranges": [
        {
          "to": 3.0
        },
        {
          "from": 3.0,
          "to": 7.0
        },
        {
          "from": 7.0,
          "to": 20.0
        },
        {
          "from": 20.0
        }
      ]
    }
  }
}"#;
        let agg_req2: String = serde_json::to_string_pretty(&agg_req1).unwrap();
        assert_eq!(agg_req2, elasticsearch_compatible_json_req);
    }

    #[test]
    fn test_get_fast_field_names() {
        let agg_req2: Aggregations = vec![
            (
                "range".to_string(),
                Aggregation::Bucket(BucketAggregation {
                    bucket_agg: BucketAggregationType::Range(RangeAggregation {
                        field: "score2".to_string(),
                        ranges: vec![
                            (f64::MIN..3f64).into(),
                            (3f64..7f64).into(),
                            (7f64..20f64).into(),
                            (20f64..f64::MAX).into(),
                        ],
                    }),
                    sub_aggregation: Default::default(),
                }),
            ),
            (
                "metric".to_string(),
                Aggregation::Metric(MetricAggregation::Average(
                    AverageAggregation::from_field_name("field123".to_string()),
                )),
            ),
        ]
        .into_iter()
        .collect();

        let agg_req1: Aggregations = vec![(
            "range".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Range(RangeAggregation {
                    field: "score".to_string(),
                    ranges: vec![
                        (f64::MIN..3f64).into(),
                        (3f64..7f64).into(),
                        (7f64..20f64).into(),
                        (20f64..f64::MAX).into(),
                    ],
                }),
                sub_aggregation: agg_req2,
            }),
        )]
        .into_iter()
        .collect();

        assert_eq!(
            get_fast_field_names(&agg_req1),
            vec![
                "score".to_string(),
                "score2".to_string(),
                "field123".to_string()
            ]
            .into_iter()
            .collect()
        )
    }
}
