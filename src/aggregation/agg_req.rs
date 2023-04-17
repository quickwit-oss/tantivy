//! Contains the aggregation request tree. Used to build an
//! [`AggregationCollector`](super::AggregationCollector).
//!
//! [`Aggregations`] is the top level entry point to create a request, which is a `HashMap<String,
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
//!         Aggregation::Bucket(Box::new(BucketAggregation {
//!             bucket_agg: BucketAggregationType::Range(RangeAggregation{
//!                 field: "score".to_string(),
//!                 ranges: vec![(3f64..7f64).into(), (7f64..20f64).into()],
//!                 keyed: false,
//!             }),
//!             sub_aggregation: Default::default(),
//!         })),
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

use super::bucket::{
    DateHistogramAggregationReq, HistogramAggregation, RangeAggregation, TermsAggregation,
};
use super::metric::{
    AverageAggregation, CountAggregation, MaxAggregation, MinAggregation,
    PercentilesAggregationReq, StatsAggregation, SumAggregation,
};
use super::VecWithNames;

/// The top-level aggregation request structure, which contains [`Aggregation`] and their user
/// defined names. It is also used in [buckets](BucketAggregation) to define sub-aggregations.
///
/// The key is the user defined name of the aggregation.
pub type Aggregations = HashMap<String, Aggregation>;

/// Like Aggregations, but optimized to work with the aggregation result
#[derive(Clone, Debug)]
pub(crate) struct AggregationsInternal {
    pub(crate) metrics: VecWithNames<MetricAggregation>,
    pub(crate) buckets: VecWithNames<BucketAggregationInternal>,
}

impl From<Aggregations> for AggregationsInternal {
    fn from(aggs: Aggregations) -> Self {
        let mut metrics = vec![];
        let mut buckets = vec![];
        for (key, agg) in aggs {
            match agg {
                Aggregation::Bucket(bucket) => {
                    let sub_aggregation = bucket.get_sub_aggs().clone().into();
                    buckets.push((
                        key,
                        BucketAggregationInternal {
                            bucket_agg: bucket.bucket_agg,
                            sub_aggregation,
                        },
                    ))
                }
                Aggregation::Metric(metric) => metrics.push((key, metric)),
            }
        }
        Self {
            metrics: VecWithNames::from_entries(metrics),
            buckets: VecWithNames::from_entries(buckets),
        }
    }
}

#[derive(Clone, Debug)]
// Like BucketAggregation, but optimized to work with the result
pub(crate) struct BucketAggregationInternal {
    /// Bucket aggregation strategy to group documents.
    pub bucket_agg: BucketAggregationType,
    /// The sub_aggregations in the buckets. Each bucket will aggregate on the document set in the
    /// bucket.
    pub sub_aggregation: AggregationsInternal,
}

impl BucketAggregationInternal {
    pub(crate) fn as_range(&self) -> Option<&RangeAggregation> {
        match &self.bucket_agg {
            BucketAggregationType::Range(range) => Some(range),
            _ => None,
        }
    }
    pub(crate) fn as_histogram(&self) -> crate::Result<Option<HistogramAggregation>> {
        match &self.bucket_agg {
            BucketAggregationType::Histogram(histogram) => Ok(Some(histogram.clone())),
            BucketAggregationType::DateHistogram(histogram) => {
                Ok(Some(histogram.to_histogram_req()?))
            }
            _ => Ok(None),
        }
    }
    pub(crate) fn as_term(&self) -> Option<&TermsAggregation> {
        match &self.bucket_agg {
            BucketAggregationType::Terms(terms) => Some(terms),
            _ => None,
        }
    }
}

/// Extract all fast field names used in the tree.
pub fn get_fast_field_names(aggs: &Aggregations) -> HashSet<String> {
    let mut fast_field_names = Default::default();
    for el in aggs.values() {
        el.get_fast_field_names(&mut fast_field_names)
    }
    fast_field_names
}

/// Aggregation request of [`BucketAggregation`] or [`MetricAggregation`].
///
/// An aggregation is either a bucket or a metric.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Aggregation {
    /// Bucket aggregation, see [`BucketAggregation`] for details.
    Bucket(Box<BucketAggregation>),
    /// Metric aggregation, see [`MetricAggregation`] for details.
    Metric(MetricAggregation),
}

impl Aggregation {
    fn get_fast_field_names(&self, fast_field_names: &mut HashSet<String>) {
        match self {
            Aggregation::Bucket(bucket) => bucket.get_fast_field_names(fast_field_names),
            Aggregation::Metric(metric) => {
                fast_field_names.insert(metric.get_fast_field_name().to_string());
            }
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
    pub(crate) fn get_sub_aggs(&self) -> &Aggregations {
        &self.sub_aggregation
    }
    fn get_fast_field_names(&self, fast_field_names: &mut HashSet<String>) {
        let fast_field_name = self.bucket_agg.get_fast_field_name();
        fast_field_names.insert(fast_field_name.to_string());
        fast_field_names.extend(get_fast_field_names(&self.sub_aggregation));
    }
}

/// The bucket aggregation types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BucketAggregationType {
    /// Put data into buckets of user-defined ranges.
    #[serde(rename = "range")]
    Range(RangeAggregation),
    /// Put data into buckets of user-defined ranges.
    #[serde(rename = "histogram")]
    Histogram(HistogramAggregation),
    /// Put data into buckets of user-defined ranges.
    #[serde(rename = "date_histogram")]
    DateHistogram(DateHistogramAggregationReq),
    /// Put data into buckets of terms.
    #[serde(rename = "terms")]
    Terms(TermsAggregation),
}

impl BucketAggregationType {
    fn get_fast_field_name(&self) -> &str {
        match self {
            BucketAggregationType::Terms(terms) => terms.field.as_str(),
            BucketAggregationType::Range(range) => range.field.as_str(),
            BucketAggregationType::Histogram(histogram) => histogram.field.as_str(),
            BucketAggregationType::DateHistogram(histogram) => histogram.field.as_str(),
        }
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
    /// Computes the average of the extracted values.
    #[serde(rename = "avg")]
    Average(AverageAggregation),
    /// Counts the number of extracted values.
    #[serde(rename = "value_count")]
    Count(CountAggregation),
    /// Finds the maximum value.
    #[serde(rename = "max")]
    Max(MaxAggregation),
    /// Finds the minimum value.
    #[serde(rename = "min")]
    Min(MinAggregation),
    /// Computes a collection of statistics (`min`, `max`, `sum`, `count`, and `avg`) over the
    /// extracted values.
    #[serde(rename = "stats")]
    Stats(StatsAggregation),
    /// Computes the sum of the extracted values.
    #[serde(rename = "sum")]
    Sum(SumAggregation),
    /// Computes the sum of the extracted values.
    #[serde(rename = "percentiles")]
    Percentiles(PercentilesAggregationReq),
}

impl MetricAggregation {
    pub(crate) fn as_percentile(&self) -> Option<&PercentilesAggregationReq> {
        match &self {
            MetricAggregation::Percentiles(percentile_req) => Some(percentile_req),
            _ => None,
        }
    }

    fn get_fast_field_name(&self) -> &str {
        match self {
            MetricAggregation::Average(avg) => avg.field_name(),
            MetricAggregation::Count(count) => count.field_name(),
            MetricAggregation::Max(max) => max.field_name(),
            MetricAggregation::Min(min) => min.field_name(),
            MetricAggregation::Stats(stats) => stats.field_name(),
            MetricAggregation::Sum(sum) => sum.field_name(),
            MetricAggregation::Percentiles(per) => per.field_name(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_aggregations_deser() {
        let agg_req_json = r#"{
            "price_avg": { "avg": { "field": "price" } },
            "price_count": { "value_count": { "field": "price" } },
            "price_max": { "max": { "field": "price" } },
            "price_min": { "min": { "field": "price" } },
            "price_stats": { "stats": { "field": "price" } },
            "price_sum": { "sum": { "field": "price" } }
        }"#;
        let agg_req: Aggregations = serde_json::from_str(agg_req_json).unwrap();

        assert!(
            matches!(agg_req.get("price_avg").unwrap(), Aggregation::Metric(MetricAggregation::Average(avg)) if avg.field == "price")
        );
        assert!(
            matches!(agg_req.get("price_count").unwrap(), Aggregation::Metric(MetricAggregation::Count(count)) if count.field == "price")
        );
        assert!(
            matches!(agg_req.get("price_max").unwrap(), Aggregation::Metric(MetricAggregation::Max(max)) if max.field == "price")
        );
        assert!(
            matches!(agg_req.get("price_min").unwrap(), Aggregation::Metric(MetricAggregation::Min(min)) if min.field == "price")
        );
        assert!(
            matches!(agg_req.get("price_stats").unwrap(), Aggregation::Metric(MetricAggregation::Stats(stats)) if stats.field == "price")
        );
        assert!(
            matches!(agg_req.get("price_sum").unwrap(), Aggregation::Metric(MetricAggregation::Sum(sum)) if sum.field == "price")
        );
    }

    #[test]
    fn serialize_to_json_test() {
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
      ],
      "keyed": true
    }
  }
}"#;

        let agg_req1: Aggregations =
            { serde_json::from_str(elasticsearch_compatible_json_req).unwrap() };

        let agg_req2: String = serde_json::to_string_pretty(&agg_req1).unwrap();
        assert_eq!(agg_req2, elasticsearch_compatible_json_req);
    }

    #[test]
    fn test_get_fast_field_names() {
        let range_agg: Aggregation = {
            serde_json::from_value(json!({
                "range": {
                    "field": "score",
                    "ranges": [
                        { "to": 3.0 },
                        { "from": 3.0, "to": 7.0 },
                        { "from": 7.0, "to": 20.0 },
                        { "from": 20.0 }
                    ],
                }

            }))
            .unwrap()
        };

        let agg_req1: Aggregations = {
            serde_json::from_value(json!({
                "range1": range_agg,
                "range2":{
                    "range": {
                        "field": "score2",
                        "ranges": [
                            { "to": 3.0 },
                            { "from": 3.0, "to": 7.0 },
                            { "from": 7.0, "to": 20.0 },
                            { "from": 20.0 }
                        ],
                    },
                    "aggs": {
                        "metric": {
                            "avg": {
                                "field": "field123"
                            }
                        }
                    }
                }
            }))
            .unwrap()
        };

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
