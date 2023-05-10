//! Contains the final aggregation tree.
//! This tree can be converted via the `into()` method from `IntermediateAggregationResults`.
//! This conversion computes the final result. For example: The intermediate result contains
//! intermediate average results, which is the sum and the number of values. The actual average is
//! calculated on the step from intermediate to final aggregation result tree.

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::bucket::GetDocCount;
use super::metric::{PercentilesMetricResult, SingleMetricResult, Stats};
use super::{AggregationError, Key};
use crate::TantivyError;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
/// The final aggegation result.
pub struct AggregationResults(pub FxHashMap<String, AggregationResult>);

impl AggregationResults {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        self.0
            .values()
            .map(|agg| agg.get_bucket_count())
            .sum::<u64>()
    }

    pub(crate) fn get_value_from_aggregation(
        &self,
        name: &str,
        agg_property: &str,
    ) -> crate::Result<Option<f64>> {
        if let Some(agg) = self.0.get(name) {
            agg.get_value_from_aggregation(name, agg_property)
        } else {
            // Validation is be done during request parsing, so we can't reach this state.
            Err(TantivyError::InternalError(format!(
                "Can't find aggregation {name:?} in sub-aggregations"
            )))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// An aggregation is either a bucket or a metric.
pub enum AggregationResult {
    /// Bucket result variant.
    BucketResult(BucketResult),
    /// Metric result variant.
    MetricResult(MetricResult),
}

impl AggregationResult {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        match self {
            AggregationResult::BucketResult(bucket) => bucket.get_bucket_count(),
            AggregationResult::MetricResult(_) => 0,
        }
    }

    pub(crate) fn get_value_from_aggregation(
        &self,
        _name: &str,
        agg_property: &str,
    ) -> crate::Result<Option<f64>> {
        match self {
            AggregationResult::BucketResult(_bucket) => Err(TantivyError::InternalError(
                "Tried to retrieve value from bucket aggregation. This is not supported and \
                 should not happen during collection phase, but should be caught during validation"
                    .to_string(),
            )),
            AggregationResult::MetricResult(metric) => metric.get_value(agg_property),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// MetricResult
pub enum MetricResult {
    /// Average metric result.
    Average(SingleMetricResult),
    /// Count metric result.
    Count(SingleMetricResult),
    /// Max metric result.
    Max(SingleMetricResult),
    /// Min metric result.
    Min(SingleMetricResult),
    /// Stats metric result.
    Stats(Stats),
    /// Sum metric result.
    Sum(SingleMetricResult),
    /// Sum metric result.
    Percentiles(PercentilesMetricResult),
}

impl MetricResult {
    fn get_value(&self, agg_property: &str) -> crate::Result<Option<f64>> {
        match self {
            MetricResult::Average(avg) => Ok(avg.value),
            MetricResult::Count(count) => Ok(count.value),
            MetricResult::Max(max) => Ok(max.value),
            MetricResult::Min(min) => Ok(min.value),
            MetricResult::Stats(stats) => stats.get_value(agg_property),
            MetricResult::Sum(sum) => Ok(sum.value),
            MetricResult::Percentiles(_) => Err(TantivyError::AggregationError(
                AggregationError::InvalidRequest("percentiles can't be used to order".to_string()),
            )),
        }
    }
}

/// BucketEntry holds bucket aggregation result types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BucketResult {
    /// This is the range entry for a bucket, which contains a key, count, from, to, and optionally
    /// sub-aggregations.
    Range {
        /// The range buckets sorted by range.
        buckets: BucketEntries<RangeBucketEntry>,
    },
    /// This is the histogram entry for a bucket, which contains a key, count, and optionally
    /// sub-aggregations.
    Histogram {
        /// The buckets.
        ///
        /// If there are holes depends on the request, if min_doc_count is 0, then there are no
        /// holes between the first and last bucket.
        /// See [`HistogramAggregation`](super::bucket::HistogramAggregation)
        buckets: BucketEntries<BucketEntry>,
    },
    /// This is the term result
    Terms {
        /// The buckets.
        ///
        /// See [`TermsAggregation`](super::bucket::TermsAggregation)
        buckets: Vec<BucketEntry>,
        /// The number of documents that didn’t make it into to TOP N due to shard_size or size
        sum_other_doc_count: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        /// The upper bound error for the doc count of each term.
        doc_count_error_upper_bound: Option<u64>,
    },
}

impl BucketResult {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        match self {
            BucketResult::Range { buckets } => {
                buckets.iter().map(|bucket| bucket.get_bucket_count()).sum()
            }
            BucketResult::Histogram { buckets } => {
                buckets.iter().map(|bucket| bucket.get_bucket_count()).sum()
            }
            BucketResult::Terms {
                buckets,
                sum_other_doc_count: _,
                doc_count_error_upper_bound: _,
            } => buckets.iter().map(|bucket| bucket.get_bucket_count()).sum(),
        }
    }
}

/// This is the wrapper of buckets entries, which can be vector or hashmap
/// depending on if it's keyed or not.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BucketEntries<T> {
    /// Vector format bucket entries
    Vec(Vec<T>),
    /// HashMap format bucket entries
    HashMap(FxHashMap<String, T>),
}

impl<T> BucketEntries<T> {
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &T> + 'a> {
        match self {
            BucketEntries::Vec(vec) => Box::new(vec.iter()),
            BucketEntries::HashMap(map) => Box::new(map.values()),
        }
    }
}

/// This is the default entry for a bucket, which contains a key, count, and optionally
/// sub-aggregations.
///
/// # JSON Format
/// ```json
/// {
///   ...
///     "my_histogram": {
///       "buckets": [
///         {
///           "key": "2.0",
///           "doc_count": 5
///         },
///         {
///           "key": "4.0",
///           "doc_count": 2
///         },
///         {
///           "key": "6.0",
///           "doc_count": 3
///         }
///       ]
///    }
///    ...
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BucketEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The string representation of the bucket.
    pub key_as_string: Option<String>,
    /// The identifier of the bucket.
    pub key: Key,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    #[serde(flatten)]
    /// Sub-aggregations in this bucket.
    pub sub_aggregation: AggregationResults,
}
impl BucketEntry {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        1 + self.sub_aggregation.get_bucket_count()
    }
}
impl GetDocCount for &BucketEntry {
    fn doc_count(&self) -> u64 {
        self.doc_count
    }
}
impl GetDocCount for BucketEntry {
    fn doc_count(&self) -> u64 {
        self.doc_count
    }
}

/// This is the range entry for a bucket, which contains a key, count, and optionally
/// sub-aggregations.
///
/// # JSON Format
/// ```json
/// {
///   ...
///     "my_ranges": {
///       "buckets": [
///         {
///           "key": "*-10",
///           "to": 10,
///           "doc_count": 5
///         },
///         {
///           "key": "10-20",
///           "from": 10,
///           "to": 20,
///           "doc_count": 2
///         },
///         {
///           "key": "20-*",
///           "from": 20,
///           "doc_count": 3
///         }
///       ]
///    }
///    ...
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangeBucketEntry {
    /// The identifier of the bucket.
    pub key: Key,
    /// Number of documents in the bucket.
    pub doc_count: u64,
    #[serde(flatten)]
    /// Sub-aggregations in this bucket.
    pub sub_aggregation: AggregationResults,
    /// The from range of the bucket. Equals `f64::MIN` when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<f64>,
    /// The to range of the bucket. Equals `f64::MAX` when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<f64>,
    /// The optional string representation for the `from` range.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_as_string: Option<String>,
    /// The optional string representation for the `to` range.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_as_string: Option<String>,
}
impl RangeBucketEntry {
    pub(crate) fn get_bucket_count(&self) -> u64 {
        1 + self.sub_aggregation.get_bucket_count()
    }
}
