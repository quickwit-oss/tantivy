use std::collections::HashMap;

use super::bucket::RangeAggregationReq;

pub type Aggregations = HashMap<String, Aggregation>;

/// Aggregation tree.
///
#[derive(Clone, Debug, PartialEq)]
pub enum Aggregation {
    Bucket(BucketAggregation),
    Metric(MetricAggregation),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BucketAggregation {
    pub bucket_agg: BucketAggregationType,
    pub sub_aggregation: Aggregations,
}

/// BucketAggregations don’t calculate metrics over fields like the metrics aggregations do, but instead, they create buckets of documents.
/// Each bucket is associated with a criterion (depending on the aggregation type) which determines whether or not a document in the current context
/// "falls" into it. In other words, the buckets effectively define document sets. In addition to the buckets themselves, the bucket aggregations also
/// compute and return the number of documents that "fell into" each bucket.
/// Bucket aggregations, as opposed to metrics aggregations, can hold sub-aggregations. These sub-aggregations will be aggregated for the buckets created
/// by their "parent" bucket aggregation.
/// There are different bucket aggregators, each with a different "bucketing" strategy. Some define a single bucket, some define fixed number of multiple
/// buckets, and others dynamically create the buckets during the aggregation process.
#[derive(Clone, Debug, PartialEq)]
pub enum BucketAggregationType {
    TermAggregation {
        /// The field to aggregate on.
        field_name: String, // Produces as leaf doc_counts, but as intermediate additionally doc id list for the sub steps
    },
    /// Put data into predefined buckets.
    RangeAggregation(RangeAggregationReq),
    //   /// Put data into predefined buckets.
    //Histogram {
    ///// The field to aggregate on.
    //field_name: String,
    ///// Interval of a bucket.
    //interval: u64,
    //}
}

/// The aggregations in this family compute metrics based on values extracted in one way or another from the documents that are being aggregated.
/// The values are extracted from the fast field of the document.

/// Numeric metrics aggregations are a special type of metrics aggregation which output numeric values. Some aggregations output a single numeric metric
/// (e.g. Average) and are called single-value numeric metrics aggregation, others generate multiple metrics (e.g. stats) and are called multi-value numeric
/// metrics aggregation. The distinction between single-value and multi-value numeric metrics aggregations plays a role when these aggregations serve as
/// direct sub-aggregations of some bucket aggregations (some bucket aggregations enable you to sort the returned buckets based on the numeric metrics in
/// each bucket).
#[derive(Clone, Debug, PartialEq)]
pub enum MetricAggregation {
    /// Calculates the average.
    Average {
        /// The field name to compute the average on.
        field_name: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blub_test() {
        let mut sub_aggregation = HashMap::new();
        sub_aggregation.insert(
            "the_average".to_string(),
            Aggregation::Metric(MetricAggregation::Average {
                field_name: "my_field".to_string(),
            }),
        );

        BucketAggregation {
            bucket_agg: BucketAggregationType::RangeAggregation(RangeAggregationReq {
                field_name: "test".to_string(),
                buckets: vec![0..60, 60..90, 90..110],
            }),
            sub_aggregation,
        };
    }
}
