use std::{collections::HashMap, ops::Range};

use crate::DocId;

type Aggregations = HashMap<String, Aggregation>;

/// Aggregation tree.
///
pub enum Aggregation {
    BucketAggregation {
        bucket_agg: BucketAggregation,
        sub_aggregation: Option<Box<Aggregations>>,
    },
    MetricAggregation(MetricAggregation),
}

/// BucketAggregations
/// don’t calculate metrics over fields like the metrics aggregations do, but instead, they create buckets of documents.
/// Each bucket is associated with a criterion (depending on the aggregation type) which determines whether or not a document in the current context
/// "falls" into it. In other words, the buckets effectively define document sets. In addition to the buckets themselves, the bucket aggregations also
/// compute and return the number of documents that "fell into" each bucket.
/// Bucket aggregations, as opposed to metrics aggregations, can hold sub-aggregations. These sub-aggregations will be aggregated for the buckets created
/// by their "parent" bucket aggregation.
/// There are different bucket aggregators, each with a different "bucketing" strategy. Some define a single bucket, some define fixed number of multiple
/// buckets, and others dynamically create the buckets during the aggregation process.
pub enum BucketAggregation {
    TermAggregation {
        /// The field to aggregate on.
        field: String,
    },
    /// Put data into predefined buckets.
    RangeAggregation {
        /// The field to aggregate on.
        field: String,
        /// Note that this aggregation includes the from value and excludes the to value for each range.
        /// Extra buckets will be created until the first to, and last from.
        buckets: Vec<Range<i64>>,
    },
}

/// BucketAggregation Results
pub struct BucketAggregationResult {
    /// [Bucket Index] -> List of DocIds
    buckets: Vec<Vec<DocId>>,
    /// [Bucket Index] -> Counts
    counts: Vec<usize>,
    /// [Bucket Index] -> Bucket Name
    bucket_metadata: Vec<String>,
}

/// The aggregations in this family compute metrics based on values extracted in one way or another from the documents that are being aggregated.
/// The values are extracted from the fast field of the document.

/// Numeric metrics aggregations are a special type of metrics aggregation which output numeric values. Some aggregations output a single numeric metric
/// (e.g. Average) and are called single-value numeric metrics aggregation, others generate multiple metrics (e.g. stats) and are called multi-value numeric
/// metrics aggregation. The distinction between single-value and multi-value numeric metrics aggregations plays a role when these aggregations serve as
/// direct sub-aggregations of some bucket aggregations (some bucket aggregations enable you to sort the returned buckets based on the numeric metrics in
/// each bucket).
pub enum MetricAggregation {
    /// Calculates the average.
    Average,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blub_test() {
        let mut sub_aggregation = HashMap::new();
        sub_aggregation.insert(
            "the_average".to_string(),
            Aggregation::MetricAggregation(MetricAggregation::Average),
        );

        Aggregation::BucketAggregation {
            bucket_agg: BucketAggregation::RangeAggregation {
                field: "test".to_string(),
                buckets: vec![0..60, 60..90, 90..110],
            },
            sub_aggregation: Some(Box::new(sub_aggregation)),
        };
    }
}
