use std::{collections::HashMap, ops::Range};

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

/// BucketAggregations don’t calculate metrics over fields like the metrics aggregations do, but instead, they create buckets of documents.
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
        field: String, // Produces as leaf doc_counts, but as intermediate additionally doc id list for the sub steps
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

/// The aggregations in this family compute metrics based on values extracted in one way or another from the documents that are being aggregated.
/// The values are extracted from the fast field of the document.

/// Numeric metrics aggregations are a special type of metrics aggregation which output numeric values. Some aggregations output a single numeric metric
/// (e.g. Average) and are called single-value numeric metrics aggregation, others generate multiple metrics (e.g. stats) and are called multi-value numeric
/// metrics aggregation. The distinction between single-value and multi-value numeric metrics aggregations plays a role when these aggregations serve as
/// direct sub-aggregations of some bucket aggregations (some bucket aggregations enable you to sort the returned buckets based on the numeric metrics in
/// each bucket).
pub enum MetricAggregation {
    /// Calculates the average.
    Average {
        /// The field name to compute the average on.
        field_name: String,
    },
}

//pub struct NamedBucketAggregationResult {
///// [Bucket Index] -> BucketAggregationResultVariant
////buckets: BucketAggregationResultVariant,
//buckets: Vec<Box<dyn BucketEntry>>,
///// [Bucket Index] -> Bucket Name
//bucket_metadata: Vec<String>,
//}

//pub enum BucketAggregationResultVariant {
//SubBucket(Vec<Box<NamedBucketAggregationResult>>),
//DocidBucket(Vec<Vec<DocId>>),
//Counts(Vec<usize>),
//Global(Vec<DocId>),
//}

//#[derive(Default)]
//pub struct BucketAggregationResult {
///// [Bucket Index] -> BucketAggregationResult
//sub_buckets: Vec<Box<BucketAggregationResult>>,
///// [Bucket Index] -> List of DocIds
//buckets: Vec<Vec<DocId>>,
///// [Bucket Index] -> Counts
//counts: Vec<usize>,
///// [Bucket Index] -> Bucket Name
//bucket_metadata: Vec<String>,
//}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blub_test() {
        let mut sub_aggregation = HashMap::new();
        sub_aggregation.insert(
            "the_average".to_string(),
            Aggregation::MetricAggregation(MetricAggregation::Average {
                field_name: "my_field".to_string(),
            }),
        );

        Aggregation::BucketAggregation {
            bucket_agg: BucketAggregation::RangeAggregation {
                field: "test".to_string(),
                buckets: vec![0..60, 60..90, 90..110],
            },
            sub_aggregation: Some(Box::new(sub_aggregation)),
        };
    }
    //#[test]
    //fn bucketbucket_result_variant() {
    //let sub_bucket1 = NamedBucketAggregationResult {
    //buckets: BucketAggregationResultVariant::Counts(vec![10, 30]),
    //bucket_metadata: vec!["2018".to_string(), "2019".to_string()],
    //};

    //let sub_bucket2 = NamedBucketAggregationResult {
    //buckets: BucketAggregationResultVariant::Counts(vec![40, 20]),
    //bucket_metadata: vec!["2018".to_string(), "2019".to_string()],
    //};

    //let _asdf = NamedBucketAggregationResult {
    //buckets: BucketAggregationResultVariant::SubBucket(vec![
    //Box::new(sub_bucket1),
    //Box::new(sub_bucket2),
    //]),
    //bucket_metadata: vec!["green".to_string(), "blue".to_string()],
    //};
    //}

    //#[test]
    //fn bucketbucket_result_test() {
    //let _geht = BucketAggregationResult {
    //sub_buckets: vec![
    //Box::new(BucketAggregationResult {
    //counts: vec![10, 30],
    //bucket_metadata: vec!["2018".to_string(), "2019".to_string()],
    //..Default::default()
    //}),
    //Box::new(BucketAggregationResult {
    //counts: vec![40, 20],
    //bucket_metadata: vec!["2018".to_string(), "2019".to_string()],
    //..Default::default()
    //}),
    //],
    //bucket_metadata: vec!["green".to_string(), "blue".to_string()],
    //..Default::default()
    //};
    //}
}
