use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor,
};
use crate::aggregation::f64_from_fastfield_u64;
use crate::aggregation::intermediate_agg_result::IntermediateBucketResult;
use crate::aggregation::segment_agg_result::{
    SegmentAggregationResultsCollector, SegmentHistogramBucketEntry,
};
use crate::fastfield::{DynamicFastFieldReader, FastFieldReader};
use crate::schema::Type;
use crate::{DocId, TantivyError};

/// Histogram is a bucket aggregation, where buckets are created dynamically for given `interval`.
/// Each document value is rounded down to its bucket.
///
/// E.g. if we have a price 18 and an interval of 5, the document will fall into the bucket with
/// the key 15. The formula used for this is:
/// `((val - offset) / interval).floor() * interval + offset`
///
/// For this calculation all fastfield values are converted to f64.
///
/// # Returned Buckets
/// By default buckets are returned between the min and max value of the documents, including empty
/// buckets.
/// Setting min_doc_count to != 0 will filter empty buckets.
///
/// The value range of the buckets can bet extended via
/// [extended_bounds](HistogramAggregation::extended_bounds) or set to a predefined range via
/// [hard_bounds](HistogramAggregation::hard_bounds).
///
/// # Result
/// Result type is [BucketResult](crate::aggregation::agg_result::BucketResult) with
/// [RangeBucketEntry](crate::aggregation::agg_result::BucketEntry) on the
/// AggregationCollector.
///
/// Result type is
/// [crate::aggregation::intermediate_agg_result::IntermediateBucketResult] with
/// [crate::aggregation::intermediate_agg_result::IntermediateHistogramBucketEntry] on the
/// DistributedAggregationCollector.
///
/// # Request JSON Format
/// ```ignore
/// {
///     "prices": {
///         "histogram": {
///             "field": "price",
///             "interval": 10,
///         }
///     }
/// }
///  ```
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HistogramAggregation {
    /// The field to aggregate on.
    pub field: String,
    /// The interval to chunk your data range. The buckets span ranges of [0..interval).
    /// Must be a positive value.
    pub interval: f64,
    /// Intervals intersect at 0 by default, offset can move the interval.
    /// Offset has to be in the range [0, interval).
    ///
    /// As an example. If there are two documents with value 8 and 12 and interval 10.0, they would
    /// fall into the buckets with the key 0 and 10.
    /// With offset 5 and interval 10, they would both fall into the bucket with they key 5 and the
    /// range [5..15)
    pub offset: Option<f64>,
    /// The minimum number of documents in a bucket to be returned. Defaults to 0.
    pub min_doc_count: Option<u64>,
    /// Sets a hard limit for the data range.
    /// This can be used to filter values if they are not in the data range.
    pub hard_bounds: Option<HistogramBounds>,
    /// Can be set to extend your bounds. The range of the buckets is by default defined by the
    /// data range of the values of the documents. As the name suggests, this can only be used to
    /// extend the value range. If the bounds for min or max are not extending the range, the value
    /// has no effect on the returned buckets.
    ///
    /// Cannot be set in conjunction with min_doc_count > 0, since the empty buckets from extended
    /// bounds would not be returned.
    pub extended_bounds: Option<HistogramBounds>,
}

impl HistogramAggregation {
    fn validate(&self) -> crate::Result<()> {
        if self.interval <= 0.0f64 {
            return Err(TantivyError::InvalidArgument(
                "interval must be a positive value".to_string(),
            ));
        }

        if self.min_doc_count.unwrap_or(0) > 0 && self.extended_bounds.is_some() {
            return Err(TantivyError::InvalidArgument(
                "Cannot set min_doc_count and extended_bounds at the same time".to_string(),
            ));
        }

        Ok(())
    }

    /// Returns the minimum number of documents required for a bucket to be returned.
    pub fn min_doc_count(&self) -> u64 {
        self.min_doc_count.unwrap_or(0)
    }
}

/// Used to set extended or hard bounds on the histogram.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistogramBounds {
    /// The lower bounds.
    pub min: f64,
    /// The upper bounds.
    pub max: f64,
}

impl HistogramBounds {
    fn contains(&self, val: f64) -> bool {
        val >= self.min && val <= self.max
    }
}

/// The collector puts values from the fast field into the correct buckets and does a conversion to
/// the correct datatype.
#[derive(Clone, Debug, PartialEq)]
pub struct SegmentHistogramCollector {
    /// The buckets containing the aggregation data.
    buckets: Vec<SegmentHistogramBucketEntry>,
    sub_aggregations: Option<Vec<SegmentAggregationResultsCollector>>,
    field_type: Type,
    req: HistogramAggregation,
    offset: f64,
    first_bucket_num: i64,
    bounds: HistogramBounds,
}

impl SegmentHistogramCollector {
    pub fn into_intermediate_bucket_result(self) -> IntermediateBucketResult {
        let mut buckets = Vec::with_capacity(
            self.buckets
                .iter()
                .filter(|bucket| bucket.doc_count != 0)
                .count(),
        );
        if let Some(sub_aggregations) = self.sub_aggregations {
            buckets.extend(
                self.buckets
                    .into_iter()
                    .zip(sub_aggregations.into_iter())
                    // Here we remove the empty buckets for two reasons
                    // 1. To reduce the size of the intermediate result, which may be passed on the wire.
                    // 2. To mimic elasticsearch, there are no empty buckets at the start and end.
                    //
                    // Empty buckets may be added later again in the final result, depending on the request.
                    .filter(|(bucket, _sub_aggregation)| bucket.doc_count != 0)
                    .map(|(bucket, sub_aggregation)| (bucket, Some(sub_aggregation)).into()),
            )
        } else {
            buckets.extend(
                self.buckets
                    .into_iter()
                    // Here we remove the empty buckets for two reasons
                    // 1. To reduce the size of the intermediate result, which may be passed on the wire.
                    // 2. To mimic elasticsearch, there are no empty buckets at the start and end.
                    //
                    // Empty buckets may be added later again in the final result, depending on the request.
                    .filter(|bucket| bucket.doc_count != 0)
                    .map(|bucket| (bucket, None).into()),
            );
        };

        IntermediateBucketResult::Histogram {
            buckets,
            req: self.req,
        }
    }

    pub(crate) fn from_req_and_validate(
        req: &HistogramAggregation,
        sub_aggregation: &AggregationsWithAccessor,
        field_type: Type,
        accessor: &DynamicFastFieldReader<u64>,
    ) -> crate::Result<Self> {
        req.validate()?;
        let min = f64_from_fastfield_u64(accessor.min_value(), &field_type);
        let max = f64_from_fastfield_u64(accessor.max_value(), &field_type);

        let (min, max) = get_req_min_max(req, min, max);

        // We compute and generate the buckets range (min, max) based on the request and the min
        // max in the fast field, but this is likely not ideal when this is a subbucket, where many
        // unnecessary buckets may be generated.
        let buckets = generate_buckets(req, min, max);

        let sub_aggregations = if sub_aggregation.is_empty() {
            None
        } else {
            let sub_aggregation =
                SegmentAggregationResultsCollector::from_req_and_validate(sub_aggregation)?;
            Some(buckets.iter().map(|_| sub_aggregation.clone()).collect())
        };

        let buckets = buckets
            .iter()
            .map(|bucket| SegmentHistogramBucketEntry {
                key: *bucket,
                doc_count: 0,
            })
            .collect();

        let (min, _) = get_req_min_max(req, min, max);

        let first_bucket_num =
            get_bucket_num_f64(min, req.interval, req.offset.unwrap_or(0.0)) as i64;

        let bounds = req.hard_bounds.clone().unwrap_or(HistogramBounds {
            min: f64::MIN,
            max: f64::MAX,
        });

        Ok(Self {
            buckets,
            field_type,
            req: req.clone(),
            offset: req.offset.unwrap_or(0f64),
            first_bucket_num,
            bounds,
            sub_aggregations,
        })
    }

    #[inline]
    pub(crate) fn collect_block(
        &mut self,
        doc: &[DocId],
        bucket_with_accessor: &BucketAggregationWithAccessor,
        force_flush: bool,
    ) {
        let bounds = self.bounds;
        let interval = self.req.interval;
        let offset = self.offset;
        let first_bucket_num = self.first_bucket_num;
        let get_bucket_num =
            |val| (get_bucket_num_f64(val, interval, offset) as i64 - first_bucket_num) as usize;

        let mut iter = doc.chunks_exact(4);
        for docs in iter.by_ref() {
            let val0 = self.f64_from_fastfield_u64(bucket_with_accessor.accessor.get(docs[0]));
            let val1 = self.f64_from_fastfield_u64(bucket_with_accessor.accessor.get(docs[1]));
            let val2 = self.f64_from_fastfield_u64(bucket_with_accessor.accessor.get(docs[2]));
            let val3 = self.f64_from_fastfield_u64(bucket_with_accessor.accessor.get(docs[3]));

            let bucket_pos0 = get_bucket_num(val0);
            let bucket_pos1 = get_bucket_num(val1);
            let bucket_pos2 = get_bucket_num(val2);
            let bucket_pos3 = get_bucket_num(val3);

            self.increment_bucket_if_in_bounds(
                val0,
                &bounds,
                bucket_pos0,
                docs[0],
                &bucket_with_accessor.sub_aggregation,
            );
            self.increment_bucket_if_in_bounds(
                val1,
                &bounds,
                bucket_pos1,
                docs[1],
                &bucket_with_accessor.sub_aggregation,
            );
            self.increment_bucket_if_in_bounds(
                val2,
                &bounds,
                bucket_pos2,
                docs[2],
                &bucket_with_accessor.sub_aggregation,
            );
            self.increment_bucket_if_in_bounds(
                val3,
                &bounds,
                bucket_pos3,
                docs[3],
                &bucket_with_accessor.sub_aggregation,
            );
        }
        for doc in iter.remainder() {
            let val =
                f64_from_fastfield_u64(bucket_with_accessor.accessor.get(*doc), &self.field_type);
            if !bounds.contains(val) {
                continue;
            }
            let bucket_pos = (get_bucket_num_f64(val, self.req.interval, self.offset) as i64
                - self.first_bucket_num) as usize;

            debug_assert_eq!(
                self.buckets[bucket_pos].key,
                get_bucket_val(val, self.req.interval, self.offset) as f64
            );
            self.increment_bucket(bucket_pos, *doc, &bucket_with_accessor.sub_aggregation);
        }
        if force_flush {
            if let Some(sub_aggregations) = self.sub_aggregations.as_mut() {
                for sub_aggregation in sub_aggregations {
                    sub_aggregation
                        .flush_staged_docs(&bucket_with_accessor.sub_aggregation, force_flush);
                }
            }
        }
    }

    #[inline]
    fn increment_bucket_if_in_bounds(
        &mut self,
        val: f64,
        bounds: &HistogramBounds,
        bucket_pos: usize,
        doc: DocId,
        bucket_with_accessor: &AggregationsWithAccessor,
    ) {
        if bounds.contains(val) {
            debug_assert_eq!(
                self.buckets[bucket_pos].key,
                get_bucket_val(val, self.req.interval, self.offset) as f64
            );

            self.increment_bucket(bucket_pos, doc, &bucket_with_accessor);
        }
    }

    #[inline]
    fn increment_bucket(
        &mut self,
        bucket_pos: usize,
        doc: DocId,
        bucket_with_accessor: &AggregationsWithAccessor,
    ) {
        let bucket = &mut self.buckets[bucket_pos];
        bucket.doc_count += 1;
        if let Some(sub_aggregation) = self.sub_aggregations.as_mut() {
            (&mut sub_aggregation[bucket_pos]).collect(doc, bucket_with_accessor);
        }
    }

    fn f64_from_fastfield_u64(&self, val: u64) -> f64 {
        f64_from_fastfield_u64(val, &self.field_type)
    }
}

#[inline]
fn get_bucket_num_f64(val: f64, interval: f64, offset: f64) -> f64 {
    ((val - offset) / interval).floor()
}

#[inline]
fn get_bucket_val(val: f64, interval: f64, offset: f64) -> f64 {
    let bucket_pos = get_bucket_num_f64(val, interval, offset);
    bucket_pos * interval + offset
}

fn get_req_min_max(req: &HistogramAggregation, mut min: f64, mut max: f64) -> (f64, f64) {
    if let Some(extended_bounds) = &req.extended_bounds {
        min = min.min(extended_bounds.min);
        max = max.max(extended_bounds.max);
    }
    if let Some(hard_bounds) = &req.hard_bounds {
        min = hard_bounds.min;
        max = hard_bounds.max;
    }

    (min, max)
}

/// Generates buckets with req.interval, for given min, max
pub(crate) fn generate_buckets(req: &HistogramAggregation, min: f64, max: f64) -> Vec<f64> {
    let (min, max) = get_req_min_max(req, min, max);

    let offset = req.offset.unwrap_or(0.0);
    let first_bucket_num = get_bucket_num_f64(min, req.interval, offset) as i64;
    let last_bucket_num = get_bucket_num_f64(max, req.interval, offset) as i64;
    let mut buckets = vec![];
    for bucket_pos in first_bucket_num..=last_bucket_num {
        let bucket_key = bucket_pos as f64 * req.interval + offset;
        buckets.push(bucket_key);
    }

    buckets
}

#[test]
fn generate_buckets_test() {
    let histogram_req = HistogramAggregation {
        field: "dummy".to_string(),
        interval: 2.0,
        ..Default::default()
    };

    let buckets = generate_buckets(&histogram_req, 0.0, 10.0);
    assert_eq!(buckets, vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);

    let buckets = generate_buckets(&histogram_req, 2.5, 5.5);
    assert_eq!(buckets, vec![2.0, 4.0]);

    // Single bucket
    let buckets = generate_buckets(&histogram_req, 0.5, 0.75);
    assert_eq!(buckets, vec![0.0]);

    // With offset
    let histogram_req = HistogramAggregation {
        field: "dummy".to_string(),
        interval: 2.0,
        offset: Some(0.5),
        ..Default::default()
    };

    let buckets = generate_buckets(&histogram_req, 0.0, 10.0);
    assert_eq!(buckets, vec![-1.5, 0.5, 2.5, 4.5, 6.5, 8.5]);

    let buckets = generate_buckets(&histogram_req, 2.5, 5.5);
    assert_eq!(buckets, vec![2.5, 4.5]);

    // Single bucket
    let buckets = generate_buckets(&histogram_req, 0.5, 0.75);
    assert_eq!(buckets, vec![0.5]);

    // With extended_bounds
    let histogram_req = HistogramAggregation {
        field: "dummy".to_string(),
        interval: 2.0,
        extended_bounds: Some(HistogramBounds {
            min: 0.0,
            max: 10.0,
        }),
        ..Default::default()
    };

    let buckets = generate_buckets(&histogram_req, 0.0, 10.0);
    assert_eq!(buckets, vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);

    let buckets = generate_buckets(&histogram_req, 2.5, 5.5);
    assert_eq!(buckets, vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);

    // Single bucket, but extended_bounds
    let buckets = generate_buckets(&histogram_req, 0.5, 0.75);
    assert_eq!(buckets, vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);

    // With invalid extended_bounds
    let histogram_req = HistogramAggregation {
        field: "dummy".to_string(),
        interval: 2.0,
        extended_bounds: Some(HistogramBounds { min: 3.0, max: 5.0 }),
        ..Default::default()
    };

    let buckets = generate_buckets(&histogram_req, 0.0, 10.0);
    assert_eq!(buckets, vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);

    // With hard_bounds reducing
    let histogram_req = HistogramAggregation {
        field: "dummy".to_string(),
        interval: 2.0,
        hard_bounds: Some(HistogramBounds { min: 3.0, max: 5.0 }),
        ..Default::default()
    };

    let buckets = generate_buckets(&histogram_req, 0.0, 10.0);
    assert_eq!(buckets, vec![2.0, 4.0]);

    // With hard_bounds extending
    let histogram_req = HistogramAggregation {
        field: "dummy".to_string(),
        interval: 2.0,
        hard_bounds: Some(HistogramBounds {
            min: 0.0,
            max: 10.0,
        }),
        ..Default::default()
    };

    let buckets = generate_buckets(&histogram_req, 2.5, 5.5);
    assert_eq!(buckets, vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);

    // Blubber
    let histogram_req = HistogramAggregation {
        field: "dummy".to_string(),
        interval: 2.0,
        ..Default::default()
    };

    let buckets = generate_buckets(&histogram_req, 4.0, 10.0);
    assert_eq!(buckets, vec![4.0, 6.0, 8.0, 10.0]);
}

#[cfg(test)]
mod tests {

    use serde_json::Value;

    use super::*;
    use crate::aggregation::agg_req::{
        Aggregation, Aggregations, BucketAggregation, BucketAggregationType,
    };
    use crate::aggregation::tests::{
        get_test_index_2_segments, get_test_index_from_values, get_test_index_with_num_docs,
    };
    use crate::aggregation::AggregationCollector;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::IndexRecordOption;
    use crate::{Index, Term};

    fn exec_request(agg_req: Aggregations, index: &Index) -> crate::Result<Value> {
        let collector = AggregationCollector::from_aggs(agg_req);

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        Ok(res)
    }

    #[test]
    fn histogram_test_crooked_values() -> crate::Result<()> {
        let values = vec![-12.0, 12.31, 14.33, 16.23];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req: Aggregations = vec![(
            "my_interval".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                    field: "score_f64".to_string(),
                    interval: 3.5,
                    offset: Some(0.0),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["my_interval"]["buckets"][0]["key"], -14.0);
        assert_eq!(res["my_interval"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][7]["key"], 10.5);
        assert_eq!(res["my_interval"]["buckets"][7]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][8]["key"], 14.0);
        assert_eq!(res["my_interval"]["buckets"][8]["doc_count"], 2);
        assert_eq!(res["my_interval"]["buckets"][9], Value::Null);

        // With offset
        let agg_req: Aggregations = vec![(
            "my_interval".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                    field: "score_f64".to_string(),
                    interval: 3.5,
                    offset: Some(1.2),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["my_interval"]["buckets"][0]["key"], -12.8);
        assert_eq!(res["my_interval"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][1]["key"], -9.3);
        assert_eq!(res["my_interval"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["my_interval"]["buckets"][2]["key"], -5.8);
        assert_eq!(res["my_interval"]["buckets"][2]["doc_count"], 0);
        assert_eq!(res["my_interval"]["buckets"][3]["key"], -2.3);
        assert_eq!(res["my_interval"]["buckets"][3]["doc_count"], 0);

        assert_eq!(res["my_interval"]["buckets"][7]["key"], 11.7);
        assert_eq!(res["my_interval"]["buckets"][7]["doc_count"], 2);
        assert_eq!(res["my_interval"]["buckets"][8]["key"], 15.2);
        assert_eq!(res["my_interval"]["buckets"][8]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][9], Value::Null);

        Ok(())
    }

    #[test]
    fn histogram_test_min_value_positive_force_merge_segments() -> crate::Result<()> {
        histogram_test_min_value_positive_merge_segments(true)
    }

    #[test]
    fn histogram_test_min_value_positive() -> crate::Result<()> {
        histogram_test_min_value_positive_merge_segments(false)
    }
    fn histogram_test_min_value_positive_merge_segments(merge_segments: bool) -> crate::Result<()> {
        let values = vec![10.0, 12.0, 14.0, 16.23];

        let index = get_test_index_from_values(merge_segments, &values)?;

        let agg_req: Aggregations = vec![(
            "my_interval".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                    field: "score_f64".to_string(),
                    interval: 1.0,
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["my_interval"]["buckets"][0]["key"], 10.0);
        assert_eq!(res["my_interval"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][1]["key"], 11.0);
        assert_eq!(res["my_interval"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["my_interval"]["buckets"][2]["key"], 12.0);
        assert_eq!(res["my_interval"]["buckets"][2]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][3]["key"], 13.0);
        assert_eq!(res["my_interval"]["buckets"][3]["doc_count"], 0);
        assert_eq!(res["my_interval"]["buckets"][6]["key"], 16.0);
        assert_eq!(res["my_interval"]["buckets"][6]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][7], Value::Null);

        Ok(())
    }

    #[test]
    fn histogram_simple_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = vec![(
            "histogram".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                    field: "score_f64".to_string(),
                    interval: 1.0,
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 0.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 1.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 1);
        assert_eq!(res["histogram"]["buckets"][99]["key"], 99.0);
        assert_eq!(res["histogram"]["buckets"][99]["doc_count"], 1);
        assert_eq!(res["histogram"]["buckets"][100], Value::Null);
        Ok(())
    }

    #[test]
    fn histogram_merge_test() -> crate::Result<()> {
        // Merge buckets counts from different segments
        let values = vec![10.0, 12.0, 14.0, 16.23, 10.0, 13.0, 10.0, 12.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req: Aggregations = vec![(
            "histogram".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                    field: "score_f64".to_string(),
                    interval: 1.0,
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 10.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 11.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][2]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][2]["doc_count"], 2);
        assert_eq!(res["histogram"]["buckets"][3]["key"], 13.0);
        assert_eq!(res["histogram"]["buckets"][3]["doc_count"], 1);

        Ok(())
    }
    #[test]
    fn histogram_min_doc_test_multi_segments() -> crate::Result<()> {
        histogram_min_doc_test_with_opt(false)
    }
    #[test]
    fn histogram_min_doc_test_single_segments() -> crate::Result<()> {
        histogram_min_doc_test_with_opt(true)
    }
    fn histogram_min_doc_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let values = vec![10.0, 12.0, 14.0, 16.23, 10.0, 13.0, 10.0, 12.0];

        let index = get_test_index_from_values(merge_segments, &values)?;

        let agg_req: Aggregations = vec![(
            "histogram".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                    field: "score_f64".to_string(),
                    interval: 1.0,
                    min_doc_count: Some(2),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 10.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["histogram"]["buckets"][2], Value::Null);

        Ok(())
    }
    #[test]
    fn histogram_hard_bounds_test_multi_segment() -> crate::Result<()> {
        histogram_hard_bounds_test_with_opt(false)
    }
    #[test]
    fn histogram_hard_bounds_test_single_segment() -> crate::Result<()> {
        histogram_hard_bounds_test_with_opt(true)
    }
    fn histogram_hard_bounds_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let values = vec![10.0, 12.0, 14.0, 16.23, 10.0, 13.0, 10.0, 12.0];

        let index = get_test_index_from_values(merge_segments, &values)?;

        let agg_req: Aggregations = vec![(
            "histogram".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                    field: "score_f64".to_string(),
                    interval: 1.0,
                    hard_bounds: Some(HistogramBounds {
                        min: 2.0,
                        max: 12.0,
                    }),
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 2.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][10]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][10]["doc_count"], 2);
        assert_eq!(res["histogram"]["buckets"][11], Value::Null);

        Ok(())
    }

    #[test]
    fn histogram_empty_bucket_behaviour_test_single_segment() -> crate::Result<()> {
        histogram_empty_bucket_behaviour_test_with_opt(true)
    }

    #[test]
    fn histogram_empty_bucket_behaviour_test_multi_segment() -> crate::Result<()> {
        histogram_empty_bucket_behaviour_test_with_opt(false)
    }

    fn histogram_empty_bucket_behaviour_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let index = get_test_index_2_segments(merge_segments)?;

        let agg_req: Aggregations = vec![(
            "histogram".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Histogram(HistogramAggregation {
                    field: "score_f64".to_string(),
                    interval: 1.0,
                    ..Default::default()
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        // let res = exec_request(agg_req, &index)?;

        let reader = index.reader()?;
        let text_field = reader.searcher().schema().get_field("text").unwrap();

        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "nohit"),
            IndexRecordOption::Basic,
        );

        let collector = AggregationCollector::from_aggs(agg_req);

        let searcher = reader.searcher();
        let agg_res = searcher.search(&term_query, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
        println!("{}", &serde_json::to_string_pretty(&agg_res)?);

        assert_eq!(res["histogram"]["buckets"][0]["key"], 6.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["histogram"]["buckets"][37]["key"], 43.0);
        assert_eq!(res["histogram"]["buckets"][37]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][38]["key"], 44.0);
        assert_eq!(res["histogram"]["buckets"][38]["doc_count"], 1);
        assert_eq!(res["histogram"]["buckets"][39], Value::Null);

        Ok(())
    }
}
