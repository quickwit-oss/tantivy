use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor,
};
use crate::aggregation::intermediate_agg_result::IntermediateBucketResult;
use crate::aggregation::segment_agg_result::{
    SegmentAggregationResultsCollector, SegmentRangeBucketEntry,
};
use crate::aggregation::{f64_from_fastfield_u64, f64_to_fastfield_u64, Key};
use crate::fastfield::FastFieldReader;
use crate::schema::Type;
use crate::{DocId, TantivyError};

/// Provide user-defined buckets to aggregate on.
/// Two special buckets will automatically be created to cover the whole range of values.
/// The provided buckets have to be continous.
/// During the aggregation, the values extracted from the fast_field `field` will be checked
/// against each bucket range. Note that this aggregation includes the from value and excludes the
/// to value for each range.
///
/// Result type is [BucketResult](crate::aggregation::agg_result::BucketResult) with
/// [RangeBucketEntry](crate::aggregation::agg_result::RangeBucketEntry) on the
/// AggregationCollector.
///
/// Result type is
/// [crate::aggregation::intermediate_agg_result::IntermediateBucketResult] with
/// [crate::aggregation::intermediate_agg_result::IntermediateRangeBucketEntry] on the
/// DistributedAggregationCollector.
///
/// # Request JSON Format
/// ```json
/// {
///     "range": {
///         "field": "score",
///         "ranges": [
///             { "to": 3.0 },
///             { "from": 3.0, "to": 7.0 },
///             { "from": 7.0, "to": 20.0 }
///             { "from": 20.0 }
///         ]
///     }
///  }
///  ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangeAggregation {
    /// The field to aggregate on.
    pub field: String,
    /// Note that this aggregation includes the from value and excludes the to value for each
    /// range. Extra buckets will be created until the first to, and last from, if necessary.
    pub ranges: Vec<RangeAggregationRange>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// The range for one range bucket.
pub struct RangeAggregationRange {
    /// The from range value, which is inclusive in the range.
    /// None equals to an open ended interval.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub from: Option<f64>,
    /// The to range value, which is not inclusive in the range.
    /// None equals to an open ended interval.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to: Option<f64>,
}

impl From<Range<f64>> for RangeAggregationRange {
    fn from(range: Range<f64>) -> Self {
        let from = if range.start == f64::MIN {
            None
        } else {
            Some(range.start)
        };
        let to = if range.end == f64::MAX {
            None
        } else {
            Some(range.end)
        };
        RangeAggregationRange { from, to }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SegmentRangeAndBucketEntry {
    range: Range<u64>,
    bucket: SegmentRangeBucketEntry,
}

/// The collector puts values from the fast field into the correct buckets and does a conversion to
/// the correct datatype.
#[derive(Clone, Debug, PartialEq)]
pub struct SegmentRangeCollector {
    /// The buckets containing the aggregation data.
    buckets: Vec<SegmentRangeAndBucketEntry>,
    field_type: Type,
}

impl SegmentRangeCollector {
    pub fn into_intermediate_bucket_result(self) -> IntermediateBucketResult {
        let field_type = self.field_type;

        let buckets = self
            .buckets
            .into_iter()
            .map(move |range_bucket| {
                (
                    range_to_string(&range_bucket.range, &field_type),
                    range_bucket.bucket.into(),
                )
            })
            .collect();

        IntermediateBucketResult::Range(buckets)
    }

    pub(crate) fn from_req_and_validate(
        req: &RangeAggregation,
        sub_aggregation: &AggregationsWithAccessor,
        field_type: Type,
    ) -> crate::Result<Self> {
        // The range input on the request is f64.
        // We need to convert to u64 ranges, because we read the values as u64.
        // The mapping from the conversion is monotonic so ordering is preserved.
        let buckets = extend_validate_ranges(&req.ranges, &field_type)?
            .iter()
            .map(|range| {
                let to = if range.end == u64::MAX {
                    None
                } else {
                    Some(f64_from_fastfield_u64(range.end, &field_type))
                };
                let from = if range.start == u64::MIN {
                    None
                } else {
                    Some(f64_from_fastfield_u64(range.start, &field_type))
                };
                let sub_aggregation = if sub_aggregation.is_empty() {
                    None
                } else {
                    Some(SegmentAggregationResultsCollector::from_req_and_validate(
                        sub_aggregation,
                    )?)
                };
                Ok(SegmentRangeAndBucketEntry {
                    range: range.clone(),
                    bucket: SegmentRangeBucketEntry {
                        key: range_to_key(range, &field_type),
                        doc_count: 0,
                        sub_aggregation,
                        from,
                        to,
                    },
                })
            })
            .collect::<crate::Result<_>>()?;

        Ok(SegmentRangeCollector {
            buckets,
            field_type,
        })
    }

    #[inline]
    pub(crate) fn collect_block(
        &mut self,
        doc: &[DocId],
        bucket_with_accessor: &BucketAggregationWithAccessor,
        force_flush: bool,
    ) {
        let mut iter = doc.chunks_exact(4);
        for docs in iter.by_ref() {
            let val1 = bucket_with_accessor.accessor.get(docs[0]);
            let val2 = bucket_with_accessor.accessor.get(docs[1]);
            let val3 = bucket_with_accessor.accessor.get(docs[2]);
            let val4 = bucket_with_accessor.accessor.get(docs[3]);
            let bucket_pos1 = self.get_bucket_pos(val1);
            let bucket_pos2 = self.get_bucket_pos(val2);
            let bucket_pos3 = self.get_bucket_pos(val3);
            let bucket_pos4 = self.get_bucket_pos(val4);

            self.increment_bucket(bucket_pos1, docs[0], &bucket_with_accessor.sub_aggregation);
            self.increment_bucket(bucket_pos2, docs[1], &bucket_with_accessor.sub_aggregation);
            self.increment_bucket(bucket_pos3, docs[2], &bucket_with_accessor.sub_aggregation);
            self.increment_bucket(bucket_pos4, docs[3], &bucket_with_accessor.sub_aggregation);
        }
        for doc in iter.remainder() {
            let val = bucket_with_accessor.accessor.get(*doc);
            let bucket_pos = self.get_bucket_pos(val);
            self.increment_bucket(bucket_pos, *doc, &bucket_with_accessor.sub_aggregation);
        }
        if force_flush {
            for bucket in &mut self.buckets {
                if let Some(sub_aggregation) = &mut bucket.bucket.sub_aggregation {
                    sub_aggregation
                        .flush_staged_docs(&bucket_with_accessor.sub_aggregation, force_flush);
                }
            }
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

        bucket.bucket.doc_count += 1;
        if let Some(sub_aggregation) = &mut bucket.bucket.sub_aggregation {
            sub_aggregation.collect(doc, bucket_with_accessor);
        }
    }

    #[inline]
    fn get_bucket_pos(&self, val: u64) -> usize {
        let pos = self
            .buckets
            .binary_search_by_key(&val, |probe| probe.range.start)
            .unwrap_or_else(|pos| pos - 1);
        debug_assert!(self.buckets[pos].range.contains(&val));
        pos
    }
}

/// Converts the user provided f64 range value to fast field value space.
///
/// Internally fast field values are always stored as u64.
/// If the fast field has u64 [1,2,5], these values are stored as is in the fast field.
/// A fast field with f64 [1.0, 2.0, 5.0] is converted to u64 space, using a
/// monotonic mapping function, so the order is preserved.
///
/// Consequently, a f64 user range 1.0..3.0 needs to be converted to fast field value space using
/// the same monotonic mapping function, so that the provided ranges contain the u64 values in the
/// fast field.
/// The alternative would be that every value read would be converted to the f64 range, but that is
/// more computational expensive when many documents are hit.
fn to_u64_range(range: &RangeAggregationRange, field_type: &Type) -> crate::Result<Range<u64>> {
    let start = if let Some(from) = range.from {
        f64_to_fastfield_u64(from, field_type)
            .ok_or_else(|| TantivyError::InvalidArgument("invalid field type".to_string()))?
    } else {
        u64::MIN
    };

    let end = if let Some(to) = range.to {
        f64_to_fastfield_u64(to, field_type)
            .ok_or_else(|| TantivyError::InvalidArgument("invalid field type".to_string()))?
    } else {
        u64::MAX
    };

    Ok(start..end)
}

/// Extends the provided buckets to contain the whole value range, by inserting buckets at the
/// beginning and end.
fn extend_validate_ranges(
    buckets: &[RangeAggregationRange],
    field_type: &Type,
) -> crate::Result<Vec<Range<u64>>> {
    let mut converted_buckets = buckets
        .iter()
        .map(|range| to_u64_range(range, field_type))
        .collect::<crate::Result<Vec<_>>>()?;

    converted_buckets.sort_by_key(|bucket| bucket.start);
    if converted_buckets[0].start != u64::MIN {
        converted_buckets.insert(0, u64::MIN..converted_buckets[0].start);
    }

    if converted_buckets[converted_buckets.len() - 1].end != u64::MAX {
        converted_buckets.push(converted_buckets[converted_buckets.len() - 1].end..u64::MAX);
    }

    // fill up holes in the ranges
    let find_hole = |converted_buckets: &[Range<u64>]| {
        for (pos, ranges) in converted_buckets.windows(2).enumerate() {
            if ranges[0].end > ranges[1].start {
                return Err(TantivyError::InvalidArgument(format!(
                    "Overlapping ranges not supported range {:?}, range+1 {:?}",
                    ranges[0], ranges[1]
                )));
            }
            if ranges[0].end != ranges[1].start {
                return Ok(Some(pos));
            }
        }
        Ok(None)
    };

    while let Some(hole_pos) = find_hole(&converted_buckets)? {
        let new_range = converted_buckets[hole_pos].end..converted_buckets[hole_pos + 1].start;
        converted_buckets.insert(hole_pos + 1, new_range);
    }

    Ok(converted_buckets)
}

pub(crate) fn range_to_string(range: &Range<u64>, field_type: &Type) -> String {
    // is_start is there for malformed requests, e.g. ig the user passes the range u64::MIN..0.0,
    // it should be rendererd as "*-0" and not "*-*"
    let to_str = |val: u64, is_start: bool| {
        if (is_start && val == u64::MIN) || (!is_start && val == u64::MAX) {
            "*".to_string()
        } else {
            f64_from_fastfield_u64(val, field_type).to_string()
        }
    };

    format!("{}-{}", to_str(range.start, true), to_str(range.end, false))
}

pub(crate) fn range_to_key(range: &Range<u64>, field_type: &Type) -> Key {
    Key::Str(range_to_string(range, field_type))
}

#[cfg(test)]
mod tests {

    use serde_json::Value;

    use super::*;
    use crate::aggregation::agg_req::{
        Aggregation, Aggregations, BucketAggregation, BucketAggregationType,
    };
    use crate::aggregation::tests::get_test_index_with_num_docs;
    use crate::aggregation::AggregationCollector;
    use crate::fastfield::FastValue;
    use crate::query::AllQuery;

    pub fn get_collector_from_ranges(
        ranges: Vec<RangeAggregationRange>,
        field_type: Type,
    ) -> SegmentRangeCollector {
        let req = RangeAggregation {
            field: "dummy".to_string(),
            ranges,
        };

        SegmentRangeCollector::from_req_and_validate(&req, &Default::default(), field_type).unwrap()
    }

    #[test]
    fn range_fraction_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = vec![(
            "range".to_string(),
            Aggregation::Bucket(BucketAggregation {
                bucket_agg: BucketAggregationType::Range(RangeAggregation {
                    field: "fraction_f64".to_string(),
                    ranges: vec![(0f64..0.1f64).into(), (0.1f64..0.2f64).into()],
                }),
                sub_aggregation: Default::default(),
            }),
        )]
        .into_iter()
        .collect();

        let collector = AggregationCollector::from_aggs(agg_req);

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(res["range"]["buckets"][0]["key"], "*-0");
        assert_eq!(res["range"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["range"]["buckets"][1]["key"], "0-0.1");
        assert_eq!(res["range"]["buckets"][1]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][2]["key"], "0.1-0.2");
        assert_eq!(res["range"]["buckets"][2]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][3]["key"], "0.2-*");
        assert_eq!(res["range"]["buckets"][3]["doc_count"], 80);

        Ok(())
    }

    #[test]
    fn bucket_test_extend_range_hole() {
        let buckets = vec![(10f64..20f64).into(), (30f64..40f64).into()];
        let collector = get_collector_from_ranges(buckets, Type::F64);

        let buckets = collector.buckets;
        assert_eq!(buckets[0].range.start, u64::MIN);
        assert_eq!(buckets[0].range.end, 10f64.to_u64());
        assert_eq!(buckets[1].range.start, 10f64.to_u64());
        assert_eq!(buckets[1].range.end, 20f64.to_u64());
        // Added bucket to fill hole
        assert_eq!(buckets[2].range.start, 20f64.to_u64());
        assert_eq!(buckets[2].range.end, 30f64.to_u64());
        assert_eq!(buckets[3].range.start, 30f64.to_u64());
        assert_eq!(buckets[3].range.end, 40f64.to_u64());
    }

    #[test]
    fn bucket_test_range_conversion_special_case() {
        // the monotonic conversion between f64 and u64, does not map f64::MIN.to_u64() ==
        // u64::MIN, but the into trait converts f64::MIN/MAX to None
        let buckets = vec![
            (f64::MIN..10f64).into(),
            (10f64..20f64).into(),
            (20f64..f64::MAX).into(),
        ];
        let collector = get_collector_from_ranges(buckets, Type::F64);

        let buckets = collector.buckets;
        assert_eq!(buckets[0].range.start, u64::MIN);
        assert_eq!(buckets[0].range.end, 10f64.to_u64());
        assert_eq!(buckets[1].range.start, 10f64.to_u64());
        assert_eq!(buckets[1].range.end, 20f64.to_u64());
        assert_eq!(buckets[2].range.start, 20f64.to_u64());
        assert_eq!(buckets[2].range.end, u64::MAX);
        assert_eq!(buckets.len(), 3);
    }

    #[test]
    fn bucket_range_test_negative_vals() {
        let buckets = vec![(-10f64..-1f64).into()];
        let collector = get_collector_from_ranges(buckets, Type::F64);

        let buckets = collector.buckets;
        assert_eq!(&buckets[0].bucket.key.to_string(), "*--10");
        assert_eq!(&buckets[buckets.len() - 1].bucket.key.to_string(), "-1-*");
    }
    #[test]
    fn bucket_range_test_positive_vals() {
        let buckets = vec![(0f64..10f64).into()];
        let collector = get_collector_from_ranges(buckets, Type::F64);

        let buckets = collector.buckets;
        assert_eq!(&buckets[0].bucket.key.to_string(), "*-0");
        assert_eq!(&buckets[buckets.len() - 1].bucket.key.to_string(), "10-*");
    }

    #[test]
    fn range_binary_search_test_u64() {
        let check_ranges = |ranges: Vec<RangeAggregationRange>| {
            let collector = get_collector_from_ranges(ranges, Type::U64);
            let search = |val: u64| collector.get_bucket_pos(val);

            assert_eq!(search(u64::MIN), 0);
            assert_eq!(search(9), 0);
            assert_eq!(search(10), 1);
            assert_eq!(search(11), 1);
            assert_eq!(search(99), 1);
            assert_eq!(search(100), 2);
            assert_eq!(search(u64::MAX - 1), 2); // Since the end range is never included, the max
                                                 // value
        };

        let ranges = vec![(10.0..100.0).into()];
        check_ranges(ranges);

        let ranges = vec![
            RangeAggregationRange {
                to: Some(10.0),
                from: None,
            },
            (10.0..100.0).into(),
        ];
        check_ranges(ranges);

        let ranges = vec![
            RangeAggregationRange {
                to: Some(10.0),
                from: None,
            },
            (10.0..100.0).into(),
            RangeAggregationRange {
                to: None,
                from: Some(100.0),
            },
        ];
        check_ranges(ranges);
    }

    #[test]
    fn range_binary_search_test_f64() {
        let ranges = vec![
            //(f64::MIN..10.0).into(),
            (10.0..100.0).into(),
            //(100.0..f64::MAX).into(),
        ];

        let collector = get_collector_from_ranges(ranges, Type::F64);
        let search = |val: u64| collector.get_bucket_pos(val);

        assert_eq!(search(u64::MIN), 0);
        assert_eq!(search(9f64.to_u64()), 0);
        assert_eq!(search(10f64.to_u64()), 1);
        assert_eq!(search(11f64.to_u64()), 1);
        assert_eq!(search(99f64.to_u64()), 1);
        assert_eq!(search(100f64.to_u64()), 2);
        assert_eq!(search(u64::MAX - 1), 2); // Since the end range is never included,
                                             // the max value
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use super::*;
    use crate::aggregation::bucket::range::tests::get_collector_from_ranges;

    const TOTAL_DOCS: u64 = 1_000_000u64;
    const NUM_DOCS: u64 = 50_000u64;

    fn get_collector_with_buckets(num_buckets: u64, num_docs: u64) -> SegmentRangeCollector {
        let bucket_size = num_docs / num_buckets;
        let mut buckets: Vec<RangeAggregationRange> = vec![];
        for i in 0..num_buckets {
            let bucket_start = (i * bucket_size) as f64;
            buckets.push((bucket_start..bucket_start + bucket_size as f64).into())
        }

        get_collector_from_ranges(buckets, Type::U64)
    }

    fn get_rand_docs(total_docs: u64, num_docs_returned: u64) -> Vec<u64> {
        let mut rng = thread_rng();

        let all_docs = (0..total_docs - 1).collect_vec();
        let mut vals = all_docs
            .as_slice()
            .choose_multiple(&mut rng, num_docs_returned as usize)
            .cloned()
            .collect_vec();
        vals.sort();
        vals
    }

    fn bench_range_binary_search(b: &mut test::Bencher, num_buckets: u64) {
        let collector = get_collector_with_buckets(num_buckets, TOTAL_DOCS);
        let vals = get_rand_docs(TOTAL_DOCS, NUM_DOCS);
        b.iter(|| {
            let mut bucket_pos = 0;
            for val in &vals {
                bucket_pos = collector.get_bucket_pos(*val);
            }
            bucket_pos
        })
    }

    #[bench]
    fn bench_range_100_buckets(b: &mut test::Bencher) {
        bench_range_binary_search(b, 100)
    }

    #[bench]
    fn bench_range_10_buckets(b: &mut test::Bencher) {
        bench_range_binary_search(b, 10)
    }
}
