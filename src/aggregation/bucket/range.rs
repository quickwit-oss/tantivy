use std::cmp::Ordering;
use std::ops::Range;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor,
};
use crate::aggregation::intermediate_agg_result::IntermediateBucketResult;
use crate::aggregation::segment_agg_result::{
    SegmentAggregationResultsCollector, SegmentBucketEntry, SegmentBucketEntryKeyCount,
};
use crate::aggregation::{f64_from_fastfield_u64, f64_to_fastfield_u64, Key};
use crate::fastfield::FastFieldReader;
use crate::schema::Type;
use crate::{DocId, TantivyError};

/// Provide user-defined buckets to aggregate on.
/// Two special buckets will automatically be created to cover the whole range of values.
/// The provided buckets have to be continous.
/// During the aggregation, the values extracted from the fast_field `field_name` will be checked
/// against each bucket range. Note that this aggregation includes the from value and excludes the
/// to value for each range.
///
/// Result type is [BucketResult](crate::aggregation::agg_result::BucketResult) with
/// [BucketEntryKeyCount](crate::aggregation::agg_result::BucketEntryKeyCount) on the
/// AggregationCollector.
///
/// Result type is
/// [crate::aggregation::intermediate_agg_result::IntermediateBucketResult] with
/// [crate::aggregation::intermediate_agg_result::IntermediateBucketEntryKeyCount] on the
/// DistributedAggregationCollector.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangeAggregation {
    /// The field to aggregate on.
    pub field_name: String,
    /// Note that this aggregation includes the from value and excludes the to value for each
    /// range. Extra buckets will be created until the first to, and last from.
    pub buckets: Vec<Range<f64>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SegmentRangeBucketEntry {
    range: Range<u64>,
    bucket: SegmentBucketEntry,
}

/// The collector puts values from the fast field into the correct buckets and does a conversion to
/// the correct datatype.
#[derive(Clone, Debug, PartialEq)]
pub struct SegmentRangeCollector {
    /// The buckets containing the aggregation data.
    buckets: Vec<SegmentRangeBucketEntry>,
    field_type: Type,
}

impl SegmentRangeCollector {
    pub fn into_intermediate_agg_result(self) -> IntermediateBucketResult {
        let field_type = self.field_type;
        let buckets = self
            .buckets
            .into_iter()
            .map(move |range_bucket| {
                (
                    range_to_key(&range_bucket.range, &field_type),
                    range_bucket.bucket.into(),
                )
            })
            .collect();

        IntermediateBucketResult { buckets }
    }

    pub(crate) fn from_req(
        req: &RangeAggregation,
        sub_aggregation: &AggregationsWithAccessor,
        field_type: Type,
    ) -> crate::Result<Self> {
        // The range input on the request is f64.
        // We need to convert to u64 ranges, because we read the values as u64.
        // The mapping from the conversion is monotonic so ordering is preserved.
        let buckets = extend_validate_ranges(&req.buckets, &field_type)?
            .iter()
            .map(|range| {
                Ok(SegmentRangeBucketEntry {
                    range: range.clone(),
                    bucket: SegmentBucketEntry::KeyCount(SegmentBucketEntryKeyCount {
                        key: range_to_key(range, &field_type),
                        doc_count: 0,
                        sub_aggregation: SegmentAggregationResultsCollector::from_req(
                            sub_aggregation,
                        )?,
                    }),
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

            self.increment_bucket(
                bucket_pos1,
                docs[0],
                &bucket_with_accessor.sub_aggregation,
                force_flush,
            );
            self.increment_bucket(
                bucket_pos2,
                docs[1],
                &bucket_with_accessor.sub_aggregation,
                force_flush,
            );
            self.increment_bucket(
                bucket_pos3,
                docs[2],
                &bucket_with_accessor.sub_aggregation,
                force_flush,
            );
            self.increment_bucket(
                bucket_pos4,
                docs[3],
                &bucket_with_accessor.sub_aggregation,
                force_flush,
            );
        }
        for doc in iter.remainder() {
            let val = bucket_with_accessor.accessor.get(*doc);
            let bucket_pos = self.get_bucket_pos(val);
            self.increment_bucket(
                bucket_pos,
                *doc,
                &bucket_with_accessor.sub_aggregation,
                force_flush,
            );
        }
    }

    #[inline]
    fn increment_bucket(
        &mut self,
        bucket_pos: usize,
        doc: DocId,
        bucket_with_accessor: &AggregationsWithAccessor,
        force_flush: bool,
    ) {
        let bucket = &mut self.buckets[bucket_pos];

        match &mut bucket.bucket {
            SegmentBucketEntry::KeyCount(key_count) => {
                key_count.doc_count += 1;
                key_count
                    .sub_aggregation
                    .collect(doc, bucket_with_accessor, force_flush);
            }
        }
    }

    #[inline]
    fn get_bucket_pos(&mut self, val: u64) -> usize {
        self.buckets
            .binary_search_by(|probe| match probe.range.contains(&val) {
                true => Ordering::Equal,
                false => {
                    // range end does not include the value
                    if probe.range.end == val {
                        Ordering::Less
                    } else {
                        probe.range.end.cmp(&val)
                    }
                }
            })
            .expect(&format!("could not find range for value {}", val))
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
fn to_u64_range(range: &Range<f64>, field_type: &Type) -> Range<u64> {
    f64_to_fastfield_u64(range.start, field_type)..f64_to_fastfield_u64(range.end, field_type)
}

/// Extends the provided buckets to contain the whole value range, by inserting buckets at the
/// beginning and end.
fn extend_validate_ranges(
    buckets: &[Range<f64>],
    field_type: &Type,
) -> crate::Result<Vec<Range<u64>>> {
    let mut converted_buckets = buckets
        .iter()
        .map(|range| to_u64_range(range, field_type))
        .collect_vec();

    converted_buckets.sort_by_key(|bucket| bucket.start);
    if buckets[0].start != f64::MIN {
        converted_buckets.insert(
            0,
            u64::MIN..f64_to_fastfield_u64(buckets[0].start, field_type),
        );
    }

    if buckets[buckets.len() - 1].end != f64::MAX {
        converted_buckets
            .push(f64_to_fastfield_u64(buckets[buckets.len() - 1].end, field_type)..u64::MAX);
    }

    // fill up holes in the ranges
    let find_hole = |converted_buckets: &[Range<u64>]| {
        for (pos, ranges) in converted_buckets.windows(2).enumerate() {
            if ranges[0].end > ranges[1].start {
                return Err(TantivyError::InvalidArgument(
                    "Overlapping ranges not supported".to_string(),
                ));
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

pub fn range_to_string(range: &Range<u64>, field_type: &Type) -> String {
    let to_str = |val: u64| {
        if val == u64::MIN || val == u64::MAX {
            "*".to_string()
        } else {
            f64_from_fastfield_u64(val, field_type).to_string()
        }
    };

    format!("{}-{}", to_str(range.start), to_str(range.end))
}

pub fn range_to_key(range: &Range<u64>, field_type: &Type) -> Key {
    Key::Str(range_to_string(range, field_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_test_extend_range_hole() {
        let buckets = vec![(10f64..20f64), (30f64..40f64)];
        let buckets = extend_validate_ranges(&buckets, &Type::U64).unwrap();
        assert_eq!(buckets[0].start, u64::MIN);
        assert_eq!(buckets[0].end, 10);
        assert_eq!(buckets[1].start, 10);
        assert_eq!(buckets[1].end, 20);
        // Added bucket to fill hole
        assert_eq!(buckets[2].start, 20);
        assert_eq!(buckets[2].end, 30);
        assert_eq!(buckets[3].start, 30);
        assert_eq!(buckets[3].end, 40);
    }

    #[test]
    fn bucket_range_test_negative_vals() {
        let buckets = vec![(-10f64..-1f64)];
        let buckets = extend_validate_ranges(&buckets, &Type::F64)
            .unwrap()
            .iter()
            .map(|bucket| range_to_string(bucket, &Type::F64))
            .collect_vec();
        assert_eq!(buckets[0], "*--10");
        assert_eq!(buckets[buckets.len() - 1], "-1-*");
    }
    #[test]
    fn bucket_range_test_positive_vals() {
        let buckets = vec![(0f64..10f64)];
        let buckets = extend_validate_ranges(&buckets, &Type::F64)
            .unwrap()
            .iter()
            .map(|bucket| range_to_string(bucket, &Type::F64))
            .collect_vec();
        assert_eq!(buckets[0], "*-0");
        assert_eq!(buckets[buckets.len() - 1], "10-*");
    }

    #[test]
    fn range_binary_search_test() {
        let ranges = vec![(u64::MIN..10), (10..100), (100..u64::MAX)];

        let search = |val: u64| {
            ranges
                .binary_search_by(|range| match range.contains(&val) {
                    true => Ordering::Equal,
                    false => {
                        if range.end == val {
                            Ordering::Less
                        } else {
                            range.end.cmp(&val)
                        }
                    }
                })
                .unwrap_or_else(|val| val - 1) // U64::MAX case, TODO technically not possible,
                                               // since range doesn't cover end
        };

        assert_eq!(search(u64::MIN), 0);
        assert_eq!(search(9), 0);
        assert_eq!(search(10), 1);
        assert_eq!(search(11), 1);
        assert_eq!(search(99), 1);
        assert_eq!(search(100), 2);
        assert_eq!(search(u64::MAX), 2);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use std::cmp::Ordering;

    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use super::*;

    #[derive(Clone)]
    struct Junk(String, u64, u64, u64, u64, u64, u64, u64);
    fn add_junk() -> Junk {
        Junk("asdf".to_string(), 1, 1, 1, 1, 1, 1, 1)
    }
    fn get_buckets_with_opt(small: bool) -> Vec<(Range<u64>, Junk)> {
        let buckets = if small {
            vec![
                (0f64..300000f64),
                (300000f64..600000f64),
                (600000f64..900000f64),
            ]
        } else {
            vec![
                (0f64..100000f64),
                (100000f64..200000f64),
                (200000f64..300000f64),
                (300000f64..500000f64),
                (500000f64..600000f64),
                (600000f64..700000f64),
                (700000f64..800000f64),
                (800000f64..900000f64),
                (900000f64..1000000f64),
            ]
        };

        let buckets = extend_validate_ranges(&buckets, &Type::U64).unwrap();
        buckets
            .into_iter()
            .map(|bucket| (bucket, add_junk()))
            .collect_vec()
    }

    fn get_buckets() -> Vec<(Range<u64>, Junk)> {
        get_buckets_with_opt(false)
    }

    fn get_rand_docs() -> Vec<u64> {
        let mut rng = thread_rng();

        let all_docs = (0..1_000_000u64).collect_vec();
        let mut vals = all_docs
            .as_slice()
            .choose_multiple(&mut rng, 50000)
            .cloned()
            .collect_vec();
        vals.sort();
        vals
    }

    #[bench]
    fn bench_small_range_contains_linear_search(b: &mut test::Bencher) {
        let buckets = get_buckets();
        let vals = get_rand_docs();
        b.iter(|| {
            let mut bucket = 0u64..10;
            for val in &vals {
                bucket = buckets
                    .iter()
                    .find(|bucket| bucket.0.contains(&val))
                    .map(|el| el.0.clone())
                    .unwrap();
            }
            bucket
        })
    }

    #[bench]
    fn bench_small_range_contains_linear_search_overlapping_buckets(b: &mut test::Bencher) {
        let buckets = get_buckets();
        let vals = get_rand_docs();
        b.iter(|| {
            let mut bucket = 0u64..10;
            for val in &vals {
                for bucket_cand in &buckets {
                    if bucket_cand.0.contains(&val) {
                        bucket = bucket_cand.0.clone();
                    }
                }
            }
            bucket
        })
    }

    #[bench]
    fn bench_small_range_contains_linear_search_only_float(b: &mut test::Bencher) {
        let buckets_orig = get_buckets();
        let buckets = get_buckets().iter().map(|range| range.0.end).collect_vec();
        let vals = get_rand_docs();
        b.iter(|| {
            let mut bucket = buckets_orig[0].0.clone();

            for val in &vals {
                let bucket_pos = buckets.iter().position(|end| end > val).unwrap();
                bucket = buckets_orig[bucket_pos].0.clone();
            }
            bucket
        })
    }

    #[bench]
    fn bench_small_range_contains_binary_search(b: &mut test::Bencher) {
        let buckets = get_buckets();
        let vals = get_rand_docs();
        b.iter(|| {
            let mut bucket = 0u64..10;
            for val in &vals {
                let bucket_pos = buckets
                    .binary_search_by(|probe| match probe.0.contains(&val) {
                        true => Ordering::Equal,
                        false => {
                            if probe.0.end == *val {
                                Ordering::Less
                            } else {
                                probe.0.end.cmp(&val)
                            }
                        }
                    })
                    .unwrap_or_else(|val| val); // U64::MAX case

                bucket = buckets[bucket_pos].0.clone();
            }
            bucket
        })
    }

    #[bench]
    fn bench_small_range_binary_search_only_float(b: &mut test::Bencher) {
        let buckets_orig = get_buckets();
        let buckets = get_buckets().iter().map(|range| range.0.end).collect_vec();
        let vals = get_rand_docs();
        b.iter(|| {
            let mut bucket = buckets_orig[0].0.clone();
            for val in &vals {
                let bucket_pos = buckets.binary_search(&val).unwrap_or_else(|val| val);
                bucket = buckets_orig[bucket_pos].0.clone();
            }
            bucket
        })
    }
}
