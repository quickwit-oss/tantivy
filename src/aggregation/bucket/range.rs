use std::ops::Range;

use itertools::Itertools;

use crate::aggregation::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor,
};
use crate::aggregation::intermediate_agg_result::IntermediateBucketResult;
use crate::aggregation::segment_agg_result::{
    SegmentAggregationResultsCollector, SegmentBucketDataEntry, SegmentBucketDataEntryKeyCount,
};
use crate::aggregation::{f64_from_fastfield_u64, f64_to_fastfield_u64, Key};
use crate::fastfield::FastFieldReader;
use crate::schema::Type;
use crate::DocId;

#[derive(Clone, Debug, PartialEq)]
/// Provide user-defined buckets to aggregate on.
///
/// Two special buckets will automatically be created to cover the whole range of values.
/// The provided buckets have to be continous.
///
/// During the aggregation, the values extracted from the fast_field `field_name` will be checked
/// against each bucket range. Note that this aggregation includes the from value and excludes the
/// to value for each range.
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
    bucket: SegmentBucketDataEntry,
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
        let field_type = self.field_type.clone();
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
    ) -> Self {
        // The range input on the request is f64.
        // We need to convert to u64 ranges, because we read the values as u64.
        // The mapping from the conversion is monotonic so ordering is preserved.
        let buckets = extend_range(&req.buckets, &field_type)
            .iter()
            .map(|range| SegmentRangeBucketEntry {
                range: range.clone(),
                bucket: SegmentBucketDataEntry::KeyCount(SegmentBucketDataEntryKeyCount {
                    key: range_to_key(&range, &field_type),
                    doc_count: 0,
                    values: None,
                    sub_aggregation: SegmentAggregationResultsCollector::from_req(sub_aggregation),
                }),
            })
            .collect();

        SegmentRangeCollector {
            buckets,
            field_type,
        }
    }

    #[inline]
    pub(crate) fn collect(
        &mut self,
        doc: DocId,
        bucket_with_accessor: &BucketAggregationWithAccessor,
    ) {
        let val = bucket_with_accessor.accessor.get(doc);
        self.collect_val(val, doc, bucket_with_accessor);
    }

    #[inline]
    fn collect_val(
        &mut self,
        val: u64,
        doc: DocId,
        bucket_with_accessor: &BucketAggregationWithAccessor,
    ) {
        let bucket = self
            .buckets
            .iter_mut()
            .find(|bucket| bucket.range.contains(&val))
            .unwrap();
        match &mut bucket.bucket {
            SegmentBucketDataEntry::KeyCount(key_count) => {
                key_count.doc_count += 1;
                key_count
                    .sub_aggregation
                    .collect(doc, &bucket_with_accessor.sub_aggregation);
            }
        }
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
    f64_to_fastfield_u64(range.start, field_type)..f64_to_fastfield_u64(range.end, &field_type)
}

/// Extends the provided buckets to contain the whole value range, by inserting buckets at the
/// beginning and end.
/// TODO: validate that provided buckets are continous or add buckets to ensure continuity.
fn extend_range(buckets: &[Range<f64>], field_type: &Type) -> Vec<Range<u64>> {
    let mut converted_buckets = buckets
        .iter()
        .map(|range| to_u64_range(range, &field_type))
        .collect_vec();

    if buckets[0].start != f64::MIN {
        converted_buckets.insert(
            0,
            u64::MIN..f64_to_fastfield_u64(buckets[0].start, &field_type),
        );
    }

    if buckets[buckets.len() - 1].end != f64::MAX {
        converted_buckets
            .push(f64_to_fastfield_u64(buckets[buckets.len() - 1].end, &field_type)..u64::MAX);
    }

    converted_buckets
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
    fn bucket_range_test_negative_vals() {
        let buckets = vec![(-10f64..-1f64)];
        let buckets = extend_range(&buckets, &Type::F64)
            .iter()
            .map(|bucket| range_to_string(bucket, &Type::F64))
            .collect_vec();
        // TODO add brackets?
        assert_eq!(buckets[0], "*--10");
        assert_eq!(buckets[buckets.len() - 1], "10-*");
    }
    #[test]
    fn bucket_range_test_positive_vals() {
        let buckets = vec![(0f64..10f64)];
        let buckets = extend_range(&buckets, &Type::F64)
            .iter()
            .map(|bucket| range_to_string(bucket, &Type::F64))
            .collect_vec();
        assert_eq!(buckets[0], "*-0");
        assert_eq!(buckets[buckets.len() - 1], "10-*");
    }
}
