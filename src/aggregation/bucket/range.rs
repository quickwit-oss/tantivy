use std::iter;
use std::ops::Range;

use crate::aggregation::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor,
};
use crate::aggregation::intermediate_agg_result::IntermediateBucketAggregationResult;
use crate::aggregation::segment_agg_result::{
    SegmentAggregationResults, SegmentBucketDataEntry, SegmentBucketDataEntryKeyCount,
};
use crate::aggregation::{f64_from_fastfield_u64, f64_to_fastfield_u64, Key};
use crate::fastfield::FastFieldReader;
use crate::schema::Type;
use crate::DocId;

#[derive(Clone, Debug, PartialEq)]
pub struct RangeAggregationReq {
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

#[derive(Clone, Debug, PartialEq)]
pub struct SegmentRangeCollector {
    pub buckets: Vec<SegmentRangeBucketEntry>,
    field_type: Type,
}

pub fn range_to_key(range: &Range<u64>, field_type: &Type) -> Key {
    let to_str = |val: u64| {
        if val == u64::MIN || val == u64::MAX {
            "*".to_string()
        } else {
            f64_from_fastfield_u64(val, field_type).to_string()
        }
    };

    Key::Str(format!("{}-{}", to_str(range.start), to_str(range.end)))
}

impl SegmentRangeCollector {
    pub fn into_bucket_agg_result(self) -> IntermediateBucketAggregationResult {
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

        IntermediateBucketAggregationResult { buckets }
    }

    pub fn from_req(
        req: &RangeAggregationReq,
        sub_aggregation: &AggregationsWithAccessor,
        field_type: Type,
    ) -> Self {
        // The range input on the request is f64.
        // We need to convert to u64 ranges, because we read the values as u64.
        // The mapping from the conversion is monotonic so ordering is preserved.
        let buckets = iter::once(u64::MIN..f64_to_fastfield_u64(req.buckets[0].start, &field_type))
            .chain(
                req.buckets
                    .iter()
                    .map(|range| to_u64_range(range, &field_type)),
            )
            .chain(iter::once(
                f64_to_fastfield_u64(req.buckets[req.buckets.len() - 1].end, &field_type)..u64::MAX,
            ))
            .map(|range| SegmentRangeBucketEntry {
                range: range.clone(),
                bucket: SegmentBucketDataEntry::KeyCount(SegmentBucketDataEntryKeyCount {
                    key: range_to_key(&range, &field_type),
                    doc_count: 0,
                    values: None,
                    sub_aggregation: SegmentAggregationResults::from_req(sub_aggregation),
                }),
            })
            .collect();

        SegmentRangeCollector {
            buckets,
            field_type,
        }
    }

    pub(crate) fn collect(
        &mut self,
        doc: DocId,
        bucket_with_accessor: &BucketAggregationWithAccessor,
    ) {
        let val = bucket_with_accessor.accessor.get(doc);
        self.collect_val(val, doc, bucket_with_accessor);
    }

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

/// Converts the user provided f64 range value to fast field value space
///
/// If the fast field has u64 [1,2,5], these values are stored as u64 in the fast field.
/// A f64 user range 1.0..3.0 therefore needs to be converted to 1u64..3u64
///
/// If the fast field has f64 [1.0,2.0,5.0], these values are converted and stored to u64 using a
/// monotonic mapping. A f64 user range 1.0..3.0 needs to be converted using the same monotonic
/// conversion function, so that the user defined ranges contain the values stored in the fast
/// field.
fn to_u64_range(range: &Range<f64>, field_type: &Type) -> Range<u64> {
    f64_to_fastfield_u64(range.start, field_type)..f64_to_fastfield_u64(range.end, &field_type)
}
