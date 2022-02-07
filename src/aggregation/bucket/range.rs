use std::iter;
use std::ops::Range;

use crate::aggregation::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor,
};
use crate::aggregation::intermediate_agg_result::IntermediateBucketAggregationResult;
use crate::aggregation::segment_agg_result::{
    SegmentAggregationResults, SegmentBucketDataEntry, SegmentBucketDataEntryKeyCount,
};
use crate::aggregation::Key;
use crate::fastfield::FastFieldReader;
use crate::DocId;

#[derive(Clone, Debug, PartialEq)]
pub struct RangeAggregationReq {
    /// The field to aggregate on.
    pub field_name: String,
    /// Note that this aggregation includes the from value and excludes the to value for each
    /// range. Extra buckets will be created until the first to, and last from.
    pub buckets: Vec<Range<i64>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SegmentRangeBucketEntry {
    key: Key,
    range: Range<i64>,
    bucket: SegmentBucketDataEntry,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SegmentRangeCollector {
    pub buckets: Vec<SegmentRangeBucketEntry>,
}

fn range_to_key(range: &Range<i64>) -> Key {
    let to_str = |val: i64| {
        if val == i64::MIN || val == i64::MAX {
            "*".to_string()
        } else {
            val.to_string()
        }
    };

    Key::Str(format!("{}-{}", to_str(range.start), to_str(range.end)))
}
impl SegmentRangeCollector {
    pub fn into_bucket_agg_result(self) -> IntermediateBucketAggregationResult {
        let buckets = self
            .buckets
            .into_iter()
            .map(|range_bucket| (range_bucket.key, range_bucket.bucket.into()))
            .collect();

        IntermediateBucketAggregationResult { buckets }
    }

    pub fn from_req(req: &RangeAggregationReq, sub_aggregation: &AggregationsWithAccessor) -> Self {
        let buckets = iter::once(i64::MIN..req.buckets[0].start)
            .chain(req.buckets.iter().cloned())
            .chain(iter::once(req.buckets[req.buckets.len() - 1].end..i64::MAX))
            .map(|range| SegmentRangeBucketEntry {
                key: range_to_key(&range),
                range: range.clone(),
                bucket: SegmentBucketDataEntry::KeyCount(SegmentBucketDataEntryKeyCount {
                    key: range_to_key(&range),
                    doc_count: 0,
                    values: None,
                    sub_aggregation: SegmentAggregationResults::from_req(sub_aggregation),
                }),
            })
            .collect();

        SegmentRangeCollector { buckets }
    }

    pub(crate) fn collect(
        &mut self,
        doc: DocId,
        bucket_with_accessor: &BucketAggregationWithAccessor,
    ) {
        let val = bucket_with_accessor.accessor.get(doc);
        self.collect_val(val as i64, doc, bucket_with_accessor);
    }

    fn collect_val(
        &mut self,
        val: i64,
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
