use crate::{
    aggregation::{
        segment_agg_result::{BucketDataEntry, BucketDataEntryKeyCount},
        Key,
    },
    fastfield::{DynamicFastFieldReader, FastFieldReader},
    DocId,
};
use std::{iter, ops::Range};

#[derive(Clone, Debug, PartialEq)]
pub struct RangeAggregationReq {
    /// The field to aggregate on.
    pub field_name: String,
    /// Note that this aggregation includes the from value and excludes the to value for each range.
    /// Extra buckets will be created until the first to, and last from.
    pub buckets: Vec<Range<i64>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RangeBucketEntry {
    key: Key,
    range: Range<i64>,
    bucket: BucketDataEntry,
}

impl RangeBucketEntry {
    //fn merge_fruits(&mut self, other: &RangeBucketEntry) {
    //self.bucket.merge_fruits(&other.bucket);
    //}
}

#[derive(Clone, Debug, PartialEq)]
pub struct SegmentRangeCollector {
    pub buckets: Vec<RangeBucketEntry>,
}

fn range_to_key(range: &Range<i64>) -> Key {
    Key::Str(format!("{}-{}", range.start, range.end))
}
impl SegmentRangeCollector {
    //pub fn merge_fruits(&mut self, other: &SegmentRangeCollector) {
    //for (left, right) in self.buckets.iter_mut().zip(other.buckets.iter()) {
    //left.merge_fruits(right);
    //}
    //}

    pub fn from_req(req: &RangeAggregationReq) -> Self {
        let buckets = iter::once(i64::MIN..req.buckets[0].start)
            .chain(req.buckets.iter().cloned())
            .chain(iter::once(req.buckets[req.buckets.len() - 1].end..i64::MAX))
            .map(|range| RangeBucketEntry {
                key: range_to_key(&range),
                range: range.clone(),
                bucket: BucketDataEntry::KeyCount(BucketDataEntryKeyCount {
                    key: range_to_key(&range),
                    doc_count: 0,
                    values: None,
                    //todo
                    sub_aggregation: None,
                }),
            })
            .collect();

        SegmentRangeCollector { buckets }
    }

    pub(crate) fn collect(&mut self, doc: DocId, accessor: &DynamicFastFieldReader<u64>) {
        let val = accessor.get(doc);
        self.collect_val(val as i64);
    }

    fn collect_val(&mut self, val: i64) {
        let bucket = self
            .buckets
            .iter_mut()
            .find(|bucket| bucket.range.contains(&val))
            .unwrap();
        match &mut bucket.bucket {
            BucketDataEntry::KeyCount(key_count) => {
                key_count.doc_count += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_test() {
        let req = RangeAggregationReq {
            field_name: "cool".to_string(),
            buckets: vec![10..20, 20..30],
        };
        let mut collector = SegmentRangeCollector::from_req(&req);
        collector.collect_val(1);
        collector.collect_val(11);
        collector.collect_val(12);
        collector.collect_val(20);
        collector.collect_val(22);
        collector.collect_val(32);
        assert_eq!(collector.buckets[0].bucket.doc_count(), 1);
        assert_eq!(collector.buckets[1].bucket.doc_count(), 2);
        assert_eq!(collector.buckets[2].bucket.doc_count(), 2);
        assert_eq!(collector.buckets[3].bucket.doc_count(), 1);
    }
}
