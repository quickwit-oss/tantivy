use super::segment_agg_result::SegmentAggregationCollector;
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::BucketId;
use crate::DocId;

#[derive(Clone, Debug)]
/// A cache for sub-aggregations, storing doc ids per bucket id.
/// Depending on the cardinality of the parent aggregation, we use different
/// storage strategies.
///
/// ## Low Cardinality
/// Cardinality here refers to the number of unique flattened buckets that can be created
/// by the parent aggregation.
/// Flattened buckets are the result of combining all buckets per collector
/// into a single list of buckets, where each bucket is identified by its BucketId.
///
/// ## Usage
/// Since this is caching for sub-aggregations, it is only used by bucket
/// aggregations.
///
/// TODO: consider using a more advanced data structure for high cardinality
/// aggregations.
/// What this datastructure does in general is to group docs by bucket id.
pub(crate) struct CachedSubAggs<const LOWCARD: bool = false> {
    /// Only used when LOWCARD is true.
    /// Cache doc ids per bucket for sub-aggregations.
    ///
    /// The outer Vec is indexed by BucketId.
    per_bucket_docs: Vec<Vec<DocId>>,
    /// Only used when LOWCARD is false.
    /// For higher cardinalities we use a partitioned approach to store
    ///
    /// partitioned Vec<(BucketId, DocId)> pairs to improve grouping locality.
    partitions: [PartitionEntry; NUM_PARTITIONS],
    pub(crate) sub_agg_collector: Box<dyn SegmentAggregationCollector>,
    num_docs: usize,
}

const FLUSH_THRESHOLD: usize = 1024;
const NUM_PARTITIONS: usize = 16;

impl<const LOWCARD: bool> CachedSubAggs<LOWCARD> {
    pub fn get_sub_agg_collector(&mut self) -> &mut Box<dyn SegmentAggregationCollector> {
        &mut self.sub_agg_collector
    }

    pub fn new(sub_agg: Box<dyn SegmentAggregationCollector>) -> Self {
        Self {
            per_bucket_docs: Vec::new(),
            num_docs: 0,
            sub_agg_collector: sub_agg,
            partitions: core::array::from_fn(|_| PartitionEntry::new()),
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        for v in &mut self.per_bucket_docs {
            v.clear();
        }
        for partition in &mut self.partitions {
            partition.clear();
        }
        self.num_docs = 0;
    }

    #[inline]
    pub fn push(&mut self, bucket_id: BucketId, doc_id: DocId) {
        if LOWCARD {
            let idx = bucket_id as usize;
            if self.per_bucket_docs.len() <= idx {
                self.per_bucket_docs.resize_with(idx + 1, Vec::new);
            }
            self.per_bucket_docs[idx].push(doc_id);
        } else {
            let idx = bucket_id % NUM_PARTITIONS as u32;
            let slot = &mut self.partitions[idx as usize];
            slot.bucket_ids.push(bucket_id);
            slot.docs.push(doc_id);
        }
        self.num_docs += 1;
    }

    #[inline]
    pub fn extend_with_bucket_zero(&mut self, docs: &[DocId]) {
        debug_assert!(
            LOWCARD,
            "extend_with_bucket_zero only valid for single bucket"
        );
        if self.per_bucket_docs.is_empty() {
            self.per_bucket_docs.resize_with(1, Vec::new);
        }
        self.per_bucket_docs[0].extend_from_slice(docs);
        self.num_docs += docs.len();
    }

    /// Check if we need to flush based on the number of documents cached.
    /// If so, flushes the cache to the provided aggregation collector.
    pub fn check_flush_local(
        &mut self,
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        if self.num_docs >= FLUSH_THRESHOLD {
            self.flush_local(agg_data)?;
        }
        Ok(())
    }

    /// Note: this does _not_ flush the sub aggregations
    fn flush_local(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        if LOWCARD {
            // Pre-aggregated: call collect per bucket.
            let max_bucket = (self.per_bucket_docs.len() as BucketId).saturating_sub(1);
            self.sub_agg_collector
                .prepare_max_bucket(max_bucket, agg_data)?;
            for (bucket_id, docs) in self
                .per_bucket_docs
                .iter()
                .enumerate()
                .filter(|(_, docs)| !docs.is_empty())
            {
                self.sub_agg_collector
                    .collect(bucket_id as BucketId, docs, agg_data)?;
            }
        } else {
            let mut max_bucket = 0u32;
            for partition in &self.partitions {
                if let Some(&local_max) = partition.bucket_ids.iter().max() {
                    max_bucket = max_bucket.max(local_max);
                }
            }

            self.sub_agg_collector
                .prepare_max_bucket(max_bucket, agg_data)?;

            for slot in &self.partitions {
                if !slot.bucket_ids.is_empty() {
                    // Reduce dynamic dispatch overhead by collecting a full partition in one call.
                    self.sub_agg_collector.collect_multiple(
                        &slot.bucket_ids,
                        &slot.docs,
                        agg_data,
                    )?;
                }
            }
        }
        self.clear();
        Ok(())
    }

    /// Note: this _does_ flush the sub aggregations
    pub fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        if self.num_docs != 0 {
            self.flush_local(agg_data)?;
        }
        self.sub_agg_collector.flush(agg_data)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct PartitionEntry {
    bucket_ids: Vec<BucketId>,
    docs: Vec<DocId>,
}

impl PartitionEntry {
    #[inline]
    fn new() -> Self {
        Self {
            bucket_ids: Vec::with_capacity(FLUSH_THRESHOLD / NUM_PARTITIONS),
            docs: Vec::with_capacity(FLUSH_THRESHOLD / NUM_PARTITIONS),
        }
    }

    #[inline]
    fn clear(&mut self) {
        self.bucket_ids.clear();
        self.docs.clear();
    }
}
