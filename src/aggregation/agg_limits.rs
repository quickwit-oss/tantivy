use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use common::ByteCount;

use super::collector::DEFAULT_MEMORY_LIMIT;
use super::{AggregationError, DEFAULT_BUCKET_LIMIT};
use crate::TantivyError;

/// An estimate for memory consumption. Non recursive
pub trait MemoryConsumption {
    fn memory_consumption(&self) -> usize;
}

impl<K, V, S> MemoryConsumption for HashMap<K, V, S> {
    fn memory_consumption(&self) -> usize {
        let capacity = self.capacity();
        (std::mem::size_of::<K>() + std::mem::size_of::<V>() + 1) * capacity
    }
}

/// Aggregation memory limit after which the request fails. Defaults to DEFAULT_MEMORY_LIMIT
/// (500MB). The limit is shared by all SegmentCollectors
pub struct AggregationLimits {
    /// The counter which is shared between the aggregations for one request.
    memory_consumption: Arc<AtomicU64>,
    /// The counter that is specific to one segment.
    /// It's used mainly to release memory on the global counter.
    /// Since this is copied to every aggregation in a aggregation tree, this is also
    /// the memory consumption of the aggregation per segment.
    ///
    /// This could be Cell, but it's part of the SegmentCollector, which is Send + Sync.
    segment_memory_consumption: AtomicU64,
    /// The memory_limit in bytes
    memory_limit: ByteCount,
    /// The maximum number of buckets _returned_
    /// This is not counting intermediate buckets.
    bucket_limit: u32,
}
impl Clone for AggregationLimits {
    fn clone(&self) -> Self {
        Self {
            memory_consumption: Arc::clone(&self.memory_consumption),
            segment_memory_consumption: AtomicU64::new(0),
            memory_limit: self.memory_limit,
            bucket_limit: self.bucket_limit,
        }
    }
}

impl Default for AggregationLimits {
    fn default() -> Self {
        Self {
            memory_consumption: Default::default(),
            segment_memory_consumption: Default::default(),
            memory_limit: DEFAULT_MEMORY_LIMIT.into(),
            bucket_limit: DEFAULT_BUCKET_LIMIT,
        }
    }
}

impl AggregationLimits {
    /// *memory_limit*
    /// memory_limit is defined in bytes.
    /// Aggregation fails when the estimated memory consumption of the aggregation is higher than
    /// memory_limit.     
    /// memory_limit will default to `DEFAULT_MEMORY_LIMIT` (500MB)
    ///
    /// *bucket_limit*
    /// Limits the maximum number of buckets returned from an aggregation request.
    /// bucket_limit will default to `DEFAULT_BUCKET_LIMIT` (65000)
    pub fn new(memory_limit: Option<u64>, bucket_limit: Option<u32>) -> Self {
        Self {
            memory_consumption: Default::default(),
            segment_memory_consumption: Default::default(),
            memory_limit: memory_limit.unwrap_or(DEFAULT_MEMORY_LIMIT).into(),
            bucket_limit: bucket_limit.unwrap_or(DEFAULT_BUCKET_LIMIT),
        }
    }

    pub(crate) fn validate_memory_consumption(&self) -> crate::Result<()> {
        if self.get_memory_consumed() > self.memory_limit {
            return Err(TantivyError::AggregationError(
                AggregationError::MemoryExceeded {
                    limit: self.memory_limit,
                    current: self.get_memory_consumed(),
                },
            ));
        }
        Ok(())
    }
    pub(crate) fn add_memory_consumed(&self, num_bytes: u64) {
        self.segment_memory_consumption
            .fetch_add(num_bytes, Ordering::Relaxed);
        self.memory_consumption
            .fetch_add(num_bytes, Ordering::Relaxed);
    }
    /// Returns the estimated memory consumed by the aggregations
    pub fn get_memory_consumed(&self) -> ByteCount {
        self.memory_consumption
            .load(std::sync::atomic::Ordering::Relaxed)
            .into()
    }

    /// Removes the memory consumed tracked by this _instance_ of AggregationLimits.
    /// This is used to clear the segment specific memory consumption all at once.
    pub fn clear_local_memory_consumption(&self) {
        self.memory_consumption.fetch_sub(
            self.segment_memory_consumption.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.segment_memory_consumption.store(0, Ordering::Relaxed);
    }

    /// Returns the non-shared memory counter.
    pub fn get_local_memory_consumption(&self) -> ByteCount {
        self.segment_memory_consumption
            .load(Ordering::Relaxed)
            .into()
    }

    pub(crate) fn get_bucket_limit(&self) -> u32 {
        self.bucket_limit
    }
}
