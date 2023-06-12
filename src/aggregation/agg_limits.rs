use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use common::ByteCount;

use super::collector::DEFAULT_MEMORY_LIMIT;
use super::{AggregationError, DEFAULT_BUCKET_LIMIT};

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
            memory_limit: self.memory_limit,
            bucket_limit: self.bucket_limit,
        }
    }
}

impl Default for AggregationLimits {
    fn default() -> Self {
        Self {
            memory_consumption: Default::default(),
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
    ///
    /// Note: The returned instance contains a Arc shared counter to track memory consumption.
    pub fn new(memory_limit: Option<u64>, bucket_limit: Option<u32>) -> Self {
        Self {
            memory_consumption: Default::default(),
            memory_limit: memory_limit.unwrap_or(DEFAULT_MEMORY_LIMIT).into(),
            bucket_limit: bucket_limit.unwrap_or(DEFAULT_BUCKET_LIMIT),
        }
    }

    /// Create a new ResourceLimitGuard, that will release the memory when dropped.
    pub fn new_guard(&self) -> ResourceLimitGuard {
        ResourceLimitGuard {
            /// The counter which is shared between the aggregations for one request.
            memory_consumption: Arc::clone(&self.memory_consumption),
            /// The memory_limit in bytes
            memory_limit: self.memory_limit,
            allocated_with_the_guard: 0,
        }
    }

    pub(crate) fn add_memory_consumed(&self, num_bytes: u64) -> crate::Result<()> {
        self.memory_consumption
            .fetch_add(num_bytes, Ordering::Relaxed);
        validate_memory_consumption(&self.memory_consumption, self.memory_limit)?;
        Ok(())
    }

    pub(crate) fn get_bucket_limit(&self) -> u32 {
        self.bucket_limit
    }
}

fn validate_memory_consumption(
    memory_consumption: &AtomicU64,
    memory_limit: ByteCount,
) -> Result<(), AggregationError> {
    // Load the estimated memory consumed by the aggregations
    let memory_consumed: ByteCount = memory_consumption.load(Ordering::Relaxed).into();
    if memory_consumed > memory_limit {
        return Err(AggregationError::MemoryExceeded {
            limit: memory_limit,
            current: memory_consumed,
        });
    }
    Ok(())
}

pub struct ResourceLimitGuard {
    /// The counter which is shared between the aggregations for one request.
    memory_consumption: Arc<AtomicU64>,
    /// The memory_limit in bytes
    memory_limit: ByteCount,
    /// Allocated memory with this guard.
    allocated_with_the_guard: u64,
}

impl ResourceLimitGuard {
    pub(crate) fn add_memory_consumed(&self, num_bytes: u64) -> crate::Result<()> {
        self.memory_consumption
            .fetch_add(num_bytes, Ordering::Relaxed);
        validate_memory_consumption(&self.memory_consumption, self.memory_limit)?;
        Ok(())
    }
}

impl Drop for ResourceLimitGuard {
    /// Removes the memory consumed tracked by this _instance_ of AggregationLimits.
    /// This is used to clear the segment specific memory consumption all at once.
    fn drop(&mut self) {
        self.memory_consumption
            .fetch_sub(self.allocated_with_the_guard, Ordering::Relaxed);
    }
}
