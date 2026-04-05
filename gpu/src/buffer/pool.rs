//! GPU buffer pool — pre-allocated, reusable buffers.
//!
//! Eliminates per-dispatch buffer allocation overhead (~1ms on RTX 3050) by
//! maintaining a pool of pre-allocated buffers organized by size bucket.
//!
//! ## Usage
//!
//! ```ignore
//! let pool = BufferPool::new(ctx.device_arc());
//! let buf = pool.lease(1024, BufferUsage::STORAGE)?;  // from pool or new
//! buf.upload(data)?;
//! // ... dispatch ...
//! drop(buf);  // returns to pool, NOT destroyed
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytemuck::{Pod, Zeroable};

use crate::device::{BufferUsage, GpuBufferRaw, GpuContext, GpuDevice};
use crate::error::GpuResult;

/// Size buckets for the pool.
/// Buffers are rounded up to the nearest bucket size.
const BUCKET_SIZES: &[u64] = &[
    256,         // 256 B
    1024,        // 1 KB
    4096,        // 4 KB
    16_384,      // 16 KB
    65_536,      // 64 KB
    262_144,     // 256 KB
    1_048_576,   // 1 MB
    4_194_304,   // 4 MB
    16_777_216,  // 16 MB
    67_108_864,  // 64 MB
    268_435_456, // 256 MB
];

/// Find the smallest bucket that fits `size` bytes.
fn bucket_for(size: u64) -> u64 {
    for &bucket in BUCKET_SIZES {
        if bucket >= size {
            return bucket;
        }
    }
    // Larger than any bucket — round up to next power of 2
    size.next_power_of_two()
}

/// A pool of reusable GPU buffers.
///
/// Buffers are organized by (bucket_size, usage_key) where usage_key
/// encodes the BufferUsage flags. When a buffer is returned to the pool,
/// it can be re-leased for a future dispatch without GPU allocation.
pub struct BufferPool {
    device: Arc<dyn GpuDevice>,
    /// Free lists: (bucket_size, usage_key) → Vec<GpuBufferRaw>
    free: Mutex<HashMap<(u64, u8), Vec<GpuBufferRaw>>>,
    /// Total bytes currently in the pool (for monitoring).
    #[allow(dead_code)]
    pool_bytes: Mutex<u64>,
}

/// Encode BufferUsage into a single byte key for pool bucketing.
fn usage_key(usage: &BufferUsage) -> u8 {
    let mut key = 0u8;
    if usage.storage {
        key |= 1;
    }
    if usage.uniform {
        key |= 2;
    }
    if usage.copy_src {
        key |= 4;
    }
    if usage.copy_dst {
        key |= 8;
    }
    if usage.map_read {
        key |= 16;
    }
    key
}

impl BufferPool {
    /// Create a new empty buffer pool.
    pub fn new(device: Arc<dyn GpuDevice>) -> Self {
        Self {
            device,
            free: Mutex::new(HashMap::new()),
            pool_bytes: Mutex::new(0),
        }
    }

    /// Create a buffer pool from a GpuContext.
    pub fn from_ctx(ctx: &GpuContext) -> Self {
        Self::new(ctx.device_arc())
    }

    /// Lease a buffer of at least `size` bytes with the given usage.
    ///
    /// Returns a pooled buffer from the free list if available,
    /// otherwise allocates a new one from the GPU.
    pub fn lease(&self, size: u64, usage: BufferUsage) -> GpuResult<PooledBuffer<'_>> {
        let bucket = bucket_for(size);
        let key = usage_key(&usage);

        // Try to get from free list
        if let Ok(mut free) = self.free.lock() {
            if let Some(list) = free.get_mut(&(bucket, key)) {
                if let Some(raw) = list.pop() {
                    if let Ok(mut pb) = self.pool_bytes.lock() {
                        *pb = pb.saturating_sub(bucket);
                    }
                    return Ok(PooledBuffer {
                        raw,
                        actual_size: size,
                        bucket_size: bucket,
                        usage_key: key,
                        pool: self,
                        device: Arc::clone(&self.device),
                    });
                }
            }
        }

        // Allocate new buffer at bucket size
        let raw = self.device.create_buffer("pooled", bucket, usage)?;

        Ok(PooledBuffer {
            raw,
            actual_size: size,
            bucket_size: bucket,
            usage_key: key,
            pool: self,
            device: Arc::clone(&self.device),
        })
    }

    /// Lease a typed buffer for `count` elements of type T.
    pub fn lease_typed<T: Pod>(
        &self,
        count: usize,
        usage: BufferUsage,
    ) -> GpuResult<PooledBuffer<'_>> {
        let size = (count * std::mem::size_of::<T>()) as u64;
        self.lease(size, usage)
    }

    /// Return a buffer to the pool for reuse.
    fn return_buffer(&self, raw: GpuBufferRaw, bucket_size: u64, ukey: u8) {
        if let Ok(mut free) = self.free.lock() {
            let list = free.entry((bucket_size, ukey)).or_default();
            // Cap free list per bucket to avoid unbounded memory
            if list.len() < 32 {
                if let Ok(mut pb) = self.pool_bytes.lock() {
                    *pb += bucket_size;
                }
                list.push(raw);
                return;
            }
        }
        // Exceeded cap or lock failed — destroy
        self.device.destroy_buffer(raw.id);
    }

    /// Clear all pooled buffers, releasing GPU memory.
    pub fn clear(&self) {
        if let Ok(mut free) = self.free.lock() {
            for (_, list) in free.drain() {
                for raw in list {
                    self.device.destroy_buffer(raw.id);
                }
            }
        }
        if let Ok(mut pb) = self.pool_bytes.lock() {
            *pb = 0;
        }
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        self.clear();
    }
}

/// A buffer leased from a [`BufferPool`].
///
/// Automatically returned to the pool on drop (not destroyed).
pub struct PooledBuffer<'pool> {
    raw: GpuBufferRaw,
    /// Actual data size (may be smaller than bucket_size).
    actual_size: u64,
    bucket_size: u64,
    usage_key: u8,
    pool: &'pool BufferPool,
    device: Arc<dyn GpuDevice>,
}

impl<'pool> PooledBuffer<'pool> {
    /// Upload data to this buffer.
    ///
    /// Data must fit within `actual_size` (not the larger bucket allocation).
    pub fn upload<T: Pod>(&self, data: &[T]) -> GpuResult<()> {
        let bytes = bytemuck::cast_slice(data);
        if bytes.len() as u64 > self.actual_size {
            return Err(crate::error::GpuError::BufferAllocation {
                requested: bytes.len() as u64,
                limit: self.actual_size,
            });
        }
        self.device.write_buffer(&self.raw, 0, bytes)
    }

    /// Download data from this buffer.
    pub fn download<T: Pod + Zeroable>(&self) -> GpuResult<Vec<T>> {
        let bytes = self.device.read_buffer(&self.raw, 0, self.actual_size)?;
        Ok(bytemuck::cast_slice(&bytes).to_vec())
    }

    /// Get the raw buffer handle for bind group creation.
    pub fn raw(&self) -> &GpuBufferRaw {
        &self.raw
    }

    /// Create a bind group entry.
    pub fn as_bind_entry(&self, binding: u32) -> crate::device::BindGroupEntry {
        crate::device::BindGroupEntry {
            binding,
            buffer: self.raw.clone(),
        }
    }
}

impl Drop for PooledBuffer<'_> {
    fn drop(&mut self) {
        // Return to pool instead of destroying.
        // We need to take the raw out — use a sentinel.
        let raw = GpuBufferRaw {
            id: self.raw.id,
            size: self.raw.size,
            cpu_data: self.raw.cpu_data.take(),
        };
        self.pool
            .return_buffer(raw, self.bucket_size, self.usage_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::GpuContext;

    #[test]
    fn test_pool_lease_and_return() {
        let ctx = GpuContext::cpu_fallback();
        let pool = BufferPool::from_ctx(&ctx);

        // Lease a buffer
        let buf = pool.lease(1000, BufferUsage::STORAGE).unwrap();
        let buf_id = buf.raw().id;
        assert!(buf.actual_size == 1000);
        assert!(buf.bucket_size == 1024); // rounded up

        // Drop returns to pool
        drop(buf);

        // Next lease of same size should reuse
        let buf2 = pool.lease(512, BufferUsage::STORAGE).unwrap();
        assert_eq!(buf2.raw().id, buf_id); // same buffer reused
    }

    #[test]
    fn test_pool_different_usage() {
        let ctx = GpuContext::cpu_fallback();
        let pool = BufferPool::from_ctx(&ctx);

        let buf_storage = pool.lease(1000, BufferUsage::STORAGE).unwrap();
        let id_storage = buf_storage.raw().id;
        drop(buf_storage);

        // Different usage — should NOT reuse
        let buf_uniform = pool.lease(1000, BufferUsage::UNIFORM).unwrap();
        assert_ne!(buf_uniform.raw().id, id_storage);
    }

    #[test]
    fn test_pool_upload_download() {
        let ctx = GpuContext::cpu_fallback();
        let pool = BufferPool::from_ctx(&ctx);

        let buf = pool
            .lease_typed::<f32>(100, BufferUsage::STORAGE_READBACK)
            .unwrap();
        let data: Vec<f32> = (0..100).map(|i| i as f32).collect();
        buf.upload(&data).unwrap();
        let downloaded: Vec<f32> = buf.download().unwrap();
        assert_eq!(downloaded, data);
    }

    #[test]
    fn test_pool_clear() {
        let ctx = GpuContext::cpu_fallback();
        let pool = BufferPool::from_ctx(&ctx);

        let buf = pool.lease(4096, BufferUsage::STORAGE).unwrap();
        drop(buf);

        pool.clear();

        // After clear, next lease should create new buffer
        let _buf2 = pool.lease(4096, BufferUsage::STORAGE).unwrap();
    }

    #[test]
    fn test_bucket_sizing() {
        assert_eq!(bucket_for(1), 256);
        assert_eq!(bucket_for(256), 256);
        assert_eq!(bucket_for(257), 1024);
        assert_eq!(bucket_for(1_000_000), 1_048_576);
        assert_eq!(bucket_for(1_048_577), 4_194_304);
    }
}
