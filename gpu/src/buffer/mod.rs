//! GPU buffer management and column data transfer.
//!
//! Handles transferring Tantivy FastField `Column<u64>` data to GPU memory
//! with appropriate format conversion.

pub mod pool;

use std::sync::Arc;

use bytemuck::{Pod, Zeroable};

use crate::device::{BindGroupEntry, BufferUsage, GpuBufferRaw, GpuContext, GpuDevice};
use crate::error::GpuResult;

/// High-level GPU buffer wrapper with typed operations.
///
/// Automatically releases GPU resources when dropped.
pub struct GpuBuffer {
    raw: GpuBufferRaw,
    len: usize,
    element_size: usize,
    /// Shared reference to device for cleanup on drop.
    device: Arc<dyn GpuDevice>,
}

impl Drop for GpuBuffer {
    fn drop(&mut self) {
        self.device.destroy_buffer(self.raw.id);
    }
}

impl GpuBuffer {
    /// Create a new GPU buffer sized for `count` elements of type T.
    pub fn new<T: Pod>(
        ctx: &GpuContext,
        label: &str,
        count: usize,
        usage: BufferUsage,
    ) -> GpuResult<Self> {
        let element_size = std::mem::size_of::<T>();
        let size = (count * element_size) as u64;
        let raw = ctx.device().create_buffer(label, size, usage)?;
        Ok(Self {
            raw,
            len: count,
            element_size,
            device: ctx.device_arc(),
        })
    }

    /// Upload data from a slice to the GPU buffer.
    pub fn upload<T: Pod>(&self, ctx: &GpuContext, data: &[T]) -> GpuResult<()> {
        let bytes = bytemuck::cast_slice(data);
        ctx.device().write_buffer(&self.raw, 0, bytes)
    }

    /// Download data from GPU buffer to a Vec.
    pub fn download<T: Pod + Zeroable>(&self, ctx: &GpuContext) -> GpuResult<Vec<T>> {
        let size = (self.len * self.element_size) as u64;
        let bytes = ctx.device().read_buffer(&self.raw, 0, size)?;
        // Safe: T is Pod (no padding/alignment issues for these numeric types)
        let result: Vec<T> = bytemuck::cast_slice(&bytes).to_vec();
        Ok(result)
    }

    /// Download a single element at the given index.
    pub fn download_one<T: Pod + Zeroable>(&self, ctx: &GpuContext, index: usize) -> GpuResult<T> {
        let offset = (index * self.element_size) as u64;
        let size = self.element_size as u64;
        let bytes = ctx.device().read_buffer(&self.raw, offset, size)?;
        Ok(bytemuck::cast_slice::<u8, T>(&bytes)[0])
    }

    /// Get the raw buffer handle for binding.
    pub fn raw(&self) -> &GpuBufferRaw {
        &self.raw
    }

    /// Number of elements.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Create a bind group entry for this buffer.
    pub fn as_bind_entry(&self, binding: u32) -> BindGroupEntry {
        BindGroupEntry {
            binding,
            buffer: self.raw.clone(),
        }
    }
}

/// Parameters passed as a uniform buffer to GPU kernels.
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct KernelParams {
    /// Number of elements to process
    pub num_elements: u32,
    /// Number of workgroups dispatched
    pub num_workgroups: u32,
    /// Kernel-specific parameter 1 (e.g., histogram bin_width)
    pub param1: f32,
    /// Kernel-specific parameter 2 (e.g., histogram min_value)
    pub param2: f32,
}

/// Result of a stats reduction kernel.
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct StatsResult {
    /// Number of values processed.
    pub count: u32,
    /// Padding for 8-byte alignment.
    pub _pad0: u32,
    /// Sum of all values.
    pub sum: f64,
    /// Minimum value.
    pub min: f64,
    /// Maximum value.
    pub max: f64,
    /// Sum of squared values (for variance).
    pub sum_of_squares: f64,
    /// Kahan summation compensation term
    pub compensation: f64,
}

/// GPU-side stats result using f32 (matches WGSL shader output layout).
///
/// The WGSL shader uses f32 arithmetic and writes results as u32 bitpatterns.
/// This struct reads those back correctly, then converts to f64 StatsResult on CPU.
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct GpuStatsResultF32 {
    /// Number of values processed.
    pub count: u32,
    /// Padding for 8-byte alignment.
    pub _pad0: u32,
    /// f32 sum stored as u32 bits (bitcast in WGSL), then _hi=0 padding
    pub sum_bits: u32,
    /// High 32 bits padding for sum (unused, ensures 8-byte slot).
    pub _sum_hi: u32,
    /// f32 min stored as u32 bits.
    pub min_bits: u32,
    /// High 32 bits padding for min.
    pub _min_hi: u32,
    /// f32 max stored as u32 bits.
    pub max_bits: u32,
    /// High 32 bits padding for max.
    pub _max_hi: u32,
    /// f32 sum-of-squares stored as u32 bits.
    pub sum_sq_bits: u32,
    /// High 32 bits padding for sum-of-squares.
    pub _sum_sq_hi: u32,
    /// f32 compensation stored as u32 bits.
    pub comp_bits: u32,
    /// High 32 bits padding for compensation.
    pub _comp_hi: u32,
}

impl GpuStatsResultF32 {
    /// Convert GPU f32 result to f64 StatsResult for final CPU-side merge.
    pub fn to_stats_result(&self) -> StatsResult {
        StatsResult {
            count: self.count,
            _pad0: 0,
            sum: f32::from_bits(self.sum_bits) as f64,
            min: f32::from_bits(self.min_bits) as f64,
            max: f32::from_bits(self.max_bits) as f64,
            sum_of_squares: f32::from_bits(self.sum_sq_bits) as f64,
            compensation: 0.0,
        }
    }
}

impl StatsResult {
    /// Create an identity (zero) stats result for use as an accumulator.
    pub fn identity() -> Self {
        Self {
            count: 0,
            _pad0: 0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            sum_of_squares: 0.0,
            compensation: 0.0,
        }
    }

    /// Merge another StatsResult into this one.
    pub fn merge(&mut self, other: &StatsResult) {
        self.count += other.count;
        // Kahan summation for merge
        let y = other.sum - self.compensation;
        let t = self.sum + y;
        self.compensation = (t - self.sum) - y;
        self.sum = t;
        self.sum_of_squares += other.sum_of_squares;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }

    /// Average value.
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    /// Variance (population).
    pub fn variance(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            let avg = self.avg();
            self.sum_of_squares / self.count as f64 - avg * avg
        }
    }

    /// Standard deviation (population).
    pub fn std_deviation(&self) -> f64 {
        self.variance().sqrt()
    }
}

/// Result of a BM25 batch scoring kernel.
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Bm25Params {
    /// IDF weight * (1 + K1)
    pub weight: f32,
    /// K1 parameter
    pub k1: f32,
    /// B parameter
    pub b: f32,
    /// Average field norm
    pub avg_fieldnorm: f32,
}

/// Per-document BM25 input (packed for GPU).
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Bm25DocInput {
    /// Document ID
    pub doc_id: u32,
    /// Term frequency in this document
    pub term_freq: u32,
    /// Field norm ID (0-255, indexes into fieldnorm table)
    pub fieldnorm_id: u32,
    /// Padding for 16-byte alignment.
    pub _pad: u32,
}

/// Per-document BM25 output.
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Bm25DocOutput {
    /// Document ID.
    pub doc_id: u32,
    /// Computed BM25 score.
    pub score: f32,
}

/// Transfers a batch of u64 column values to a GPU buffer.
///
/// This is the primary mechanism for moving FastField data to the GPU.
/// The values are already decoded (from bitpacking) by Tantivy's columnar reader.
pub fn upload_column_values(ctx: &GpuContext, label: &str, values: &[u64]) -> GpuResult<GpuBuffer> {
    let buf = GpuBuffer::new::<u64>(ctx, label, values.len(), BufferUsage::STORAGE)?;
    buf.upload(ctx, values)?;
    Ok(buf)
}

/// Transfers a batch of f64 values to a GPU buffer.
pub fn upload_f64_values(ctx: &GpuContext, label: &str, values: &[f64]) -> GpuResult<GpuBuffer> {
    let buf = GpuBuffer::new::<f64>(ctx, label, values.len(), BufferUsage::STORAGE)?;
    buf.upload(ctx, values)?;
    Ok(buf)
}

/// Transfers BM25 document inputs to a GPU buffer.
pub fn upload_bm25_inputs(ctx: &GpuContext, inputs: &[Bm25DocInput]) -> GpuResult<GpuBuffer> {
    let buf =
        GpuBuffer::new::<Bm25DocInput>(ctx, "bm25-input", inputs.len(), BufferUsage::STORAGE)?;
    buf.upload(ctx, inputs)?;
    Ok(buf)
}
