//! GPU-accelerated collectors for Tantivy.
//!
//! These collectors implement Tantivy's `SegmentCollector` trait and offload
//! aggregation computation to the GPU via `collect_block`.

use crate::buffer::StatsResult;
use crate::device::GpuContext;
use crate::error::GpuResult;
use crate::kernel::histogram::HistogramParams;
use crate::kernel::{GpuKernel, HistogramKernel, StatsKernel};

/// GPU-accelerated aggregation segment collector.
///
/// Buffers document values on the CPU side, then dispatches GPU kernels
/// in batches for efficient parallel processing.
///
/// Usage flow:
/// 1. Create with a `GpuContext` and column accessor
/// 2. Call `collect_values` with batches of f64 values (from FastField)
/// 3. Call `harvest_stats` / `harvest_histogram` to get results
pub struct GpuAggregationCollector {
    #[allow(dead_code)]
    ctx: GpuContext,
    /// Buffered values waiting for GPU dispatch
    value_buffer: Vec<f64>,
    /// Threshold for auto-flushing to GPU
    flush_threshold: usize,
    /// Accumulated stats result (merged across flushes)
    accumulated_stats: Option<StatsResult>,
    /// Stats kernel (lazily compiled)
    stats_kernel: Option<StatsKernel>,
    /// Histogram kernel (lazily compiled)
    histogram_kernel: Option<HistogramKernel>,
    /// Histogram params (if histogram aggregation is requested)
    histogram_params: Option<HistogramParams>,
    /// Accumulated histogram buckets
    histogram_buckets: Option<Vec<u32>>,
}

/// Minimum number of values before dispatching to GPU.
/// Below this threshold, CPU is faster due to GPU dispatch overhead.
const GPU_DISPATCH_THRESHOLD: usize = 4096;

/// Default flush threshold — flush to GPU every N values.
const DEFAULT_FLUSH_THRESHOLD: usize = 65536;

impl GpuAggregationCollector {
    /// Create a new GPU aggregation collector for stats computation.
    pub fn new_stats(ctx: GpuContext) -> GpuResult<Self> {
        let stats_kernel = StatsKernel::compile(&ctx)?;
        Ok(Self {
            ctx,
            value_buffer: Vec::with_capacity(DEFAULT_FLUSH_THRESHOLD),
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            accumulated_stats: None,
            stats_kernel: Some(stats_kernel),
            histogram_kernel: None,
            histogram_params: None,
            histogram_buckets: None,
        })
    }

    /// Create a new GPU aggregation collector for histogram computation.
    pub fn new_histogram(ctx: GpuContext, params: HistogramParams) -> GpuResult<Self> {
        let histogram_kernel = HistogramKernel::compile(&ctx)?;
        let num_buckets = params.num_buckets as usize;
        Ok(Self {
            ctx,
            value_buffer: Vec::with_capacity(DEFAULT_FLUSH_THRESHOLD),
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            accumulated_stats: None,
            stats_kernel: None,
            histogram_kernel: Some(histogram_kernel),
            histogram_params: Some(params),
            histogram_buckets: Some(vec![0u32; num_buckets]),
        })
    }

    /// Create a collector that computes both stats and histogram.
    pub fn new_combined(ctx: GpuContext, histogram_params: HistogramParams) -> GpuResult<Self> {
        let stats_kernel = StatsKernel::compile(&ctx)?;
        let histogram_kernel = HistogramKernel::compile(&ctx)?;
        let num_buckets = histogram_params.num_buckets as usize;
        Ok(Self {
            ctx,
            value_buffer: Vec::with_capacity(DEFAULT_FLUSH_THRESHOLD),
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            accumulated_stats: None,
            stats_kernel: Some(stats_kernel),
            histogram_kernel: Some(histogram_kernel),
            histogram_params: Some(histogram_params),
            histogram_buckets: Some(vec![0u32; num_buckets]),
        })
    }

    /// Set the flush threshold (number of values before auto-dispatching to GPU).
    pub fn with_flush_threshold(mut self, threshold: usize) -> Self {
        self.flush_threshold = threshold.max(GPU_DISPATCH_THRESHOLD);
        self.value_buffer = Vec::with_capacity(self.flush_threshold);
        self
    }

    /// Push a batch of f64 values for aggregation.
    ///
    /// This is the primary method called from the Tantivy collector loop.
    /// Values are buffered until `flush_threshold` is reached, then
    /// dispatched to GPU.
    pub fn collect_values(&mut self, values: &[f64]) -> GpuResult<()> {
        self.value_buffer.extend_from_slice(values);
        if self.value_buffer.len() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }

    /// Push a single f64 value.
    #[inline]
    pub fn collect_value(&mut self, value: f64) -> GpuResult<()> {
        self.value_buffer.push(value);
        if self.value_buffer.len() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush buffered values to GPU and accumulate results.
    pub fn flush(&mut self) -> GpuResult<()> {
        if self.value_buffer.is_empty() {
            return Ok(());
        }

        let values = std::mem::take(&mut self.value_buffer);

        // Dispatch stats kernel
        if let Some(ref kernel) = self.stats_kernel {
            let result = kernel.execute_f64(&values)?;
            match self.accumulated_stats.as_mut() {
                Some(acc) => acc.merge(&result),
                None => self.accumulated_stats = Some(result),
            }
        }

        // Dispatch histogram kernel
        if let (Some(ref kernel), Some(ref params)) =
            (&self.histogram_kernel, &self.histogram_params)
        {
            let bucket_counts = kernel.execute(&values, params)?;
            if let Some(ref mut acc_buckets) = self.histogram_buckets {
                for (acc, new) in acc_buckets.iter_mut().zip(bucket_counts.iter()) {
                    *acc += *new;
                }
            }
        }

        self.value_buffer = Vec::with_capacity(self.flush_threshold);
        Ok(())
    }

    /// Flush remaining values and return the final stats result.
    pub fn harvest_stats(mut self) -> GpuResult<StatsResult> {
        self.flush()?;
        Ok(self.accumulated_stats.unwrap_or_else(StatsResult::identity))
    }

    /// Flush remaining values and return the final histogram bucket counts.
    pub fn harvest_histogram(mut self) -> GpuResult<Vec<u32>> {
        self.flush()?;
        Ok(self.histogram_buckets.unwrap_or_default())
    }

    /// Flush remaining values and return both stats and histogram.
    pub fn harvest_combined(mut self) -> GpuResult<(StatsResult, Vec<u32>)> {
        self.flush()?;
        Ok((
            self.accumulated_stats.unwrap_or_else(StatsResult::identity),
            self.histogram_buckets.unwrap_or_default(),
        ))
    }
}
