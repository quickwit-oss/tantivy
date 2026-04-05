//! GPU-accelerated stats aggregation collector.
//!
//! Replaces Tantivy's SIMD-based `IntermediateStats::collect_block_f64` with
//! GPU kernel dispatch when the batch size exceeds the GPU dispatch threshold.
//!
//! ## Integration
//!
//! This collector works at the same level as `SegmentStatsCollector::collect()`:
//! 1. Receives a batch of DocIds from `LowCardCachedSubAggs::flush_local()`
//! 2. Uses `ColumnBlockAccessor::fetch_block_with_missing()` to batch-read column values
//! 3. Instead of `collect_stats()` → CPU SIMD, sends values to GPU stats kernel
//! 4. Merges GPU partial results into `IntermediateStats` on CPU

use crate::buffer::StatsResult;
use crate::collector::GpuAggregationCollector;
use crate::device::GpuContext;
use crate::error::GpuResult;

/// Minimum number of values for GPU dispatch to be worthwhile.
/// Below this threshold, CPU SIMD (AVX2/NEON) is faster than GPU dispatch overhead.
const GPU_STATS_THRESHOLD: usize = 8192;

/// GPU-accelerated stats accumulator that can replace `IntermediateStats`.
///
/// Usage from Tantivy's aggregation pipeline:
/// ```ignore
/// // In SegmentStatsCollector::collect(), instead of:
/// //   collect_stats(stats, vals, is_number_or_date)
/// // Use:
/// //   gpu_stats.collect_f64_values(vals_as_f64_slice)
/// ```
pub struct GpuStatsAccumulator {
    gpu_collector: GpuAggregationCollector,
    /// Fallback CPU accumulator for small batches
    cpu_count: u64,
    cpu_sum: f64,
    cpu_delta: f64,
    cpu_min: f64,
    cpu_max: f64,
}

impl GpuStatsAccumulator {
    /// Create a new GPU stats accumulator.
    pub fn new(ctx: GpuContext) -> GpuResult<Self> {
        let gpu_collector = GpuAggregationCollector::new_stats(ctx)?;
        Ok(Self {
            gpu_collector,
            cpu_count: 0,
            cpu_sum: 0.0,
            cpu_delta: 0.0,
            cpu_min: f64::MAX,
            cpu_max: f64::MIN,
        })
    }

    /// Collect a block of f64 values.
    ///
    /// Routes to GPU or CPU based on batch size.
    #[inline]
    pub fn collect_block_f64(&mut self, values: &[f64]) -> GpuResult<()> {
        if values.len() >= GPU_STATS_THRESHOLD {
            self.gpu_collector.collect_values(values)
        } else {
            // CPU path for small batches (matches IntermediateStats logic)
            for &val in values {
                self.cpu_count += 1;
                let y = val - self.cpu_delta;
                let t = self.cpu_sum + y;
                self.cpu_delta = (t - self.cpu_sum) - y;
                self.cpu_sum = t;
                self.cpu_min = self.cpu_min.min(val);
                self.cpu_max = self.cpu_max.max(val);
            }
            Ok(())
        }
    }

    /// Collect a single value.
    #[inline]
    pub fn collect(&mut self, value: f64) {
        self.cpu_count += 1;
        let y = value - self.cpu_delta;
        let t = self.cpu_sum + y;
        self.cpu_delta = (t - self.cpu_sum) - y;
        self.cpu_sum = t;
        self.cpu_min = self.cpu_min.min(value);
        self.cpu_max = self.cpu_max.max(value);
    }

    /// Flush GPU buffers and return final merged stats.
    pub fn finalize(self) -> GpuResult<StatsResult> {
        let gpu_result = self.gpu_collector.harvest_stats()?;

        // Merge CPU partial results
        let mut merged = StatsResult {
            count: self.cpu_count as u32,
            _pad0: 0,
            sum: self.cpu_sum,
            min: self.cpu_min,
            max: self.cpu_max,
            sum_of_squares: 0.0, // Not tracked in IntermediateStats
            compensation: self.cpu_delta,
        };
        merged.merge(&gpu_result);
        Ok(merged)
    }

    /// Convert to Tantivy-compatible IntermediateStats format.
    ///
    /// Returns (count, sum, delta, min, max) matching `IntermediateStats` fields.
    pub fn to_intermediate_stats(self) -> GpuResult<(u64, f64, f64, f64, f64)> {
        let result = self.finalize()?;
        Ok((
            result.count as u64,
            result.sum,
            result.compensation,
            result.min,
            result.max,
        ))
    }
}

/// Checks whether GPU stats acceleration is beneficial for the given number of values.
pub fn should_use_gpu(num_values: usize) -> bool {
    num_values >= GPU_STATS_THRESHOLD
}
