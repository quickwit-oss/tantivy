//! Stats reduction kernel: count, sum, min, max, sum_of_squares.

use crate::buffer::{GpuBuffer, GpuStatsResultF32, KernelParams, StatsResult};
use crate::device::{BufferUsage, GpuBackend, GpuContext, GpuPipelineRaw};
use crate::error::GpuResult;
use crate::kernel::GpuKernel;

const STATS_SHADER: &str = include_str!("../shaders/stats_reduction.wgsl");
const WORKGROUP_SIZE: u32 = 256;

/// GPU kernel for computing stats (count/sum/min/max/sum_sq) over a column of f32 values.
pub struct StatsKernel {
    pipeline: GpuPipelineRaw,
    ctx: GpuContext,
}

impl GpuKernel for StatsKernel {
    type Params = Vec<f32>;
    type Result = StatsResult;

    fn compile(ctx: &GpuContext) -> GpuResult<Self> {
        let pipeline =
            ctx.device()
                .create_pipeline("stats-reduction", STATS_SHADER, "stats_reduce")?;
        Ok(Self {
            pipeline,
            ctx: ctx.clone(),
        })
    }
}

impl StatsKernel {
    /// Execute stats reduction over a slice of f32 values.
    ///
    /// Returns a single `StatsResult` with count, sum, min, max, sum_of_squares.
    /// The final merge uses CPU-side Kahan summation for f64 precision.
    pub fn execute(&self, values: &[f32]) -> GpuResult<StatsResult> {
        if values.is_empty() {
            return Ok(StatsResult::identity());
        }

        let num_elements = values.len() as u32;
        let num_workgroups = num_elements.div_ceil(WORKGROUP_SIZE);

        // Upload input data (f32 values packed as u32 pairs — only .x is used)
        let input_packed: Vec<[u32; 2]> = values.iter().map(|&v| [v.to_bits(), 0]).collect();
        let input_buf = GpuBuffer::new::<[u32; 2]>(
            &self.ctx,
            "stats-input",
            values.len(),
            BufferUsage::STORAGE,
        )?;
        input_buf.upload(&self.ctx, &input_packed)?;

        // Output: one StatsResult per workgroup, initialized to identity values
        // so that unused workgroup slots don't corrupt the final merge
        // (e.g., min=f64::MAX, max=f64::MIN, count=0, sum=0).
        let identity_values = vec![StatsResult::identity(); num_workgroups as usize];
        let output_buf = GpuBuffer::new::<StatsResult>(
            &self.ctx,
            "stats-output",
            num_workgroups as usize,
            BufferUsage::STORAGE_READBACK,
        )?;
        output_buf.upload(&self.ctx, &identity_values)?;

        // Params uniform
        let params = KernelParams {
            num_elements,
            num_workgroups,
            param1: 0.0,
            param2: 0.0,
        };
        let params_buf =
            GpuBuffer::new::<KernelParams>(&self.ctx, "stats-params", 1, BufferUsage::UNIFORM)?;
        params_buf.upload(&self.ctx, &[params])?;

        // Bind group
        let bind_group = self.ctx.device().create_bind_group(
            &self.pipeline,
            0,
            &[
                input_buf.as_bind_entry(0),
                output_buf.as_bind_entry(1),
                params_buf.as_bind_entry(2),
            ],
        )?;

        // Dispatch
        self.ctx
            .device()
            .dispatch(&self.pipeline, &[bind_group], (num_workgroups, 1, 1))?;

        // Read back and merge workgroup results on CPU.
        //
        // CPU fallback writes native f64 StatsResult directly.
        // WGSL GPU kernel writes f32 bitpatterns into u32 slots (the lo half of each
        // f64 field), so we must read back as GpuStatsResultF32 and convert to f64.
        let mut final_result = StatsResult::identity();

        if self.ctx.info().backend == GpuBackend::CpuFallback {
            // CPU fallback: output is native f64 StatsResult
            let wg_results: Vec<StatsResult> = output_buf.download(&self.ctx)?;
            for wg in &wg_results[..num_workgroups as usize] {
                final_result.merge(wg);
            }
        } else {
            // GPU path: output is GpuStatsResultF32 (f32 bits in u32 slots)
            let wg_results: Vec<GpuStatsResultF32> = output_buf.download(&self.ctx)?;
            for wg in &wg_results[..num_workgroups as usize] {
                final_result.merge(&wg.to_stats_result());
            }
        }

        Ok(final_result)
    }

    /// Execute stats reduction over f64 values (converts to f32 for GPU, merges in f64).
    pub fn execute_f64(&self, values: &[f64]) -> GpuResult<StatsResult> {
        let f32_values: Vec<f32> = values.iter().map(|&v| v as f32).collect();
        self.execute(&f32_values)
    }

    /// Execute stats reduction using a buffer pool (zero alloc per dispatch).
    pub fn execute_pooled(
        &self,
        values: &[f32],
        pool: &crate::buffer::pool::BufferPool,
    ) -> GpuResult<StatsResult> {
        if values.is_empty() {
            return Ok(StatsResult::identity());
        }

        let num_elements = values.len() as u32;
        let num_workgroups = num_elements.div_ceil(WORKGROUP_SIZE);

        // Lease buffers from pool
        let input_packed: Vec<[u32; 2]> = values.iter().map(|&v| [v.to_bits(), 0]).collect();
        let input_buf = pool.lease_typed::<[u32; 2]>(values.len(), BufferUsage::STORAGE)?;
        input_buf.upload(&input_packed)?;

        let identity_values = vec![StatsResult::identity(); num_workgroups as usize];
        let output_buf = pool
            .lease_typed::<StatsResult>(num_workgroups as usize, BufferUsage::STORAGE_READBACK)?;
        output_buf.upload(&identity_values)?;

        let params = KernelParams {
            num_elements,
            num_workgroups,
            param1: 0.0,
            param2: 0.0,
        };
        let params_buf = pool.lease_typed::<KernelParams>(1, BufferUsage::UNIFORM)?;
        params_buf.upload(&[params])?;

        let bind_group = self.ctx.device().create_bind_group(
            &self.pipeline,
            0,
            &[
                input_buf.as_bind_entry(0),
                output_buf.as_bind_entry(1),
                params_buf.as_bind_entry(2),
            ],
        )?;

        self.ctx
            .device()
            .dispatch(&self.pipeline, &[bind_group], (num_workgroups, 1, 1))?;

        let mut final_result = StatsResult::identity();
        if self.ctx.info().backend == GpuBackend::CpuFallback {
            let wg_results: Vec<StatsResult> = output_buf.download()?;
            for wg in &wg_results[..num_workgroups as usize] {
                final_result.merge(wg);
            }
        } else {
            let wg_results: Vec<GpuStatsResultF32> = output_buf.download()?;
            for wg in &wg_results[..num_workgroups as usize] {
                final_result.merge(&wg.to_stats_result());
            }
        }

        // Buffers returned to pool on drop
        Ok(final_result)
    }
}
