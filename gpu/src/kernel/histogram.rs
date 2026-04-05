//! Histogram bucketing kernel.

use crate::buffer::GpuBuffer;
use crate::device::{BufferUsage, GpuContext, GpuPipelineRaw};
use crate::error::GpuResult;
use crate::kernel::GpuKernel;

const HISTOGRAM_SHADER: &str = include_str!("../shaders/histogram.wgsl");
const WORKGROUP_SIZE: u32 = 256;

/// Parameters for histogram bucketing.
#[derive(Debug, Clone)]
pub struct HistogramParams {
    /// Number of buckets
    pub num_buckets: u32,
    /// Width of each bucket
    pub interval: f64,
    /// Offset (minimum value / bucket start)
    pub offset: f64,
}

/// GPU kernel for histogram bucketing.
pub struct HistogramKernel {
    pipeline: GpuPipelineRaw,
    ctx: GpuContext,
}

impl GpuKernel for HistogramKernel {
    type Params = HistogramParams;
    type Result = Vec<u32>;

    fn compile(ctx: &GpuContext) -> GpuResult<Self> {
        let pipeline =
            ctx.device()
                .create_pipeline("histogram", HISTOGRAM_SHADER, "histogram_bucket")?;
        Ok(Self {
            pipeline,
            ctx: ctx.clone(),
        })
    }
}

/// Uniform buffer matching HistogramParams in WGSL.
#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
struct GpuHistogramParams {
    num_elements: u32,
    num_buckets: u32,
    interval: f32,
    offset: f32,
}

impl HistogramKernel {
    /// Execute histogram bucketing over a slice of f64 values.
    ///
    /// Returns a Vec<u32> of bucket counts with length `params.num_buckets`.
    pub fn execute(&self, values: &[f64], params: &HistogramParams) -> GpuResult<Vec<u32>> {
        if values.is_empty() {
            return Ok(vec![0u32; params.num_buckets as usize]);
        }

        let num_elements = values.len() as u32;
        let num_workgroups = num_elements.div_ceil(WORKGROUP_SIZE);

        // Upload input as f32 packed into u32
        let input_data: Vec<u32> = values.iter().map(|&v| (v as f32).to_bits()).collect();
        let input_buf =
            GpuBuffer::new::<u32>(&self.ctx, "hist-input", values.len(), BufferUsage::STORAGE)?;
        input_buf.upload(&self.ctx, &input_data)?;

        // Output bucket counts (initialized to 0)
        let output_buf = GpuBuffer::new::<u32>(
            &self.ctx,
            "hist-output",
            params.num_buckets as usize,
            BufferUsage::STORAGE_READBACK,
        )?;

        // Params uniform
        let gpu_params = GpuHistogramParams {
            num_elements,
            num_buckets: params.num_buckets,
            interval: params.interval as f32,
            offset: params.offset as f32,
        };
        let params_buf = GpuBuffer::new::<GpuHistogramParams>(
            &self.ctx,
            "hist-params",
            1,
            BufferUsage::UNIFORM,
        )?;
        params_buf.upload(&self.ctx, &[gpu_params])?;

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

        // Read back
        output_buf.download(&self.ctx)
    }
}
