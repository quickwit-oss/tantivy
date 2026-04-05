//! Distance computation for vector similarity search.
//!
//! Supports L2 (Euclidean), cosine similarity, and dot product metrics.
//! GPU kernel computes distances for batches of candidate vectors against a query vector.

use bytemuck::{Pod, Zeroable};

use crate::buffer::GpuBuffer;
use crate::device::{BufferUsage, GpuContext, GpuPipelineRaw};
use crate::error::GpuResult;
use crate::kernel::GpuKernel;

/// Distance metric for vector similarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// L2 (Euclidean) distance — lower is more similar.
    L2,
    /// Cosine similarity — higher is more similar (converted to distance: 1 - cos).
    Cosine,
    /// Dot product — higher is more similar (converted to distance: -dot).
    DotProduct,
}

/// WGSL shader for batch distance computation.
const DISTANCE_SHADER: &str = r#"
struct DistanceParams {
    num_candidates: u32,
    dimensions: u32,
    metric: u32,  // 0=L2, 1=Cosine, 2=DotProduct
    _pad: u32,
}

@group(0) @binding(0) var<storage, read> query_vec: array<f32>;
@group(0) @binding(1) var<storage, read> candidate_vecs: array<f32>;
@group(0) @binding(2) var<storage, read_write> distances: array<f32>;
@group(0) @binding(3) var<uniform> params: DistanceParams;

@compute @workgroup_size(256)
fn compute_distances(
    @builtin(global_invocation_id) global_id: vec3<u32>,
) {
    let idx = global_id.x;
    if idx >= params.num_candidates {
        return;
    }

    let dim = params.dimensions;
    let base = idx * dim;

    var dot_product: f32 = 0.0;
    var norm_q: f32 = 0.0;
    var norm_c: f32 = 0.0;
    var l2_sum: f32 = 0.0;

    for (var d: u32 = 0u; d < dim; d = d + 1u) {
        let q = query_vec[d];
        let c = candidate_vecs[base + d];
        let diff = q - c;

        dot_product += q * c;
        norm_q += q * q;
        norm_c += c * c;
        l2_sum += diff * diff;
    }

    var distance: f32;
    if params.metric == 0u {
        // L2 distance
        distance = l2_sum;
    } else if params.metric == 1u {
        // Cosine distance: 1 - cos(q, c)
        let denom = sqrt(norm_q * norm_c);
        if denom > 0.0 {
            distance = 1.0 - dot_product / denom;
        } else {
            distance = 1.0;
        }
    } else {
        // Dot product distance: -dot(q, c) (negate so lower = more similar)
        distance = -dot_product;
    }

    distances[idx] = distance;
}
"#;

/// Uniform buffer for distance kernel.
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
struct DistanceParams {
    num_candidates: u32,
    dimensions: u32,
    metric: u32,
    _pad: u32,
}

/// GPU kernel for batch vector distance computation.
pub struct GpuDistanceKernel {
    pipeline: GpuPipelineRaw,
    ctx: GpuContext,
}

impl GpuKernel for GpuDistanceKernel {
    type Params = DistanceMetric;
    type Result = Vec<f32>;

    fn compile(ctx: &GpuContext) -> GpuResult<Self> {
        let pipeline = ctx.device().create_pipeline(
            "vector-distance",
            DISTANCE_SHADER,
            "compute_distances",
        )?;
        Ok(Self {
            pipeline,
            ctx: ctx.clone(),
        })
    }
}

impl GpuDistanceKernel {
    /// Compute distances between a query vector and a batch of candidate vectors.
    ///
    /// # Arguments
    /// - `query`: Query vector (dimension D)
    /// - `candidates`: Flat array of candidate vectors (N * D floats)
    /// - `dimensions`: Vector dimension D
    /// - `metric`: Distance metric to use
    ///
    /// # Returns
    /// Vec of N distances, one per candidate.
    pub fn compute(
        &self,
        query: &[f32],
        candidates: &[f32],
        dimensions: usize,
        metric: DistanceMetric,
    ) -> GpuResult<Vec<f32>> {
        let num_candidates = candidates.len() / dimensions;
        if num_candidates == 0 {
            return Ok(Vec::new());
        }

        // Upload query vector
        let query_buf =
            GpuBuffer::new::<f32>(&self.ctx, "query-vec", query.len(), BufferUsage::STORAGE)?;
        query_buf.upload(&self.ctx, query)?;

        // Upload candidate vectors
        let cand_buf = GpuBuffer::new::<f32>(
            &self.ctx,
            "candidate-vecs",
            candidates.len(),
            BufferUsage::STORAGE,
        )?;
        cand_buf.upload(&self.ctx, candidates)?;

        // Output distances
        let dist_buf = GpuBuffer::new::<f32>(
            &self.ctx,
            "distances",
            num_candidates,
            BufferUsage::STORAGE_READBACK,
        )?;

        // Params
        let params = DistanceParams {
            num_candidates: num_candidates as u32,
            dimensions: dimensions as u32,
            metric: match metric {
                DistanceMetric::L2 => 0,
                DistanceMetric::Cosine => 1,
                DistanceMetric::DotProduct => 2,
            },
            _pad: 0,
        };
        let params_buf =
            GpuBuffer::new::<DistanceParams>(&self.ctx, "dist-params", 1, BufferUsage::UNIFORM)?;
        params_buf.upload(&self.ctx, &[params])?;

        // Bind and dispatch
        let bind_group = self.ctx.device().create_bind_group(
            &self.pipeline,
            0,
            &[
                query_buf.as_bind_entry(0),
                cand_buf.as_bind_entry(1),
                dist_buf.as_bind_entry(2),
                params_buf.as_bind_entry(3),
            ],
        )?;

        let num_workgroups = (num_candidates as u32).div_ceil(256);
        self.ctx
            .device()
            .dispatch(&self.pipeline, &[bind_group], (num_workgroups, 1, 1))?;

        dist_buf.download(&self.ctx)
    }
}

/// CPU fallback for distance computation.
pub fn compute_distance_cpu(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    match metric {
        DistanceMetric::L2 => a
            .iter()
            .zip(b.iter())
            .map(|(&x, &y)| (x - y) * (x - y))
            .sum(),
        DistanceMetric::Cosine => {
            let mut dot = 0.0f32;
            let mut norm_a = 0.0f32;
            let mut norm_b = 0.0f32;
            for (&x, &y) in a.iter().zip(b.iter()) {
                dot += x * y;
                norm_a += x * x;
                norm_b += y * y;
            }
            let denom = (norm_a * norm_b).sqrt();
            if denom > 0.0 {
                1.0 - dot / denom
            } else {
                1.0
            }
        }
        DistanceMetric::DotProduct => {
            let dot: f32 = a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum();
            -dot
        }
    }
}
