//! GPU compute kernel management.
//!
//! Kernels are compiled from WGSL shaders and dispatched via the [`GpuDevice`] abstraction.
//! Each kernel type (Stats, Histogram, BM25) has a dedicated struct that manages
//! pipeline creation and dispatch with correctly typed parameters.

pub mod bm25;
pub mod cpu_dispatch;
pub mod histogram;
mod stats;

pub use bm25::Bm25Kernel;
pub use histogram::HistogramKernel;
pub use stats::StatsKernel;

use crate::device::GpuContext;
use crate::error::GpuResult;

/// Trait for GPU compute kernels.
pub trait GpuKernel {
    /// The parameter type for this kernel.
    type Params;
    /// The result type returned after execution.
    type Result;

    /// Create and compile this kernel's pipeline.
    fn compile(ctx: &GpuContext) -> GpuResult<Self>
    where Self: Sized;
}

/// Aggregation kernel — umbrella for stats + histogram.
pub struct AggregationKernel {
    /// Stats reduction kernel (min/max/sum/count/variance).
    pub stats: StatsKernel,
    /// Histogram kernel (bucket counting).
    pub histogram: HistogramKernel,
}

impl AggregationKernel {
    /// Compile all aggregation kernels.
    pub fn compile(ctx: &GpuContext) -> GpuResult<Self> {
        Ok(Self {
            stats: StatsKernel::compile(ctx)?,
            histogram: HistogramKernel::compile(ctx)?,
        })
    }
}
