#![warn(missing_docs)]

//! # tantivy-gpu
//!
//! GPU acceleration layer for the Tantivy search engine.
//!
//! This crate provides GPU-accelerated implementations of:
//! - **Aggregation reduction** (stats, histogram, min/max/sum/count)
//! - **BM25 batch scoring** (block-level scoring offloaded to GPU compute shaders)
//! - **Vector similarity search** (planned: HNSW/IVF-PQ on GPU)
//!
//! ## Architecture
//!
//! The GPU layer integrates with Tantivy's existing traits:
//! - [`SegmentCollector::collect_block`] — GPU aggregation receives doc ID blocks
//! - [`Weight::for_each`] — GPU scorer processes 128-doc blocks from `BlockSegmentPostings`
//! - [`Column<u64>`] — FastField columns transfer directly to GPU buffers
//!
//! ## Feature Flags
//!
//! - `wgpu-backend` (default): Uses wgpu for cross-platform GPU compute (Vulkan/Metal/DX12)
//! - `cpu-fallback`: Pure-CPU reference implementation for testing and environments without GPU

pub mod buffer;
pub mod collector;
pub mod device;
/// GPU error types and result aliases.
pub mod error;
pub mod integration;
pub mod kernel;
pub mod scorer;
pub mod vector;

pub use device::{GpuContext, GpuDevice};
pub use error::{GpuError, GpuResult};

/// Re-export key types for convenience.
pub mod prelude {
    pub use crate::buffer::GpuBuffer;
    pub use crate::collector::GpuAggregationCollector;
    pub use crate::device::{GpuContext, GpuDevice};
    pub use crate::error::{GpuError, GpuResult};
    pub use crate::kernel::{
        AggregationKernel, Bm25Kernel, GpuKernel, HistogramKernel, StatsKernel,
    };
    pub use crate::scorer::GpuBm25Weight;
}
