//! GPU device abstraction layer.
//!
//! Provides a unified interface over wgpu (Vulkan/Metal/DX12) and a CPU fallback.
//! The [`GpuContext`] is the entry point — it discovers a GPU, creates a device,
//! and manages shader pipeline caches.

/// Core GPU device traits, context, and handle types.
pub mod context;

#[cfg(feature = "wgpu-backend")]
mod wgpu_backend;

mod cpu_fallback;

pub use context::{
    BindGroupEntry, BufferUsage, GpuBackend, GpuBindGroupRaw, GpuBufferRaw, GpuContext, GpuDevice,
    GpuDeviceInfo, GpuPipelineRaw,
};
