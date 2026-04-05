use std::sync::Arc;

use crate::error::GpuResult;

/// Information about the discovered GPU device.
#[derive(Debug, Clone)]
pub struct GpuDeviceInfo {
    /// Human-readable device name (e.g. "NVIDIA RTX 4090")
    pub name: String,
    /// Backend API used
    pub backend: GpuBackend,
    /// Maximum buffer size in bytes
    pub max_buffer_size: u64,
    /// Maximum workgroup size (x dimension)
    pub max_workgroup_size_x: u32,
    /// Maximum number of workgroups (x dimension)
    pub max_dispatch_x: u32,
}

/// GPU backend API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuBackend {
    /// Vulkan backend.
    Vulkan,
    /// Metal backend (macOS/iOS).
    Metal,
    /// DirectX 12 backend (Windows).
    Dx12,
    /// DirectX 11 backend (Windows, legacy).
    Dx11,
    /// OpenGL backend.
    Gl,
    /// CPU-only fallback (no actual GPU)
    CpuFallback,
}

impl std::fmt::Display for GpuBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Vulkan => write!(f, "Vulkan"),
            Self::Metal => write!(f, "Metal"),
            Self::Dx12 => write!(f, "DX12"),
            Self::Dx11 => write!(f, "DX11"),
            Self::Gl => write!(f, "OpenGL"),
            Self::CpuFallback => write!(f, "CPU Fallback"),
        }
    }
}

/// Trait for GPU device operations.
///
/// Abstracts over wgpu and CPU-fallback implementations.
pub trait GpuDevice: Send + Sync + 'static {
    /// Device information.
    fn info(&self) -> &GpuDeviceInfo;

    /// Create a GPU buffer of the given size in bytes.
    /// Returns an opaque handle.
    fn create_buffer(&self, label: &str, size: u64, usage: BufferUsage) -> GpuResult<GpuBufferRaw>;

    /// Write data to a buffer at the given offset.
    fn write_buffer(&self, buffer: &GpuBufferRaw, offset: u64, data: &[u8]) -> GpuResult<()>;

    /// Read data back from a GPU buffer (synchronous, blocks until complete).
    fn read_buffer(&self, buffer: &GpuBufferRaw, offset: u64, size: u64) -> GpuResult<Vec<u8>>;

    /// Create a compute pipeline from WGSL shader source.
    fn create_pipeline(
        &self,
        label: &str,
        shader_source: &str,
        entry_point: &str,
    ) -> GpuResult<GpuPipelineRaw>;

    /// Dispatch a compute shader.
    fn dispatch(
        &self,
        pipeline: &GpuPipelineRaw,
        bind_groups: &[GpuBindGroupRaw],
        workgroups: (u32, u32, u32),
    ) -> GpuResult<()>;

    /// Create a bind group from buffer bindings.
    fn create_bind_group(
        &self,
        pipeline: &GpuPipelineRaw,
        group_index: u32,
        entries: &[BindGroupEntry],
    ) -> GpuResult<GpuBindGroupRaw>;

    /// Release a buffer's GPU resources.
    /// Called automatically when `GpuBuffer` is dropped.
    fn destroy_buffer(&self, buffer_id: u64);
}

/// Buffer usage flags.
#[derive(Debug, Clone, Copy)]
pub struct BufferUsage {
    /// Used as a shader storage buffer.
    pub storage: bool,
    /// Used as a uniform buffer.
    pub uniform: bool,
    /// Can be used as a copy source.
    pub copy_src: bool,
    /// Can be used as a copy destination.
    pub copy_dst: bool,
    /// Can be mapped for CPU read-back.
    pub map_read: bool,
}

impl BufferUsage {
    /// GPU storage buffer (read/write from shaders).
    pub const STORAGE: Self = Self {
        storage: true,
        uniform: false,
        copy_src: false,
        copy_dst: true,
        map_read: false,
    };

    /// GPU storage buffer with readback capability.
    pub const STORAGE_READBACK: Self = Self {
        storage: true,
        uniform: false,
        copy_src: true,
        copy_dst: true,
        map_read: false,
    };

    /// Uniform buffer (small, read-only from shaders).
    pub const UNIFORM: Self = Self {
        storage: false,
        uniform: true,
        copy_src: false,
        copy_dst: true,
        map_read: false,
    };

    /// Staging buffer for CPU readback.
    pub const MAP_READ: Self = Self {
        storage: false,
        uniform: false,
        copy_src: false,
        copy_dst: true,
        map_read: true,
    };
}

/// Opaque GPU buffer handle.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GpuBufferRaw {
    /// Backend-specific ID
    pub(crate) id: u64,
    pub(crate) size: u64,
    /// For CPU fallback: actual data
    pub(crate) cpu_data: Option<Arc<std::sync::Mutex<Vec<u8>>>>,
}

/// Opaque compute pipeline handle.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GpuPipelineRaw {
    pub(crate) id: u64,
    /// For CPU fallback: the entry point name to dispatch
    pub(crate) entry_point: String,
}

/// Opaque bind group handle.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GpuBindGroupRaw {
    pub(crate) id: u64,
    /// For CPU fallback: references to bound buffers
    pub(crate) buffer_ids: Vec<u64>,
}

/// A single entry in a bind group.
#[derive(Debug, Clone)]
pub struct BindGroupEntry {
    /// Binding index in the shader.
    pub binding: u32,
    /// Buffer to bind.
    pub buffer: GpuBufferRaw,
}

/// The GPU context — entry point for all GPU operations.
///
/// Manages device lifecycle, pipeline caching, and provides
/// a thread-safe handle for use across Tantivy's parallel segment
/// processing.
pub struct GpuContext {
    device: Arc<dyn GpuDevice>,
}

impl GpuContext {
    /// Initialize a GPU context, discovering the best available device.
    ///
    /// Tries wgpu backends in order (Vulkan > Metal > DX12 > GL),
    /// falls back to CPU if no GPU is available.
    pub fn init() -> GpuResult<Self> {
        #[cfg(feature = "wgpu-backend")]
        {
            match super::wgpu_backend::WgpuDevice::new() {
                Ok(device) => {
                    log::info!(
                        "GPU initialized: {} ({})",
                        device.info().name,
                        device.info().backend
                    );
                    return Ok(Self {
                        device: Arc::new(device),
                    });
                }
                Err(e) => {
                    log::warn!("wgpu initialization failed: {e}, falling back to CPU");
                }
            }
        }

        // CPU fallback
        let fallback = super::cpu_fallback::CpuFallbackDevice::new();
        log::info!("GPU context initialized with CPU fallback");
        Ok(Self {
            device: Arc::new(fallback),
        })
    }

    /// Initialize with explicit CPU fallback (useful for testing).
    pub fn cpu_fallback() -> Self {
        Self {
            device: Arc::new(super::cpu_fallback::CpuFallbackDevice::new()),
        }
    }

    /// Get a reference to the underlying device.
    pub fn device(&self) -> &dyn GpuDevice {
        self.device.as_ref()
    }

    /// Get an Arc to the device for sharing across threads.
    pub fn device_arc(&self) -> Arc<dyn GpuDevice> {
        Arc::clone(&self.device)
    }

    /// Device info.
    pub fn info(&self) -> &GpuDeviceInfo {
        self.device.info()
    }

    /// Returns true if using actual GPU hardware (not CPU fallback).
    pub fn is_hardware_gpu(&self) -> bool {
        self.device.info().backend != GpuBackend::CpuFallback
    }
}

impl Clone for GpuContext {
    fn clone(&self) -> Self {
        Self {
            device: Arc::clone(&self.device),
        }
    }
}

// GpuContext is Send + Sync via Arc<dyn GpuDevice> where GpuDevice: Send + Sync + 'static.
// No manual unsafe impl needed — the compiler auto-derives these from the Arc.
