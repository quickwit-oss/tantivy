//! Lazily-initialized, thread-safe cache for GPU context and compiled kernels.
//!
//! Avoids the cost of GPU adapter discovery and shader compilation on every search call.
//! The cache is initialized once on first use and shared across all threads.

use std::sync::OnceLock;

use crate::device::GpuContext;
use crate::kernel::GpuKernel;
use crate::vector::distance::GpuDistanceKernel;

/// Cached GPU resources for vector distance computation.
struct GpuDistanceCache {
    ctx: GpuContext,
    kernel: GpuDistanceKernel,
}

static GPU_DISTANCE_CACHE: OnceLock<Option<GpuDistanceCache>> = OnceLock::new();

/// Get (or lazily initialize) the cached GPU context and distance kernel.
///
/// Returns `None` if GPU initialization fails (no adapter, shader error, etc.).
/// Thread-safe: `OnceLock` ensures initialization happens exactly once.
pub fn cached_gpu_distance() -> Option<(&'static GpuContext, &'static GpuDistanceKernel)> {
    let cache = GPU_DISTANCE_CACHE.get_or_init(|| {
        let ctx = GpuContext::init().ok()?;
        let kernel = GpuDistanceKernel::compile(&ctx).ok()?;
        Some(GpuDistanceCache { ctx, kernel })
    });
    cache.as_ref().map(|c| (&c.ctx, &c.kernel))
}

/// Check whether GPU distance computation is available.
pub fn is_gpu_available() -> bool {
    cached_gpu_distance().is_some()
}
