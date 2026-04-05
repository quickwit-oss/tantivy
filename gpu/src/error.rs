/// Errors that can occur in GPU operations.
#[derive(Debug, thiserror::Error)]
pub enum GpuError {
    /// No suitable GPU adapter was found on this system.
    #[error("No suitable GPU adapter found")]
    NoAdapter,

    /// Failed to request a GPU device from the adapter.
    #[error("Failed to request GPU device: {0}")]
    DeviceRequest(String),

    /// GPU buffer allocation exceeded the device limit.
    #[error("GPU buffer allocation failed: requested {requested} bytes, limit {limit} bytes")]
    BufferAllocation {
        /// Bytes requested.
        requested: u64,
        /// Device buffer size limit.
        limit: u64,
    },

    /// Shader (WGSL) compilation failed.
    #[error("Shader compilation failed: {0}")]
    ShaderCompilation(String),

    /// Compute shader dispatch failed.
    #[error("GPU compute dispatch failed: {0}")]
    Dispatch(String),

    /// Reading data back from a GPU buffer failed.
    #[error("Buffer readback failed: {0}")]
    Readback(String),

    /// A GPU operation exceeded the timeout threshold.
    #[error("GPU operation timed out after {0}ms")]
    Timeout(u64),

    /// Column type did not match the expected type.
    #[error("Column type mismatch: expected {expected}, got {actual}")]
    ColumnTypeMismatch {
        /// Expected column type name.
        expected: String,
        /// Actual column type name.
        actual: String,
    },

    /// GPU context has not been initialized yet.
    #[error("GPU context not initialized — call GpuContext::init() first")]
    NotInitialized,

    /// Operation fell back to CPU execution.
    #[error("CPU fallback: {reason}")]
    CpuFallback {
        /// Reason the GPU path was not used.
        reason: String,
    },
}

/// Result type alias for GPU operations.
pub type GpuResult<T> = Result<T, GpuError>;
