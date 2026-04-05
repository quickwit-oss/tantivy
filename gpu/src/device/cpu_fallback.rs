//! CPU fallback implementation of `GpuDevice`.
//!
//! Provides identical semantics to GPU execution but runs on CPU.
//! Used for testing and environments without GPU hardware.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::context::*;
use crate::error::{GpuError, GpuResult};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// CPU-based fallback that emulates GPU buffer and dispatch semantics.
pub struct CpuFallbackDevice {
    info: GpuDeviceInfo,
    buffers: Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>,
}

impl CpuFallbackDevice {
    pub fn new() -> Self {
        Self {
            info: GpuDeviceInfo {
                name: "CPU Fallback".to_string(),
                backend: GpuBackend::CpuFallback,
                max_buffer_size: u64::MAX,
                max_workgroup_size_x: 256,
                max_dispatch_x: u32::MAX,
            },
            buffers: Mutex::new(HashMap::new()),
        }
    }
}

impl GpuDevice for CpuFallbackDevice {
    fn info(&self) -> &GpuDeviceInfo {
        &self.info
    }

    fn create_buffer(
        &self,
        _label: &str,
        size: u64,
        _usage: BufferUsage,
    ) -> GpuResult<GpuBufferRaw> {
        let id = next_id();
        let data = Arc::new(Mutex::new(vec![0u8; size as usize]));
        self.buffers
            .lock()
            .map_err(|e| GpuError::Dispatch(e.to_string()))?
            .insert(id, Arc::clone(&data));
        Ok(GpuBufferRaw {
            id,
            size,
            cpu_data: Some(data),
        })
    }

    fn write_buffer(&self, buffer: &GpuBufferRaw, offset: u64, data: &[u8]) -> GpuResult<()> {
        let cpu_data = buffer
            .cpu_data
            .as_ref()
            .ok_or_else(|| GpuError::Dispatch("No CPU data for buffer".to_string()))?;
        let mut lock = cpu_data
            .lock()
            .map_err(|e| GpuError::Dispatch(e.to_string()))?;
        let start = offset as usize;
        let end = start + data.len();
        if end > lock.len() {
            return Err(GpuError::BufferAllocation {
                requested: end as u64,
                limit: lock.len() as u64,
            });
        }
        lock[start..end].copy_from_slice(data);
        Ok(())
    }

    fn read_buffer(&self, buffer: &GpuBufferRaw, offset: u64, size: u64) -> GpuResult<Vec<u8>> {
        let cpu_data = buffer
            .cpu_data
            .as_ref()
            .ok_or_else(|| GpuError::Readback("No CPU data for buffer".to_string()))?;
        let lock = cpu_data
            .lock()
            .map_err(|e| GpuError::Readback(e.to_string()))?;
        let start = offset as usize;
        let end = start + size as usize;
        if end > lock.len() {
            return Err(GpuError::Readback(format!(
                "Read past end: offset={offset}, size={size}, buffer_len={}",
                lock.len()
            )));
        }
        Ok(lock[start..end].to_vec())
    }

    fn create_pipeline(
        &self,
        _label: &str,
        _shader_source: &str,
        entry_point: &str,
    ) -> GpuResult<GpuPipelineRaw> {
        Ok(GpuPipelineRaw {
            id: next_id(),
            entry_point: entry_point.to_string(),
        })
    }

    fn dispatch(
        &self,
        pipeline: &GpuPipelineRaw,
        bind_groups: &[GpuBindGroupRaw],
        workgroups: (u32, u32, u32),
    ) -> GpuResult<()> {
        // CPU fallback: dispatch to CPU kernel implementations
        crate::kernel::cpu_dispatch::dispatch_cpu_kernel(
            &pipeline.entry_point,
            bind_groups,
            workgroups,
            &self.buffers,
        )
    }

    fn create_bind_group(
        &self,
        _pipeline: &GpuPipelineRaw,
        _group_index: u32,
        entries: &[BindGroupEntry],
    ) -> GpuResult<GpuBindGroupRaw> {
        Ok(GpuBindGroupRaw {
            id: next_id(),
            buffer_ids: entries.iter().map(|e| e.buffer.id).collect(),
        })
    }

    fn destroy_buffer(&self, buffer_id: u64) {
        if let Ok(mut map) = self.buffers.lock() {
            map.remove(&buffer_id);
        }
    }
}
