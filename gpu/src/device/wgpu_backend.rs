//! wgpu-based GPU device implementation.
//!
//! Supports Vulkan, Metal, DX12, and GL backends via wgpu.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use wgpu;

use super::context::*;
use crate::error::{GpuError, GpuResult};

static NEXT_ID: AtomicU64 = AtomicU64::new(1_000_000);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// wgpu-backed GPU device.
pub struct WgpuDevice {
    info: GpuDeviceInfo,
    device: wgpu::Device,
    queue: wgpu::Queue,
    buffers: Mutex<HashMap<u64, wgpu::Buffer>>,
    pipelines: Mutex<HashMap<u64, wgpu::ComputePipeline>>,
    bind_group_layouts: Mutex<HashMap<u64, Vec<wgpu::BindGroupLayout>>>,
    bind_groups: Mutex<HashMap<u64, WgpuBindGroupData>>,
}

struct WgpuBindGroupData {
    bind_group: wgpu::BindGroup,
}

impl WgpuDevice {
    /// Create a new wgpu device, selecting the best available adapter.
    pub fn new() -> GpuResult<Self> {
        pollster::block_on(Self::new_async())
    }

    async fn new_async() -> GpuResult<Self> {
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::VULKAN
                | wgpu::Backends::METAL
                | wgpu::Backends::DX12
                | wgpu::Backends::GL,
            ..Default::default()
        });

        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: None,
                force_fallback_adapter: false,
            })
            .await
            .ok_or(GpuError::NoAdapter)?;

        let adapter_info = adapter.get_info();
        let limits = adapter.limits();

        let (device, queue) = adapter
            .request_device(
                &wgpu::DeviceDescriptor {
                    label: Some("tantivy-gpu"),
                    required_features: wgpu::Features::empty(),
                    required_limits: wgpu::Limits {
                        max_storage_buffer_binding_size: limits.max_storage_buffer_binding_size,
                        max_buffer_size: limits.max_buffer_size,
                        max_compute_workgroup_size_x: limits.max_compute_workgroup_size_x,
                        max_compute_workgroups_per_dimension: limits
                            .max_compute_workgroups_per_dimension,
                        ..wgpu::Limits::downlevel_defaults()
                    },
                    memory_hints: wgpu::MemoryHints::Performance,
                },
                None, // Option<&Path>
            )
            .await
            .map_err(|e| GpuError::DeviceRequest(e.to_string()))?;

        let backend = match adapter_info.backend {
            wgpu::Backend::Vulkan => GpuBackend::Vulkan,
            wgpu::Backend::Metal => GpuBackend::Metal,
            wgpu::Backend::Dx12 => GpuBackend::Dx12,
            wgpu::Backend::Gl => GpuBackend::Gl,
            _ => GpuBackend::Vulkan, // default
        };

        let device_limits = device.limits();
        let info = GpuDeviceInfo {
            name: adapter_info.name.clone(),
            backend,
            max_buffer_size: device_limits.max_buffer_size,
            max_workgroup_size_x: device_limits.max_compute_workgroup_size_x,
            max_dispatch_x: device_limits.max_compute_workgroups_per_dimension,
        };

        Ok(Self {
            info,
            device,
            queue,
            buffers: Mutex::new(HashMap::new()),
            pipelines: Mutex::new(HashMap::new()),
            bind_group_layouts: Mutex::new(HashMap::new()),
            bind_groups: Mutex::new(HashMap::new()),
        })
    }
}

impl GpuDevice for WgpuDevice {
    fn info(&self) -> &GpuDeviceInfo {
        &self.info
    }

    fn create_buffer(&self, label: &str, size: u64, usage: BufferUsage) -> GpuResult<GpuBufferRaw> {
        let mut wgpu_usage = wgpu::BufferUsages::empty();
        if usage.storage {
            wgpu_usage |= wgpu::BufferUsages::STORAGE;
        }
        if usage.uniform {
            wgpu_usage |= wgpu::BufferUsages::UNIFORM;
        }
        if usage.copy_src {
            wgpu_usage |= wgpu::BufferUsages::COPY_SRC;
        }
        if usage.copy_dst {
            wgpu_usage |= wgpu::BufferUsages::COPY_DST;
        }
        if usage.map_read {
            wgpu_usage |= wgpu::BufferUsages::MAP_READ;
        }

        if size > self.info.max_buffer_size {
            return Err(GpuError::BufferAllocation {
                requested: size,
                limit: self.info.max_buffer_size,
            });
        }

        let buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some(label),
            size,
            usage: wgpu_usage,
            mapped_at_creation: false,
        });

        let id = next_id();
        self.buffers
            .lock()
            .map_err(|e| GpuError::Dispatch(e.to_string()))?
            .insert(id, buffer);

        Ok(GpuBufferRaw {
            id,
            size,
            cpu_data: None,
        })
    }

    fn write_buffer(&self, buffer: &GpuBufferRaw, offset: u64, data: &[u8]) -> GpuResult<()> {
        let buffers = self
            .buffers
            .lock()
            .map_err(|e| GpuError::Dispatch(e.to_string()))?;
        let wgpu_buf = buffers
            .get(&buffer.id)
            .ok_or_else(|| GpuError::Dispatch(format!("Buffer {} not found", buffer.id)))?;
        self.queue.write_buffer(wgpu_buf, offset, data);
        Ok(())
    }

    fn read_buffer(&self, buffer: &GpuBufferRaw, offset: u64, size: u64) -> GpuResult<Vec<u8>> {
        // Create a staging buffer for readback
        let staging = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("readback-staging"),
            size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let buffers = self
            .buffers
            .lock()
            .map_err(|e| GpuError::Readback(e.to_string()))?;
        let src_buf = buffers
            .get(&buffer.id)
            .ok_or_else(|| GpuError::Readback(format!("Buffer {} not found", buffer.id)))?;

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("readback-encoder"),
            });
        encoder.copy_buffer_to_buffer(src_buf, offset, &staging, 0, size);
        self.queue.submit(std::iter::once(encoder.finish()));

        // Map and read
        let buffer_slice = staging.slice(..);
        let (sender, receiver) = std::sync::mpsc::channel();
        buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
            let _ = sender.send(result);
        });
        self.device.poll(wgpu::Maintain::Wait);

        receiver
            .recv()
            .map_err(|e| GpuError::Readback(e.to_string()))?
            .map_err(|e| GpuError::Readback(e.to_string()))?;

        let data = buffer_slice.get_mapped_range();
        let result = data.to_vec();
        drop(data);
        staging.unmap();

        Ok(result)
    }

    fn create_pipeline(
        &self,
        label: &str,
        shader_source: &str,
        entry_point: &str,
    ) -> GpuResult<GpuPipelineRaw> {
        let shader_module = self
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some(label),
                source: wgpu::ShaderSource::Wgsl(shader_source.into()),
            });

        let pipeline = self
            .device
            .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: Some(label),
                layout: None, // auto-layout
                module: &shader_module,
                entry_point: Some(entry_point),
                compilation_options: Default::default(),
                cache: None,
            });

        let id = next_id();

        // Determine the number of bind groups by counting @group() annotations in shader.
        // wgpu's auto-layout only creates layouts for groups actually referenced.
        let num_groups = count_bind_groups(shader_source);
        let layouts: Vec<wgpu::BindGroupLayout> = (0..num_groups)
            .map(|i| pipeline.get_bind_group_layout(i))
            .collect();

        self.bind_group_layouts
            .lock()
            .map_err(|e| GpuError::ShaderCompilation(e.to_string()))?
            .insert(id, layouts);

        self.pipelines
            .lock()
            .map_err(|e| GpuError::ShaderCompilation(e.to_string()))?
            .insert(id, pipeline);

        Ok(GpuPipelineRaw {
            id,
            entry_point: entry_point.to_string(),
        })
    }

    fn dispatch(
        &self,
        pipeline: &GpuPipelineRaw,
        bind_groups: &[GpuBindGroupRaw],
        workgroups: (u32, u32, u32),
    ) -> GpuResult<()> {
        let pipelines = self
            .pipelines
            .lock()
            .map_err(|e| GpuError::Dispatch(e.to_string()))?;
        let wgpu_pipeline = pipelines
            .get(&pipeline.id)
            .ok_or_else(|| GpuError::Dispatch(format!("Pipeline {} not found", pipeline.id)))?;

        let bgs = self
            .bind_groups
            .lock()
            .map_err(|e| GpuError::Dispatch(e.to_string()))?;

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("compute-encoder"),
            });

        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("compute-pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(wgpu_pipeline);
            for (i, bg) in bind_groups.iter().enumerate() {
                let bg_data = bgs
                    .get(&bg.id)
                    .ok_or_else(|| GpuError::Dispatch(format!("Bind group {} not found", bg.id)))?;
                pass.set_bind_group(i as u32, &bg_data.bind_group, &[]);
            }
            pass.dispatch_workgroups(workgroups.0, workgroups.1, workgroups.2);
        }

        self.queue.submit(std::iter::once(encoder.finish()));
        self.device.poll(wgpu::Maintain::Wait);

        Ok(())
    }

    fn create_bind_group(
        &self,
        pipeline: &GpuPipelineRaw,
        group_index: u32,
        entries: &[BindGroupEntry],
    ) -> GpuResult<GpuBindGroupRaw> {
        let layouts = self
            .bind_group_layouts
            .lock()
            .map_err(|e| GpuError::Dispatch(e.to_string()))?;
        let pipeline_layouts = layouts.get(&pipeline.id).ok_or_else(|| {
            GpuError::Dispatch(format!("Pipeline {} layouts not found", pipeline.id))
        })?;
        let layout = pipeline_layouts.get(group_index as usize).ok_or_else(|| {
            GpuError::Dispatch(format!("Bind group layout {group_index} not found"))
        })?;

        let buffers_map = self
            .buffers
            .lock()
            .map_err(|e| GpuError::Dispatch(e.to_string()))?;

        let wgpu_entries: Vec<wgpu::BindGroupEntry<'_>> = entries
            .iter()
            .map(|e| {
                let buf = buffers_map
                    .get(&e.buffer.id)
                    .expect("Buffer not found in bind group creation — was it already dropped?");
                wgpu::BindGroupEntry {
                    binding: e.binding,
                    resource: buf.as_entire_binding(),
                }
            })
            .collect();

        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("compute-bind-group"),
            layout,
            entries: &wgpu_entries,
        });

        let id = next_id();
        self.bind_groups
            .lock()
            .map_err(|e| GpuError::Dispatch(e.to_string()))?
            .insert(id, WgpuBindGroupData { bind_group });

        Ok(GpuBindGroupRaw {
            id,
            buffer_ids: entries.iter().map(|e| e.buffer.id).collect(),
        })
    }

    fn destroy_buffer(&self, buffer_id: u64) {
        if let Ok(mut map) = self.buffers.lock() {
            if let Some(buf) = map.remove(&buffer_id) {
                buf.destroy();
            }
        }
    }
}

// SAFETY: WgpuDevice is safe to send and share across threads because:
// - wgpu::Device and wgpu::Queue are documented as Send + Sync (see https://docs.rs/wgpu/latest/wgpu/struct.Device.html)
// - All mutable state (buffers, pipelines, bind_groups, bind_group_layouts) is wrapped in Mutex,
//   providing synchronized access
// - WgpuBindGroupData contains wgpu::BindGroup which is Send + Sync in wgpu 24
unsafe impl Send for WgpuDevice {}
unsafe impl Sync for WgpuDevice {}

/// Count the number of distinct @group(N) indices in WGSL shader source.
/// Skips comment lines (// ...) to avoid matching @group in documentation.
fn count_bind_groups(shader_source: &str) -> u32 {
    let mut max_group: Option<u32> = None;
    for line in shader_source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("//") {
            continue;
        }
        if let Some(pos) = trimmed.find("@group(") {
            let rest = &trimmed[pos + 7..];
            if let Some(end) = rest.find(')') {
                if let Ok(g) = rest[..end].trim().parse::<u32>() {
                    max_group = Some(max_group.map_or(g, |m: u32| m.max(g)));
                }
            }
        }
    }
    max_group.map_or(0, |m| m + 1)
}
