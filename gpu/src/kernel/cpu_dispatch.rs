//! CPU fallback dispatch for GPU kernels.
//!
//! When no GPU is available, kernels are executed on the CPU with
//! semantically identical behavior. This enables testing and
//! graceful degradation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::device::context::GpuBindGroupRaw;
use crate::error::{GpuError, GpuResult};

/// Dispatch a CPU kernel by entry point name.
///
/// This is called by
/// [`CpuFallbackDevice::dispatch`](crate::device::cpu_fallback::CpuFallbackDevice) and emulates GPU
/// kernel execution on the CPU.
pub fn dispatch_cpu_kernel(
    entry_point: &str,
    bind_groups: &[GpuBindGroupRaw],
    workgroups: (u32, u32, u32),
    buffers: &Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>,
) -> GpuResult<()> {
    match entry_point {
        "stats_reduce" => dispatch_stats_reduce(bind_groups, workgroups, buffers),
        "histogram_bucket" => dispatch_histogram_bucket(bind_groups, workgroups, buffers),
        "bm25_score" => dispatch_bm25_score(bind_groups, workgroups, buffers),
        "compute_distances" => dispatch_compute_distances(bind_groups, workgroups, buffers),
        _ => Err(GpuError::CpuFallback {
            reason: format!("Unknown kernel entry point: {entry_point}"),
        }),
    }
}

// ─── Byte-level parsing helpers (no unwrap) ───

fn parse_err(msg: &str) -> GpuError {
    GpuError::Dispatch(format!("Buffer parse error: {msg}"))
}

fn read_u32_at(data: &[u8], offset: usize) -> GpuResult<u32> {
    let end = offset + 4;
    let bytes: [u8; 4] = data
        .get(offset..end)
        .ok_or_else(|| parse_err(&format!("u32 at offset {offset}, buf len {}", data.len())))?
        .try_into()
        .map_err(|_| parse_err("u32 slice conversion"))?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_f32_at(data: &[u8], offset: usize) -> GpuResult<f32> {
    Ok(f32::from_bits(read_u32_at(data, offset)?))
}

// ─── Buffer access helpers ───

fn read_buf(buffers: &Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>, id: u64) -> GpuResult<Vec<u8>> {
    let map = buffers
        .lock()
        .map_err(|e| GpuError::Dispatch(e.to_string()))?;
    let buf = map
        .get(&id)
        .ok_or_else(|| GpuError::Dispatch(format!("Buffer {id} not found")))?;
    let data = buf.lock().map_err(|e| GpuError::Dispatch(e.to_string()))?;
    Ok(data.clone())
}

fn write_buf(
    buffers: &Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>,
    id: u64,
    data: &[u8],
) -> GpuResult<()> {
    let map = buffers
        .lock()
        .map_err(|e| GpuError::Dispatch(e.to_string()))?;
    let buf = map
        .get(&id)
        .ok_or_else(|| GpuError::Dispatch(format!("Buffer {id} not found")))?;
    let mut lock = buf.lock().map_err(|e| GpuError::Dispatch(e.to_string()))?;
    if data.len() > lock.len() {
        return Err(GpuError::BufferAllocation {
            requested: data.len() as u64,
            limit: lock.len() as u64,
        });
    }
    lock[..data.len()].copy_from_slice(data);
    Ok(())
}

// ─── Kernel implementations ───

/// CPU implementation of stats_reduce kernel.
fn dispatch_stats_reduce(
    bind_groups: &[GpuBindGroupRaw],
    _workgroups: (u32, u32, u32),
    buffers: &Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>,
) -> GpuResult<()> {
    if bind_groups.is_empty() || bind_groups[0].buffer_ids.len() < 3 {
        return Err(GpuError::Dispatch(
            "stats_reduce: expected 3 buffers in bind group 0".to_string(),
        ));
    }

    let bg = &bind_groups[0];
    let input_data = read_buf(buffers, bg.buffer_ids[0])?;
    let params_data = read_buf(buffers, bg.buffer_ids[2])?;

    let num_elements = read_u32_at(&params_data, 0)? as usize;

    let mut count = 0u32;
    let mut sum = 0.0f64;
    let mut compensation = 0.0f64;
    let mut min_val = f64::MAX;
    let mut max_val = f64::MIN;
    let mut sum_sq = 0.0f64;

    for i in 0..num_elements {
        let offset = i * 8; // vec2<u32> = 8 bytes
        if offset + 4 > input_data.len() {
            break;
        }
        let val = read_f32_at(&input_data, offset)? as f64;

        count += 1;
        let y = val - compensation;
        let t = sum + y;
        compensation = (t - sum) - y;
        sum = t;
        min_val = min_val.min(val);
        max_val = max_val.max(val);
        sum_sq += val * val;
    }

    let mut output = Vec::with_capacity(48);
    output.extend_from_slice(&count.to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&sum.to_le_bytes());
    output.extend_from_slice(&min_val.to_le_bytes());
    output.extend_from_slice(&max_val.to_le_bytes());
    output.extend_from_slice(&sum_sq.to_le_bytes());
    output.extend_from_slice(&compensation.to_le_bytes());

    write_buf(buffers, bg.buffer_ids[1], &output)?;
    Ok(())
}

/// CPU implementation of histogram_bucket kernel.
fn dispatch_histogram_bucket(
    bind_groups: &[GpuBindGroupRaw],
    _workgroups: (u32, u32, u32),
    buffers: &Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>,
) -> GpuResult<()> {
    if bind_groups.is_empty() || bind_groups[0].buffer_ids.len() < 3 {
        return Err(GpuError::Dispatch(
            "histogram_bucket: expected 3 buffers in bind group 0".to_string(),
        ));
    }

    let bg = &bind_groups[0];
    let input_data = read_buf(buffers, bg.buffer_ids[0])?;
    let params_data = read_buf(buffers, bg.buffer_ids[2])?;

    let num_elements = read_u32_at(&params_data, 0)? as usize;
    let num_buckets = read_u32_at(&params_data, 4)? as usize;
    let interval = read_f32_at(&params_data, 8)?;
    let offset = read_f32_at(&params_data, 12)?;

    let mut buckets = vec![0u32; num_buckets];

    for i in 0..num_elements {
        let byte_offset = i * 4;
        if byte_offset + 4 > input_data.len() {
            break;
        }
        let val = read_f32_at(&input_data, byte_offset)?;
        let shifted = val - offset;

        let bucket_idx = if shifted < 0.0 {
            0
        } else {
            let idx = (shifted / interval) as usize;
            idx.min(num_buckets - 1)
        };
        buckets[bucket_idx] += 1;
    }

    let output: Vec<u8> = buckets.iter().flat_map(|&c| c.to_le_bytes()).collect();
    write_buf(buffers, bg.buffer_ids[1], &output)?;
    Ok(())
}

/// CPU implementation of bm25_score kernel.
fn dispatch_bm25_score(
    bind_groups: &[GpuBindGroupRaw],
    _workgroups: (u32, u32, u32),
    buffers: &Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>,
) -> GpuResult<()> {
    if bind_groups.len() < 2 {
        return Err(GpuError::Dispatch(
            "bm25_score: expected 2 bind groups".to_string(),
        ));
    }

    let bg0 = &bind_groups[0];
    if bg0.buffer_ids.len() < 4 {
        return Err(GpuError::Dispatch(
            "bm25_score: expected 4 buffers in bind group 0".to_string(),
        ));
    }

    let input_data = read_buf(buffers, bg0.buffer_ids[0])?;
    let params_data = read_buf(buffers, bg0.buffer_ids[2])?;
    let fieldnorm_data = read_buf(buffers, bg0.buffer_ids[3])?;

    let bg1 = &bind_groups[1];
    let dispatch_data = read_buf(buffers, bg1.buffer_ids[0])?;

    let weight = read_f32_at(&params_data, 0)?;
    let k1 = read_f32_at(&params_data, 4)?;
    let b = read_f32_at(&params_data, 8)?;
    let avg_fieldnorm = read_f32_at(&params_data, 12)?;
    let num_docs = read_u32_at(&dispatch_data, 0)? as usize;

    let fieldnorm_table: Vec<u32> = (0..256)
        .map(|i| {
            let off = i * 4;
            if off + 4 <= fieldnorm_data.len() {
                read_u32_at(&fieldnorm_data, off).unwrap_or(0)
            } else {
                0
            }
        })
        .collect();

    let mut output = Vec::with_capacity(num_docs * 8);

    for i in 0..num_docs {
        let off = i * 16;
        if off + 16 > input_data.len() {
            break;
        }

        let doc_id = read_u32_at(&input_data, off)?;
        let term_freq = read_u32_at(&input_data, off + 4)?;
        let fieldnorm_id = read_u32_at(&input_data, off + 8)? as usize;

        let dl = fieldnorm_table[fieldnorm_id.min(255)] as f32;
        let tf = term_freq as f32;
        let norm = k1 * (1.0 - b + b * dl / avg_fieldnorm);
        let tf_factor = tf / (tf + norm);
        let score = weight * tf_factor;

        output.extend_from_slice(&doc_id.to_le_bytes());
        output.extend_from_slice(&score.to_le_bytes());
    }

    write_buf(buffers, bg0.buffer_ids[1], &output)?;
    Ok(())
}

/// CPU implementation of compute_distances kernel.
fn dispatch_compute_distances(
    bind_groups: &[GpuBindGroupRaw],
    _workgroups: (u32, u32, u32),
    buffers: &Mutex<HashMap<u64, Arc<Mutex<Vec<u8>>>>>,
) -> GpuResult<()> {
    if bind_groups.is_empty() || bind_groups[0].buffer_ids.len() < 4 {
        return Err(GpuError::Dispatch(
            "compute_distances: expected 4 buffers in bind group 0".to_string(),
        ));
    }

    let bg = &bind_groups[0];
    let query_data = read_buf(buffers, bg.buffer_ids[0])?;
    let cand_data = read_buf(buffers, bg.buffer_ids[1])?;
    let params_data = read_buf(buffers, bg.buffer_ids[3])?;

    let num_candidates = read_u32_at(&params_data, 0)? as usize;
    let dimensions = read_u32_at(&params_data, 4)? as usize;
    let metric = read_u32_at(&params_data, 8)?;

    let query: Vec<f32> = (0..dimensions)
        .map(|i| read_f32_at(&query_data, i * 4))
        .collect::<GpuResult<_>>()?;

    let mut output = Vec::with_capacity(num_candidates * 4);
    for c in 0..num_candidates {
        let base = c * dimensions;
        let cand: Vec<f32> = (0..dimensions)
            .map(|d| read_f32_at(&cand_data, (base + d) * 4))
            .collect::<GpuResult<_>>()?;

        let distance = match metric {
            0 => query
                .iter()
                .zip(cand.iter())
                .map(|(&q, &c)| (q - c) * (q - c))
                .sum::<f32>(),
            1 => {
                let mut dot = 0.0f32;
                let mut nq = 0.0f32;
                let mut nc = 0.0f32;
                for (&q, &c) in query.iter().zip(cand.iter()) {
                    dot += q * c;
                    nq += q * q;
                    nc += c * c;
                }
                let denom = (nq * nc).sqrt();
                if denom > 0.0 {
                    1.0 - dot / denom
                } else {
                    1.0
                }
            }
            _ => {
                let dot: f32 = query.iter().zip(cand.iter()).map(|(&q, &c)| q * c).sum();
                -dot
            }
        };

        output.extend_from_slice(&distance.to_le_bytes());
    }

    write_buf(buffers, bg.buffer_ids[2], &output)?;
    Ok(())
}
