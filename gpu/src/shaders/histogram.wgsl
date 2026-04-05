// Histogram bucketing kernel for Tantivy GPU aggregations.
//
// Assigns each value to a bucket and atomically increments bucket counts.
//
// Layout:
//   @group(0) @binding(0) — input f32 values (packed as u32)
//   @group(0) @binding(1) — output bucket counts (array<atomic<u32>>)
//   @group(0) @binding(2) — uniform params (num_elements, num_buckets, interval, offset)

struct HistogramParams {
    num_elements: u32,
    num_buckets: u32,
    interval: f32,
    offset: f32,
}

@group(0) @binding(0) var<storage, read> input_data: array<u32>;
@group(0) @binding(1) var<storage, read_write> bucket_counts: array<atomic<u32>>;
@group(0) @binding(2) var<uniform> params: HistogramParams;

@compute @workgroup_size(256)
fn histogram_bucket(
    @builtin(global_invocation_id) global_id: vec3<u32>,
) {
    let gid = global_id.x;
    if gid >= params.num_elements {
        return;
    }

    let val = bitcast<f32>(input_data[gid]);
    let shifted = val - params.offset;

    // Compute bucket index
    var bucket_idx: u32;
    if shifted < 0.0 {
        bucket_idx = 0u;
    } else {
        bucket_idx = u32(shifted / params.interval);
        if bucket_idx >= params.num_buckets {
            bucket_idx = params.num_buckets - 1u;
        }
    }

    atomicAdd(&bucket_counts[bucket_idx], 1u);
}
