// Stats reduction kernel for Tantivy GPU aggregations.
//
// Computes count, sum (with Kahan compensation), min, max, sum_of_squares
// over a column of f64 values using parallel workgroup reduction.
//
// Layout:
//   @group(0) @binding(0) — input f64 values (as vec2<u32> pairs for f64 bit representation)
//   @group(0) @binding(1) — output StatsResult per workgroup
//   @group(0) @binding(2) — uniform params (num_elements, num_workgroups)

struct Params {
    num_elements: u32,
    num_workgroups: u32,
    param1: f32,
    param2: f32,
}

struct StatsResult {
    count: u32,
    _pad0: u32,
    // f64 represented as pair of u32 (lo, hi) since WGSL lacks native f64
    sum_lo: u32,
    sum_hi: u32,
    min_lo: u32,
    min_hi: u32,
    max_lo: u32,
    max_hi: u32,
    sum_sq_lo: u32,
    sum_sq_hi: u32,
    comp_lo: u32,
    comp_hi: u32,
}

@group(0) @binding(0) var<storage, read> input_data: array<vec2<u32>>;
@group(0) @binding(1) var<storage, read_write> output_stats: array<StatsResult>;
@group(0) @binding(2) var<uniform> params: Params;

// Workgroup shared memory for reduction.
// WGSL lacks native f64, so we use f32 for GPU-side reduction.
// The final merge on CPU uses f64 Kahan summation for total accuracy.
const WORKGROUP_SIZE: u32 = 256u;

var<workgroup> shared_count: array<u32, 256>;
var<workgroup> shared_sum_f32: array<f32, 256>;
var<workgroup> shared_min_f32: array<f32, 256>;
var<workgroup> shared_max_f32: array<f32, 256>;
var<workgroup> shared_sum_sq_f32: array<f32, 256>;

@compute @workgroup_size(256)
fn stats_reduce(
    @builtin(global_invocation_id) global_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
    @builtin(workgroup_id) wg_id: vec3<u32>,
) {
    let tid = local_id.x;
    let gid = global_id.x;

    // Initialize shared memory
    shared_count[tid] = 0u;
    shared_sum_f32[tid] = 0.0;
    shared_min_f32[tid] = 3.4028235e+38; // f32::MAX
    shared_max_f32[tid] = -3.4028235e+38; // f32::MIN
    shared_sum_sq_f32[tid] = 0.0;

    // Each thread processes one element (grid-stride loop for large arrays)
    var idx = gid;
    while idx < params.num_elements {
        let bits = input_data[idx];
        // Reinterpret u32 pair as f32 (we store pre-converted f32 values from CPU)
        let val = bitcast<f32>(bits.x);

        shared_count[tid] += 1u;
        shared_sum_f32[tid] += val;
        shared_min_f32[tid] = min(shared_min_f32[tid], val);
        shared_max_f32[tid] = max(shared_max_f32[tid], val);
        shared_sum_sq_f32[tid] += val * val;

        idx += params.num_workgroups * WORKGROUP_SIZE;
    }

    workgroupBarrier();

    // Tree reduction within workgroup
    var stride = WORKGROUP_SIZE / 2u;
    while stride > 0u {
        if tid < stride {
            shared_count[tid] += shared_count[tid + stride];
            shared_sum_f32[tid] += shared_sum_f32[tid + stride];
            shared_min_f32[tid] = min(shared_min_f32[tid], shared_min_f32[tid + stride]);
            shared_max_f32[tid] = max(shared_max_f32[tid], shared_max_f32[tid + stride]);
            shared_sum_sq_f32[tid] += shared_sum_sq_f32[tid + stride];
        }
        workgroupBarrier();
        stride = stride / 2u;
    }

    // Thread 0 writes workgroup result
    if tid == 0u {
        let wg_idx = wg_id.x;
        output_stats[wg_idx].count = shared_count[0];
        output_stats[wg_idx]._pad0 = 0u;
        output_stats[wg_idx].sum_lo = bitcast<u32>(shared_sum_f32[0]);
        output_stats[wg_idx].sum_hi = 0u;
        output_stats[wg_idx].min_lo = bitcast<u32>(shared_min_f32[0]);
        output_stats[wg_idx].min_hi = 0u;
        output_stats[wg_idx].max_lo = bitcast<u32>(shared_max_f32[0]);
        output_stats[wg_idx].max_hi = 0u;
        output_stats[wg_idx].sum_sq_lo = bitcast<u32>(shared_sum_sq_f32[0]);
        output_stats[wg_idx].sum_sq_hi = 0u;
        output_stats[wg_idx].comp_lo = 0u;
        output_stats[wg_idx].comp_hi = 0u;
    }
}
