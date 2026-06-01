use std::arch::aarch64::*;
use std::ops::RangeInclusive;

// SVE vector length (in u32 lanes) is not a compile-time constant; query at runtime.
// Safe to call only when SVE is confirmed available via is_aarch64_feature_detected!("sve").
#[target_feature(enable = "sve")]
fn num_lanes() -> usize {
    svcntw() as usize
}

pub fn filter_vec_in_place(range: RangeInclusive<u32>, offset: u32, output: &mut Vec<u32>) {
    if range.start() > range.end() {
        output.clear();
        return;
    }
    let vl = unsafe { num_lanes() };
    let num_words = output.len() / vl;
    let range_start = *range.start();
    // Unsigned subtraction trick: val ∈ [lo, hi] ↔ (val - lo) ≤ᵤ (hi - lo).
    // Values below lo wrap around to large u32, so the single unsigned ≤ excludes them.
    let range_width = range.end().wrapping_sub(range_start);
    let mut output_len = unsafe {
        filter_vec_sve_aux(
            output.as_ptr(),
            range_start,
            range_width,
            output.as_mut_ptr(),
            offset,
            num_words,
            vl,
        )
    };
    let remainder_start = num_words * vl;
    for i in remainder_start..output.len() {
        let val = output[i];
        output[output_len] = offset + i as u32;
        output_len += if range.contains(&val) { 1 } else { 0 };
    }
    output.truncate(output_len);
}

#[target_feature(enable = "sve")]
unsafe fn filter_vec_sve_aux(
    input: *const u32,
    range_start: u32,
    range_width: u32,
    output: *mut u32,
    offset: u32,
    num_words: usize,
    vl: usize,
) -> usize {
    unsafe {
        let all_true = svptrue_b32();
        let range_start_simd = svdup_n_u32(range_start);
        let range_width_simd = svdup_n_u32(range_width);
        // ids_a covers [offset .. offset+vl), ids_b covers the next vl ids.
        // Keeping them separate breaks the loop-carried dependency through ids so
        // both compact/cntp chains are fully independent within each unrolled body.
        let mut ids_a = svindex_u32(offset, 1);
        let step = svdup_n_u32(vl as u32);
        let step2 = svdup_n_u32(2 * vl as u32);
        let mut ids_b = svadd_u32_x(all_true, ids_a, step);

        let mut input = input;
        let mut output_tail = output;

        // Unrolled ×2: both cntp calls have independent inputs and execute in parallel.
        // The two output_tail updates are sequential but together cost 4+1+1=6 cy per
        // pair vs 5+5=10 cy for two scalar iterations, breaking the cntp latency chain.
        let num_pairs = num_words / 2;
        for _ in 0..num_pairs {
            let word_a = svld1_u32(all_true, input);
            let word_b = svld1_u32(all_true, input.add(vl));

            let shifted_a = svsub_u32_x(all_true, word_a, range_start_simd);
            let shifted_b = svsub_u32_x(all_true, word_b, range_start_simd);

            let in_range_a = svcmple_u32(all_true, shifted_a, range_width_simd);
            let in_range_b = svcmple_u32(all_true, shifted_b, range_width_simd);

            let compacted_a = svcompact_u32(in_range_a, ids_a);
            let compacted_b = svcompact_u32(in_range_b, ids_b);
            // cntp_a and cntp_b have independent inputs: OOO engine issues them in parallel.
            let added_len_a = svcntp_b32(all_true, in_range_a) as usize;
            let added_len_b = svcntp_b32(all_true, in_range_b) as usize;

            // Write the full vector — only the first added_len slots are valid.
            // Subsequent iterations overwrite the trailing zeros before truncate.
            svst1_u32(all_true, output_tail, compacted_a);
            output_tail = output_tail.add(added_len_a);
            svst1_u32(all_true, output_tail, compacted_b);
            output_tail = output_tail.add(added_len_b);

            ids_a = svadd_u32_x(all_true, ids_a, step2);
            ids_b = svadd_u32_x(all_true, ids_b, step2);
            input = input.add(2 * vl);
        }

        // Handle an odd trailing word.
        if num_words % 2 == 1 {
            let word = svld1_u32(all_true, input);
            let shifted = svsub_u32_x(all_true, word, range_start_simd);
            let in_range = svcmple_u32(all_true, shifted, range_width_simd);
            let added_len = svcntp_b32(all_true, in_range) as usize;
            let compacted_ids = svcompact_u32(in_range, ids_a);
            svst1_u32(all_true, output_tail, compacted_ids);
            output_tail = output_tail.add(added_len);
        }

        output_tail.offset_from(output) as usize
    }
}
