use std::ops::RangeInclusive;

// SVE vector length (in u32 lanes) is not a compile-time constant; query at runtime.
// Safe to call only when SVE is confirmed available via is_aarch64_feature_detected!("sve").
#[target_feature(enable = "sve")]
unsafe fn num_lanes() -> usize {
    let vl: usize;
    unsafe {
        core::arch::asm!(
            "cntw {vl}",
            vl = out(reg) vl,
            options(nostack, nomem, preserves_flags),
        );
    }
    vl
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

// Register allocation for the asm! blocks:
//   z0        ids_a (index vector for first half of each pair, advances by step2 each iter)
//   z1        range_width broadcast
//   z2        range_start broadcast
//   z3        step2 broadcast (2 * vl)
//   z4        ids_b (index vector for second half, = ids_a + step, advances by step2)
//   z5        scratch: loaded word_a, then compacted_a
//   z6        scratch: loaded word_b, then compacted_b
//   p0        all-true predicate (ptrue p0.s)
//   p1        in-range mask for word_a
//   p2        in-range mask for word_b
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
    let num_pairs = num_words / 2;
    let mut input_ptr = input;
    let mut output_tail = output;

    if num_pairs > 0 {
        unsafe {
            core::arch::asm!(
                // --- Setup ---
                // All-true predicate for 32-bit lanes.
                "ptrue p0.s",
                // ids_a = [offset, offset+1, offset+2, ...]
                "index z0.s, {offset:w}, #1",
                // Broadcast scalars into SVE vectors.
                "mov z1.s, {range_width:w}",
                "mov z2.s, {range_start:w}",
                // vl_gpr = number of 32-bit lanes (cntw).
                "cntw {vl_gpr}",
                // step2_bytes will first hold 2*vl (for the step2 vector), then 2*VL in bytes.
                "lsl {step2_bytes}, {vl_gpr}, #1",
                // z4 = step = [vl, vl, ...]; will become ids_b after the add below.
                "mov z4.s, {vl_gpr:w}",
                // z3 = step2 = [2*vl, 2*vl, ...], used to advance both id vectors each iter.
                "mov z3.s, {step2_bytes:w}",
                // Repurpose step2_bytes to hold the byte stride for advancing the input pointer
                // by two full SVE vectors per iteration.
                "rdvl {step2_bytes}, #2",
                // ids_b = ids_a + step = [offset+vl, offset+vl+1, ...]
                "add z4.s, z0.s, z4.s",

                // --- Main loop: process two SVE vectors (ids_a and ids_b) per iteration ---
                "0:",
                // Load two consecutive SVE vectors from input.
                "ld1w {{z5.s}}, p0/z, [{input}]",
                "ld1w {{z6.s}}, p0/z, [{input}, #1, mul vl]",
                // Advance input pointer by 2 * VL bytes.
                "add {input}, {input}, {step2_bytes}",
                // Unsigned shift: subtract range_start so in-range check becomes a single cmpu ≤.
                "sub z5.s, z5.s, z2.s",
                "sub z6.s, z6.s, z2.s",
                // in_range: shifted value ≤ range_width  (unsigned, so values below lo also fail).
                "cmphs p1.s, p0/z, z1.s, z5.s",
                "cmphs p2.s, p0/z, z1.s, z6.s",
                // Count matching lanes; both cntp calls have independent inputs for OOO parallelism.
                "cntp {cnt_a}, p0, p1.s",
                "compact z5.s, p1, z0.s",
                "compact z6.s, p2, z4.s",
                "cntp {cnt_b}, p0, p2.s",
                // Advance id vectors for the next iteration.
                "add z0.s, z0.s, z3.s",
                "add z4.s, z4.s, z3.s",
                // Store compacted ids. Only the first cnt_a / cnt_b slots are valid; the rest
                // will be overwritten by subsequent iterations before the final truncate.
                "str z5, [{out}]",
                "st1w {{z6.s}}, p0, [{out}, {cnt_a}, lsl #2]",
                "add {out}, {out}, {cnt_a}, lsl #2",
                "add {out}, {out}, {cnt_b}, lsl #2",
                "subs {pairs}, {pairs}, #1",
                "b.ne 0b",

                // --- Operands ---
                input       = inout(reg) input_ptr,
                out         = inout(reg) output_tail,
                pairs       = inout(reg) num_pairs => _,
                offset      = in(reg) offset,
                range_start = in(reg) range_start,
                range_width = in(reg) range_width,
                vl_gpr      = out(reg) _,
                step2_bytes = out(reg) _,
                cnt_a       = out(reg) _,
                cnt_b       = out(reg) _,
                out("p0") _, out("p1") _, out("p2") _,
                out("v0") _, out("v1") _, out("v2") _, out("v3") _,
                out("v4") _, out("v5") _, out("v6") _,
                options(nostack),
            );
        }
    }

    // Handle an odd trailing vector.
    if num_words % 2 == 1 {
        // ids_a for the odd word starts at offset + num_pairs * 2 * vl.
        // input_ptr was advanced by the main loop and now points at the odd word.
        let odd_offset =
            offset.wrapping_add((num_pairs as u32).wrapping_mul(2).wrapping_mul(vl as u32));
        unsafe {
            core::arch::asm!(
                "ptrue p0.s",
                "index z0.s, {odd_offset:w}, #1",
                "mov z1.s, {range_width:w}",
                "mov z2.s, {range_start:w}",
                "ld1w {{z3.s}}, p0/z, [{input}]",
                "sub z3.s, z3.s, z2.s",
                "cmphs p1.s, p0/z, z1.s, z3.s",
                "cntp {cnt}, p0, p1.s",
                "compact z0.s, p1, z0.s",
                "str z0, [{out}]",
                "add {out}, {out}, {cnt}, lsl #2",
                odd_offset  = in(reg) odd_offset,
                range_width = in(reg) range_width,
                range_start = in(reg) range_start,
                input       = in(reg) input_ptr,
                out         = inout(reg) output_tail,
                cnt         = out(reg) _,
                out("p0") _, out("p1") _,
                out("v0") _, out("v1") _, out("v2") _, out("v3") _,
                options(nostack),
            );
        }
    }

    unsafe { output_tail.offset_from(output) as usize }
}
