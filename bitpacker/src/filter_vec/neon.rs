use std::arch::aarch64::*;
use std::ops::RangeInclusive;

const NUM_LANES: usize = 4;

// Compacts matching lanes to the front using a byte-level shuffle.
// `mask` is a 4-bit value: bit k=1 means lane k should appear in the output.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn compact(data: uint32x4_t, mask: u8) -> uint32x4_t {
    unsafe {
        // SAFETY: mask is always in [0, 15] by construction (max sum of [1,2,4,8]).
        // BYTE_SHUFFLE_TABLE has 16 entries, so this is always in bounds.
        let shuffle = BYTE_SHUFFLE_TABLE.get_unchecked(mask as usize);
        let shuffle_vec = vld1q_u8(shuffle.as_ptr());
        vreinterpretq_u32_u8(vqtbl1q_u8(vreinterpretq_u8_u32(data), shuffle_vec))
    }
}

// Safe (not unsafe) because NEON is mandatory on aarch64: no runtime feature check needed.
#[inline(never)]
pub fn filter_vec_in_place(range: RangeInclusive<u32>, offset: u32, output: &mut Vec<u32>) {
    let num_words = output.len() / NUM_LANES;
    let mut output_len = unsafe {
        filter_vec_neon_aux(
            output.as_ptr(),
            range.clone(),
            output.as_mut_ptr(),
            offset,
            num_words,
        )
    };
    let remainder_start = num_words * NUM_LANES;
    for i in remainder_start..output.len() {
        let val = output[i];
        output[output_len] = offset + i as u32;
        output_len += if range.contains(&val) { 1 } else { 0 };
    }
    output.truncate(output_len);
}

#[target_feature(enable = "neon")]
unsafe fn filter_vec_neon_aux(
    input: *const u32,
    range: RangeInclusive<u32>,
    output: *mut u32,
    offset: u32,
    num_words: usize,
) -> usize {
    unsafe {
        let mut input = input;
        let mut output_tail = output;
        let range_start_simd = vdupq_n_u32(*range.start());
        let range_end_simd = vdupq_n_u32(*range.end());
        let mut ids = vld1q_u32([offset, offset + 1, offset + 2, offset + 3].as_ptr());
        let shift = vdupq_n_u32(NUM_LANES as u32);
        let bit_weights = vld1q_u32([1u32, 2, 4, 8].as_ptr());

        for _ in 0..num_words {
            let word = vld1q_u32(input);

            // Unsigned compares: CMHS (compare higher or same) tests `word >= start`
            // and `end >= word`. ANDing both gives the inside-range mask directly,
            // which is cheaper than computing `outside` and then negating.
            let ge_start = vcgeq_u32(word, range_start_simd);
            let le_end = vcleq_u32(word, range_end_simd);
            // inside[k] = 0xFFFFFFFF if val[k] is in range, 0 otherwise.
            let inside = vandq_u32(ge_start, le_end);

            // Build the 4-bit mask: AND bit_weights with the inside lane mask, so each
            // inside lane contributes its bit_weight (1, 2, 4, or 8). Summing yields the
            // 4-bit mask in one addv.
            let inside_bits = vandq_u32(bit_weights, inside);
            let mask = vaddvq_u32(inside_bits) as u8;
            // mask is mathematically bounded: max value is 1+2+4+8=15 (all lanes match)
            debug_assert!(mask <= 15, "mask must fit in 4 bits: {}", mask);

            // Count of matching lanes = popcount(mask). Derives the count directly from
            // the mask instead of running a parallel SIMD reduction over `outside`.
            let added_len = mask.count_ones() as usize;

            // Safe because mask is guaranteed to be in [0, 15]
            let filtered_ids = compact(ids, mask);
            vst1q_u32(output_tail, filtered_ids);
            output_tail = output_tail.add(added_len);
            ids = vaddq_u32(ids, shift);
            input = input.add(NUM_LANES);
        }

        output_tail.offset_from(output) as usize
    }
}

// Byte shuffle patterns to compact matching lanes to the front of the vector.
// Index is a 4-bit mask: bit k=1 means lane k (bytes 4k..4k+3) is in-range.
// The j-th set bit determines which input lane goes to output position j.
const BYTE_SHUFFLE_TABLE: [[u8; 16]; 16] = [
    [
        16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
    ], // 0b0000: none
    [0, 1, 2, 3, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16], // 0b0001: lane 0
    [4, 5, 6, 7, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16], // 0b0010: lane 1
    [0, 1, 2, 3, 4, 5, 6, 7, 16, 16, 16, 16, 16, 16, 16, 16],     // 0b0011: lanes 0,1
    [8, 9, 10, 11, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16], // 0b0100: lane 2
    [0, 1, 2, 3, 8, 9, 10, 11, 16, 16, 16, 16, 16, 16, 16, 16],   // 0b0101: lanes 0,2
    [4, 5, 6, 7, 8, 9, 10, 11, 16, 16, 16, 16, 16, 16, 16, 16],   // 0b0110: lanes 1,2
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 16, 16, 16, 16],       // 0b0111: lanes 0,1,2
    [
        12, 13, 14, 15, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
    ], // 0b1000: lane 3
    [0, 1, 2, 3, 12, 13, 14, 15, 16, 16, 16, 16, 16, 16, 16, 16], // 0b1001: lanes 0,3
    [4, 5, 6, 7, 12, 13, 14, 15, 16, 16, 16, 16, 16, 16, 16, 16], // 0b1010: lanes 1,3
    [0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 16, 16, 16, 16],     // 0b1011: lanes 0,1,3
    [8, 9, 10, 11, 12, 13, 14, 15, 16, 16, 16, 16, 16, 16, 16, 16], // 0b1100: lanes 2,3
    [0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 16, 16, 16],   // 0b1101: lanes 0,2,3
    [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 16, 16, 16],   // 0b1110: lanes 1,2,3
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],       // 0b1111: all lanes
];
