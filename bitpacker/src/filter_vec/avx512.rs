// https://quickwit.io/blog/simd-range
use std::ops::RangeInclusive;
use std::arch::x86_64::_mm512_add_epi32 as op_add;
use std::arch::x86_64::_mm512_cmple_epi32_mask as op_less_or_equal;
use std::arch::x86_64::_mm512_loadu_epi32 as load_unaligned;
use std::arch::x86_64::_mm512_set1_epi32 as set1;
use std::arch::x86_64::_mm512_mask_compressstoreu_epi32 as compress;
use std::arch::x86_64::__m512i;

const NUM_LANES: usize = 16;

pub fn filter_vec_in_place(//input: &[u32],
                           range: RangeInclusive<u32>, offset: u32,
                           output: &mut Vec<u32>) {
    //assert_eq!(output.len() % NUM_LANES, 0); // Not required. // but maybe we need some padding on the output for avx512 to work well?
    // We restrict the accepted boundary, because unsigned integers & SIMD don't
    // play well.
    // TODO.
    let accepted_range = 0u32..(i32::MAX as u32);
    assert!(accepted_range.contains(range.start()), "{:?}", range);
    assert!(accepted_range.contains(range.end()), "{:?}", range);
    //output.clear();
    //output.reserve(input.len());
    let num_words = output.len() / NUM_LANES;
    let mut output_len = unsafe {
        filter_vec_avx512_aux(
            //output.as_ptr() as *const __m512i,
            output.as_ptr(),
            range.clone(),
            output.as_mut_ptr(),
            offset,
            num_words,
        )
    };
    let reminder_start = num_words * NUM_LANES;
    for i in reminder_start..output.len() {
        let val = output[i];
        output[output_len] = offset + i as u32;
        //output[output_len] = i as u32;
        output_len += if range.contains(&val) { 1 } else { 0 };
    }
    output.truncate(output_len);
}

#[target_feature(enable = "avx512f")]
pub unsafe fn filter_vec_avx512_aux(
    mut input: *const u32,
    range: RangeInclusive<u32>,
    output: *mut u32,
    offset: u32,
    num_words: usize,
) -> usize {
    let mut output_end = output;
    let range_simd =
        set1(*range.start() as i32)..=set1(*range.end() as i32);
    let mut ids = from_u32x16([offset + 0, offset + 1, offset + 2, offset + 3, offset + 4, offset + 5, offset + 6, offset + 7,
        offset + 8, offset + 9, offset + 10, offset + 11, offset + 12, offset + 13, offset + 14, offset + 15]);
    const SHIFT: __m512i = from_u32x16([NUM_LANES as u32; NUM_LANES]);
    for _ in 0..num_words {
        let word = load_unaligned(input as *const i32);
        let keeper_bitset = compute_filter_bitset(word, range_simd.clone());
        compress(output_end as *mut u8, keeper_bitset, ids);
        let added_len = keeper_bitset.count_ones();
        output_end = output_end.offset(added_len as isize);
        ids = op_add(ids, SHIFT);
        input = input.offset(16);
    }
    output_end.offset_from(output) as usize
}

#[inline]
unsafe fn compute_filter_bitset(
    val: __m512i,
    range: RangeInclusive<__m512i>) -> u16 {
    let low = op_less_or_equal(*range.start(), val);
    let high = op_less_or_equal(val, *range.end());
    low & high
}

const fn from_u32x16(vals: [u32; NUM_LANES]) -> __m512i {
    union U8x64 {
        vector: __m512i,
        vals: [u32; NUM_LANES],
    }
    unsafe { U8x64 { vals }.vector }
}