use std::arch::x86_64::*;

use super::Lane;

/// AVX2 unpack for bit widths 1..=25
///
/// 8 consecutive values occupy exactly `w` bytes and always start
/// byte-aligned, so the (byte offset, bit shift) pattern of the 8 values is a
/// constant per width. `vpshufb` shuffles within each 128-bit half
/// independently, so one 256-bit register does both halves of a group at once:
/// the low half gathers values 0..4 from `ptr`, the high half gathers values
/// 4..8 from `ptr + hi_base`. Then `vpsrlvd` applies the per-lane shift and
/// `vpand` the width mask.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn decode_block_avx2<L: Lane>(w: usize, data: &[u8], out: &mut [L]) {
    debug_assert!((1..=25).contains(&w));
    debug_assert!(out.len() % 8 == 0);
    debug_assert!(data.len() >= out.len() / 8 * w + 16);
    let hi_base = (4 * w) / 8;
    let mut idx = [0i8; 32];
    let mut sh = [0i32; 8];
    for j in 0..4 {
        let bit_lo = j * w;
        let bit_hi = (4 + j) * w;
        for k in 0..4 {
            idx[j * 4 + k] = (bit_lo / 8 + k) as i8;
            idx[16 + j * 4 + k] = (bit_hi / 8 - hi_base + k) as i8;
        }
        sh[j] = (bit_lo % 8) as i32;
        sh[4 + j] = (bit_hi % 8) as i32;
    }
    unsafe {
        let idx_v = _mm256_loadu_si256(idx.as_ptr() as *const __m256i);
        let sh_v = _mm256_loadu_si256(sh.as_ptr() as *const __m256i);
        let mask = _mm256_set1_epi32(((1u64 << w) - 1) as i32);
        let num_groups = out.len() / 8;
        let mut ptr = data.as_ptr();
        let mut out_ptr = out.as_mut_ptr();
        for _ in 0..num_groups {
            let bytes_lo = _mm_loadu_si128(ptr as *const __m128i);
            let bytes_hi = _mm_loadu_si128(ptr.add(hi_base) as *const __m128i);
            let bytes = _mm256_set_m128i(bytes_hi, bytes_lo);
            let vals = _mm256_and_si256(
                _mm256_srlv_epi32(_mm256_shuffle_epi8(bytes, idx_v), sh_v),
                mask,
            );
            if L::IS_U64 {
                let dst = out_ptr as *mut __m256i;
                _mm256_storeu_si256(dst, _mm256_cvtepu32_epi64(_mm256_castsi256_si128(vals)));
                _mm256_storeu_si256(
                    dst.add(1),
                    _mm256_cvtepu32_epi64(_mm256_extracti128_si256::<1>(vals)),
                );
            } else {
                _mm256_storeu_si256(out_ptr as *mut __m256i, vals);
            }
            ptr = ptr.add(w);
            out_ptr = out_ptr.add(8);
        }
    }
}

/// AVX2 unpack for bit widths 26..=56, the counterpart of
/// `neon::decode_block_neon_wide`.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn decode_block_avx2_wide<L: Lane>(w: usize, data: &[u8], out: &mut [L]) {
    debug_assert!((26..=57).contains(&w));
    debug_assert!(out.len() % 8 == 0);
    debug_assert!(data.len() >= out.len() / 8 * w + 16);
    let mut lo_base = [0usize; 2];
    let mut hi_base = [0usize; 2];
    let mut idx = [[0i8; 32]; 2];
    let mut sh = [[0i64; 4]; 2];
    for q in 0..2 {
        for half in 0..2 {
            let v = 4 * q + 2 * half;
            let bit_lo = v * w;
            let bit_hi = (v + 1) * w;
            let base = bit_lo / 8;
            if half == 0 {
                lo_base[q] = base;
            } else {
                hi_base[q] = base;
            }
            let hi_delta = bit_hi / 8 - base;
            for k in 0..8 {
                idx[q][16 * half + k] = k as i8;
                idx[q][16 * half + 8 + k] = (hi_delta + k) as i8;
            }
            sh[q][2 * half] = (bit_lo % 8) as i64;
            sh[q][2 * half + 1] = (bit_hi % 8) as i64;
        }
    }
    unsafe {
        let mask = _mm256_set1_epi64x(((1u64 << w) - 1) as i64);
        let num_groups = out.len() / 8;
        let mut ptr = data.as_ptr();
        let mut out_ptr = out.as_mut_ptr();
        for _ in 0..num_groups {
            for q in 0..2 {
                let idx_v = _mm256_loadu_si256(idx[q].as_ptr() as *const __m256i);
                let sh_v = _mm256_loadu_si256(sh[q].as_ptr() as *const __m256i);
                let bytes_lo = _mm_loadu_si128(ptr.add(lo_base[q]) as *const __m128i);
                let bytes_hi = _mm_loadu_si128(ptr.add(hi_base[q]) as *const __m128i);
                let bytes = _mm256_set_m128i(bytes_hi, bytes_lo);
                let vals = _mm256_and_si256(
                    _mm256_srlv_epi64(_mm256_shuffle_epi8(bytes, idx_v), sh_v),
                    mask,
                );
                if L::IS_U64 {
                    _mm256_storeu_si256(out_ptr.add(4 * q) as *mut __m256i, vals);
                } else {
                    let packed = _mm256_permutevar8x32_epi32(
                        vals,
                        _mm256_setr_epi32(0, 2, 4, 6, 0, 0, 0, 0),
                    );
                    _mm_storeu_si128(
                        out_ptr.add(4 * q) as *mut __m128i,
                        _mm256_castsi256_si128(packed),
                    );
                }
            }
            ptr = ptr.add(w);
            out_ptr = out_ptr.add(8);
        }
    }
}
