//! AVX2 kernels: block unpack and fused decode-and-filter over bitpacked data.
//!
//! A group of 8 consecutive values occupies exactly `w` bytes and always
//! starts byte-aligned, so the (byte offset, bit shift) pattern of a group is
//! a constant per width, computed once per call. Narrow kernels (`w <= 25`)
//! decode a whole group into the `u32` lanes of one 256-bit register:
//! `vpshufb` shuffles within each 128-bit half independently, so the low half
//! gathers values 0..4 from `ptr` and the high half values 4..8 from
//! `ptr + hi_base`, then `vpsrlvd` shifts and `vpand` masks. Wide kernels
//! (26..=57) do the same in `u64` lanes, two quarters per group.
//!
//! Lane compaction for the filters follows
//! <https://quickwit.io/blog/simd-range>.
use std::arch::x86_64::*;
use std::ops::RangeInclusive;

use super::{AffineAdd, AffineMul, Plain, Store};

/// The AVX2 half of [`super::Lane`]: how the kernels' registers land in lanes
/// of this type.
pub trait SimdLane {
    /// Stores the 8 values held in the `u32` lanes of `vals`.
    ///
    /// # Safety
    /// `out` must have room for 8 values.
    unsafe fn store_u32x8(out: *mut Self, vals: __m256i);

    /// Stores the 4 values held in the `u64` lanes of `vals`.
    ///
    /// # Safety
    /// `out` must have room for 4 values.
    unsafe fn store_u64x4(out: *mut Self, vals: __m256i);
}

impl SimdLane for u64 {
    #[inline(always)]
    unsafe fn store_u32x8(out: *mut u64, vals: __m256i) {
        unsafe {
            let dst = out as *mut __m256i;
            _mm256_storeu_si256(dst, _mm256_cvtepu32_epi64(_mm256_castsi256_si128(vals)));
            _mm256_storeu_si256(
                dst.add(1),
                _mm256_cvtepu32_epi64(_mm256_extracti128_si256::<1>(vals)),
            );
        }
    }

    #[inline(always)]
    unsafe fn store_u64x4(out: *mut u64, vals: __m256i) {
        unsafe { _mm256_storeu_si256(out as *mut _, vals) }
    }
}

impl SimdLane for u32 {
    #[inline(always)]
    unsafe fn store_u32x8(out: *mut u32, vals: __m256i) {
        unsafe { _mm256_storeu_si256(out as *mut _, vals) }
    }

    #[inline(always)]
    unsafe fn store_u64x4(out: *mut u32, vals: __m256i) {
        unsafe {
            // The 4 values sit in the even 32-bit lanes; `vpermd` gathers them
            // into the low half, which is then one 128-bit store.
            let packed =
                _mm256_permutevar8x32_epi32(vals, _mm256_setr_epi32(0, 2, 4, 6, 0, 0, 0, 0));
            _mm_storeu_si128(out as *mut _, _mm256_castsi256_si128(packed));
        }
    }
}

/// The AVX2 half of [`Store`]: how a sink lands the kernels' registers in
/// memory.
pub trait SimdStore<Out> {
    /// Stores the 8 values held in the `u32` lanes of `vals`.
    ///
    /// # Safety
    /// `out` must have room for 8 values.
    unsafe fn store_u32x8(&self, out: *mut Out, vals: __m256i);

    /// Stores the 4 values held in the `u64` lanes of `vals`.
    ///
    /// # Safety
    /// `out` must have room for 4 values.
    unsafe fn store_u64x4(&self, out: *mut Out, vals: __m256i);
}

impl<T: SimdLane> SimdStore<T> for Plain<T> {
    #[inline(always)]
    unsafe fn store_u32x8(&self, out: *mut T, vals: __m256i) {
        unsafe { T::store_u32x8(out, vals) }
    }

    #[inline(always)]
    unsafe fn store_u64x4(&self, out: *mut T, vals: __m256i) {
        unsafe { T::store_u64x4(out, vals) }
    }
}

impl SimdStore<u64> for AffineAdd {
    #[inline(always)]
    unsafe fn store_u32x8(&self, out: *mut u64, vals: __m256i) {
        unsafe {
            let min = _mm256_set1_epi64x(self.min as i64);
            let dst = out as *mut __m256i;
            let lo = _mm256_cvtepu32_epi64(_mm256_castsi256_si128(vals));
            let hi = _mm256_cvtepu32_epi64(_mm256_extracti128_si256::<1>(vals));
            _mm256_storeu_si256(dst, _mm256_add_epi64(lo, min));
            _mm256_storeu_si256(dst.add(1), _mm256_add_epi64(hi, min));
        }
    }

    #[inline(always)]
    unsafe fn store_u64x4(&self, out: *mut u64, vals: __m256i) {
        unsafe {
            let sum = _mm256_add_epi64(vals, _mm256_set1_epi64x(self.min as i64));
            _mm256_storeu_si256(out as *mut _, sum);
        }
    }
}

impl SimdStore<u64> for AffineMul {
    #[inline(always)]
    unsafe fn store_u32x8(&self, out: *mut u64, vals: __m256i) {
        unsafe {
            // `_mm256_mul_epu32` takes the low 32 bits of each 64-bit lane, so
            // widening first lines every value up as its own multiplicand.
            let gcd = _mm256_set1_epi64x(self.gcd as i64);
            let min = _mm256_set1_epi64x(self.min as i64);
            let dst = out as *mut __m256i;
            let lo = _mm256_cvtepu32_epi64(_mm256_castsi256_si128(vals));
            let hi = _mm256_cvtepu32_epi64(_mm256_extracti128_si256::<1>(vals));
            let lo = _mm256_add_epi64(_mm256_mul_epu32(lo, gcd), min);
            let hi = _mm256_add_epi64(_mm256_mul_epu32(hi, gcd), min);
            _mm256_storeu_si256(dst, lo);
            _mm256_storeu_si256(dst.add(1), hi);
        }
    }

    #[inline(always)]
    unsafe fn store_u64x4(&self, out: *mut u64, vals: __m256i) {
        unsafe {
            // `_mm256_mul_epu32` reads the low 32 bits of each 64-bit lane,
            // which `fusable`'s width cap makes the whole value.
            let prod = _mm256_mul_epu32(vals, _mm256_set1_epi64x(self.gcd as i64));
            let sum = _mm256_add_epi64(prod, _mm256_set1_epi64x(self.min as i64));
            _mm256_storeu_si256(out as *mut _, sum);
        }
    }
}

/// Per-lane shuffle indices and shifts for the narrow kernels: the low half
/// gathers values 0..4 from a group's first byte, the high half values 4..8
/// from `hi_base` bytes further in.
#[inline]
fn narrow_setup(w: usize) -> (usize, [i8; 32], [i32; 8]) {
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
    (hi_base, idx, sh)
}

/// Decodes one group of 8 into `u32` lanes: both 128-bit halves loaded,
/// shuffled, shifted and masked in one 256-bit register.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn narrow_group(
    ptr: *const u8,
    hi_base: usize,
    idx_v: __m256i,
    sh_v: __m256i,
    mask: __m256i,
) -> __m256i {
    unsafe {
        let bytes_lo = _mm_loadu_si128(ptr as *const __m128i);
        let bytes_hi = _mm_loadu_si128(ptr.add(hi_base) as *const __m128i);
        let bytes = _mm256_set_m128i(bytes_hi, bytes_lo);
        _mm256_and_si256(
            _mm256_srlv_epi32(_mm256_shuffle_epi8(bytes, idx_v), sh_v),
            mask,
        )
    }
}

/// Unpack for bit widths 1..=25.
///
/// # Safety
/// `data.len() >= out.len() / 8 * w + 16` must hold.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn decode_block<S: Store>(w: usize, data: &[u8], out: &mut [S::Out], sink: &S) {
    debug_assert!((1..=25).contains(&w));
    debug_assert!(out.len().is_multiple_of(8));
    debug_assert!(data.len() >= out.len() / 8 * w + 16);
    let (hi_base, idx, sh) = narrow_setup(w);
    unsafe {
        let idx_v = _mm256_loadu_si256(idx.as_ptr() as *const __m256i);
        let sh_v = _mm256_loadu_si256(sh.as_ptr() as *const __m256i);
        let mask = _mm256_set1_epi32(((1u64 << w) - 1) as i32);
        let num_groups = out.len() / 8;
        let mut ptr = data.as_ptr();
        let mut out_ptr = out.as_mut_ptr();
        for _ in 0..num_groups {
            let vals = narrow_group(ptr, hi_base, idx_v, sh_v, mask);
            sink.store_u32x8(out_ptr, vals);
            ptr = ptr.add(w);
            out_ptr = out_ptr.add(8);
        }
    }
}

/// Unpack for bit widths 26..=57, in `u64` lanes.
///
/// # Safety
/// As [`decode_block`].
#[target_feature(enable = "avx2")]
pub(super) unsafe fn decode_block_wide<S: Store>(
    w: usize,
    data: &[u8],
    out: &mut [S::Out],
    sink: &S,
) {
    debug_assert!((26..=57).contains(&w));
    debug_assert!(out.len().is_multiple_of(8));
    debug_assert!(data.len() >= out.len() / 8 * w + 16);
    let (lo_base, hi_base, idx, sh) = wide_setup(w);
    unsafe {
        let mask = _mm256_set1_epi64x(((1u64 << w) - 1) as i64);
        let num_groups = out.len() / 8;
        let mut ptr = data.as_ptr();
        let mut out_ptr = out.as_mut_ptr();
        for _ in 0..num_groups {
            for q in 0..2 {
                let vals = wide_group(ptr, q, &lo_base, &hi_base, &idx, &sh, mask);
                sink.store_u64x4(out_ptr.add(4 * q), vals);
            }
            ptr = ptr.add(w);
            out_ptr = out_ptr.add(8);
        }
    }
}

/// Sign-flip mask for the 64-bit lane compares. AVX2 only has signed 64-bit
/// comparison, so both values and bounds are mapped monotonically into `i64`.
const SIGN64: i64 = i64::MIN;

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn ids_x8(first_id: u32) -> __m256i {
    _mm256_setr_epi32(
        first_id as i32,
        first_id.wrapping_add(1) as i32,
        first_id.wrapping_add(2) as i32,
        first_id.wrapping_add(3) as i32,
        first_id.wrapping_add(4) as i32,
        first_id.wrapping_add(5) as i32,
        first_id.wrapping_add(6) as i32,
        first_id.wrapping_add(7) as i32,
    )
}

/// Fused decode + range filter for bit widths 1..=25: values are range-tested
/// in register and never stored, only the matching row ids are.
///
/// # Safety
/// `data.len() >= num_groups * w + 16` must hold, as for [`decode_block`].
/// `out` must have room for `num_groups * 8` ids.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn decode_filter(
    w: usize,
    data: &[u8],
    num_groups: usize,
    value_range: RangeInclusive<u32>,
    first_id: u32,
    out: *mut u32,
) -> usize {
    debug_assert!((1..=25).contains(&w));
    debug_assert!(data.len() >= num_groups * w + 16);
    let (hi_base, idx, sh) = narrow_setup(w);
    unsafe {
        let idx_v = _mm256_loadu_si256(idx.as_ptr() as *const __m256i);
        let sh_v = _mm256_loadu_si256(sh.as_ptr() as *const __m256i);
        let mask = _mm256_set1_epi32(((1u64 << w) - 1) as i32);
        let range_i32 = _mm256_set1_epi32(u32_to_i32(*value_range.start()))
            ..=_mm256_set1_epi32(u32_to_i32(*value_range.end()));
        let step = _mm256_set1_epi32(8);
        let mut ids = ids_x8(first_id);

        let mut ptr = data.as_ptr();
        let mut tail = out;
        for _ in 0..num_groups {
            let vals = narrow_group(ptr, hi_base, idx_v, sh_v, mask);
            let bitset = compute_filter_bitset(u32_to_i32_avx2(vals), range_i32.clone());
            _mm256_storeu_si256(tail as *mut __m256i, compact(ids, bitset));
            tail = tail.add(bitset.count_ones() as usize);
            ids = _mm256_add_epi32(ids, step);
            ptr = ptr.add(w);
        }
        tail.offset_from(out) as usize
    }
}

/// Fused decode + range filter for bit widths 26..=32.
///
/// Decodes with the wide kernel's `u64` lanes, then narrows each half back to
/// 4 `u32` and recombines them into the 8-lane group the compaction table
/// expects. Exact only while `w <= 32`.
///
/// # Safety
/// As [`decode_filter`], with [`decode_block_wide`]'s layout.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn decode_filter_wide(
    w: usize,
    data: &[u8],
    num_groups: usize,
    value_range: RangeInclusive<u32>,
    first_id: u32,
    out: *mut u32,
) -> usize {
    debug_assert!((26..=32).contains(&w));
    debug_assert!(data.len() >= num_groups * w + 16);
    let (lo_base, hi_base, idx, sh) = wide_setup(w);
    unsafe {
        let mask = _mm256_set1_epi64x(((1u64 << w) - 1) as i64);
        let range_i32 = _mm256_set1_epi32(u32_to_i32(*value_range.start()))
            ..=_mm256_set1_epi32(u32_to_i32(*value_range.end()));
        const GATHER_LOW: __m256i = unsafe { std::mem::transmute([0i32, 2, 4, 6, 0, 0, 0, 0]) };
        let step = _mm256_set1_epi32(8);
        let mut ids = ids_x8(first_id);

        let mut ptr = data.as_ptr();
        let mut tail = out;
        for _ in 0..num_groups {
            let mut halves = [_mm_setzero_si128(); 2];
            for (q, half) in halves.iter_mut().enumerate() {
                let vals = wide_group(ptr, q, &lo_base, &hi_base, &idx, &sh, mask);
                *half = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(vals, GATHER_LOW));
            }
            let vals = _mm256_set_m128i(halves[1], halves[0]);
            let bitset = compute_filter_bitset(u32_to_i32_avx2(vals), range_i32.clone());
            _mm256_storeu_si256(tail as *mut __m256i, compact(ids, bitset));
            tail = tail.add(bitset.count_ones() as usize);
            ids = _mm256_add_epi32(ids, step);
            ptr = ptr.add(w);
        }
        tail.offset_from(out) as usize
    }
}

/// Fused decode + range filter for bit widths 26..=56 and 64.
///
/// Compares in `u64` lanes and compacts in `u32` lanes: each `__m256i` of 4
/// `u64` yields 4 mask bits, and the two halves of a group OR into the 8-bit
/// mask the permutation table below already indexes.
///
/// # Safety
/// As [`decode_filter`], with [`decode_block_wide`]'s layout.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn decode_filter64(
    w: usize,
    data: &[u8],
    num_groups: usize,
    value_range: RangeInclusive<u64>,
    first_id: u32,
    out: *mut u32,
) -> usize {
    debug_assert!((26..=56).contains(&w) || w == 64);
    debug_assert!(data.len() >= num_groups * w + 16);
    let (lo_base, hi_base, idx, sh) = wide_setup(w);
    unsafe {
        let mask = _mm256_set1_epi64x(if w == 64 {
            -1
        } else {
            ((1u64 << w) - 1) as i64
        });
        let sign = _mm256_set1_epi64x(SIGN64);
        let lo64 = _mm256_set1_epi64x((*value_range.start() as i64) ^ SIGN64);
        let hi64 = _mm256_set1_epi64x((*value_range.end() as i64) ^ SIGN64);
        let step = _mm256_set1_epi32(8);
        let mut ids = ids_x8(first_id);

        let mut ptr = data.as_ptr();
        let mut tail = out;
        for _ in 0..num_groups {
            let mut bitset = 0u8;
            for q in 0..2 {
                let vals = wide_group(ptr, q, &lo_base, &hi_base, &idx, &sh, mask);
                let v = _mm256_xor_si256(vals, sign);
                let outside =
                    _mm256_or_si256(_mm256_cmpgt_epi64(lo64, v), _mm256_cmpgt_epi64(v, hi64));
                let m = 15 - _mm256_movemask_pd(_mm256_castsi256_pd(outside)) as u8;
                bitset |= m << (4 * q);
            }
            _mm256_storeu_si256(tail as *mut __m256i, compact(ids, bitset));
            tail = tail.add(bitset.count_ones() as usize);
            ids = _mm256_add_epi32(ids, step);
            ptr = ptr.add(w);
        }
        tail.offset_from(out) as usize
    }
}

/// Per-quarter byte offsets, shuffle indices and shifts that map a group's
/// bytes onto four `u64` lanes: `(lo_base, hi_base, idx, sh)`.
type WideSetup = ([usize; 2], [usize; 2], [[i8; 32]; 2], [[i64; 4]; 2]);

/// Shared per-width setup for the two wide kernels.
#[inline]
fn wide_setup(w: usize) -> WideSetup {
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
    (lo_base, hi_base, idx, sh)
}

/// Decodes quarter `q` of a group into 4 `u64` lanes.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn wide_group(
    ptr: *const u8,
    q: usize,
    lo_base: &[usize; 2],
    hi_base: &[usize; 2],
    idx: &[[i8; 32]; 2],
    sh: &[[i64; 4]; 2],
    mask: __m256i,
) -> __m256i {
    unsafe {
        let idx_v = _mm256_loadu_si256(idx[q].as_ptr() as *const __m256i);
        let sh_v = _mm256_loadu_si256(sh[q].as_ptr() as *const __m256i);
        let bytes_lo = _mm_loadu_si128(ptr.add(lo_base[q]) as *const __m128i);
        let bytes_hi = _mm_loadu_si128(ptr.add(hi_base[q]) as *const __m128i);
        let bytes = _mm256_set_m128i(bytes_hi, bytes_lo);
        _mm256_and_si256(
            _mm256_srlv_epi64(_mm256_shuffle_epi8(bytes, idx_v), sh_v),
            mask,
        )
    }
}

const NUM_LANES: usize = 8;

const HIGHEST_BIT: u32 = 1 << 31;

#[inline]
fn u32_to_i32(val: u32) -> i32 {
    (val ^ HIGHEST_BIT) as i32
}

#[inline]
unsafe fn u32_to_i32_avx2(vals_u32x8s: __m256i) -> __m256i {
    const HIGHEST_BIT_MASK: __m256i = from_u32x8([HIGHEST_BIT; NUM_LANES]);
    unsafe { _mm256_xor_si256(vals_u32x8s, HIGHEST_BIT_MASK) }
}

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn compact(data: __m256i, mask: u8) -> __m256i {
    let vperm_mask = MASK_TO_PERMUTATION[mask as usize];
    _mm256_permutevar8x32_epi32(data, vperm_mask)
}

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn compute_filter_bitset(val: __m256i, range: std::ops::RangeInclusive<__m256i>) -> u8 {
    let too_low = _mm256_cmpgt_epi32(*range.start(), val);
    let too_high = _mm256_cmpgt_epi32(val, *range.end());
    let inside = _mm256_or_si256(too_low, too_high);
    255 - std::arch::x86_64::_mm256_movemask_ps(_mm256_castsi256_ps(inside)) as u8
}

union U8x32 {
    vector: __m256i,
    vals: [u32; NUM_LANES],
}

const fn from_u32x8(vals: [u32; NUM_LANES]) -> __m256i {
    unsafe { U8x32 { vals }.vector }
}

/// Entry `m` compacts the lanes whose bits are set in `m` to the front, in
/// lane order; the rest of the output lanes repeat lane 0, which the caller's
/// cursor advance never exposes.
const MASK_TO_PERMUTATION: [__m256i; 256] = {
    let mut table = [from_u32x8([0; NUM_LANES]); 256];
    let mut m = 0;
    while m < 256 {
        let mut vals = [0u32; NUM_LANES];
        let mut lane = 0;
        let mut slot = 0;
        while lane < NUM_LANES {
            if m & (1 << lane) != 0 {
                vals[slot] = lane as u32;
                slot += 1;
            }
            lane += 1;
        }
        table[m] = from_u32x8(vals);
        m += 1;
    }
    table
};
