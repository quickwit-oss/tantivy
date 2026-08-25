//! NEON kernels: block unpack and fused decode-and-filter over bitpacked data.
//!
//! A group of 8 consecutive values occupies exactly `w` bytes and always
//! starts byte-aligned, so the (byte offset, bit shift) pattern of a group is
//! a constant per width, computed once per call. `vqtbl1q_u8` only shuffles
//! within one 128-bit register, so narrow kernels (`w <= 25`, the widest a
//! `u32` lane fits with its shift of up to 7) decode a group in two halves of
//! 4 values; wide kernels (26..=57) decode in `u64` lanes, four value pairs
//! per group, each pair byte-aligned as a whole. `vshlq` shifts left by signed
//! counts, so the per-lane right shifts are negative.
use super::{AffineAdd, AffineMul, Plain, Store};
use std::arch::aarch64::*;
use std::ops::RangeInclusive;

/// The NEON half of [`super::Lane`]: how the kernels' registers land in lanes
/// of this type.
pub trait SimdLane {
    /// Stores the 4 values held in the `u32` lanes of `vals`.
    ///
    /// # Safety
    /// `out` must have room for 4 values.
    unsafe fn store_u32x4(out: *mut Self, vals: uint32x4_t);

    /// Stores the 2 values held in the `u64` lanes of `vals`.
    ///
    /// # Safety
    /// `out` must have room for 2 values.
    unsafe fn store_u64x2(out: *mut Self, vals: uint64x2_t);
}

impl SimdLane for u64 {
    #[inline(always)]
    unsafe fn store_u32x4(out: *mut u64, vals: uint32x4_t) {
        unsafe {
            vst1q_u64(out, vmovl_u32(vget_low_u32(vals)));
            vst1q_u64(out.add(2), vmovl_high_u32(vals));
        }
    }

    #[inline(always)]
    unsafe fn store_u64x2(out: *mut u64, vals: uint64x2_t) {
        unsafe { vst1q_u64(out, vals) }
    }
}

impl SimdLane for u32 {
    #[inline(always)]
    unsafe fn store_u32x4(out: *mut u32, vals: uint32x4_t) {
        unsafe { vst1q_u32(out, vals) }
    }

    #[inline(always)]
    unsafe fn store_u64x2(out: *mut u32, vals: uint64x2_t) {
        unsafe { vst1_u32(out, vmovn_u64(vals)) }
    }
}

/// The NEON half of [`Store`]: how a sink lands the kernels' registers in
/// memory.
pub trait SimdStore<Out> {
    /// Stores the 4 values held in the `u32` lanes of `vals`.
    ///
    /// # Safety
    /// `out` must have room for 4 values.
    unsafe fn store_u32x4(&self, out: *mut Out, vals: uint32x4_t);

    /// Stores the 2 values held in the `u64` lanes of `vals`.
    ///
    /// # Safety
    /// `out` must have room for 2 values.
    unsafe fn store_u64x2(&self, out: *mut Out, vals: uint64x2_t);
}

impl<T: SimdLane> SimdStore<T> for Plain<T> {
    #[inline(always)]
    unsafe fn store_u32x4(&self, out: *mut T, vals: uint32x4_t) {
        unsafe { T::store_u32x4(out, vals) }
    }

    #[inline(always)]
    unsafe fn store_u64x2(&self, out: *mut T, vals: uint64x2_t) {
        unsafe { T::store_u64x2(out, vals) }
    }
}

impl SimdStore<u64> for AffineAdd {
    #[inline(always)]
    unsafe fn store_u32x4(&self, out: *mut u64, vals: uint32x4_t) {
        unsafe {
            let min = vdupq_n_u64(self.min);
            vst1q_u64(out, vaddq_u64(vmovl_u32(vget_low_u32(vals)), min));
            vst1q_u64(out.add(2), vaddq_u64(vmovl_high_u32(vals), min));
        }
    }

    #[inline(always)]
    unsafe fn store_u64x2(&self, out: *mut u64, vals: uint64x2_t) {
        unsafe { vst1q_u64(out, vaddq_u64(vals, vdupq_n_u64(self.min))) }
    }
}

impl SimdStore<u64> for AffineMul {
    #[inline(always)]
    unsafe fn store_u32x4(&self, out: *mut u64, vals: uint32x4_t) {
        unsafe {
            // `vmull_u32` widens as it multiplies, so the fused form needs no
            // separate widening step: it replaces the plain sink's `vmovl_u32`.
            let gcd = vdupq_n_u32(self.gcd);
            let min = vdupq_n_u64(self.min);
            let lo = vmull_u32(vget_low_u32(vals), vget_low_u32(gcd));
            let hi = vmull_high_u32(vals, gcd);
            vst1q_u64(out, vaddq_u64(lo, min));
            vst1q_u64(out.add(2), vaddq_u64(hi, min));
        }
    }

    #[inline(always)]
    unsafe fn store_u64x2(&self, out: *mut u64, vals: uint64x2_t) {
        unsafe {
            // Exact without an emulation: `fusable` caps the width at 32, so
            // narrowing to the low halves loses nothing.
            debug_assert_eq!(vgetq_lane_u64::<0>(vals) >> 32, 0);
            let prod = vmull_u32(vmovn_u64(vals), vdup_n_u32(self.gcd));
            vst1q_u64(out, vaddq_u64(prod, vdupq_n_u64(self.min)));
        }
    }
}

/// Per-lane shuffle indices and shifts for the narrow kernels: values 0..4
/// gather from a group's first byte, values 4..8 from `hi_base` bytes further
/// in.
#[inline]
fn narrow_setup(w: usize) -> (usize, [u8; 16], [u8; 16], [i32; 4], [i32; 4]) {
    let hi_base = (4 * w) / 8;
    let mut idx_lo = [0u8; 16];
    let mut idx_hi = [0u8; 16];
    let mut sh_lo = [0i32; 4];
    let mut sh_hi = [0i32; 4];
    for j in 0..4 {
        let bit_lo = j * w;
        let bit_hi = (4 + j) * w;
        for k in 0..4 {
            idx_lo[j * 4 + k] = (bit_lo / 8 + k) as u8;
            idx_hi[j * 4 + k] = (bit_hi / 8 - hi_base + k) as u8;
        }
        sh_lo[j] = -((bit_lo % 8) as i32);
        sh_hi[j] = -((bit_hi % 8) as i32);
    }
    (hi_base, idx_lo, idx_hi, sh_lo, sh_hi)
}

/// Gathers, shifts and masks one half-group of 4 values into `u32` lanes.
#[inline(always)]
unsafe fn narrow_half(bytes: uint8x16_t, idx_v: uint8x16_t, sh_v: int32x4_t, mask: uint32x4_t) -> uint32x4_t {
    unsafe { vandq_u32(vshlq_u32(vreinterpretq_u32_u8(vqtbl1q_u8(bytes, idx_v)), sh_v), mask) }
}

/// Per-pair byte bases, shuffle indices and shifts for the wide kernels: a
/// group of 8 is four value pairs, each byte-aligned as a whole.
#[inline]
#[allow(clippy::type_complexity)]
fn wide_setup(w: usize) -> ([usize; 4], [[u8; 16]; 4], [[i64; 2]; 4]) {
    let mut idx = [[0u8; 16]; 4];
    let mut sh = [[0i64; 2]; 4];
    let mut base = [0usize; 4];
    for (p, ((idx, sh), base)) in idx.iter_mut().zip(&mut sh).zip(&mut base).enumerate() {
        let bit_lo = 2 * p * w;
        let bit_hi = (2 * p + 1) * w;
        *base = bit_lo / 8;
        let hi_delta = bit_hi / 8 - *base;
        for k in 0..8 {
            idx[k] = k as u8;
            idx[8 + k] = (hi_delta + k) as u8;
        }
        *sh = [-((bit_lo % 8) as i64), -((bit_hi % 8) as i64)];
    }
    (base, idx, sh)
}

/// Decodes value pair `p` of a group into 2 `u64` lanes.
#[inline(always)]
unsafe fn wide_pair(
    ptr: *const u8,
    p: usize,
    base: &[usize; 4],
    idx_v: &[uint8x16_t; 4],
    sh_v: &[int64x2_t; 4],
    mask: uint64x2_t,
) -> uint64x2_t {
    unsafe {
        let bytes = vld1q_u8(ptr.add(base[p]));
        vandq_u64(
            vshlq_u64(vreinterpretq_u64_u8(vqtbl1q_u8(bytes, idx_v[p])), sh_v[p]),
            mask,
        )
    }
}

/// Unpack for bit widths 1..=25. `out.len()` must be a multiple of 8.
///
/// # Safety
/// `data.len() >= out.len() / 8 * w + 16` must hold: group `g` reads 16 bytes
/// at `g * w + (4 * w) / 8`, and the last group has `g = out.len() / 8 - 1`.
pub(super) unsafe fn decode_block<S: Store>(
    w: usize,
    data: &[u8],
    out: &mut [S::Out],
    sink: &S,
) {
    debug_assert!((1..=25).contains(&w));
    debug_assert!(out.len().is_multiple_of(8));
    debug_assert!(data.len() >= out.len() / 8 * w + 16);
    let (hi_base, idx_lo, idx_hi, sh_lo, sh_hi) = narrow_setup(w);
    unsafe {
        let idx_lo_v = vld1q_u8(idx_lo.as_ptr());
        let idx_hi_v = vld1q_u8(idx_hi.as_ptr());
        let sh_lo_v = vld1q_s32(sh_lo.as_ptr());
        let sh_hi_v = vld1q_s32(sh_hi.as_ptr());
        let mask = vdupq_n_u32(((1u64 << w) - 1) as u32);
        let mut ptr = data.as_ptr();
        let mut out_ptr = out.as_mut_ptr();
        for _ in 0..out.len() / 8 {
            let lo = narrow_half(vld1q_u8(ptr), idx_lo_v, sh_lo_v, mask);
            let hi = narrow_half(vld1q_u8(ptr.add(hi_base)), idx_hi_v, sh_hi_v, mask);
            sink.store_u32x4(out_ptr, lo);
            sink.store_u32x4(out_ptr.add(4), hi);
            ptr = ptr.add(w);
            out_ptr = out_ptr.add(8);
        }
    }
}

/// Unpack for bit widths 26..=57, in `u64` lanes. `out.len()` must be a
/// multiple of 8. Callers never exceed 56 -- `BitUnpacker` stores nothing
/// between 57 and 63.
///
/// # Safety
/// `data.len() >= out.len() / 8 * w + 16` must hold, as for
/// [`decode_block`]: the last pair of the last group loads 16 bytes at
/// `(out.len() / 8 - 1) * w + 6 * w / 8`.
pub(super) unsafe fn decode_block_wide<S: Store>(
    w: usize,
    data: &[u8],
    out: &mut [S::Out],
    sink: &S,
) {
    debug_assert!((26..=57).contains(&w));
    debug_assert!(out.len().is_multiple_of(8));
    debug_assert!(data.len() >= out.len() / 8 * w + 16);
    let (base, idx, sh) = wide_setup(w);
    unsafe {
        let idx_v = idx.map(|idx| vld1q_u8(idx.as_ptr()));
        let sh_v = sh.map(|sh| vld1q_s64(sh.as_ptr()));
        let mask = vdupq_n_u64((1u64 << w) - 1);
        let mut ptr = data.as_ptr();
        let mut out_ptr = out.as_mut_ptr();
        for _ in 0..out.len() / 8 {
            for p in 0..4 {
                let vals = wide_pair(ptr, p, &base, &idx_v, &sh_v, mask);
                sink.store_u64x2(out_ptr.add(2 * p), vals);
            }
            ptr = ptr.add(w);
            out_ptr = out_ptr.add(8);
        }
    }
}

/// Fused decode + range filter for bit widths 1..=25: values are range-tested
/// in register and never stored, only the matching row ids are. Ids ride in
/// two `u32` vectors stepped by 8 per group. Returns how many were written.
///
/// # Safety
/// `data.len() >= num_groups * w + 16` must hold, as for [`decode_block`].
/// `out` must have room for `num_groups * 8` ids: each half-group stores a full
/// 4-lane vector at the cursor before advancing it by the match count.
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
    let (hi_base, idx_lo, idx_hi, sh_lo, sh_hi) = narrow_setup(w);
    unsafe {
        let idx_lo_v = vld1q_u8(idx_lo.as_ptr());
        let idx_hi_v = vld1q_u8(idx_hi.as_ptr());
        let sh_lo_v = vld1q_s32(sh_lo.as_ptr());
        let sh_hi_v = vld1q_s32(sh_hi.as_ptr());
        let mask = vdupq_n_u32(((1u64 << w) - 1) as u32);

        let range_start = vdupq_n_u32(*value_range.start());
        let range_end = vdupq_n_u32(*value_range.end());
        let bit_weights = vld1q_u32([1u32, 2, 4, 8].as_ptr());
        let step = vdupq_n_u32(8);
        let mut ids_lo = vld1q_u32([first_id, first_id + 1, first_id + 2, first_id + 3].as_ptr());
        let mut ids_hi = vaddq_u32(ids_lo, vdupq_n_u32(4));

        let mut ptr = data.as_ptr();
        let mut tail = out;
        for _ in 0..num_groups {
            let lo = narrow_half(vld1q_u8(ptr), idx_lo_v, sh_lo_v, mask);
            let hi = narrow_half(vld1q_u8(ptr.add(hi_base)), idx_hi_v, sh_hi_v, mask);

            for (vals, ids) in [(lo, ids_lo), (hi, ids_hi)] {
                // `vals >= start` and `vals <= end`, ANDed into an all-ones
                // lane mask for the values inside the range, then reduced to
                // one 4-bit mask by weighting the lanes 1,2,4,8 and summing.
                let inside = vandq_u32(vcgeq_u32(vals, range_start), vcleq_u32(vals, range_end));
                let m = vaddvq_u32(vandq_u32(bit_weights, inside)) as u8;
                vst1q_u32(tail, compact(ids, m));
                tail = tail.add(m.count_ones() as usize);
            }

            ids_lo = vaddq_u32(ids_lo, step);
            ids_hi = vaddq_u32(ids_hi, step);
            ptr = ptr.add(w);
        }
        tail.offset_from(out) as usize
    }
}

/// Fused decode + range filter for bit widths 26..=32: wide-kernel decode,
/// each pair of `uint64x2_t` narrowed back to one `uint32x4_t` for the `u32`
/// range test -- exact only while `w <= 32`. Returns how many ids were
/// written.
///
/// # Safety
/// `data.len() >= num_groups * w + 16` must hold, as for
/// [`decode_block_wide`]. `out` must have room for `num_groups * 8` ids.
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
    let (base, idx, sh) = wide_setup(w);
    unsafe {
        let idx_v = idx.map(|idx| vld1q_u8(idx.as_ptr()));
        let sh_v = sh.map(|sh| vld1q_s64(sh.as_ptr()));
        let mask = vdupq_n_u64((1u64 << w) - 1);

        let range_start = vdupq_n_u32(*value_range.start());
        let range_end = vdupq_n_u32(*value_range.end());
        let bit_weights = vld1q_u32([1u32, 2, 4, 8].as_ptr());
        let step = vdupq_n_u32(8);
        let mut ids_lo = vld1q_u32([first_id, first_id + 1, first_id + 2, first_id + 3].as_ptr());
        let mut ids_hi = vaddq_u32(ids_lo, vdupq_n_u32(4));

        let mut ptr = data.as_ptr();
        let mut tail = out;
        for _ in 0..num_groups {
            let mut quad = [vdupq_n_u64(0); 4];
            for (p, q) in quad.iter_mut().enumerate() {
                *q = wide_pair(ptr, p, &base, &idx_v, &sh_v, mask);
            }
            let lo = vcombine_u32(vmovn_u64(quad[0]), vmovn_u64(quad[1]));
            let hi = vcombine_u32(vmovn_u64(quad[2]), vmovn_u64(quad[3]));

            for (vals, ids) in [(lo, ids_lo), (hi, ids_hi)] {
                let inside = vandq_u32(vcgeq_u32(vals, range_start), vcleq_u32(vals, range_end));
                let m = vaddvq_u32(vandq_u32(bit_weights, inside)) as u8;
                vst1q_u32(tail, compact(ids, m));
                tail = tail.add(m.count_ones() as usize);
            }

            ids_lo = vaddq_u32(ids_lo, step);
            ids_hi = vaddq_u32(ids_hi, step);
            ptr = ptr.add(w);
        }
        tail.offset_from(out) as usize
    }
}

/// Fused decode + range filter for bit widths 26..=56 and 64: compares in
/// `u64` lanes, compacts in `u32` lanes -- two 2-bit masks OR into the 4-bit
/// mask [`compact`] already takes. Callers route only `33..=56` and `64` here;
/// `26..=32` prefer [`decode_filter_wide`], which tests 4 values per compare.
/// Returns how many ids were written.
///
/// # Safety
/// `data.len() >= num_groups * w + 16` must hold, as for
/// [`decode_block_wide`]. `out` must have room for `num_groups * 8` ids.
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
    // At w == 64 every value is its own aligned 8-byte word, so the shuffle
    // becomes the identity and the shift zero: the setup below degenerates to
    // a plain 16-byte load per value pair, which is exactly what is wanted.
    let (base, idx, sh) = wide_setup(w);
    unsafe {
        let idx_v = idx.map(|idx| vld1q_u8(idx.as_ptr()));
        let sh_v = sh.map(|sh| vld1q_s64(sh.as_ptr()));
        let mask = vdupq_n_u64(if w == 64 { !0u64 } else { (1u64 << w) - 1 });

        let range_start = vdupq_n_u64(*value_range.start());
        let range_end = vdupq_n_u64(*value_range.end());
        // Lane weights for the two halves of a 4-value run: values 0,1 carry
        // bits 1,2 and values 2,3 carry bits 4,8.
        let w_lo = vld1q_u64([1u64, 2].as_ptr());
        let w_hi = vld1q_u64([4u64, 8].as_ptr());
        let step = vdupq_n_u32(8);
        let mut ids_lo = vld1q_u32([first_id, first_id + 1, first_id + 2, first_id + 3].as_ptr());
        let mut ids_hi = vaddq_u32(ids_lo, vdupq_n_u32(4));

        let mut ptr = data.as_ptr();
        let mut tail = out;
        for _ in 0..num_groups {
            let mut quad = [vdupq_n_u64(0); 4];
            for (p, q) in quad.iter_mut().enumerate() {
                *q = wide_pair(ptr, p, &base, &idx_v, &sh_v, mask);
            }

            for (half, ids) in [(0usize, ids_lo), (2usize, ids_hi)] {
                let in0 = vandq_u64(
                    vcgeq_u64(quad[half], range_start),
                    vcleq_u64(quad[half], range_end),
                );
                let in1 = vandq_u64(
                    vcgeq_u64(quad[half + 1], range_start),
                    vcleq_u64(quad[half + 1], range_end),
                );
                let m = (vaddvq_u64(vandq_u64(w_lo, in0)) | vaddvq_u64(vandq_u64(w_hi, in1))) as u8;
                debug_assert!(m <= 15);
                vst1q_u32(tail, compact(ids, m));
                tail = tail.add(m.count_ones() as usize);
            }

            ids_lo = vaddq_u32(ids_lo, step);
            ids_hi = vaddq_u32(ids_hi, step);
            ptr = ptr.add(w);
        }
        tail.offset_from(out) as usize
    }
}

// Compacts matching lanes to the front using a byte-level shuffle.
// `mask` is a 4-bit value: bit k=1 means lane k should appear in the output.
// Lanes that do not match are not removed, only pushed past the end of the
// match count, so callers must store the whole vector and then advance their
// cursor by `mask.count_ones()`.
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
