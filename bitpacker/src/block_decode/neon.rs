use std::arch::aarch64::*;

use super::{BLOCK_LEN, Lane};

/// Stores 4 unpacked values held in `u32` lanes at `out`, widening when the
/// output lane is `u64`. The branch folds away at monomorphization.
///
/// # Safety
/// `out` must be writable for 4 elements.
#[inline(always)]
unsafe fn store_u32x4<L: Lane>(out: *mut L, vals: uint32x4_t) {
    unsafe {
        if L::IS_U64 {
            let out = out as *mut u64;
            vst1q_u64(out, vmovl_u32(vget_low_u32(vals)));
            vst1q_u64(out.add(2), vmovl_high_u32(vals));
        } else {
            vst1q_u32(out as *mut u32, vals);
        }
    }
}

/// Stores 2 unpacked values held in `u64` lanes at `out`, narrowing when the
/// output lane is `u32`. The branch folds away at monomorphization.
///
/// # Safety
/// `out` must be writable for 2 elements, and every lane must fit `L` (the
/// `u32` lane is only ever used at widths `<= 32`).
#[inline(always)]
unsafe fn store_u64x2<L: Lane>(out: *mut L, vals: uint64x2_t) {
    unsafe {
        if L::IS_U64 {
            vst1q_u64(out as *mut u64, vals);
        } else {
            vst1_u32(out as *mut u32, vmovn_u64(vals));
        }
    }
}

/// NEON unpack for bit widths 1..=25.
///
/// 8 consecutive values occupy exactly `w` bytes and always start
/// byte-aligned, so the (byte offset, bit shift) pattern of the 8 values
/// is a compile-time-free constant per width. For each group of 4 values
/// we gather the 4 relevant bytes of each value into u32 lanes with
/// `tbl` (shuffle), then variable-shift right and mask.
///
/// Constraint: bit_shift (<=7) + w <= 32 => w <= 25.
///
/// # Safety
/// `data.len() >= 16 * w + 16` must hold (we read 16 bytes at
/// `15 * w + (4 * w) / 8`, which is `< 16 * w + 16`).
pub(super) unsafe fn decode_block_neon<L: Lane>(w: usize, data: &[u8], out: &mut [L; BLOCK_LEN]) {
    debug_assert!((1..=25).contains(&w));
    debug_assert!(data.len() >= 16 * w + 16);
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
    unsafe {
        let idx_lo_v = vld1q_u8(idx_lo.as_ptr());
        let idx_hi_v = vld1q_u8(idx_hi.as_ptr());
        let sh_lo_v = vld1q_s32(sh_lo.as_ptr());
        let sh_hi_v = vld1q_s32(sh_hi.as_ptr());
        let mask = vdupq_n_u32(((1u64 << w) - 1) as u32);
        let mut ptr = data.as_ptr();
        let mut out_ptr = out.as_mut_ptr();
        for _ in 0..BLOCK_LEN / 8 {
            let bytes_lo = vld1q_u8(ptr);
            let bytes_hi = vld1q_u8(ptr.add(hi_base));
            let lo = vandq_u32(
                vshlq_u32(
                    vreinterpretq_u32_u8(vqtbl1q_u8(bytes_lo, idx_lo_v)),
                    sh_lo_v,
                ),
                mask,
            );
            let hi = vandq_u32(
                vshlq_u32(
                    vreinterpretq_u32_u8(vqtbl1q_u8(bytes_hi, idx_hi_v)),
                    sh_hi_v,
                ),
                mask,
            );
            store_u32x4::<L>(out_ptr, lo);
            store_u32x4::<L>(out_ptr.add(4), hi);
            ptr = ptr.add(w);
            out_ptr = out_ptr.add(8);
        }
    }
}

/// NEON unpack for bit widths 26..=56, where a value plus its bit shift no
/// longer fits a `u32` lane.
///
/// Same shape as [`decode_block_neon`] with the lanes doubled: a group of 8
/// values is done as 4 pairs instead of 2 quads, each pair gathered into the
/// two `u64` lanes of one register, variable-shifted right and masked.
///
/// The gather still works off a single 16-byte `tbl` per pair. Value `2p`
/// starts at byte `b0 = 2 * p * w / 8` and value `2p + 1` at `b1`, with
/// `b1 - b0 <= ceil(w / 8) <= 7`, so loading 16 bytes at `b0` covers both
/// lanes' 8 bytes.
///
/// Constraint: bit_shift (<=7) + w <= 64 => w <= 57.
///
/// # Safety
/// `data.len() >= 16 * w + 16` must hold: the last group starts at `15 * w`
/// and its last pair loads 16 bytes at `+ 6 * w / 8`, which is `< 16 * w + 16`.
pub(super) unsafe fn decode_block_neon_wide<L: Lane>(
    w: usize,
    data: &[u8],
    out: &mut [L; BLOCK_LEN],
) {
    debug_assert!((26..=57).contains(&w));
    debug_assert!(data.len() >= 16 * w + 16);
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
    unsafe {
        let idx_v = idx.map(|idx| vld1q_u8(idx.as_ptr()));
        let sh_v = sh.map(|sh| vld1q_s64(sh.as_ptr()));
        let mask = vdupq_n_u64((1u64 << w) - 1);
        let mut ptr = data.as_ptr();
        let mut out_ptr = out.as_mut_ptr();
        for _ in 0..BLOCK_LEN / 8 {
            for p in 0..4 {
                let bytes = vld1q_u8(ptr.add(base[p]));
                let vals = vandq_u64(
                    vshlq_u64(vreinterpretq_u64_u8(vqtbl1q_u8(bytes, idx_v[p])), sh_v[p]),
                    mask,
                );
                store_u64x2::<L>(out_ptr.add(2 * p), vals);
            }
            ptr = ptr.add(w);
            out_ptr = out_ptr.add(8);
        }
    }
}
