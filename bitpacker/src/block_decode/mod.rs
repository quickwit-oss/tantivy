//! Batch unpack kernels for fixed-width bitpacked values, 128 values at a time.
//!
//! 8 consecutive values occupy exactly `bit_width` bytes, so a group of 128
//! values is `16 * bit_width` bytes and always starts byte aligned. That makes
//! the (byte offset, bit shift) pattern of a group constant per width, which
//! is what lets the kernels shuffle 8 values at a time.
//!
//! Bit widths crate-wide are `0..=56` or exactly `64` (see
//! [`crate::compute_num_bits`]); the kernels rely on that.

#[cfg(target_arch = "x86_64")]
mod avx2;
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
mod neon;

use crate::BitUnpacker;

/// Number of values decoded per [`decode_block`] call.
pub const BLOCK_LEN: usize = 128;

/// Whether a SIMD kernel is compiled in *and* supported by this CPU.
///
/// The `target_endian` guard exists because the kernels' shuffle tables
/// assume the little-endian u8 -> u32 lane mapping.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
fn kernel_available() -> bool {
    true
}
#[cfg(target_arch = "x86_64")]
fn kernel_available() -> bool {
    std::arch::is_x86_feature_detected!("avx2")
}
#[cfg(not(any(
    all(target_arch = "aarch64", target_endian = "little"),
    target_arch = "x86_64"
)))]
fn kernel_available() -> bool {
    false
}

/// Returns whether the SIMD block-decode path is enabled.
///
/// `TANTIVY_BITPACKER_SCALAR=1` forces the scalar path.
pub fn simd_enabled() -> bool {
    static SIMD: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SIMD.get_or_init(|| {
        kernel_available() && std::env::var_os("TANTIVY_BITPACKER_SCALAR").is_none()
    })
}

/// Whether [`decode_block`] reaches a SIMD kernel for this width.
///
/// Every packed width `1..=56` does where the target and CPU have kernels;
/// 0 and 64 are served by [`decode_block_lanes`] itself.
#[inline]
pub fn simd_kernel_applies(bit_width: u8) -> bool {
    (1..=56).contains(&bit_width) && simd_enabled()
}

/// Whether the *narrow* (`u32` lane) kernel takes this width: it needs
/// `bit_shift (<= 7) + bit_width <= 32`. Above that the wide (`u64` lane)
/// kernel takes over.
#[inline]
fn narrow_kernel_applies(bit_width: u8) -> bool {
    (1..=25).contains(&bit_width)
}

/// Output lane of the block kernels. Implemented for `u32` and `u64` only.
///
/// `u32` exists for [`crate::BitUnpacker::get_ids_for_value_range`], whose
/// filter operates on `u32`.
pub(crate) trait Lane: Copy {
    /// Lets the AVX2 kernel pick its store shape with a branch that folds away
    /// at monomorphization.
    #[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
    const IS_U64: bool;

    fn from_u64(val: u64) -> Self;

    /// Stores 4 unpacked values held in `u32` lanes at `out`.
    ///
    /// # Safety
    /// `out` must be writable for 4 elements.
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    unsafe fn store_u32x4(out: *mut Self, vals: std::arch::aarch64::uint32x4_t);

    /// Stores 2 unpacked values held in `u64` lanes at `out`.
    ///
    /// # Safety
    /// `out` must be writable for 2 elements, and every lane must fit `Self`
    /// (the `u32` lane is only ever used at widths `<= 32`).
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    unsafe fn store_u64x2(out: *mut Self, vals: std::arch::aarch64::uint64x2_t);
}

impl Lane for u32 {
    const IS_U64: bool = false;

    #[inline(always)]
    fn from_u64(val: u64) -> u32 {
        val as u32
    }

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    #[inline(always)]
    unsafe fn store_u32x4(out: *mut u32, vals: std::arch::aarch64::uint32x4_t) {
        unsafe { std::arch::aarch64::vst1q_u32(out, vals) }
    }

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    #[inline(always)]
    unsafe fn store_u64x2(out: *mut u32, vals: std::arch::aarch64::uint64x2_t) {
        use std::arch::aarch64::*;
        unsafe { vst1_u32(out, vmovn_u64(vals)) }
    }
}

impl Lane for u64 {
    const IS_U64: bool = true;

    #[inline(always)]
    fn from_u64(val: u64) -> u64 {
        val
    }

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    #[inline(always)]
    unsafe fn store_u32x4(out: *mut u64, vals: std::arch::aarch64::uint32x4_t) {
        use std::arch::aarch64::*;
        unsafe {
            vst1q_u64(out, vmovl_u32(vget_low_u32(vals)));
            vst1q_u64(out.add(2), vmovl_high_u32(vals));
        }
    }

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    #[inline(always)]
    unsafe fn store_u64x2(out: *mut u64, vals: std::arch::aarch64::uint64x2_t) {
        unsafe { std::arch::aarch64::vst1q_u64(out, vals) }
    }
}

/// Decodes the 128 packed `bit_width`-bit slots starting at `data[0]` into `out`.
///
/// `data` is the slice starting at the block and running to the end of the
/// data stream: the kernels read a few bytes past the block's own end, so
/// short slices fall back to a bounds-checked scalar path.
#[inline]
pub fn decode_block(bit_width: u8, data: &[u8], out: &mut [u64; BLOCK_LEN]) {
    decode_block_lanes(bit_width, data, out);
}

pub(crate) fn decode_block_lanes<L: Lane>(bit_width: u8, data: &[u8], out: &mut [L; BLOCK_LEN]) {
    let w = bit_width as usize;
    if w == 0 {
        out.fill(L::from_u64(0));
        return;
    }
    if w == 64 {
        let block = data.get(..BLOCK_LEN * 8).unwrap_or(data);
        for (i, o) in out.iter_mut().enumerate() {
            let bytes = block[i * 8..i * 8 + 8].try_into().unwrap();
            *o = L::from_u64(u64::from_le_bytes(bytes));
        }
        return;
    }
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    if simd_kernel_applies(bit_width) && data.len() >= 16 * w + 16 {
        if narrow_kernel_applies(bit_width) {
            unsafe { neon::decode_block_neon(w, data, out) };
        } else {
            unsafe { neon::decode_block_neon_wide(w, data, out) };
        }
        return;
    }

    #[cfg(target_arch = "x86_64")]
    if simd_kernel_applies(bit_width) && data.len() >= 16 * w + 16 {
        if narrow_kernel_applies(bit_width) {
            unsafe { avx2::decode_block_avx2(w, data, out) };
        } else {
            unsafe { avx2::decode_block_avx2_wide(w, data, out) };
        }
        return;
    }
    decode_block_scalar(w, data, out);
}

fn decode_block_scalar<L: Lane>(w: usize, data: &[u8], out: &mut [L; BLOCK_LEN]) {
    debug_assert!((1..=56).contains(&w));
    let mask = (1u64 << w) - 1;
    let needed = (BLOCK_LEN - 1) * w / 8 + 8;
    if needed <= data.len() {
        let block = &data[..needed];
        for (group, chunk) in out.chunks_exact_mut(8).enumerate() {
            let base = group * w;
            for (j, o) in chunk.iter_mut().enumerate() {
                let bit = j * w;
                let byte = base + bit / 8;
                let bytes = block[byte..byte + 8].try_into().unwrap();
                *o = L::from_u64((u64::from_le_bytes(bytes) >> (bit % 8)) & mask);
            }
        }
    } else {
        let unpacker = BitUnpacker::new(w as u8);
        for (i, o) in out.iter_mut().enumerate() {
            *o = L::from_u64(unpacker.get(i as u32, data));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BitPacker;

    fn pack_block(bit_width: u8, pad: usize) -> (Vec<u64>, Vec<u8>) {
        let max_val = if bit_width == 64 {
            u64::MAX
        } else if bit_width == 0 {
            0
        } else {
            (1u64 << bit_width) - 1
        };
        let vals: Vec<u64> = (0..BLOCK_LEN as u64)
            .map(|i| {
                if max_val == 0 {
                    0
                } else {
                    i.wrapping_mul(0x9E3779B97F4A7C15) % max_val.saturating_add(1)
                }
            })
            .collect();
        let mut data = Vec::new();
        let mut packer = BitPacker::new();
        for &val in &vals {
            packer.write(val, bit_width, &mut data).unwrap();
        }
        packer.close(&mut data).unwrap();
        data.resize(data.len() + pad, 0u8);
        (vals, data)
    }

    fn check_width(bit_width: u8, pad: usize) {
        let (vals, data) = pack_block(bit_width, pad);
        let mut out = [0u64; BLOCK_LEN];
        decode_block(bit_width, &data, &mut out);
        assert_eq!(&out[..], &vals[..], "width {bit_width} pad {pad}");
        if bit_width <= 32 {
            let mut out32 = [0u32; BLOCK_LEN];
            decode_block_lanes(bit_width, &data, &mut out32);
            let expected: Vec<u32> = vals.iter().map(|&v| v as u32).collect();
            assert_eq!(&out32[..], &expected[..], "width {bit_width} pad {pad} u32");
        }
    }

    #[test]
    fn test_decode_block_all_widths() {
        for bit_width in (0..=56u8).chain(std::iter::once(64u8)) {
            check_width(bit_width, 0);
            check_width(bit_width, 16);
        }
    }

    /// Sweeps both kernels against the scalar path, per width, in one process.
    ///
    /// ```text
    /// cargo test --release -p tantivy-bitpacker --lib kernel_sweep \
    ///     -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore]
    fn kernel_sweep() {
        use std::hint::black_box;
        use std::time::Instant;

        const REPS: usize = 20_000;
        assert!(simd_enabled(), "no kernel compiled in or CPU lacks it");
        println!("{:>5} {:>10} {:>10} {:>8}  kernel", "width", "simd_ns", "scalar_ns", "ratio");
        for bit_width in 1..=56u8 {
            let (vals, data) = pack_block(bit_width, 16);
            let mut out = [0u64; BLOCK_LEN];

            decode_block(bit_width, &data, &mut out);
            assert_eq!(&out[..], &vals[..], "width {bit_width}");

            let mut best = [f64::MAX; 2];
            for _ in 0..3 {
                let t0 = Instant::now();
                for _ in 0..REPS {
                    decode_block(bit_width, &data, &mut out);
                    black_box(out[0]);
                }
                best[0] = best[0].min(t0.elapsed().as_nanos() as f64 / REPS as f64);
                let t0 = Instant::now();
                for _ in 0..REPS {
                    decode_block_scalar(bit_width as usize, &data, &mut out);
                    black_box(out[0]);
                }
                best[1] = best[1].min(t0.elapsed().as_nanos() as f64 / REPS as f64);
            }
            let which = if narrow_kernel_applies(bit_width) {
                "narrow"
            } else {
                "wide"
            };
            println!(
                "{bit_width:>5} {:>10.1} {:>10.1} {:>8.2}  {which}",
                best[0],
                best[1],
                best[1] / best[0]
            );
        }
    }

    /// No packed width may fall through to the scalar unpacker: nothing else
    /// states that the wide kernel picks up exactly where the narrow one
    /// stops, and a gap is invisible to value tests.
    #[test]
    fn test_no_packed_width_falls_through_to_scalar() {
        if !simd_enabled() {
            for bit_width in 0..=64u8 {
                assert!(!simd_kernel_applies(bit_width));
            }
            return;
        }
        for bit_width in 1..=56u8 {
            assert!(
                simd_kernel_applies(bit_width),
                "width {bit_width} has no kernel"
            );
        }
        // 0 and 64 are served by `decode_block_lanes` itself.
        assert!(!simd_kernel_applies(0) && !simd_kernel_applies(64));
        // The narrow kernel's own limit: `bit_shift (<= 7) + w <= 32`.
        assert!(narrow_kernel_applies(25) && !narrow_kernel_applies(26));
    }

    #[test]
    fn test_decode_block_scalar_matches_kernel() {
        assert!(
            !simd_enabled() || simd_kernel_applies(8),
            "a kernel is available but the width gate rejects it"
        );
        for bit_width in 1..=56u8 {
            let (_, data) = pack_block(bit_width, 16);

            let mut kernel = [0u64; BLOCK_LEN];
            decode_block(bit_width, &data, &mut kernel);
            let mut scalar = [0u64; BLOCK_LEN];
            decode_block_scalar(bit_width as usize, &data, &mut scalar);
            assert_eq!(&kernel[..], &scalar[..], "u64 width {bit_width}");

            // The u32 lane takes a different store path in both kernels.
            let mut kernel32 = [0u32; BLOCK_LEN];
            decode_block_lanes(bit_width, &data, &mut kernel32);
            let mut scalar32 = [0u32; BLOCK_LEN];
            decode_block_scalar(bit_width as usize, &data, &mut scalar32);
            assert_eq!(&kernel32[..], &scalar32[..], "u32 width {bit_width}");
        }
    }
}
