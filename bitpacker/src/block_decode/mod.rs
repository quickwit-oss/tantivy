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

pub const BLOCK_LEN: usize = 128;
pub fn simd_enabled() -> bool {
    static SIMD: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SIMD.get_or_init(|| {
        if std::env::var_os("TANTIVY_BITPACKER_SCALAR").is_some() {
            return false;
        }
        #[cfg(target_arch = "x86_64")]
        return std::arch::is_x86_feature_detected!("avx2");
        #[cfg(not(target_arch = "x86_64"))]
        cfg!(all(target_arch = "aarch64", target_endian = "little"))
    })
}

/// Whether [`decode_groups`] reaches a SIMD kernel for this width.
///
/// Every packed width `1..=56` does where the target and CPU have kernels;
/// 0 and 64 are served by [`decode_groups`] itself.
#[inline(always)]
pub fn simd_kernel_applies(bit_width: u8) -> bool {
    (1..=56).contains(&bit_width) && simd_enabled()
}

/// Whether the *narrow* (`u32` lane) kernel takes this width: it needs
/// `bit_shift (<= 7) + bit_width <= 32`. Above that the wide (`u64` lane)
/// kernel takes over.
#[inline(always)]
fn narrow_kernel_applies(bit_width: u8) -> bool {
    (1..=25).contains(&bit_width)
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for u32 {}
    impl Sealed for u64 {}
}

/// Output lane of the block kernels. Sealed; implemented for `u32` and `u64`
pub trait Lane: Copy + sealed::Sealed {
    /// Lets the kernels pick their store shape with a branch that folds away
    /// at monomorphization.
    #[cfg_attr(
        not(any(
            target_arch = "x86_64",
            all(target_arch = "aarch64", target_endian = "little")
        )),
        allow(dead_code)
    )]
    const IS_U64: bool;

    fn from_u64(val: u64) -> Self;
}

impl Lane for u32 {
    const IS_U64: bool = false;

    #[inline(always)]
    fn from_u64(val: u64) -> u32 {
        val as u32
    }
}

impl Lane for u64 {
    const IS_U64: bool = true;

    #[inline(always)]
    fn from_u64(val: u64) -> u64 {
        val
    }
}

/// Extracts the single packed `num_bits`-bit slot `idx` via an unaligned
/// 8-byte load, shift and mask. Works at any index; no block alignment needed.
///
/// `mask` must be `BitUnpacker`'s value mask for `num_bits` (all ones for 64),
/// passed in precomputed so per-value callers pay nothing extra.
#[inline(always)]
pub(crate) fn decode_value(num_bits: usize, mask: u64, idx: usize, data: &[u8]) -> u64 {
    let addr_in_bits = idx * num_bits;
    let addr = addr_in_bits >> 3;
    let bit_shift = addr_in_bits & 7;
    if addr + 8 > data.len() {
        if num_bits == 0 {
            return 0;
        }
        return decode_value_slow_path(mask, addr, bit_shift as u32, data);
    }
    let bytes: [u8; 8] = (&data[addr..addr + 8]).try_into().unwrap();
    let val_unshifted_unmasked: u64 = u64::from_le_bytes(bytes);
    (val_unshifted_unmasked >> bit_shift) & mask
}

#[inline(never)]
fn decode_value_slow_path(mask: u64, addr: usize, bit_shift: u32, data: &[u8]) -> u64 {
    let mut bytes: [u8; 8] = [0u8; 8];
    let available_bytes = data.len() - addr;
    debug_assert!(available_bytes < 8);
    bytes[..available_bytes].copy_from_slice(&data[addr..]);
    let val_unshifted_unmasked: u64 = u64::from_le_bytes(bytes);
    (val_unshifted_unmasked >> bit_shift) & mask
}

#[inline(always)]
pub fn decode_range<L: Lane>(bit_width: u8, start_idx: usize, data: &[u8], output: &mut [L]) {
    let num_bits = bit_width as usize;
    let end_idx = start_idx + output.len();
    assert!(
        (end_idx * num_bits).div_ceil(8) <= data.len(),
        "Requested index is out of bounds."
    );

    let mask = if bit_width == 64 {
        !0u64
    } else {
        (1u64 << num_bits) - 1u64
    };

    let ramp = ((8 - start_idx % 8) % 8).min(output.len());
    let groups = (output.len() - ramp) / 8;
    if groups < MIN_KERNEL_GROUPS {
        for (i, out) in output.iter_mut().enumerate() {
            *out = L::from_u64(decode_value(num_bits, mask, start_idx + i, data));
        }
        return;
    }

    for (i, out) in output[..ramp].iter_mut().enumerate() {
        *out = L::from_u64(decode_value(num_bits, mask, start_idx + i, data));
    }
    let mid_start_bytes = (start_idx + ramp) * num_bits / 8;
    let done = ramp + groups * 8;
    decode_groups(bit_width, &data[mid_start_bytes..], &mut output[ramp..done]);
    for (i, out) in output[done..].iter_mut().enumerate() {
        *out = L::from_u64(decode_value(num_bits, mask, start_idx + done + i, data));
    }
}

const MIN_KERNEL_GROUPS: usize = 2;

#[inline(always)]
fn decode_groups<L: Lane>(bit_width: u8, data: &[u8], out: &mut [L]) {
    debug_assert_eq!(out.len() % 8, 0);
    if out.is_empty() {
        return;
    };
    let w = bit_width as usize;
    if w == 0 {
        out.fill(L::from_u64(0));
        return;
    }

    if w == 64 {
        let block = data.get(..out.len() * 8).unwrap_or(data);
        for (i, o) in out.iter_mut().enumerate() {
            let bytes = block[i * 8..i * 8 + 8].try_into().unwrap();
            *o = L::from_u64(u64::from_le_bytes(bytes));
        }
        return;
    }

    #[cfg(any(
        all(target_arch = "aarch64", target_endian = "little"),
        target_arch = "x86_64"
    ))]
    if simd_kernel_applies(bit_width) && data.len() >= out.len() / 8 * w + 16 {
        #[cfg(target_arch = "x86_64")]
        use avx2::{decode_block_avx2 as narrow_kernel, decode_block_avx2_wide as wide_kernel};
        #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
        use neon::{decode_block_neon as narrow_kernel, decode_block_neon_wide as wide_kernel};

        if narrow_kernel_applies(bit_width) {
            unsafe { narrow_kernel(w, data, out) }
        } else {
            unsafe { wide_kernel(w, data, out) }
        }
        return;
    }
    decode_groups_scalar(w, data, out);
}

fn decode_groups_scalar<L: Lane>(w: usize, data: &[u8], out: &mut [L]) {
    debug_assert!((1..=56).contains(&w));
    let mask = (1u64 << w) - 1;
    let needed = (out.len() - 1) * w / 8 + 8;
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
        for (i, o) in out.iter_mut().enumerate() {
            *o = L::from_u64(decode_value(w, mask, i, data));
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
        decode_groups(bit_width, &data, &mut out);
        assert_eq!(&out[..], &vals[..], "width {bit_width} pad {pad}");
        if bit_width <= 32 {
            let mut out32 = [0u32; BLOCK_LEN];
            decode_groups(bit_width, &data, &mut out32);
            let expected: Vec<u32> = vals.iter().map(|&v| v as u32).collect();
            assert_eq!(&out32[..], &expected[..], "width {bit_width} pad {pad} u32");
        }
    }

    #[test]
    fn test_decode_groups_all_widths() {
        for bit_width in (0..=56u8).chain(std::iter::once(64u8)) {
            check_width(bit_width, 0);
            check_width(bit_width, 16);
        }
    }

    #[test]
    fn test_decode_range_all_widths_and_offsets() {
        let num_vals = 3 * BLOCK_LEN + 17;
        for bit_width in (0..=56u8).chain(std::iter::once(64u8)) {
            let max_val = if bit_width == 64 {
                u64::MAX
            } else if bit_width == 0 {
                0
            } else {
                (1u64 << bit_width) - 1
            };
            let vals: Vec<u64> = (0..num_vals as u64)
                .map(|i| i.wrapping_mul(0x9E3779B97F4A7C15) & max_val)
                .collect();
            let mut data = Vec::new();
            let mut packer = BitPacker::new();
            for &val in &vals {
                packer.write(val, bit_width, &mut data).unwrap();
            }
            packer.close(&mut data).unwrap();

            // Every start phase modulo 8 crossed with every length up to just
            // past a block: the aligned middle runs on groups of 8, so the
            // entrance ramp, group count and exit ramp all have to line up for
            // lengths that are not multiples of 8 and never reach 128.
            for start in (0usize..24).chain([64, 127, 128, 129, 255, 256, 400]) {
                for len in (0usize..136).chain([256, num_vals - start]) {
                    if start + len > num_vals {
                        continue;
                    }
                    let mut out = vec![0u64; len];
                    decode_range(bit_width, start, &data, &mut out);
                    assert_eq!(
                        out,
                        &vals[start..start + len],
                        "w={bit_width} start={start} len={len}"
                    );
                    if bit_width <= 32 {
                        let mut out32 = vec![0u32; len];
                        decode_range(bit_width, start, &data, &mut out32);
                        let expected: Vec<u32> =
                            vals[start..start + len].iter().map(|&v| v as u32).collect();
                        assert_eq!(out32, expected, "w={bit_width} start={start} len={len} u32");
                    }
                }
            }
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
        println!(
            "{:>5} {:>10} {:>10} {:>8}  kernel",
            "width", "simd_ns", "scalar_ns", "ratio"
        );
        for bit_width in 1..=56u8 {
            let (vals, data) = pack_block(bit_width, 16);
            let mut out = [0u64; BLOCK_LEN];

            decode_groups(bit_width, &data, &mut out);
            assert_eq!(&out[..], &vals[..], "width {bit_width}");

            let mut best = [f64::MAX; 2];
            for _ in 0..3 {
                let t0 = Instant::now();
                for _ in 0..REPS {
                    decode_groups(bit_width, &data, &mut out);
                    black_box(out[0]);
                }
                best[0] = best[0].min(t0.elapsed().as_nanos() as f64 / REPS as f64);
                let t0 = Instant::now();
                for _ in 0..REPS {
                    decode_groups_scalar(bit_width as usize, &data, &mut out);
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
        // 0 and 64 are served by `decode_groups` itself.
        assert!(!simd_kernel_applies(0) && !simd_kernel_applies(64));
        // The narrow kernel's own limit: `bit_shift (<= 7) + w <= 32`.
        assert!(narrow_kernel_applies(25) && !narrow_kernel_applies(26));
    }

    #[test]
    fn test_decode_groups_scalar_matches_kernel() {
        assert!(
            !simd_enabled() || simd_kernel_applies(8),
            "a kernel is available but the width gate rejects it"
        );
        for bit_width in 1..=56u8 {
            let (_, data) = pack_block(bit_width, 16);

            let mut kernel = [0u64; BLOCK_LEN];
            decode_groups(bit_width, &data, &mut kernel);
            let mut scalar = [0u64; BLOCK_LEN];
            decode_groups_scalar(bit_width as usize, &data, &mut scalar);
            assert_eq!(&kernel[..], &scalar[..], "u64 width {bit_width}");

            // The u32 lane takes a different store path in both kernels.
            let mut kernel32 = [0u32; BLOCK_LEN];
            decode_groups(bit_width, &data, &mut kernel32);
            let mut scalar32 = [0u32; BLOCK_LEN];
            decode_groups_scalar(bit_width as usize, &data, &mut scalar32);
            assert_eq!(&kernel32[..], &scalar32[..], "u32 width {bit_width}");
        }
    }
}
