use std::io;
use std::ops::{Range, RangeInclusive};

use self::simd::{SimdLane, SimdStore};

#[cfg(target_arch = "x86_64")]
#[path = "bitpacker/avx2.rs"]
mod simd;

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[path = "bitpacker/neon.rs"]
mod simd;

#[cfg(not(any(
    all(target_arch = "aarch64", target_endian = "little"),
    target_arch = "x86_64"
)))]
#[path = "bitpacker/scalar.rs"]
mod simd;

#[cfg(all(
    target_arch = "aarch64",
    target_endian = "little",
    not(target_vendor = "apple")
))]
#[path = "bitpacker/sve.rs"]
mod sve;

pub struct BitPacker {
    mini_buffer: u64,
    mini_buffer_written: usize,
}

impl Default for BitPacker {
    fn default() -> Self {
        BitPacker::new()
    }
}
impl BitPacker {
    pub fn new() -> BitPacker {
        BitPacker {
            mini_buffer: 0u64,
            mini_buffer_written: 0,
        }
    }

    #[inline]
    pub fn write<TWrite: io::Write + ?Sized>(
        &mut self,
        val: u64,
        num_bits: u8,
        output: &mut TWrite,
    ) -> io::Result<()> {
        let num_bits = num_bits as usize;
        if self.mini_buffer_written + num_bits > 64 {
            self.mini_buffer |= val.wrapping_shl(self.mini_buffer_written as u32);
            output.write_all(self.mini_buffer.to_le_bytes().as_ref())?;
            self.mini_buffer = val.wrapping_shr((64 - self.mini_buffer_written) as u32);
            self.mini_buffer_written = self.mini_buffer_written + num_bits - 64;
        } else {
            self.mini_buffer |= val << self.mini_buffer_written;
            self.mini_buffer_written += num_bits;
            if self.mini_buffer_written == 64 {
                output.write_all(self.mini_buffer.to_le_bytes().as_ref())?;
                self.mini_buffer_written = 0;
                self.mini_buffer = 0u64;
            }
        }
        Ok(())
    }

    pub fn flush<TWrite: io::Write + ?Sized>(&mut self, output: &mut TWrite) -> io::Result<()> {
        if self.mini_buffer_written > 0 {
            let num_bytes = self.mini_buffer_written.div_ceil(8);
            let bytes = self.mini_buffer.to_le_bytes();
            output.write_all(&bytes[..num_bytes])?;
            self.mini_buffer_written = 0;
            self.mini_buffer = 0;
        }
        Ok(())
    }

    pub fn close<TWrite: io::Write + ?Sized>(&mut self, output: &mut TWrite) -> io::Result<()> {
        self.flush(output)?;
        Ok(())
    }
}

/// An output lane a block decodes into. The arch half, landing a SIMD register
/// of decoded values in these lanes, lives in each kernel file (`neon.rs`,
/// `avx2.rs`).
pub trait Lane: Copy + Default + 'static + SimdLane {
    const BITS: u8;

    /// Truncates a decoded value into the lane. Lossless whenever the width
    /// check above holds.
    fn from_u64(val: u64) -> Self;
}

impl Lane for u64 {
    const BITS: u8 = 64;

    #[inline(always)]
    fn from_u64(val: u64) -> u64 {
        val
    }
}

impl Lane for u32 {
    const BITS: u8 = 32;

    #[inline(always)]
    fn from_u64(val: u64) -> u32 {
        val as u32
    }
}

/// Where a decoded block lands, and what each value gets on the way: the
/// kernels write through this, so a transform that would otherwise be a second
/// pass over the block happens in-register.
///
/// The arch half, landing decoded SIMD registers in memory, lives in each
/// kernel file (`neon.rs`, `avx2.rs`).
pub trait Store: SimdStore<Self::Out> {
    type Out: Copy + Default + 'static;
    const BITS: u8;

    /// The per-value form, for the ramp, the tail and the scalar fallback.
    fn scalar(&self, val: u64) -> Self::Out;

    /// Whether this sink transforms values. Only SVE reads it, to stay off the
    /// affine path it cannot host.
    #[allow(dead_code)]
    const AFFINE: bool;

    /// `(min, gcd)` of this sink's transform, for SVE, which writes through a
    /// raw pointer rather than a sink. `(0, 1)` when it transforms nothing.
    #[allow(dead_code)]
    fn affine_params(&self) -> (u64, u64) {
        (0, 1)
    }
}

/// The identity sink: hands every value to [`Lane`] unchanged.
pub struct Plain<T>(std::marker::PhantomData<T>);

impl<T: Lane> Plain<T> {
    #[inline(always)]
    pub fn new() -> Self {
        Plain(std::marker::PhantomData)
    }
}

impl<T: Lane> Default for Plain<T> {
    #[inline(always)]
    fn default() -> Self {
        Plain::new()
    }
}

impl<T: Lane> Store for Plain<T> {
    type Out = T;
    const BITS: u8 = T::BITS;
    const AFFINE: bool = false;

    #[inline(always)]
    fn scalar(&self, val: u64) -> T {
        T::from_u64(val)
    }
}

/// `min + val` in-register: the `gcd == 1` case, which is most columns.
pub struct AffineAdd {
    pub min: u64,
}

impl Store for AffineAdd {
    type Out = u64;
    const BITS: u8 = 64;
    const AFFINE: bool = true;

    #[inline(always)]
    fn scalar(&self, val: u64) -> u64 {
        self.min.wrapping_add(val)
    }

    #[inline(always)]
    fn affine_params(&self) -> (u64, u64) {
        (self.min, 1)
    }
}

/// `min + gcd * val` in-register, for values that fit 32 bits.
///
/// NEON and AVX2 have no 64x64 integer multiply, but both have an exact
/// 32x32 -> 64 widening one (`vmull_u32`, `_mm256_mul_epu32`). Values fit u32
/// through width 32 even in the u64-lane kernel, so that is a single multiply;
/// wider would need a two-halves emulation that measured no faster than the
/// second pass it replaces, so [`AffineMul::fusable`] stops at 32.
pub struct AffineMul {
    pub min: u64,
    pub gcd: u32,
}

impl AffineMul {
    /// Whether `gcd` and this width can ride the fused multiply: values must
    /// fit the 32 bits the widening multiply reads, unless SVE decodes, whose
    /// `mul` is a true 64x64. With SVE present every `decode_block` path takes
    /// the SVE arm, so the 32-bit NEON store is never reached.
    #[inline(always)]
    pub fn fusable(gcd: u64, num_bits: usize) -> bool {
        gcd <= u32::MAX as u64 && (num_bits <= 32 || sve_wide_multiply())
    }
}

impl Store for AffineMul {
    type Out = u64;
    const BITS: u8 = 64;
    const AFFINE: bool = true;

    #[inline(always)]
    fn scalar(&self, val: u64) -> u64 {
        self.min.wrapping_add((self.gcd as u64).wrapping_mul(val))
    }

    #[inline(always)]
    fn affine_params(&self) -> (u64, u64) {
        (self.min, self.gcd as u64)
    }
}

#[derive(Clone, Debug, Default, Copy)]
pub struct BitUnpacker {
    num_bits: usize,
    mask: u64,
}

impl BitUnpacker {
    /// Creates a bit unpacker, that assumes the same bitwidth for all values.
    ///
    /// The bitunpacker works by doing an unaligned read of 8 bytes.
    /// For this reason, values of `num_bits` between
    /// [57..63] are forbidden.
    #[inline(always)]
    pub fn new(num_bits: u8) -> BitUnpacker {
        assert!(num_bits <= 7 * 8 || num_bits == 64);
        BitUnpacker {
            num_bits: usize::from(num_bits),
            mask: value_mask(num_bits),
        }
    }

    pub fn bit_width(&self) -> u8 {
        self.num_bits as u8
    }

    /// Keep the body to a single unaligned 8-byte load: this runs behind
    /// `Arc<dyn ColumnValues>`, where nothing hoists out of the caller's loop,
    /// so every branch here is paid per value. A width `match` plus an inlined
    /// tail costs 1.2-2.0x on `get_val` scans, and 1.1-1.5x on the short
    /// `get_range` takes that read through it per value.
    #[inline]
    pub fn get(&self, idx: u32, data: &[u8]) -> u64 {
        let addr_in_bits = idx as usize * self.num_bits;
        let addr = addr_in_bits >> 3;
        if addr + 8 > data.len() {
            if self.num_bits == 0 {
                return 0;
            }
            return self.get_slow_path(addr, (addr_in_bits & 7) as u32, data);
        }
        let bytes: [u8; 8] = data[addr..addr + 8].try_into().unwrap();
        (u64::from_le_bytes(bytes) >> (addr_in_bits & 7)) & self.mask
    }

    /// The tail, where fewer than 8 bytes remain. Out of line so [`Self::get`]
    /// stays a load, a shift and a mask.
    #[inline(never)]
    fn get_slow_path(&self, addr: usize, bit_shift: u32, data: &[u8]) -> u64 {
        let mut bytes = [0u8; 8];
        let available_bytes = data.len() - addr;
        debug_assert!(available_bytes < 8);
        bytes[..available_bytes].copy_from_slice(&data[addr..]);
        (u64::from_le_bytes(bytes) >> bit_shift) & self.mask
    }

    #[inline(always)]
    pub fn get_batch<T: Lane>(&self, start_idx: usize, data: &[u8], output: &mut [T]) {
        self.get_batch_into(&Plain::<T>::new(), start_idx, data, output);
    }

    /// Whether `min + gcd * val` rides inside the kernels for this width, i.e.
    /// whether [`Self::get_batch_affine`] beats a second pass. Adding `min`
    /// always does; the multiply only where `AffineMul::fusable`.
    #[inline(always)]
    pub fn affine_fusable(&self, gcd: u64) -> bool {
        self.num_bits < 64 && (gcd == 1 || AffineMul::fusable(gcd, self.num_bits))
    }

    /// Like [`Self::get_batch`] with `min + gcd * val` applied inside the kernels.
    /// Correct for any `min`/`gcd`; where [`Self::affine_fusable`] is false it
    /// falls back to a second pass, which a caller with its own unfused path
    /// does better inline.
    #[inline(always)]
    pub fn get_batch_affine(
        &self,
        start_idx: usize,
        data: &[u8],
        output: &mut [u64],
        min: u64,
        gcd: u64,
    ) {
        if self.num_bits >= 64 {
            // At 64 bits the decode is a plain load loop, so any sink with an
            // exact scalar form rides along, and beats a second pass at every
            // take (0.27 vs 1.0 ns/value at 16 rows, 0.15 vs 0.27 at 4096).
            if gcd == 1 {
                self.get_batch_into(&AffineAdd { min }, start_idx, data, output);
            } else if gcd <= u32::MAX as u64 {
                let sink = AffineMul {
                    min,
                    gcd: gcd as u32,
                };
                self.get_batch_into(&sink, start_idx, data, output);
            } else {
                self.get_batch_into(&Plain::<u64>::new(), start_idx, data, output);
                second_pass(output, min, gcd);
            }
        } else if gcd == 1 {
            self.get_batch_into(&AffineAdd { min }, start_idx, data, output);
        } else if AffineMul::fusable(gcd, self.num_bits) {
            let sink = AffineMul {
                min,
                gcd: gcd as u32,
            };
            self.get_batch_into(&sink, start_idx, data, output);
        } else {
            self.get_batch_into(&Plain::<u64>::new(), start_idx, data, output);
            second_pass(output, min, gcd);
        }
    }

    #[inline(always)]
    fn get_batch_into<S: Store>(
        &self,
        sink: &S,
        start_idx: usize,
        data: &[u8],
        output: &mut [S::Out],
    ) {
        debug_assert!(
            self.num_bits <= S::BITS as usize,
            "a {}-bit lane cannot hold {}-bit values",
            S::BITS,
            self.num_bits
        );
        let end_idx = start_idx + output.len();
        assert!(
            (end_idx * self.num_bits).div_ceil(8) <= data.len(),
            "Requested index is out of bounds."
        );

        let ramp = ((8 - start_idx % 8) % 8).min(output.len());
        let groups = (output.len() - ramp) / 8;
        if groups < MIN_KERNEL_GROUPS {
            for (i, out) in output.iter_mut().enumerate() {
                *out = sink.scalar(decode_value(self.num_bits, self.mask, start_idx + i, data));
            }
            return;
        }

        for (i, out) in output[..ramp].iter_mut().enumerate() {
            *out = sink.scalar(decode_value(self.num_bits, self.mask, start_idx + i, data));
        }
        let mid_start_bytes = (start_idx + ramp) * self.num_bits / 8;
        let done = ramp + groups * 8;
        decode_block(
            self.bit_width(),
            &data[mid_start_bytes..],
            &mut output[ramp..done],
            sink,
        );
        for (i, out) in output[done..].iter_mut().enumerate() {
            *out = sink.scalar(decode_value(
                self.num_bits,
                self.mask,
                start_idx + done + i,
                data,
            ));
        }
    }

    pub fn get_ids_for_value_range(
        &self,
        range: RangeInclusive<u64>,
        id_range: Range<u32>,
        data: &[u8],
        positions: &mut Vec<u32>,
    ) {
        #[cfg(any(
            all(target_arch = "aarch64", target_endian = "little"),
            target_arch = "x86_64"
        ))]
        if self.num_bits == 0 || fused_kernel_applies(self.bit_width()) {
            self.get_ids_for_value_range_fused(range, id_range, data, positions);
            return;
        }

        self.get_ids_for_value_range_slow(range, id_range, data, positions)
    }

    /// Decode and filter in one pass: values are range-tested in register as
    /// they unpack, so only surviving row ids reach memory.
    ///
    /// Three kernels cover every width, by decode lane and compare lane:
    ///
    /// | width | decode lane | compare lane |
    /// |-------|-------------|--------------|
    /// | `1..=25`  | `u32` | `u32` |
    /// | `26..=32` | `u64`, narrowed | `u32` |
    /// | `33..=56`, `64` | `u64` | `u64` |
    ///
    /// Width 0 is answered without decoding. The ramps around the
    /// group-aligned middle are scalar, as is everything when
    /// [`fused_kernel_applies`] is false.
    #[cfg(any(
        all(target_arch = "aarch64", target_endian = "little"),
        target_arch = "x86_64"
    ))]
    fn get_ids_for_value_range_fused(
        &self,
        value_range: RangeInclusive<u64>,
        id_range: Range<u32>,
        data: &[u8],
        positions: &mut Vec<u32>,
    ) {
        if self.num_bits == 0 {
            positions.clear();
            if value_range.contains(&0) {
                positions.extend(id_range);
            }
            return;
        }
        debug_assert!(fused_kernel_applies(self.bit_width()));
        positions.resize(id_range.len(), 0u32);
        let w = self.num_bits;
        let mut cursor = 0usize;

        let scalar_span = |from: u32, to: u32, cursor: &mut usize, positions: &mut Vec<u32>| {
            for i in from..to {
                let val = decode_value(w, self.mask, i as usize, data);
                positions[*cursor] = i;
                *cursor += usize::from(value_range.contains(&val));
            }
        };

        // For the `u32` kernels a range starting above `u32::MAX` selects
        // nothing and one ending above it clamps to `u32::MAX`.
        let range32 =
            || (*value_range.start() as u32)..=(*value_range.end()).min(u64::from(u32::MAX)) as u32;
        let start_over_u32 = *value_range.start() > u64::from(u32::MAX);

        // Kernels want a group-aligned start, so the ramp up to one is scalar;
        // they also read past their last group, so each arm caps its count
        // through `cap_by_data`.
        let ramp_end = id_range.start.next_multiple_of(8).min(id_range.end);
        scalar_span(id_range.start, ramp_end, &mut cursor, positions);
        let byte_off = ramp_end as usize * w / 8;

        // One backend covers the group-aligned middle, the rest is scalar; SVE
        // and NEON never mix in one call, so a measurement is never a blend.
        let mut handled_end = ramp_end;

        // SVE compacts a whole vector of ids in one instruction where NEON does
        // 4 through a lookup table, so it takes the range when present.
        #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
        let sve_takes_it = match sve::filter_middle(
            w,
            (id_range.end - ramp_end) as usize,
            &value_range,
            ramp_end,
            &data[byte_off..],
            positions[cursor..].as_mut_ptr(),
        ) {
            Some((written, covered)) => {
                cursor += written;
                handled_end = ramp_end + covered as u32;
                true
            }
            None => false,
        };
        #[cfg(not(all(target_arch = "aarch64", not(target_vendor = "apple"))))]
        let sve_takes_it = false;

        if !sve_takes_it {
            let num_groups = cap_by_data(
                ((id_range.end - ramp_end) / 8) as usize,
                byte_off,
                data.len(),
                |groups| groups * w + 16,
            );
            if num_groups > 0 {
                let block = &data[byte_off..];
                let out = positions[cursor..].as_mut_ptr();
                cursor += unsafe {
                    if self.bit_width() > 32 {
                        simd::decode_filter64(
                            w,
                            block,
                            num_groups,
                            value_range.clone(),
                            ramp_end,
                            out,
                        )
                    } else if start_over_u32 {
                        0
                    } else if narrow_kernel_applies(self.bit_width()) {
                        simd::decode_filter(w, block, num_groups, range32(), ramp_end, out)
                    } else {
                        simd::decode_filter_wide(w, block, num_groups, range32(), ramp_end, out)
                    }
                };
                handled_end = ramp_end + (num_groups * 8) as u32;
            }
        }
        scalar_span(handled_end, id_range.end, &mut cursor, positions);
        positions.truncate(cursor);
    }

    fn get_ids_for_value_range_slow(
        &self,
        range: RangeInclusive<u64>,
        id_range: Range<u32>,
        data: &[u8],
        positions: &mut Vec<u32>,
    ) {
        positions.clear();
        for i in id_range {
            // If we cared we could make this branchless, but the slow implementation should rarely
            // kick in.
            let val = self.get(i, data);
            if range.contains(&val) {
                positions.push(i);
            }
        }
    }
}

// Batch unpacking, 8 values at a time: 8 consecutive values occupy exactly
// `bit_width` bytes, so every group starts byte aligned and its (byte offset,
// bit shift) pattern is constant per width, which is what lets the kernels
// shuffle a group at once. Widths crate-wide are `0..=56` or exactly `64`
// ([`crate::compute_num_bits`]); the kernels rely on that.

/// Values per block in the columnar codecs' layout. The kernels work on groups
/// of 8 and do not depend on it.
pub const BLOCK_LEN: usize = 128;

/// The low `num_bits` bits set. Every masking site derives from this one, so
/// the single-value and batch paths cannot disagree.
#[inline(always)]
fn value_mask(num_bits: u8) -> u64 {
    if num_bits == 64 {
        !0u64
    } else {
        (1u64 << num_bits) - 1u64
    }
}

pub fn simd_enabled() -> bool {
    static SIMD: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SIMD.get_or_init(|| {
        if std::env::var_os("TANTIVY_BITPACKER_SCALAR").is_some() {
            return false;
        }

        // SVE is an aarch64 feature; on x86_64 AVX2 is the only backend.
        #[cfg(target_arch = "x86_64")]
        return std::arch::is_x86_feature_detected!("avx2");

        #[cfg(target_arch = "aarch64")]
        return std::arch::is_aarch64_feature_detected!("neon")
            || std::arch::is_aarch64_feature_detected!("sve");

        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
        return false;
    })
}

/// Whether [`decode_block`] reaches a SIMD kernel for this width: every packed
/// width `1..=56` where the CPU has kernels; 0 and 64 it serves itself.
#[inline(always)]
#[cfg(any(
    all(target_arch = "aarch64", target_endian = "little"),
    target_arch = "x86_64"
))]
fn simd_kernel_applies(bit_width: u8) -> bool {
    (1..=56).contains(&bit_width) && simd_enabled()
}

/// Whether a fused decode-and-filter kernel takes this width: any packed width
/// or 64, where the CPU has kernels. Width 0 is answered without decoding.
#[inline(always)]
#[cfg(any(
    all(target_arch = "aarch64", target_endian = "little"),
    target_arch = "x86_64"
))]
fn fused_kernel_applies(bit_width: u8) -> bool {
    ((1..=56).contains(&bit_width) || bit_width == 64) && simd_enabled()
}

/// The largest `count` of kernel steps or groups whose reads stay inside
/// `data`: kernels read past their last group, so the caller trims the count
/// rather than the kernels branching per load.
#[inline(always)]
#[cfg(any(
    all(target_arch = "aarch64", target_endian = "little"),
    target_arch = "x86_64"
))]
fn cap_by_data(
    mut count: usize,
    byte_off: usize,
    data_len: usize,
    bytes_read: impl Fn(usize) -> usize,
) -> usize {
    while count > 0 && byte_off + bytes_read(count) > data_len {
        count -= 1;
    }
    count
}

/// Whether the narrow (`u32` lane) kernel takes this width: `bit_shift (<= 7)
/// + bit_width` must fit 32. Above that the wide (`u64` lane) kernel takes over.
#[inline(always)]
#[cfg(any(
    all(target_arch = "aarch64", target_endian = "little"),
    target_arch = "x86_64"
))]
fn narrow_kernel_applies(bit_width: u8) -> bool {
    (1..=25).contains(&bit_width)
}

/// Extracts packed slot `idx` via an unaligned 8-byte load, shift and mask, at
/// any index. `mask` is [`value_mask`] for `num_bits`, precomputed by the
/// caller.
#[inline(always)]
fn decode_value(num_bits: usize, mask: u64, idx: usize, data: &[u8]) -> u64 {
    let addr_in_bits = idx * num_bits;
    let addr = addr_in_bits >> 3;
    let bit_shift = addr_in_bits & 7;
    let word = if addr + 8 <= data.len() {
        u64::from_le_bytes(data[addr..addr + 8].try_into().unwrap())
    } else if data.len() >= 8 {
        // The tail: load the last 8 bytes instead and shift the ones before
        // `addr` away, so the bytes past the end read as zero.
        let last8 = u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap());
        last8
            .checked_shr((addr - (data.len() - 8)) as u32 * 8)
            .unwrap_or(0)
    } else {
        let mut bytes = [0u8; 8];
        bytes[..data.len() - addr].copy_from_slice(&data[addr..]);
        u64::from_le_bytes(bytes)
    };
    (word >> bit_shift) & mask
}

/// `min + gcd * val` over a decoded block, for the widths no sink fuses. The
/// `gcd == 1` arm is load-bearing: a runtime multiply by 1 is a scalar multiply
/// per value (no 64-bit vector multiply on NEON/AVX2), the add-only form
/// vectorizes; 2x at width 64.
#[inline(always)]
fn second_pass(output: &mut [u64], min: u64, gcd: u64) {
    if gcd == 1 {
        for o in output.iter_mut() {
            *o = min.wrapping_add(*o);
        }
    } else {
        for o in output.iter_mut() {
            *o = min.wrapping_add(gcd.wrapping_mul(*o));
        }
    }
}

/// Whether the decode will multiply full 64-bit lanes, i.e. run on SVE.
#[inline(always)]
fn sve_wide_multiply() -> bool {
    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
    {
        simd_enabled() && sve::lanes64() > 0
    }
    #[cfg(not(all(target_arch = "aarch64", not(target_vendor = "apple"))))]
    {
        false
    }
}

const MIN_KERNEL_GROUPS: usize = 2;

#[inline(always)]
fn decode_block<S: Store>(bit_width: u8, data: &[u8], out: &mut [S::Out], sink: &S) {
    debug_assert_eq!(out.len() % 8, 0);
    if out.is_empty() {
        return;
    };
    let w = bit_width as usize;
    if w == 0 {
        out.fill(sink.scalar(0));
        return;
    }

    if w == 64 {
        let block = &data[..out.len() * 8];
        for (o, bytes) in out.iter_mut().zip(block.chunks_exact(8)) {
            *o = sink.scalar(u64::from_le_bytes(bytes.try_into().unwrap()));
        }
        return;
    }

    // SVE covers every width in one kernel and takes the block outright when
    // present, so a measurement is never a blend with NEON. `u64` lanes only:
    // the kernel ends in `svst1_u64` and narrowing would be a different
    // instruction, not a different store; `S::BITS` is constant, so a `u32`
    // lane folds this branch away.
    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
    if S::BITS == 64 && simd_enabled() && sve::lanes64() > 0 {
        let out_ptr = out.as_mut_ptr() as *mut u64;
        let (min, gcd) = sink.affine_params();
        let done = sve::decode_middle(w, data, out.len(), out_ptr, min, gcd);
        if done < out.len() {
            // Whatever a whole step could not cover finishes scalar, never NEON.
            decode_block_scalar(w, &data[done * w / 8..], &mut out[done..], sink);
        }
        return;
    }

    #[cfg(any(
        all(target_arch = "aarch64", target_endian = "little"),
        target_arch = "x86_64"
    ))]
    if simd_kernel_applies(bit_width) && data.len() >= out.len() / 8 * w + 16 {
        if narrow_kernel_applies(bit_width) {
            unsafe { simd::decode_block(w, data, out, sink) }
        } else {
            unsafe { simd::decode_block_wide(w, data, out, sink) }
        }
        return;
    }
    decode_block_scalar(w, data, out, sink);
}

fn decode_block_scalar<S: Store>(w: usize, data: &[u8], out: &mut [S::Out], sink: &S) {
    debug_assert!((1..=56).contains(&w));
    let mask = value_mask(w as u8);
    let needed = (out.len() - 1) * w / 8 + 8;
    if needed <= data.len() {
        let block = &data[..needed];
        for (group, chunk) in out.chunks_exact_mut(8).enumerate() {
            let base = group * w;
            for (j, o) in chunk.iter_mut().enumerate() {
                let bit = j * w;
                let byte = base + bit / 8;
                let bytes = block[byte..byte + 8].try_into().unwrap();
                *o = sink.scalar((u64::from_le_bytes(bytes) >> (bit % 8)) & mask);
            }
        }
    } else {
        for (i, o) in out.iter_mut().enumerate() {
            *o = sink.scalar(decode_value(w, mask, i, data));
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn create_bitpacker(len: usize, num_bits: u8) -> (BitUnpacker, Vec<u64>, Vec<u8>) {
        let mut data = Vec::new();
        let mut bitpacker = BitPacker::new();
        let max_val: u64 = (1u64 << num_bits as u64) - 1u64;
        let vals: Vec<u64> = (0u64..len as u64)
            .map(|i| if max_val == 0 { 0 } else { i % max_val })
            .collect();
        for &val in &vals {
            bitpacker.write(val, num_bits, &mut data).unwrap();
        }
        bitpacker.close(&mut data).unwrap();
        assert_eq!(data.len(), ((num_bits as usize) * len).div_ceil(8));
        let bitunpacker = BitUnpacker::new(num_bits);
        (bitunpacker, vals, data)
    }

    fn test_bitpacker_util(len: usize, num_bits: u8) {
        let (bitunpacker, vals, data) = create_bitpacker(len, num_bits);
        for (i, val) in vals.iter().enumerate() {
            assert_eq!(bitunpacker.get(i as u32, &data), *val);
        }
    }

    /// A/Bs the fused decode-and-filter route against the scalar per-value
    /// loop it replaces, per width and selectivity, plus -- for the widths
    /// that have both -- the narrowing `decode_filter_wide` kernel against
    /// `decode_filter64`, which also covers them.
    ///
    /// ```text
    /// cargo test --release -p tantivy-bitpacker ab_filter_routes \
    ///     -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore]
    fn ab_filter_routes() {
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 1 << 20;
        const ROUNDS: usize = 9;

        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };

        println!(
            "{:>5} {:>6} {:>9} {:>9} {:>8} {:>9} {:>9} {:>8}",
            "width", "sel%", "scalar", "fused", "delta", "wide", "via64", "delta"
        );
        for bits in [7u8, 20, 26, 30, 32, 33, 40, 64] {
            let max = if bits == 64 {
                u64::MAX
            } else {
                (1u64 << bits) - 1
            };
            let mut data = Vec::new();
            let mut packer = BitPacker::new();
            for _ in 0..N {
                packer.write(next() & max, bits, &mut data).unwrap();
            }
            packer.close(&mut data).unwrap();
            let unpacker = BitUnpacker::new(bits);

            for sel in [1u64, 10, 50] {
                // Values are uniform, so the selectivity is the range fraction.
                let hi = if sel == 100 {
                    max
                } else {
                    ((max as u128 * sel as u128) / 100) as u64
                };
                let range = 0..=hi;
                let mut a = Vec::new();
                let mut b = Vec::new();

                let run_scalar = |out: &mut Vec<u32>| {
                    let t = Instant::now();
                    unpacker.get_ids_for_value_range_slow(range.clone(), 0..N as u32, &data, out);
                    black_box(out.len());
                    t.elapsed().as_nanos() as f64 / N as f64
                };
                let run_fused = |out: &mut Vec<u32>| {
                    let t = Instant::now();
                    unpacker.get_ids_for_value_range(range.clone(), 0..N as u32, &data, out);
                    black_box(out.len());
                    t.elapsed().as_nanos() as f64 / N as f64
                };
                let (mut scalar, mut fused) = (f64::MAX, f64::MAX);
                for round in 0..ROUNDS {
                    let (ta, tb) = if round % 2 == 0 {
                        let ta = run_scalar(&mut a);
                        let tb = run_fused(&mut b);
                        (ta, tb)
                    } else {
                        let tb = run_fused(&mut b);
                        let ta = run_scalar(&mut a);
                        (ta, tb)
                    };
                    assert_eq!(a, b, "width {bits} sel {sel}");
                    scalar = scalar.min(ta);
                    fused = fused.min(tb);
                }
                let delta = (fused - scalar) / scalar * 100.0;

                // The narrowing kernel vs the u64 one, over the same aligned
                // middle, on the widths both can take.
                let (mut wide, mut via64) = (f64::MAX, f64::MAX);
                #[cfg(any(
                    all(target_arch = "aarch64", target_endian = "little"),
                    target_arch = "x86_64"
                ))]
                if (26..=32).contains(&bits) && simd_enabled() {
                    let w = bits as usize;
                    let mut groups = N / 8;
                    while groups > 0 && groups * w + 16 > data.len() {
                        groups -= 1;
                    }
                    let r32 = 0u32..=hi.min(u64::from(u32::MAX)) as u32;
                    let mut out = vec![0u32; N];
                    let out_ptr = out.as_mut_ptr();
                    for round in 0..ROUNDS {
                        let run_w = || {
                            let t = Instant::now();
                            let n = unsafe {
                                simd::decode_filter_wide(w, &data, groups, r32.clone(), 0, out_ptr)
                            };
                            black_box(n);
                            t.elapsed().as_nanos() as f64 / (groups * 8) as f64
                        };
                        let run_64 = || {
                            let t = Instant::now();
                            let n = unsafe {
                                simd::decode_filter64(w, &data, groups, range.clone(), 0, out_ptr)
                            };
                            black_box(n);
                            t.elapsed().as_nanos() as f64 / (groups * 8) as f64
                        };
                        if round % 2 == 0 {
                            wide = wide.min(run_w());
                            via64 = via64.min(run_64());
                        } else {
                            via64 = via64.min(run_64());
                            wide = wide.min(run_w());
                        }
                    }
                }
                if wide < f64::MAX {
                    let kd = (via64 - wide) / wide * 100.0;
                    println!(
                        "{bits:>5} {sel:>6} {scalar:>8.2}n {fused:>8.2}n {delta:>7.1}% \
                         {wide:>8.2}n {via64:>8.2}n {kd:>7.1}%"
                    );
                } else {
                    println!("{bits:>5} {sel:>6} {scalar:>8.2}n {fused:>8.2}n {delta:>7.1}%");
                }
            }
        }
    }

    #[test]
    fn test_bitpacker() {
        test_bitpacker_util(10, 3);
        test_bitpacker_util(10, 0);
        test_bitpacker_util(10, 1);
        test_bitpacker_util(6, 14);
        test_bitpacker_util(1000, 14);
    }

    use proptest::prelude::*;

    fn num_bits_strategy() -> impl Strategy<Value = u8> {
        prop_oneof!(Just(0), Just(1), 2u8..56u8, Just(56), Just(64),)
    }

    fn vals_strategy() -> impl Strategy<Value = (u8, Vec<u64>)> {
        (num_bits_strategy(), 0usize..100usize).prop_flat_map(|(num_bits, len)| {
            let max_val = if num_bits == 64 {
                u64::MAX
            } else {
                (1u64 << num_bits as u32) - 1
            };
            let vals = proptest::collection::vec(0..=max_val, len);
            vals.prop_map(move |vals| (num_bits, vals))
        })
    }

    fn test_bitpacker_aux(num_bits: u8, vals: &[u64]) {
        let mut buffer: Vec<u8> = Vec::new();
        let mut bitpacker = BitPacker::new();
        for &val in vals {
            bitpacker.write(val, num_bits, &mut buffer).unwrap();
        }
        bitpacker.flush(&mut buffer).unwrap();
        assert_eq!(buffer.len(), (vals.len() * num_bits as usize).div_ceil(8));
        let bitunpacker = BitUnpacker::new(num_bits);
        let max_val = if num_bits == 64 {
            u64::MAX
        } else {
            (1u64 << num_bits) - 1
        };
        for (i, val) in vals.iter().copied().enumerate() {
            assert!(val <= max_val);
            assert_eq!(bitunpacker.get(i as u32, &buffer), val);
        }
    }

    /// A/Bs the fused affine decode against the two-pass form it replaces:
    /// `get_batch` into the caller's slice, then `min + gcd * v` over it.
    ///
    /// Both arms run in this binary against the same buffer, so the comparison
    /// does not cross a process boundary.
    ///
    /// ```text
    /// cargo test --release -p tantivy-bitpacker ab_affine_fusion \
    ///     -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore]
    fn ab_affine_fusion() {
        use std::hint::black_box;
        use std::time::Instant;

        const REPS: usize = 20_000;
        const ROUNDS: usize = 7;

        println!("simd_enabled={}", simd_enabled());
        for (min, gcd) in [(1_000_000u64, 1u64), (1_000_000, 100)] {
            println!("\nmin={min} gcd={gcd}");
            println!(
                "{:>5} {:>10} {:>10} {:>9}",
                "width", "two_pass", "fused", "delta"
            );
            for bit_width in [1u8, 4, 8, 12, 17, 25, 26, 33, 40, 56, 64] {
                let (_, data) = pack_block(bit_width, 16);
                let unpacker = BitUnpacker::new(bit_width);
                let mut out = [0u64; BLOCK_LEN];
                let mut want = [0u64; BLOCK_LEN];
                unpacker.get_batch_affine(0, &data, &mut want, min, gcd);
                let (mut two_pass, mut fused) = (f64::MAX, f64::MAX);
                for _ in 0..ROUNDS {
                    let t = Instant::now();
                    for _ in 0..REPS {
                        unpacker.get_batch(0, &data, &mut out);
                        if gcd == 1 {
                            for o in out.iter_mut() {
                                *o += min;
                            }
                        } else {
                            for o in out.iter_mut() {
                                *o = min + gcd * *o;
                            }
                        }
                        black_box(out[0]);
                    }
                    two_pass = two_pass.min(t.elapsed().as_nanos() as f64 / REPS as f64);
                    assert_eq!(&out[..], &want[..], "two-pass width {bit_width}");

                    let t = Instant::now();
                    for _ in 0..REPS {
                        unpacker.get_batch_affine(0, &data, &mut out, min, gcd);
                        black_box(out[0]);
                    }
                    fused = fused.min(t.elapsed().as_nanos() as f64 / REPS as f64);
                    assert_eq!(&out[..], &want[..], "fused width {bit_width}");
                }
                let delta = (fused - two_pass) / two_pass * 100.0;
                println!("{bit_width:>5} {two_pass:>10.1} {fused:>10.1} {delta:>8.1}%");
            }
        }
    }

    fn affine_reference(vals: &[u64], min: u64, gcd: u64) -> Vec<u64> {
        vals.iter()
            .map(|&v| min.wrapping_add(gcd.wrapping_mul(v)))
            .collect()
    }

    fn test_get_batch_affine_aux(num_bits: u8, vals: &[u64], min: u64, gcd: u64) {
        let mut buffer: Vec<u8> = Vec::new();
        let mut bitpacker = BitPacker::new();
        for &val in vals {
            bitpacker.write(val, num_bits, &mut buffer).unwrap();
        }
        bitpacker.close(&mut buffer).unwrap();
        let unpacker = BitUnpacker::new(num_bits);
        let want = affine_reference(vals, min, gcd);
        // Every (start, len) phase: the ramp, the kernel groups and the tail
        // each apply the transform on their own path.
        for start in 0..vals.len().min(20) {
            for len in 0..=(vals.len() - start) {
                let mut got = vec![0u64; len];
                unpacker.get_batch_affine(start, &buffer, &mut got, min, gcd);
                assert_eq!(
                    got,
                    want[start..start + len],
                    "num_bits={num_bits} start={start} len={len} min={min} gcd={gcd}"
                );
            }
        }
    }

    #[test]
    fn test_get_batch_affine() {
        for num_bits in [0u8, 1, 3, 8, 17, 25, 26, 33, 56, 64] {
            let max = if num_bits >= 64 {
                u64::MAX
            } else if num_bits == 0 {
                0
            } else {
                (1u64 << num_bits) - 1
            };
            let vals: Vec<u64> = (0..300u64)
                // `max` is always `2^n - 1`, so masking keeps every value in
                // range without a `max + 1` that overflows at 64 bits.
                .map(|i| i.wrapping_mul(7919) & max)
                .collect();
            for (min, gcd) in [
                (0u64, 1u64),
                (5, 1),
                (0, 7),
                (1_000_000, 100),
                // Wider than 32 bits: rides the unfused fallback.
                (17, 1u64 << 33),
                (u64::MAX / 2, 3),
            ] {
                test_get_batch_affine_aux(num_bits, &vals, min, gcd);
            }
        }
    }

    proptest::proptest! {
        #[test]
        fn test_bitpacker_proptest((num_bits, vals) in vals_strategy()) {
            test_bitpacker_aux(num_bits, &vals);
        }

        #[test]
        fn test_get_batch_affine_proptest(
            (num_bits, vals) in vals_strategy(),
            min in proptest::prelude::any::<u64>(),
            gcd in proptest::prelude::any::<u64>(),
        ) {
            test_get_batch_affine_aux(num_bits, &vals, min, gcd.max(1));
        }
    }

    #[test]
    fn test_value_mask() {
        assert_eq!(value_mask(0), 0);
        assert_eq!(value_mask(1), 1);
        assert_eq!(value_mask(32), u32::MAX as u64);
        assert_eq!(value_mask(56), (1u64 << 56) - 1);
        assert_eq!(value_mask(64), u64::MAX);
        for num_bits in (0..=56u8).chain(std::iter::once(64u8)) {
            assert_eq!(BitUnpacker::new(num_bits).mask, value_mask(num_bits));
        }
    }

    #[test]
    fn test_get_batch_limit() {
        let bitunpacker = BitUnpacker::new(1);
        let mut output: [u64; 3] = [0u64; 3];
        bitunpacker.get_batch(8 * 4 - 3, &[0u8, 0u8, 0u8, 0u8], &mut output[..]);
    }

    #[test]
    #[should_panic]
    fn test_get_batch_panics_when_off_scope() {
        let bitunpacker = BitUnpacker::new(1);
        let mut output: [u64; 3] = [0u64; 3];
        // We are missing exactly one bit.
        bitunpacker.get_batch(8 * 4 - 2, &[0u8, 0u8, 0u8, 0u8], &mut output[..]);
    }

    /// Lengths that straddle the block grid: none, one and several whole
    /// blocks, plus both ramps.
    const BATCH_LENS: [usize; 12] = [0, 1, 2, 32, 33, 64, 127, 128, 129, 135, 256, 300];
    const NUM_VALS: u64 = 600;

    proptest::proptest! {
        #[test]
        fn test_get_batch_proptest((num_bits, _vals) in vals_strategy()) {
            let mask = value_mask(num_bits);
            // Values fill the whole width, so a kernel dropping high bits
            // cannot pass.
            let vals: Vec<u64> = (0..NUM_VALS)
                .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) & mask)
                .collect();
            let mut buffer: Vec<u8> = Vec::new();
            let mut bitpacker = BitPacker::new();
            for &val in &vals {
                bitpacker.write(val, num_bits, &mut buffer).unwrap();
            }
            bitpacker.flush(&mut buffer).unwrap();
            let bitunpacker = BitUnpacker::new(num_bits);
            let mut output: Vec<u64> = Vec::new();
            for len in BATCH_LENS {
                for start_idx in 0u32..32u32 {
                    output.resize(len, 0);
                    bitunpacker.get_batch(start_idx as usize, &buffer, &mut output);
                    for (i, got) in output.iter().enumerate() {
                        let idx = start_idx + i as u32;
                        assert_eq!(*got, vals[idx as usize], "num_bits {num_bits} len {len} idx {idx}");
                        assert_eq!(*got, bitunpacker.get(idx, &buffer));
                    }
                }
            }
        }
    }

    fn pack_block(bit_width: u8, pad: usize) -> (Vec<u64>, Vec<u8>) {
        let max_val = value_mask(bit_width);
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
        decode_block(bit_width, &data, &mut out, &Plain::<u64>::new());
        assert_eq!(&out[..], &vals[..], "width {bit_width} pad {pad}");
    }

    #[test]
    fn test_decode_block_all_widths() {
        for bit_width in (0..=56u8).chain(std::iter::once(64u8)) {
            check_width(bit_width, 0);
            check_width(bit_width, 16);
        }
    }

    #[test]
    fn test_get_batch_all_widths_and_offsets() {
        let num_vals = 3 * BLOCK_LEN + 17;
        for bit_width in (0..=56u8).chain(std::iter::once(64u8)) {
            let mask = value_mask(bit_width);
            let vals: Vec<u64> = (0..num_vals as u64)
                .map(|i| i.wrapping_mul(0x9E3779B97F4A7C15) & mask)
                .collect();
            let mut data = Vec::new();
            let mut packer = BitPacker::new();
            for &val in &vals {
                packer.write(val, bit_width, &mut data).unwrap();
            }
            packer.close(&mut data).unwrap();
            let bitunpacker = BitUnpacker::new(bit_width);

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
                    bitunpacker.get_batch(start, &data, &mut out);
                    assert_eq!(
                        out,
                        &vals[start..start + len],
                        "w={bit_width} start={start} len={len}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_get_batch_u32_lane_matches_u64() {
        let num_vals = 3 * BLOCK_LEN + 17;
        for bit_width in 0..=32u8 {
            let mask = value_mask(bit_width);
            let vals: Vec<u64> = (0..num_vals as u64)
                .map(|i| i.wrapping_mul(0x9E3779B97F4A7C15) & mask)
                .collect();
            let mut data = Vec::new();
            let mut packer = BitPacker::new();
            for &val in &vals {
                packer.write(val, bit_width, &mut data).unwrap();
            }
            packer.close(&mut data).unwrap();
            let bitunpacker = BitUnpacker::new(bit_width);

            for start in (0usize..24).chain([64, 127, 128, 129, 255, 256, 400]) {
                for len in (0usize..136).chain([256, num_vals - start]) {
                    if start + len > num_vals {
                        continue;
                    }
                    let mut narrow = vec![0u32; len];
                    bitunpacker.get_batch(start, &data, &mut narrow);
                    let expected: Vec<u32> =
                        vals[start..start + len].iter().map(|&v| v as u32).collect();
                    assert_eq!(narrow, expected, "w={bit_width} start={start} len={len}");
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

            decode_block(bit_width, &data, &mut out, &Plain::<u64>::new());
            assert_eq!(&out[..], &vals[..], "width {bit_width}");

            let mut best = [f64::MAX; 2];
            for _ in 0..3 {
                let t0 = Instant::now();
                for _ in 0..REPS {
                    decode_block(bit_width, &data, &mut out, &Plain::<u64>::new());
                    black_box(out[0]);
                }
                best[0] = best[0].min(t0.elapsed().as_nanos() as f64 / REPS as f64);
                let t0 = Instant::now();
                for _ in 0..REPS {
                    decode_block_scalar(bit_width as usize, &data, &mut out, &Plain::<u64>::new());
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
        // 0 and 64 are served by `decode_block` itself.
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
            decode_block(bit_width, &data, &mut kernel, &Plain::<u64>::new());
            let mut scalar = [0u64; BLOCK_LEN];
            decode_block_scalar(bit_width as usize, &data, &mut scalar, &Plain::<u64>::new());
            assert_eq!(&kernel[..], &scalar[..], "u64 width {bit_width}");
        }
    }

    /// Packs `NUM_FILTER_VALS` values spread over the full width, and returns
    /// them alongside the packed bytes.
    fn pack_spread(bit_width: u8) -> (Vec<u64>, Vec<u8>) {
        let mask = value_mask(bit_width);
        let vals: Vec<u64> = (0..NUM_FILTER_VALS as u64)
            .map(|i| i.wrapping_mul(0x9E3779B97F4A7C15) & mask)
            .collect();
        let mut data = Vec::new();
        let mut packer = BitPacker::new();
        for &val in &vals {
            packer.write(val, bit_width, &mut data).unwrap();
        }
        packer.close(&mut data).unwrap();
        (vals, data)
    }

    const NUM_FILTER_VALS: usize = 100_000;

    #[test]
    fn test_get_ids_for_value_range_fused_matches_scalar() {
        for bit_width in [1u8, 7, 10, 16, 20, 25, 26, 32, 33, 40, 56, 64] {
            let (vals, data) = pack_spread(bit_width);
            let mask = value_mask(bit_width);
            let unpacker = BitUnpacker::new(bit_width);
            for frac in [0u64, 1, 3, 8] {
                let hi = if frac == 0 { 0 } else { mask / 8 * frac };
                let range = 0..=hi;
                for id_range in [0u32..NUM_FILTER_VALS as u32, 5..1_003, 0..1] {
                    let expected: Vec<u32> = id_range
                        .clone()
                        .filter(|&i| range.contains(&vals[i as usize]))
                        .collect();

                    let mut got = Vec::new();
                    unpacker.get_ids_for_value_range(
                        range.clone(),
                        id_range.clone(),
                        &data,
                        &mut got,
                    );
                    assert_eq!(got, expected, "existing w={bit_width} frac={frac}");

                    #[cfg(any(
                        all(target_arch = "aarch64", target_endian = "little"),
                        target_arch = "x86_64"
                    ))]
                    {
                        let mut fused = Vec::new();
                        unpacker.get_ids_for_value_range_fused(
                            range.clone(),
                            id_range.clone(),
                            &data,
                            &mut fused,
                        );
                        assert_eq!(fused, expected, "fused w={bit_width} frac={frac}");
                    }
                }
            }
        }
    }

    /// The maxima the pre-fusion filter proptested against, which spanned all
    /// of `u32`, plus three past it. The widths they imply straddle every
    /// kernel boundary: 4 and 9 narrow, 26 and 32 wide-with-`u32`-compare, 33
    /// through 56 and 64 wide-with-`u64`-compare.
    #[cfg(any(
        all(target_arch = "aarch64", target_endian = "little"),
        target_arch = "x86_64"
    ))]
    fn fused_max_val_strategy() -> impl Strategy<Value = u64> {
        prop_oneof![
            0u64..10u64,
            255u64..258u64,
            Just(1u64 << 25),
            Just(u64::from(u32::MAX) - 1),
            Just(u64::from(u32::MAX)),
            // Past `u32::MAX` the `u64`-compare kernel takes over: widths 33,
            // 40 and the 56 ceiling.
            Just(1u64 << 32),
            Just(1u64 << 39),
            Just((1u64 << 56) - 1),
            Just(u64::MAX),
        ]
    }

    #[cfg(any(
        all(target_arch = "aarch64", target_endian = "little"),
        target_arch = "x86_64"
    ))]
    proptest::proptest! {
        #[test]
        fn test_fused_matches_shipped_over_max_val_strategy(
            max_val in fused_max_val_strategy(),
            lo in 0u64..u64::MAX,
            hi in 0u64..u64::MAX,
            len in 0usize..600usize,
            start in 0u32..40u32,
        ) {
            let bit_width = crate::compute_num_bits(max_val);
            let vals: Vec<u64> = (0..len as u64)
                .map(|i| {
                    let h = i.wrapping_mul(0x9E3779B97F4A7C15);
                    // `max_val + 1` overflows at the u64::MAX case, where every
                    // hash is already in range.
                    match max_val.checked_add(1) {
                        Some(span) => h % span,
                        None => h,
                    }
                })
                .collect();
            let mut data = Vec::new();
            let mut packer = BitPacker::new();
            for &val in &vals {
                packer.write(val, bit_width, &mut data).unwrap();
            }
            packer.close(&mut data).unwrap();
            let unpacker = BitUnpacker::new(bit_width);

            let start = start.min(len as u32);
            let id_range = start..len as u32;
            let range64 = lo.min(hi)..=lo.max(hi);

            // Reference is the plain per-value loop, never the public entry
            // point, which now dispatches to the kernel under test.
            let mut expected = Vec::new();
            unpacker.get_ids_for_value_range_slow(
                range64.clone(),
                id_range.clone(),
                &data,
                &mut expected,
            );
            let mut fused = Vec::new();
            unpacker.get_ids_for_value_range_fused(range64, id_range, &data, &mut fused);
            assert_eq!(fused, expected, "bit_width={bit_width} max_val={max_val}");
        }
    }

    /// A/Bs `get_batch` against a per-value `get` loop writing the same
    /// buffer, per width and length. This is the raw unpacker with no codec
    /// mapping on top, which is what separates it from the columnar
    /// `get_range` gate.
    ///
    /// ```text
    /// cargo test --release -p tantivy-bitpacker --lib batch_vs_scalar \
    ///     -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore]
    fn batch_vs_scalar() {
        use std::hint::black_box;
        use std::time::Instant;

        fn best<F: FnMut()>(mut f: F, reps: usize) -> f64 {
            let mut best = f64::MAX;
            for _ in 0..5 {
                let t0 = Instant::now();
                for _ in 0..reps {
                    f();
                }
                best = best.min(t0.elapsed().as_nanos() as f64 / reps as f64);
            }
            best
        }

        println!(
            "backend={} simd_enabled={}",
            fused_backend(),
            simd_enabled()
        );
        println!(
            "{:>5} {:>7} {:>12} {:>12} {:>7}",
            "width", "len", "scalar_ns/v", "batch_ns/v", "ratio"
        );
        for bit_width in [7u8, 16, 25, 32, 40, 56, 64] {
            let (_, data) = pack_spread(bit_width);
            let unpacker = BitUnpacker::new(bit_width);
            for len in [1usize, 8, 32, 64, 128, 256, 1024, 8192, 65536] {
                let reps = (2_000_000 / len).max(20);
                let mut out = vec![0u64; len];
                let scalar = best(
                    || {
                        for (i, o) in out.iter_mut().enumerate() {
                            *o = unpacker.get(i as u32, &data);
                        }
                        black_box(out[0]);
                    },
                    reps,
                );
                let batch = best(
                    || {
                        unpacker.get_batch(0, &data, &mut out);
                        black_box(out[0]);
                    },
                    reps,
                );
                println!(
                    "{bit_width:>5} {len:>7} {:>12.3} {:>12.3} {:>7.2}",
                    scalar / len as f64,
                    batch / len as f64,
                    scalar / batch
                );
            }
        }
    }

    /// Which fused backend `get_ids_for_value_range` will actually reach, so an
    /// A/B on real hardware cannot silently measure the wrong one.
    #[cfg(test)]
    fn fused_backend() -> String {
        #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
        if sve::available() {
            return format!("sve(vl={})", sve::lanes());
        }
        if !simd_enabled() {
            return "scalar".to_string();
        }
        #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
        return "neon".to_string();
        #[cfg(target_arch = "x86_64")]
        return "avx2".to_string();
        #[allow(unreachable_code)]
        "scalar".to_string()
    }

    /// Guards against the fused tests silently passing on NEON when they were
    /// meant to exercise SVE. Set `TANTIVY_EXPECT_SVE=1` in an environment that
    /// is supposed to have it (a qemu run with `-cpu max`, say) to make a
    /// missing SVE path a failure rather than an invisible fallback.
    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
    #[test]
    fn test_sve_path_is_exercised() {
        let available = sve::available();
        println!("sve_available={available} u32_lanes={}", sve::lanes());
        if std::env::var_os("TANTIVY_EXPECT_SVE").is_some() {
            assert!(available, "TANTIVY_EXPECT_SVE set but SVE was not detected");
            assert!(sve::lanes() >= 4, "SVE reported fewer than 4 u32 lanes");
            // The two SVE kernels must partition every width a `BitUnpacker`
            // can hold, minus 0, which is answered without decoding. A gap
            // would silently fall through to scalar and still pass the value
            // tests, so it is checked structurally.
            for bw in (1..=56u8).chain(std::iter::once(64u8)) {
                let narrow = sve::kernel_applies(bw);
                let wide = sve::kernel64_applies(bw);
                assert!(narrow ^ wide, "width {bw} claimed by {narrow}/{wide}");
            }
            assert!(!sve::kernel_applies(0) && !sve::kernel64_applies(0));
            assert!(sve::kernel_applies(25) && sve::kernel64_applies(26));
            assert!(
                sve::lanes64() * 2 == sve::lanes(),
                "cntd must be half of cntw"
            );
        }
    }

    /// A/Bs the fused kernels against the pre-fusion `get_ids_for_value_range`
    /// arms, per width and selectivity, in one process.
    ///
    /// ```text
    /// cargo test --release -p tantivy-bitpacker --lib filter_sweep \
    ///     -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore]
    fn filter_sweep() {
        use std::hint::black_box;
        use std::time::Instant;

        const REPS: usize = 200;

        fn best<F: FnMut()>(mut f: F) -> f64 {
            let mut best = f64::MAX;
            for _ in 0..5 {
                let t0 = Instant::now();
                for _ in 0..REPS {
                    f();
                }
                best = best.min(t0.elapsed().as_nanos() as f64 / REPS as f64);
            }
            best
        }

        println!(
            "backend={} simd_enabled={}",
            fused_backend(),
            simd_enabled()
        );
        println!(
            "{:>5} {:>5} {:>6} {:>10} {:>10} {:>6} {:>7}",
            "width", "sel%", "start", "slow_ns", "fused_ns", "ratio", "kernel"
        );
        for bit_width in [0u8, 1, 7, 10, 16, 20, 25, 26, 32, 33, 40, 48, 56, 64] {
            let (_vals, data) = pack_spread(bit_width);
            let mask = value_mask(bit_width);
            let unpacker = BitUnpacker::new(bit_width);
            for pct in [1u64, 25, 50, 100] {
                let range = 0..=(mask / 100).saturating_mul(pct);
                // start 0 is block aligned; start 5 is not, which is what the
                // first-chunk boundary cut is there for.
                for start in [0u32, 5] {
                    let id_range = start..NUM_FILTER_VALS as u32;
                    let mut positions = Vec::with_capacity(NUM_FILTER_VALS);

                    // The fallback, reached directly: the public entry point
                    // now dispatches to the fused kernel.
                    let shipped = best(|| {
                        unpacker.get_ids_for_value_range_slow(
                            range.clone(),
                            id_range.clone(),
                            &data,
                            &mut positions,
                        );
                        black_box(positions.len());
                    });
                    // Never time an arm without checking it agrees.
                    let mut expect = Vec::new();
                    unpacker.get_ids_for_value_range_slow(
                        range.clone(),
                        id_range.clone(),
                        &data,
                        &mut expect,
                    );
                    let mut fused_out = Vec::new();
                    unpacker.get_ids_for_value_range_fused(
                        range.clone(),
                        id_range.clone(),
                        &data,
                        &mut fused_out,
                    );
                    assert_eq!(fused_out, expect, "fused disagrees w={bit_width} pct={pct}");
                    let fused = best(|| {
                        unpacker.get_ids_for_value_range_fused(
                            range.clone(),
                            id_range.clone(),
                            &data,
                            &mut positions,
                        );
                        black_box(positions.len());
                    });
                    // The shipped path's first half on its own: the `resize`
                    // refill plus the batch decode into `positions`, with no
                    // filtering. Subtracting it from `shipped` isolates what
                    // the filter pass costs once the values are in memory.
                    let kernel = match bit_width {
                        1..=25 => "narrow",
                        26..=32 => "wide32",
                        33..=56 | 64 => "wide64",
                        _ => "const",
                    };
                    println!(
                        "{bit_width:>5} {pct:>5} {start:>6} {shipped:>10.0} {fused:>10.0} {:>6.2} \
                         {kernel:>7}",
                        shipped / fused
                    );
                }
            }
        }
    }
}
