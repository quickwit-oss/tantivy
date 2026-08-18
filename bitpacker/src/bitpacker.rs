use std::io;
use std::ops::{Range, RangeInclusive};

use crate::block_decode::{BLOCK_LEN, Lane, decode_block_lanes};

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
    pub fn new(num_bits: u8) -> BitUnpacker {
        assert!(num_bits <= 7 * 8 || num_bits == 64);
        let mask: u64 = if num_bits == 64 {
            !0u64
        } else {
            (1u64 << num_bits) - 1u64
        };
        BitUnpacker {
            num_bits: usize::from(num_bits),
            mask,
        }
    }

    pub fn bit_width(&self) -> u8 {
        self.num_bits as u8
    }

    #[inline]
    pub fn get(&self, idx: u32, data: &[u8]) -> u64 {
        let addr_in_bits = idx as usize * self.num_bits;
        let addr = addr_in_bits >> 3;
        if addr + 8 > data.len() {
            if self.num_bits == 0 {
                return 0;
            }
            let bit_shift = addr_in_bits & 7;
            return self.get_slow_path(addr, bit_shift as u32, data);
        }
        let bit_shift = addr_in_bits & 7;
        let bytes: [u8; 8] = (&data[addr..addr + 8]).try_into().unwrap();
        let val_unshifted_unmasked: u64 = u64::from_le_bytes(bytes);
        let val_shifted = val_unshifted_unmasked >> bit_shift;
        val_shifted & self.mask
    }

    #[inline(never)]
    fn get_slow_path(&self, addr: usize, bit_shift: u32, data: &[u8]) -> u64 {
        let mut bytes: [u8; 8] = [0u8; 8];
        let available_bytes = data.len() - addr;
        // This function is meant to only be called if we did not have 8 bytes to load.
        debug_assert!(available_bytes < 8);
        bytes[..available_bytes].copy_from_slice(&data[addr..]);
        let val_unshifted_unmasked: u64 = u64::from_le_bytes(bytes);
        let val_shifted = val_unshifted_unmasked >> bit_shift;
        val_shifted & self.mask
    }

    pub fn get_batch(&self, start_idx: u32, data: &[u8], output: &mut [u64]) {
        self.get_batch_lanes(start_idx, data, output);
    }

    // Decodes the range of bitpacked `u32` values with idx
    // in [start_idx, start_idx + output.len()).
    //
    // #Panics
    //
    // This methods panics if `num_bits` is > 32.
    fn get_batch_u32s(&self, start_idx: u32, data: &[u8], output: &mut [u32]) {
        assert!(
            self.bit_width() <= 32,
            "Bitwidth must be <= 32 to use this method."
        );
        self.get_batch_lanes(start_idx, data, output);
    }

    /// Decodes `[start_idx, start_idx + output.len())` into `output`, routing
    /// whole aligned blocks through [`crate::block_decode::decode_block`] and
    /// the two edges through [`Self::get`].
    ///
    /// A block only starts byte-aligned -- which the kernels require -- when
    /// its first index is a multiple of 8, since 8 values occupy exactly
    /// `num_bits` bytes. So the block loop is preceded by an entrance ramp of
    /// up to 7 values, and followed by an exit ramp of up to `BLOCK_LEN - 1`.
    fn get_batch_lanes<L: Lane>(&self, start_idx: u32, data: &[u8], output: &mut [L]) {
        // `usize` throughout to avoid overflow issues.
        let end_idx = start_idx as usize + output.len();
        assert!(
            (end_idx * self.num_bits).div_ceil(8) <= data.len(),
            "Requested index is out of bounds."
        );

        let ramp = ((8 - start_idx % 8) % 8) as usize;
        let mut cursor = if ramp + BLOCK_LEN <= output.len() {
            ramp
        } else {
            output.len()
        };
        for (i, out) in output[..cursor].iter_mut().enumerate() {
            *out = L::from_u64(self.get(start_idx + i as u32, data));
        }
        while cursor + BLOCK_LEN <= output.len() {
            let block_start_bytes = (start_idx as usize + cursor) * self.num_bits / 8;
            let block: &mut [L; BLOCK_LEN] = (&mut output[cursor..cursor + BLOCK_LEN])
                .try_into()
                .unwrap();
            decode_block_lanes(self.num_bits as u8, &data[block_start_bytes..], block);
            cursor += BLOCK_LEN;
        }
        for (i, out) in output[cursor..].iter_mut().enumerate() {
            let idx = start_idx + (cursor + i) as u32;
            *out = L::from_u64(self.get(idx, data));
        }
    }

    pub fn get_ids_for_value_range(
        &self,
        range: RangeInclusive<u64>,
        id_range: Range<u32>,
        data: &[u8],
        positions: &mut Vec<u32>,
    ) {
        if self.bit_width() > 32 {
            self.get_ids_for_value_range_slow(range, id_range, data, positions)
        } else {
            if *range.start() > u32::MAX as u64 {
                positions.clear();
                return;
            }
            let range_u32 = (*range.start() as u32)..=(*range.end()).min(u32::MAX as u64) as u32;
            self.get_ids_for_value_range_fast(range_u32, id_range, data, positions)
        }
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

    fn get_ids_for_value_range_fast(
        &self,
        value_range: RangeInclusive<u32>,
        id_range: Range<u32>,
        data: &[u8],
        positions: &mut Vec<u32>,
    ) {
        positions.resize(id_range.len(), 0u32);
        self.get_batch_u32s(id_range.start, data, positions);
        crate::filter_vec::filter_vec_in_place(value_range, id_range.start, positions)
    }
}

#[cfg(test)]
mod test {
    use super::{BitPacker, BitUnpacker};

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

    proptest::proptest! {
        #[test]
        fn test_bitpacker_proptest((num_bits, vals) in vals_strategy()) {
            test_bitpacker_aux(num_bits, &vals);
        }
    }

    #[test]
    #[should_panic]
    fn test_get_batch_panics_over_32_bits() {
        let bitunpacker = BitUnpacker::new(33);
        let mut output: [u32; 1] = [0u32];
        bitunpacker.get_batch_u32s(0, &[0, 0, 0, 0, 0, 0, 0, 0], &mut output[..]);
    }

    #[test]
    fn test_get_batch_limit() {
        let bitunpacker = BitUnpacker::new(1);
        let mut output: [u32; 3] = [0u32, 0u32, 0u32];
        bitunpacker.get_batch_u32s(8 * 4 - 3, &[0u8, 0u8, 0u8, 0u8], &mut output[..]);
    }

    #[test]
    #[should_panic]
    fn test_get_batch_panics_when_off_scope() {
        let bitunpacker = BitUnpacker::new(1);
        let mut output: [u32; 3] = [0u32, 0u32, 0u32];
        // We are missing exactly one bit.
        bitunpacker.get_batch_u32s(8 * 4 - 2, &[0u8, 0u8, 0u8, 0u8], &mut output[..]);
    }

    /// Lengths that straddle the block grid: none, one and several whole
    /// blocks, plus both ramps.
    const BATCH_LENS: [usize; 12] = [0, 1, 2, 32, 33, 64, 127, 128, 129, 135, 256, 300];
    const NUM_VALS: u64 = 600;

    proptest::proptest! {
        #[test]
        fn test_get_batch_u32s_proptest(num_bits in 0u8..=32u8) {
            let mask =
                if num_bits == 32u8 {
                    u32::MAX
                } else {
                    (1u32 << num_bits) - 1
                };
            let mut buffer: Vec<u8> = Vec::new();
            let mut bitpacker = BitPacker::new();
            for val in 0..NUM_VALS {
                bitpacker.write(val & mask as u64, num_bits, &mut buffer).unwrap();
            }
            bitpacker.flush(&mut buffer).unwrap();
            let bitunpacker = BitUnpacker::new(num_bits);
            let mut output: Vec<u32> = Vec::new();
            for len in BATCH_LENS {
                for start_idx in 0u32..32u32 {
                    output.resize(len, 0);
                    bitunpacker.get_batch_u32s(start_idx, &buffer, &mut output);
                    for (i, output_byte) in output.iter().enumerate() {
                        let expected = (start_idx + i as u32) & mask;
                        assert_eq!(*output_byte, expected);
                    }
                }
            }
        }
    }

    proptest::proptest! {
        #[test]
        fn test_get_batch_proptest((num_bits, _vals) in vals_strategy()) {
            let mask = if num_bits == 64 { u64::MAX } else { (1u64 << num_bits) - 1 };
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
                    bitunpacker.get_batch(start_idx, &buffer, &mut output);
                    for (i, got) in output.iter().enumerate() {
                        let idx = start_idx + i as u32;
                        assert_eq!(*got, vals[idx as usize], "num_bits {num_bits} len {len} idx {idx}");
                        assert_eq!(*got, bitunpacker.get(idx, &buffer));
                    }
                }
            }
        }
    }
}
