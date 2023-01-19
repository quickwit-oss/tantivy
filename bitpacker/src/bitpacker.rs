use std::convert::TryInto;
use std::io;

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
    pub fn write<TWrite: io::Write>(
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

    pub fn flush<TWrite: io::Write>(&mut self, output: &mut TWrite) -> io::Result<()> {
        if self.mini_buffer_written > 0 {
            let num_bytes = (self.mini_buffer_written + 7) / 8;
            let bytes = self.mini_buffer.to_le_bytes();
            output.write_all(&bytes[..num_bytes])?;
            self.mini_buffer_written = 0;
            self.mini_buffer = 0;
        }
        Ok(())
    }

    pub fn close<TWrite: io::Write>(&mut self, output: &mut TWrite) -> io::Result<()> {
        self.flush(output)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Copy)]
pub struct BitUnpacker {
    num_bits: u32,
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
            num_bits: u32::from(num_bits),
            mask,
        }
    }

    pub fn bit_width(&self) -> u8 {
        self.num_bits as u8
    }

    #[inline]
    pub fn get(&self, idx: u32, data: &[u8]) -> u64 {
        let addr_in_bits = idx * self.num_bits;
        let addr = (addr_in_bits >> 3) as usize;
        if addr + 8 > data.len() {
            if self.num_bits == 0 {
                return 0;
            }
            let bit_shift = addr_in_bits & 7;
            return self.get_slow_path(addr, bit_shift, data);
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
        assert_eq!(data.len(), ((num_bits as usize) * len + 7) / 8);
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
        assert_eq!(buffer.len(), (vals.len() * num_bits as usize + 7) / 8);
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
}
