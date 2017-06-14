use std::io::Write;
use std::io;
use common::serialize::BinarySerializable;
use std::mem;
use std::ops::Deref;

/// Computes the number of bits that will be used for bitpacking.
///
/// In general the target is the minimum number of bits
/// required to express the amplitude given in argument.
///
/// e.g. If the amplitude is 10, we can store all ints on simply 4bits.
///
/// The logic is slightly more convoluted here as for optimization
/// reasons, we want to ensure that a value spawns over at most 8 bytes
/// of aligns bytes.
///
/// Spanning over 9 bytes is possible for instance, if we do
/// bitpacking with an amplitude of 63 bits.
/// In this case, the second int will start on bit
/// 63 (which belongs to byte 7) and ends at byte 15;
/// Hence 9 bytes (from byte 7 to byte 15 included).
///
/// To avoid this, we force the number of bits to 64bits
/// when the result is greater than `64-8 = 56 bits`.
///
/// Note that this only affects rare use cases spawning over
/// a very large range of values. Even in this case, it results
/// in an extra cost of at most 12% compared to the optimal
/// number of bits.
pub fn compute_num_bits(amplitude: u64) -> u8 {
    let amplitude = (64u32 - amplitude.leading_zeros()) as u8;
    if amplitude <= 64 - 8 { amplitude } else { 64 }
}

pub struct BitPacker {
    mini_buffer: u64,
    mini_buffer_written: usize,
    num_bits: usize,
}

impl BitPacker {
    pub fn new(num_bits: usize) -> BitPacker {
        BitPacker {
            mini_buffer: 0u64,
            mini_buffer_written: 0,
            num_bits: num_bits,
        }
    }

    pub fn write<TWrite: Write>(&mut self, val: u64, output: &mut TWrite) -> io::Result<()> {
        let val_u64 = val as u64;
        if self.mini_buffer_written + self.num_bits > 64 {
            self.mini_buffer |= val_u64.wrapping_shl(self.mini_buffer_written as u32);
            self.mini_buffer.serialize(output)?;
            self.mini_buffer = val_u64.wrapping_shr((64 - self.mini_buffer_written) as u32);
            self.mini_buffer_written = self.mini_buffer_written + (self.num_bits as usize) - 64;
        } else {
            self.mini_buffer |= val_u64 << self.mini_buffer_written;
            self.mini_buffer_written += self.num_bits;
            if self.mini_buffer_written == 64 {
                self.mini_buffer.serialize(output)?;
                self.mini_buffer_written = 0;
                self.mini_buffer = 0u64;
            }
        }
        Ok(())
    }

    fn flush<TWrite: Write>(&mut self, output: &mut TWrite) -> io::Result<()> {
        if self.mini_buffer_written > 0 {
            let num_bytes = (self.mini_buffer_written + 7) / 8;
            let arr: [u8; 8] = unsafe { mem::transmute::<u64, [u8; 8]>(self.mini_buffer) };
            output.write_all(&arr[..num_bytes])?;
            self.mini_buffer_written = 0;
        }
        Ok(())
    }

    pub fn close<TWrite: Write>(&mut self, output: &mut TWrite) -> io::Result<()> {
        self.flush(output)?;
        // Padding the write file to simplify reads.
        output.write_all(&[0u8; 7])?;
        Ok(())
    }
}



pub struct BitUnpacker<Data>
    where Data: Deref<Target = [u8]>
{
    num_bits: usize,
    mask: u64,
    data: Data,
}

impl<Data> BitUnpacker<Data>
    where Data: Deref<Target = [u8]>
{
    pub fn new(data: Data, num_bits: usize) -> BitUnpacker<Data> {
        let mask: u64 = if num_bits == 64 {
            !0u64
        } else {
            (1u64 << num_bits) - 1u64
        };
        BitUnpacker {
            num_bits: num_bits,
            mask: mask,
            data: data,
        }
    }

    pub fn get(&self, idx: usize) -> u64 {
        if self.num_bits == 0 {
            return 0;
        }
        let data: &[u8] = &*self.data;
        let num_bits = self.num_bits;
        let mask = self.mask;
        let addr_in_bits = idx * num_bits;
        let addr = addr_in_bits >> 3;
        let bit_shift = addr_in_bits & 7;
        debug_assert!(addr + 8 <= data.len(),
                      "The fast field field should have been padded with 7 bytes.");
        let val_unshifted_unmasked: u64 = unsafe { *(data[addr..].as_ptr() as *const u64) };
        let val_shifted = (val_unshifted_unmasked >> bit_shift) as u64;
        (val_shifted & mask)
    }

    pub fn get_range(&self, start: u32, output: &mut [u64]) {
        if self.num_bits == 0 {
            for val in output.iter_mut() {
                *val = 0;
            }
        } else {
            let data: &[u8] = &*self.data;
            let num_bits = self.num_bits;
            let mask = self.mask;
            let mut addr_in_bits = (start as usize) * num_bits;
            for output_val in output.iter_mut() {
                let addr = addr_in_bits >> 3;
                let bit_shift = addr_in_bits & 7;
                let val_unshifted_unmasked: u64 = unsafe { *(data[addr..].as_ptr() as *const u64) };
                let val_shifted = (val_unshifted_unmasked >> bit_shift) as u64;
                *output_val = val_shifted & mask;
                addr_in_bits += num_bits;
            }
        }

    }
}




#[cfg(test)]
mod test {
    use super::{BitPacker, BitUnpacker, compute_num_bits};

    #[test]
    fn test_compute_num_bits() {
        assert_eq!(compute_num_bits(1), 1u8);
        assert_eq!(compute_num_bits(0), 0u8);
        assert_eq!(compute_num_bits(2), 2u8);
        assert_eq!(compute_num_bits(3), 2u8);
        assert_eq!(compute_num_bits(4), 3u8);
        assert_eq!(compute_num_bits(255), 8u8);
        assert_eq!(compute_num_bits(256), 9u8);
        assert_eq!(compute_num_bits(5_000_000_000), 33u8);
    }

    fn create_fastfield_bitpacker(len: usize, num_bits: usize) -> (BitUnpacker<Vec<u8>>, Vec<u64>) {
        let mut data = Vec::new();
        let mut bitpacker = BitPacker::new(num_bits);
        let max_val: u64 = (1 << num_bits) - 1;
        let vals: Vec<u64> = (0u64..len as u64)
            .map(|i| if max_val == 0 { 0 } else { i % max_val })
            .collect();
        for &val in &vals {
            bitpacker.write(val, &mut data).unwrap();
        }
        bitpacker.close(&mut data).unwrap();
        assert_eq!(data.len(), (num_bits * len + 7) / 8 + 7);
        let bitunpacker = BitUnpacker::new(data, num_bits);
        (bitunpacker, vals)
    }

    fn test_bitpacker_util(len: usize, num_bits: usize) {
        let (bitunpacker, vals) = create_fastfield_bitpacker(len, num_bits);
        for (i, val) in vals.iter().enumerate() {
            assert_eq!(bitunpacker.get(i), *val);
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

    #[test]
    fn test_bitpacker_range() {
        let (bitunpacker, vals) = create_fastfield_bitpacker(100_000, 12);
        let buffer_len = 100;
        let mut buffer = vec![0u64; buffer_len];
        for start in vec![0, 10, 20, 100, 1_000] {
            bitunpacker.get_range(start as u32, &mut buffer[..]);
            for i in 0..buffer_len {
                assert_eq!(buffer[i], vals[start + i]);
            }
        }
    }
}
