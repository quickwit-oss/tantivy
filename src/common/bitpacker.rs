use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::io;

use crate::directory::OwnedBytes;

pub(crate) struct BitPacker {
    mini_buffer: u64,
    mini_buffer_written: usize,
}

impl BitPacker {
    pub fn new() -> BitPacker {
        BitPacker {
            mini_buffer: 0u64,
            mini_buffer_written: 0,
        }
    }

    pub fn write<TWrite: io::Write>(
        &mut self,
        val: u64,
        num_bits: u8,
        output: &mut TWrite,
    ) -> io::Result<()> {
        let val_u64 = val as u64;
        let num_bits = num_bits as usize;
        if self.mini_buffer_written + num_bits > 64 {
            self.mini_buffer |= val_u64.wrapping_shl(self.mini_buffer_written as u32);
            output.write_u64::<LittleEndian>(self.mini_buffer)?;
            self.mini_buffer = val_u64.wrapping_shr((64 - self.mini_buffer_written) as u32);
            self.mini_buffer_written = self.mini_buffer_written + num_bits - 64;
        } else {
            self.mini_buffer |= val_u64 << self.mini_buffer_written;
            self.mini_buffer_written += num_bits;
            if self.mini_buffer_written == 64 {
                output.write_u64::<LittleEndian>(self.mini_buffer)?;
                self.mini_buffer_written = 0;
                self.mini_buffer = 0u64;
            }
        }
        Ok(())
    }

    pub fn flush<TWrite: io::Write>(&mut self, output: &mut TWrite) -> io::Result<()> {
        if self.mini_buffer_written > 0 {
            let num_bytes = (self.mini_buffer_written + 7) / 8;
            let mut arr: [u8; 8] = [0u8; 8];
            LittleEndian::write_u64(&mut arr, self.mini_buffer);
            output.write_all(&arr[..num_bytes])?;
            self.mini_buffer_written = 0;
        }
        Ok(())
    }

    pub fn close<TWrite: io::Write>(&mut self, output: &mut TWrite) -> io::Result<()> {
        self.flush(output)?;
        // Padding the write file to simplify reads.
        output.write_all(&[0u8; 7])?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct BitUnpacker {
    num_bits: u64,
    mask: u64,
    data: OwnedBytes,
}

impl BitUnpacker {
    pub fn new(data: OwnedBytes, num_bits: u8) -> BitUnpacker {
        let mask: u64 = if num_bits == 64 {
            !0u64
        } else {
            (1u64 << num_bits) - 1u64
        };
        BitUnpacker {
            num_bits: u64::from(num_bits),
            mask,
            data,
        }
    }

    pub fn get(&self, idx: u64) -> u64 {
        if self.num_bits == 0 {
            return 0u64;
        }
        let data: &[u8] = self.data.as_slice();
        let num_bits = self.num_bits;
        let mask = self.mask;
        let addr_in_bits = idx * num_bits;
        let addr = addr_in_bits >> 3;
        let bit_shift = addr_in_bits & 7;
        debug_assert!(
            addr + 8 <= data.len() as u64,
            "The fast field field should have been padded with 7 bytes."
        );
        let val_unshifted_unmasked: u64 = LittleEndian::read_u64(&data[(addr as usize)..]);
        let val_shifted = (val_unshifted_unmasked >> bit_shift) as u64;
        val_shifted & mask
    }
}

#[cfg(test)]
mod test {
    use super::{BitPacker, BitUnpacker};
    use crate::directory::OwnedBytes;

    fn create_fastfield_bitpacker(len: usize, num_bits: u8) -> (BitUnpacker, Vec<u64>) {
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
        assert_eq!(data.len(), ((num_bits as usize) * len + 7) / 8 + 7);
        let bitunpacker = BitUnpacker::new(OwnedBytes::new(data), num_bits);
        (bitunpacker, vals)
    }

    fn test_bitpacker_util(len: usize, num_bits: u8) {
        let (bitunpacker, vals) = create_fastfield_bitpacker(len, num_bits);
        for (i, val) in vals.iter().enumerate() {
            assert_eq!(bitunpacker.get(i as u64), *val);
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
}
