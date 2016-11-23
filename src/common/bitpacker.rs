use std::io::Write;
use std::io;
use common::serialize::BinarySerializable;
use std::mem;


pub fn compute_num_bits(amplitude: u32) -> u8 {
    (32u32 - amplitude.leading_zeros()) as u8
}

pub struct BitPacker<TWrite: Write> {
    output: TWrite,
    mini_buffer: u64,
    mini_buffer_written: usize,
    num_bits: usize,
    written_size: usize,
}

impl<TWrite: Write> BitPacker<TWrite> {   
    
    pub fn new(output: TWrite, num_bits: usize) -> BitPacker<TWrite> {
        BitPacker {
            output: output,
            mini_buffer: 0u64,
            mini_buffer_written: 0,
            num_bits: num_bits,
            written_size: 0,
        }
    }
    
    pub fn write(&mut self, val: u32) -> io::Result<()> {
        let val_u64 = val as u64;
        if self.mini_buffer_written + self.num_bits > 64 {
            self.mini_buffer |= val_u64.wrapping_shl(self.mini_buffer_written as u32);
            self.written_size += self.mini_buffer.serialize(&mut self.output)?;
            self.mini_buffer = val_u64.wrapping_shr((64 - self.mini_buffer_written) as u32);
            self.mini_buffer_written = self.mini_buffer_written + (self.num_bits as usize) - 64;
        }
        else {
            self.mini_buffer |= val_u64 << self.mini_buffer_written;
            self.mini_buffer_written += self.num_bits;
            if self.mini_buffer_written == 64 {
                self.written_size += self.mini_buffer.serialize(&mut self.output)?;
                self.mini_buffer_written = 0;
                self.mini_buffer = 0u64;
            }    
        }
        Ok(())
    }
    
    fn flush(&mut self) -> io::Result<()>{
        if self.mini_buffer_written > 0 {
            let num_bytes = (self.mini_buffer_written + 7) / 8;
            let arr: [u8; 8] =  unsafe { mem::transmute::<u64, [u8; 8]>(self.mini_buffer) };
            self.output.write_all(&arr[..num_bytes])?;
            self.written_size += num_bytes;
            self.mini_buffer_written = 0;
        }
        Ok(())
    }
    
    pub fn close(mut self) -> io::Result<(TWrite, usize)> {
        self.flush()?;
        Ok((self.output, self.written_size))
    }
}



pub struct BitUnpacker<'a> {
    data: &'a [u8],
    num_bits: usize,
    mask: u32,
}

impl<'a> BitUnpacker<'a> {
    pub fn new(data: &'a [u8], num_bits: usize) -> BitUnpacker<'a> {
        BitUnpacker {
            data: data,
            num_bits: num_bits,
            mask: (1u32 << num_bits) - 1u32,
        }
    }
    
    pub fn get(&self, idx: usize) -> u32 {
        if self.num_bits == 0 {
            return 0;
        }
        let addr = (idx * self.num_bits) / 8;
        let bit_shift = (idx * self.num_bits) - addr * 8;
        let val_unshifted_unmasked: u64;
        if addr + 8 <= self.data.len() {
            val_unshifted_unmasked = unsafe { * (self.data.as_ptr().offset(addr as isize) as *const u64) };
        }
        else {
            let mut arr = [0u8; 8];
            for i in 0..self.data.len() - addr {
                arr[i] = self.data[addr + i];
            }
            val_unshifted_unmasked = unsafe { mem::transmute::<[u8; 8], u64>(arr) };
        }
        let val_shifted = (val_unshifted_unmasked >> bit_shift) as u32;
        (val_shifted & self.mask)
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
    }
    
    fn test_bitpacker_util(len: usize, num_bits: usize) {
        let mut bitpacker = BitPacker::new(Vec::new(), num_bits);
        let max_val: u32 = (1 << num_bits) - 1;
        let vals: Vec<u32> = (0u32..len as u32).map(|i| {
            if max_val == 0 {
                0
            }
            else {
                i % max_val
            }
        }).collect();
        for &val in &vals {
            bitpacker.write(val).unwrap();
        }
        let (data, num_bytes) = bitpacker.close().unwrap();
        assert_eq!(num_bytes, (num_bits * len + 7) / 8);       
        assert_eq!(data.len(), num_bytes);
        let bitunpacker = BitUnpacker::new(&data, num_bits);
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
}