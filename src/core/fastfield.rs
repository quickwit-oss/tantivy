use std::io::Write;
use std::io;
use std::io::Cursor;
use core::serialize::BinarySerializable;
use core::directory::ReadOnlySource;
use core::schema::DocId;
use std::ops::Deref;
use core::fastdivide::count_leading_zeros;
use core::fastdivide::DividerU32;


pub fn compute_num_bits(amplitude: u32) -> u8 {
    32 - count_leading_zeros(amplitude)
}

fn serialize_packed_ints<I: Iterator<Item=u32>>(vals_it: I, num_bits: u8, write: &mut Write) -> io::Result<()> {
    let mut mini_buffer_written = 0;
    let mut mini_buffer = 0u64;
    for val in vals_it {
        if mini_buffer_written + num_bits > 64 {
            try!(mini_buffer.serialize(write));
            mini_buffer = 0;
            mini_buffer_written = 0;
        }
        mini_buffer |= (val as u64) << mini_buffer_written;
        mini_buffer_written += num_bits;
    }
    if mini_buffer_written > 0 {
        try!(mini_buffer.serialize(write));
    }
    Ok(())
}

pub struct IntFastFieldWriter {
    vals: Vec<u32>,
}

impl IntFastFieldWriter {

    pub fn new() -> IntFastFieldWriter {
        IntFastFieldWriter {
            vals: Vec::new()
        }
    }

    pub fn add(&mut self, val: u32) {
        self.vals.push(val);
    }

    pub fn close(&self, write: &mut Write) -> io::Result<()> {
        if self.vals.is_empty() {
            return Ok(())
        }
        let min = self.vals.iter().min().unwrap();
        let max = self.vals.iter().max().unwrap();
        try!(min.serialize(write));
        let amplitude: u32 = max - min;
        let num_bits: u8 = compute_num_bits(amplitude);
        try!(num_bits.serialize(write));
        let vals_it = self.vals.iter().map(|i| i-min);
        serialize_packed_ints(vals_it, num_bits, write)
    }
}

pub struct IntFastFieldReader {
    _data: ReadOnlySource,
    data_ptr: *const u64,
    min_val: u32,
    num_bits: u32,
    mask: u32,
    num_in_pack: u32,
    divider: DividerU32,
}

impl IntFastFieldReader {
    pub fn open(data: &ReadOnlySource) -> io::Result<IntFastFieldReader> {
        let mut cursor: Cursor<&[u8]> = Cursor::new(&*data);
        let min_val = try!(u32::deserialize(&mut cursor));
        let num_bits = try!(u8::deserialize(&mut cursor));
        let mask = (1 << num_bits) - 1;
        let num_in_pack = 64u32 / (num_bits as u32);
        let ptr: *const u8 = &(data.deref()[5]);
        Ok(IntFastFieldReader {
            _data: data.slice(5, data.len()),
            data_ptr: ptr as *const u64,
            min_val: min_val,
            num_bits: num_bits as u32,
            mask: mask,
            num_in_pack: num_in_pack,
            divider: DividerU32::divide_by(num_in_pack),
        })
    }

    pub fn get(&self, doc: DocId) -> u32 {
        let long_addr = self.divider.divide(doc);
        let ord_within_long = doc - long_addr * self.num_in_pack;
        let bit_shift = (self.num_bits as u32) * ord_within_long;
        let val_unshifted_unmasked: u64 = unsafe { *self.data_ptr.offset(long_addr as isize) };
        let val_shifted = (val_unshifted_unmasked >> bit_shift) as u32;
        return self.min_val + (val_shifted & self.mask);
    }
}

#[cfg(test)]
mod tests {

    use super::compute_num_bits;
    use super::IntFastFieldWriter;
    use super::IntFastFieldReader;
    use core::directory::ReadOnlySource;
    use test::Bencher;
    use test;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::XorShiftRng;


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

    #[test]
    fn test_intfastfield_small() {
        let mut buffer: Vec<u8> = Vec::new();
        {
            let mut int_fast_field_writer = IntFastFieldWriter::new();
            int_fast_field_writer.add(4u32);
            int_fast_field_writer.add(14u32);
            int_fast_field_writer.add(2u32);
            int_fast_field_writer.close(&mut buffer).unwrap();
            assert_eq!(buffer.len(), 4 + 1 + 8 as usize);
        }
        {
            let source = ReadOnlySource::Anonymous(buffer);
            let fast_field_reader = IntFastFieldReader::open(&source).unwrap();
            assert_eq!(fast_field_reader.get(0), 4u32);
            assert_eq!(fast_field_reader.get(1), 14u32);
            assert_eq!(fast_field_reader.get(2), 2u32);
        }
    }


    #[test]
    fn test_intfastfield_large() {
        let mut buffer: Vec<u8> = Vec::new();
        {
            let mut int_fast_field_writer = IntFastFieldWriter::new();
            int_fast_field_writer.add(4u32);
            int_fast_field_writer.add(14_082_001u32);
            int_fast_field_writer.add(3_052u32);
            int_fast_field_writer.close(&mut buffer).unwrap();
            assert_eq!(buffer.len(), 21 as usize);
        }
        {
            let source = ReadOnlySource::Anonymous(buffer);
            let fast_field_reader = IntFastFieldReader::open(&source).unwrap();
            assert_eq!(fast_field_reader.get(0), 4u32);
            assert_eq!(fast_field_reader.get(1), 14_082_001u32);
            assert_eq!(fast_field_reader.get(2), 3_052u32);
        }
    }

    fn generate_permutation() -> Vec<u32> {
        let seed: &[u32; 4] = &[1, 2, 3, 4];
        let mut rng = XorShiftRng::from_seed(*seed);
        let mut permutation: Vec<u32> = (0u32..1_000_000u32).collect();
        rng.shuffle(&mut permutation);
        permutation
    }

    #[test]
    fn test_intfastfield_permutation() {
        let mut buffer: Vec<u8> = Vec::new();
        let permutation = generate_permutation();
        {
            let mut int_fast_field_writer = IntFastFieldWriter::new();
            for x in permutation.iter() {
                int_fast_field_writer.add(*x);
            }
            int_fast_field_writer.close(&mut buffer).unwrap();
        }
        let source = ReadOnlySource::Anonymous(buffer);
        let int_fast_field_reader = IntFastFieldReader::open(&source).unwrap();

        let n = test::black_box(100);
        let mut a = 0u32;
        for _ in 0..n {
            assert_eq!(int_fast_field_reader.get(a as u32), permutation[a as usize]);
            a = int_fast_field_reader.get(a as u32);
        }
    }

    #[bench]
    fn bench_intfastfield_linear_veclookup(b: &mut Bencher) {
        let permutation = generate_permutation();
        b.iter(|| {
            let n = test::black_box(7000u32);
            let mut a = 0u32;
            for i in (0u32..n).step_by(7) {
                a ^= permutation[i as usize];
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_veclookup(b: &mut Bencher) {
        let permutation = generate_permutation();
        b.iter(|| {
            let n = test::black_box(1000u32);
            let mut a = 0u32;
            for _ in 0u32..n {
                a = permutation[a as usize];
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_linear_fflookup(b: &mut Bencher) {
        let mut buffer: Vec<u8> = Vec::new();
        {
            let permutation = generate_permutation();
            let mut int_fast_field_writer = IntFastFieldWriter::new();
            for x in permutation.iter() {
                int_fast_field_writer.add(*x);
            }
            int_fast_field_writer.close(&mut buffer).unwrap();
        }
        let source = ReadOnlySource::Anonymous(buffer);
        let int_fast_field_reader = IntFastFieldReader::open(&source).unwrap();
        b.iter(|| {
            let n = test::black_box(7000u32);
            let mut a = 0u32;
            for i in (0u32..n).step_by(7) {
                a ^= int_fast_field_reader.get(i);
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_fflookup(b: &mut Bencher) {
        let mut buffer: Vec<u8> = Vec::new();
        {
            let permutation = generate_permutation();
            let mut int_fast_field_writer = IntFastFieldWriter::new();
            for x in permutation.iter() {
                int_fast_field_writer.add(*x);
            }
            int_fast_field_writer.close(&mut buffer).unwrap();
        }
        let source = ReadOnlySource::Anonymous(buffer);
        let int_fast_field_reader = IntFastFieldReader::open(&source).unwrap();
        b.iter(|| {
            let n = test::black_box(1000);
            let mut a = 0u32;
            for _ in 0..n {
                a = int_fast_field_reader.get(a as u32);
            }
            a
        });
    }
}
