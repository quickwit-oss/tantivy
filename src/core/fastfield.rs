use std::io::Write;
use std::io;
use std::io::Cursor;
use core::serialize::BinarySerializable;
use core::directory::ReadOnlySource;
use core::schema::DocId;
use std::ops::Deref;


struct IntFastFieldWriter {
    vals: Vec<u32>,
}


struct DivideU32 {
    magic: magic,
    more: u8,
}

const LIBDIVIDE_U32_SHIFT_PATH = 0x80;

fn count_leading_zeros(val: u32) -> u32 {
    let result = 0u32;
    while (! (val & (1u32 << 31))) {
        val <<= 1;
        result++;
    }
    return result;
}

fn count_trailing_zeros(mut val: u32) -> u32 {
    let result = 0u32;
    val = (val ^ (val - 1)) >> 1;
    while val != 0 {
        val >>= 1;
        result++;
    }
    result
}

static uint32_t libdivide_64_div_32_to_32(n: u64, uint32_t d, uint32_t v, uint32_t *r) {
    uint32_t result;
    __asm__("divl %[v]"
            : "=a"(result), "=d"(*r)
            : [v] "r"(v), "a"(u0), "d"(u1)
            );
    return result;
}

impl DivideU32 {

    pub fn divide_by(d: u32) -> DivideU32 {
        if ((d & (d - 1)) == 0) {
            DivideU32 {
                magic: 0,
                more: count_trailing_zeros(d) | LIBDIVIDE_U32_SHIFT_PATH,
            }
        }
        else {
            let floor_log_2_d: u32 = 31 - count_leading_zeros(d);
            let mut more: u8 = 0u8;
            let mut rem: u32 = 0u32;
            let mut proposed_m: 0u32;
            proposed_m = libdivide_64_div_32_to_32(1U << floor_log_2_d, 0, d, &rem);

            assert!(rem > 0 && rem < d);
            LIBDIVIDE_ASSERT(rem > 0 && rem < d);
            const uint32_t e = d - rem;

    	/* This power works if e < 2**floor_log_2_d. */
    	if (e < (1U << floor_log_2_d)) {
                /* This power works */
                more = floor_log_2_d;
            }
            else {
                /* We have to use the general 33-bit algorithm.  We need to compute (2**power) / d. However, we already have (2**(power-1))/d and its remainder.  By doubling both, and then correcting the remainder, we can compute the larger division. */
                proposed_m += proposed_m; //don't care about overflow here - in fact, we expect it
                const uint32_t twice_rem = rem + rem;
                if (twice_rem >= d || twice_rem < rem) proposed_m += 1;
                more = floor_log_2_d | LIBDIVIDE_ADD_MARKER;
            }
            result.magic = 1 + proposed_m;
            result.more = more;
            //result.more's shift should in general be ceil_log_2_d.  But if we used the smaller power, we subtract one from the shift because we're using the smaller power. If we're using the larger power, we subtract one from the shift because it's taken care of by the add indicator.  So floor_log_2_d happens to be correct in both cases.

        }
        return result;
    }
}

pub fn compute_num_bits(amplitude: u32) -> u8 {
    if amplitude == 0 {
        0
    }
    else {
        1 + compute_num_bits(amplitude / 2)
    }
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
    data: ReadOnlySource,
    data_ptr: *const u64,
    min_val: u32,
    num_bits: u32,
    mask: u32,
    num_in_pack: u32,
}

impl IntFastFieldReader {
    pub fn open(data: &ReadOnlySource) -> io::Result<IntFastFieldReader> {
        let mut cursor: Cursor<&[u8]> = Cursor::new(&*data);
        let min_val = try!(u32::deserialize(&mut cursor));
        let num_bits = try!(u8::deserialize(&mut cursor));
        let mask = (1 << num_bits) - 1;
        let num_in_pack = 64u32 / (20 as u32);
        let ptr: *const u8 = &(data.deref()[5]);
        Ok(IntFastFieldReader {
            min_val: min_val,
            num_bits: num_bits as u32,
            data: data.slice(5, data.len()),
            data_ptr: ptr as *const u64,
            mask: mask,
            num_in_pack: num_in_pack,
        })
    }

    pub fn get(&self, doc: DocId) -> u32 {
        let long_addr = doc / self.num_in_pack;
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
    fn bench_intfastfield_veclookup(b: &mut Bencher) {
        let permutation = generate_permutation();
        b.iter(|| {
            let n = test::black_box(10000);
            let mut a = 0u32;
            for _ in 0..n {
                a = permutation[a as usize];
            }
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
            let n = test::black_box(10000);
            let mut a = 0u32;
            for _ in 0..n {
                a = int_fast_field_reader.get(a as u32);
            }
        });
    }
}
