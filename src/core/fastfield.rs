use std::io::Write;
use std::io;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::Cursor;
use core::directory::WritePtr;
use core::serialize::BinarySerializable;
use core::directory::ReadOnlySource;
use core::schema::DocId;
use core::schema::Schema;
use core::schema::Document;
use core::schema::FAST_U32;
use std::ops::Deref;
use core::fastdivide::count_leading_zeros;
use core::fastdivide::DividerU32;
use core::schema::U32Field;

pub fn compute_num_bits(amplitude: u32) -> u8 {
    32u8 - count_leading_zeros(amplitude)
}

fn serialize_packed_ints<I: Iterator<Item=u32>>(vals_it: I, num_bits: u8, write: &mut Write) -> io::Result<usize> {
    let mut mini_buffer_written = 0;
    let mut mini_buffer = 0u64;
    let mut written_size = 0;
    for val in vals_it {
        if mini_buffer_written + num_bits > 64 {
            written_size += try!(mini_buffer.serialize(write));
            mini_buffer = 0;
            mini_buffer_written = 0;
        }
        mini_buffer |= (val as u64) << mini_buffer_written;
        mini_buffer_written += num_bits;
    }
    if mini_buffer_written > 0 {
        written_size += try!(mini_buffer.serialize(write));
    }
    Ok(written_size)
}

pub struct FastFieldSerializer {
    write: WritePtr,
    written_size: usize,
    fields: Vec<(U32Field, u64)>,
    num_bits: u8,

    field_open: bool,
    mini_buffer_written: usize,
    mini_buffer: u64,
}

impl FastFieldSerializer {
    pub fn new(mut write: WritePtr) -> io::Result<FastFieldSerializer> {
        // just making room for the pointer to header.
        let written_size: usize = try!(0u64.serialize(&mut write));
        Ok(FastFieldSerializer {
            write: write,
            written_size: written_size,
            fields: Vec::new(),
            num_bits: 0u8,
            field_open: false,
            mini_buffer_written: 0,
            mini_buffer: 0,
        })
    }

    pub fn new_u32_fast_field(&mut self, field: U32Field, min_value: u32, max_value: u32) -> io::Result<()> {
        if !self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Previous field not closed"));
        }
        self.field_open = true;
        self.fields.push((field, self.written_size as u64));
        let write: &mut Write = &mut self.write;
        self.written_size += try!(min_value.serialize(write));
        let amplitude = max_value - min_value;
        self.written_size += try!(amplitude.serialize(write));
        self.num_bits = compute_num_bits(amplitude);
        Ok(())
    }

    pub fn add_val(&mut self, val: u32) -> io::Result<()> {
        let write: &mut Write = &mut self.write;
        if self.mini_buffer_written + (self.num_bits as usize) > 64 {
            self.written_size += try!(self.mini_buffer.serialize(write));
            self.mini_buffer = 0;
            self.mini_buffer_written = 0;
        }
        self.mini_buffer |= (val as u64) << self.mini_buffer_written;
        self.mini_buffer_written += self.num_bits as usize;
        if self.mini_buffer_written > 0 {
            self.written_size += try!(self.mini_buffer.serialize(write));
        }
        Ok(())
    }

    pub fn close_field(&mut self,) -> io::Result<()> {
        if self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Current field is already closed"));
        }
        self.field_open = false;
        self.mini_buffer = 0;
        self.mini_buffer_written = 0;
        if self.mini_buffer_written > 0 {
            self.written_size += try!(self.mini_buffer.serialize(&mut self.write));
        }
        Ok(())
    }

    pub fn close(&mut self,) -> io::Result<usize> {
        if !self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Last field not closed"));
        }
        let header_offset: usize = self.written_size;
        self.written_size += try!(self.fields.serialize(&mut self.write));
        self.write.seek(SeekFrom::Current(-(header_offset as i64)));
        (header_offset as u64).serialize(&mut self.write);
        Ok(self.written_size)
    }
}

pub struct U32FastFieldWriters {
    field_writers: Vec<U32FastFieldWriter>,
}

impl U32FastFieldWriters {

    pub fn from_schema(schema: &Schema) -> U32FastFieldWriters {
        let u32_fields: Vec<U32Field> = schema.get_u32_fields()
            .iter()
            .enumerate()
            .filter(|&(_, field_entry)| field_entry.option.is_fast())
            .map(|(field_id, _)| U32Field(field_id as u8))
            .collect();
        U32FastFieldWriters::new(u32_fields)
    }

    pub fn new(fields: Vec<U32Field>) -> U32FastFieldWriters {
        U32FastFieldWriters {
            field_writers: fields
                .iter()
                .map(|field| U32FastFieldWriter::new(&field))
                .collect(),
        }
    }

    pub fn add_document(&mut self, doc: &Document) {
        for field_writer in self.field_writers.iter_mut() {
            field_writer.add_document(doc);
        }
    }

    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        for field_writer in self.field_writers.iter() {
            try!(field_writer.serialize(serializer));
        }
        serializer.close().map(|_| ())
    }
}

pub struct U32FastFieldWriter {
    field: U32Field,
    vals: Vec<u32>,
}

impl U32FastFieldWriter {
    pub fn new(field: &U32Field) -> U32FastFieldWriter {
        U32FastFieldWriter {
            field: field.clone(),
            vals: Vec::new(),
        }
    }

    pub fn add_val(&mut self, val: u32) {
        self.vals.push(val);
    }

    pub fn add_document(&mut self, doc: &Document) {
        let val = doc.get_u32(&self.field).unwrap_or(0u32);
        self.add_val(val);

    }

    pub fn serialize(&self, serializer: &mut FastFieldSerializer) -> io::Result<()> {
        let zero = 0;
        let min = self.vals.iter().min().unwrap_or(&zero).clone();
        let max = self.vals.iter().max().unwrap_or(&min).clone();
        try!(serializer.new_u32_fast_field(self.field.clone(), min, max));
        for val in self.vals.iter() {
            try!(serializer.add_val(val.clone()));
        }
        serializer.close_field()
    }
}

pub struct U32FastFieldReader {
    _data: ReadOnlySource,
    data_ptr: *const u64,
    min_val: u32,
    num_bits: u8,
    mask: u32,
    num_in_pack: u32,
    divider: DividerU32,
}

impl U32FastFieldReader {
    pub fn open(data: &ReadOnlySource) -> io::Result<U32FastFieldReader> {
        let mut cursor: Cursor<&[u8]> = Cursor::new(&*data);
        let min_val = try!(u32::deserialize(&mut cursor));
        let amplitude = try!(u32::deserialize(&mut cursor));
        let num_bits = compute_num_bits(amplitude);
        let mask = (1 << num_bits) - 1;
        let num_in_pack = 64u32 / (num_bits as u32);
        let ptr: *const u8 = &(data.deref()[8]);
        Ok(U32FastFieldReader {
            _data: data.slice(8, data.len()),
            data_ptr: ptr as *const u64,
            min_val: min_val,
            num_bits: num_bits,
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
    use super::U32FastFieldWriter;
    use super::U32FastFieldReader;
    use core::directory::WritePtr;
    use core::schema::Schema;
    use core::schema::FAST_U32;
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
            let write: WritePtr = Box::new(Vec::new());
            let serializer = FastFieldSerializer::new(write);
            let mut schema = Schema::new();
            let field = schema.add_u32_field("field", FAST_U32);
            let mut int_fast_field_writer = U32FastFieldWriter::new(&field);
            int_fast_field_writer.add_val(4u32);
            int_fast_field_writer.add_val(14u32);
            int_fast_field_writer.add_val(2u32);
            int_fast_field_writer.serialize(&mut serializer).unwrap();
            assert_eq!(buffer.len(), 4 + 4 + 8 as usize);
        }
        {
            let source = ReadOnlySource::Anonymous(buffer);
            let fast_field_reader = U32FastFieldReader::open(&source).unwrap();
            assert_eq!(fast_field_reader.get(0), 4u32);
            assert_eq!(fast_field_reader.get(1), 14u32);
            assert_eq!(fast_field_reader.get(2), 2u32);
        }
    }


    #[test]
    fn test_intfastfield_large() {
        let mut buffer: Vec<u8> = Vec::new();
        {
            let mut schema = Schema::new();
            let field = schema.add_u32_field("field", FAST_U32);
            let mut int_fast_field_writer = U32FastFieldWriter::new(&field);
            int_fast_field_writer.add_val(4u32);
            int_fast_field_writer.add_val(14_082_001u32);
            int_fast_field_writer.add_val(3_052u32);
            int_fast_field_writer.serialize(&mut buffer).unwrap();
            assert_eq!(buffer.len(), 24 as usize);
        }
        {
            let source = ReadOnlySource::Anonymous(buffer);
            let fast_field_reader = U32FastFieldReader::open(&source).unwrap();
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
            let mut schema = Schema::new();
            let field = schema.add_u32_field("field", FAST_U32);
            let mut int_fast_field_writer = U32FastFieldWriter::new(&field);
            for x in permutation.iter() {
                int_fast_field_writer.add_val(*x);
            }
            int_fast_field_writer.serialize(&mut buffer).unwrap();
        }
        let source = ReadOnlySource::Anonymous(buffer);
        let int_fast_field_reader = U32FastFieldReader::open(&source).unwrap();

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
            let mut schema = Schema::new();
            let field = schema.add_u32_field("field", FAST_U32);
            let permutation = generate_permutation();
            let mut int_fast_field_writer = U32FastFieldWriter::new(&field);
            for x in permutation.iter() {
                int_fast_field_writer.add_val(*x);
            }
            int_fast_field_writer.serialize(&mut buffer).unwrap();
        }
        let source = ReadOnlySource::Anonymous(buffer);
        let int_fast_field_reader = U32FastFieldReader::open(&source).unwrap();
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
            let mut schema = Schema::new();
            let field = schema.add_u32_field("field", FAST_U32);
            let mut int_fast_field_writer = U32FastFieldWriter::new(&field);
            for x in permutation.iter() {
                int_fast_field_writer.add_val(*x);
            }
            int_fast_field_writer.serialize(&mut buffer).unwrap();
        }
        let source = ReadOnlySource::Anonymous(buffer);
        let int_fast_field_reader = U32FastFieldReader::open(&source).unwrap();
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
