use std::io::Write;
use std::io;
use std::io::SeekFrom;
use std::io::Seek;
use core::directory::WritePtr;
use core::serialize::BinarySerializable;
use core::directory::ReadOnlySource;
use std::collections::HashMap;
use core::schema::DocId;
use core::schema::Schema;
use core::schema::Document;
use std::ops::Deref;
use core::fastdivide::count_leading_zeros;
use core::fastdivide::DividerU32;
use core::schema::U32Field;

pub fn compute_num_bits(amplitude: u32) -> u8 {
    32u8 - count_leading_zeros(amplitude)
}

pub struct FastFieldSerializer {
    write: WritePtr,
    written_size: usize,
    fields: Vec<(U32Field, u32)>,
    num_bits: u8,

    min_value: u32,

    field_open: bool,
    mini_buffer_written: usize,
    mini_buffer: u64,
}

impl FastFieldSerializer {
    pub fn new(mut write: WritePtr) -> io::Result<FastFieldSerializer> {
        // just making room for the pointer to header.
        let written_size: usize = try!(0u32.serialize(&mut write));
        Ok(FastFieldSerializer {
            write: write,
            written_size: written_size,
            fields: Vec::new(),
            num_bits: 0u8,
            field_open: false,
            mini_buffer_written: 0,
            mini_buffer: 0,
            min_value: 0,
        })
    }

    pub fn new_u32_fast_field(&mut self, field: U32Field, min_value: u32, max_value: u32) -> io::Result<()> {
        if self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Previous field not closed"));
        }
        self.min_value = min_value;
        self.field_open = true;
        self.fields.push((field, self.written_size as u32));
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
        self.mini_buffer |= ((val - self.min_value) as u64) << self.mini_buffer_written;
        self.mini_buffer_written += self.num_bits as usize;
        Ok(())
    }

    pub fn close_field(&mut self,) -> io::Result<()> {
        if !self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Current field is already closed"));
        }
        self.field_open = false;
        if self.mini_buffer_written > 0 {
            self.mini_buffer_written = 0;
            self.written_size += try!(self.mini_buffer.serialize(&mut self.write));
        }
        self.mini_buffer = 0;
        Ok(())
    }

    pub fn close(&mut self,) -> io::Result<usize> {
        if self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Last field not closed"));
        }
        let header_offset: usize = self.written_size;
        self.written_size += try!(self.fields.serialize(&mut self.write));
        try!(self.write.seek(SeekFrom::Start(0)));
        try!((header_offset as u32).serialize(&mut self.write));
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
    pub fn open(data: ReadOnlySource) -> io::Result<U32FastFieldReader> {
        let min_val;
        let amplitude;
        {
            let mut cursor = data.cursor();
            min_val = try!(u32::deserialize(&mut cursor));
            amplitude = try!(u32::deserialize(&mut cursor));
        }
        let num_bits = compute_num_bits(amplitude);
        let mask = (1 << num_bits) - 1;
        let num_in_pack = 64u32 / (num_bits as u32);
        let ptr: *const u8 = &(data.deref()[8 as usize]);
        Ok(U32FastFieldReader {
            _data: data,
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

pub struct U32FastFieldReaders {
    source: ReadOnlySource,
    field_offsets: HashMap<U32Field, (u32, u32)>,
}

impl U32FastFieldReaders {
    pub fn open(source: &ReadOnlySource) -> io::Result<U32FastFieldReaders> {
        let mut cursor = source.cursor();
        let header_offset = try!(u32::deserialize(&mut cursor));
        try!(cursor.seek(SeekFrom::Start(header_offset as u64)));
        let field_offsets: Vec<(U32Field, u32)> = try!(Vec::deserialize(&mut cursor));
        let mut end_offsets: Vec<u32> = field_offsets
            .iter()
            .map(|&(_, offset)| offset.clone())
            .collect();
        end_offsets.push(header_offset);

        let mut field_offsets_map: HashMap<U32Field, (u32, u32)> = HashMap::new();
        for (field_start_offsets, stop_offset) in field_offsets.iter().zip(end_offsets.iter().skip(1)) {
            let (field, start_offset) = field_start_offsets.clone();
            field_offsets_map.insert(field.clone(), (start_offset.clone(), stop_offset.clone()));
        }
        Ok(U32FastFieldReaders {
            field_offsets: field_offsets_map,
            source: (*source).clone(),
        })
    }

    pub fn get_field(&self, field: &U32Field) -> io::Result<U32FastFieldReader> {
        match self.field_offsets.get(field) {
            Some(&(start, stop)) => {
                let field_source = self.source.slice(start as usize, stop as usize);
                U32FastFieldReader::open(field_source)
            }
            None => {
                Err(io::Error::new(io::ErrorKind::InvalidInput, "Could not find field, has it been set as a fast field?"))
            }

        }

    }
}

#[cfg(test)]
mod tests {

    use super::compute_num_bits;
    // use super::U32FastFieldWriter;
    // use super::U32FastFieldReader;
    use super::U32FastFieldReaders;
    use super::U32FastFieldWriters;
    use core::schema::U32Field;
    use std::path::Path;
    use core::directory::WritePtr;
    use core::directory::Directory;
    use core::schema::Document;
    // use core::directory::MmapDirectory;
    use core::directory::RAMDirectory;
    use core::schema::Schema;
    use core::schema::FAST_U32;
    // use core::directory::ReadOnlySource;
    use core::fastfield::FastFieldSerializer;
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

    fn add_single_field_doc(fast_field_writers: &mut U32FastFieldWriters, field: &U32Field, value: u32) {
        let mut doc = Document::new();
        doc.set_u32(field, value);
        fast_field_writers.add_document(&doc);
    }

    #[test]
    fn test_intfastfield_small() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        let mut schema = Schema::new();
        let field = schema.add_u32_field("field", FAST_U32);
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldWriters::from_schema(&schema);
            add_single_field_doc(&mut fast_field_writers, &field, 13u32);
            add_single_field_doc(&mut fast_field_writers, &field, 14u32);
            add_single_field_doc(&mut fast_field_writers, &field, 2u32);
            fast_field_writers.serialize(&mut serializer).unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 29 as usize);
        }
        {
            let fast_field_readers = U32FastFieldReaders::open(&source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(&field).unwrap();
            assert_eq!(fast_field_reader.get(0), 13u32);
            assert_eq!(fast_field_reader.get(1), 14u32);
            assert_eq!(fast_field_reader.get(2), 2u32);
        }
    }

    #[test]
    fn test_intfastfield_large() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        let mut schema = Schema::new();
        let field = schema.add_u32_field("field", FAST_U32);
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldWriters::from_schema(&schema);
            add_single_field_doc(&mut fast_field_writers, &field, 4u32);
            add_single_field_doc(&mut fast_field_writers, &field, 14_082_001u32);
            add_single_field_doc(&mut fast_field_writers, &field, 3_052u32);
            add_single_field_doc(&mut fast_field_writers, &field, 9002u32);
            add_single_field_doc(&mut fast_field_writers, &field, 15_001u32);
            add_single_field_doc(&mut fast_field_writers, &field, 777u32);
            add_single_field_doc(&mut fast_field_writers, &field, 1_002u32);
            add_single_field_doc(&mut fast_field_writers, &field, 1_501u32);
            add_single_field_doc(&mut fast_field_writers, &field, 215u32);
            fast_field_writers.serialize(&mut serializer).unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 61 as usize);
        }
        {
            let fast_field_readers = U32FastFieldReaders::open(&source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(&field).unwrap();
            assert_eq!(fast_field_reader.get(0), 4u32);
            assert_eq!(fast_field_reader.get(1), 14_082_001u32);
            assert_eq!(fast_field_reader.get(2), 3_052u32);
            assert_eq!(fast_field_reader.get(3), 9002u32);
            assert_eq!(fast_field_reader.get(4), 15_001u32);
            assert_eq!(fast_field_reader.get(5), 777u32);
            assert_eq!(fast_field_reader.get(6), 1_002u32);
            assert_eq!(fast_field_reader.get(7), 1_501u32);
            assert_eq!(fast_field_reader.get(8), 215u32);
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
        let path = Path::new("test");
        let permutation = generate_permutation();
        let n = permutation.len();
        let mut directory = RAMDirectory::create();
        let mut schema = Schema::new();
        let field = schema.add_u32_field("field", FAST_U32);
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldWriters::from_schema(&schema);
            for x in permutation.iter() {
                add_single_field_doc(&mut fast_field_writers, &field, x.clone());
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = U32FastFieldReaders::open(&source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(&field).unwrap();
            let mut a = 0u32;
            for _ in 0..n {
                assert_eq!(fast_field_reader.get(a as u32), permutation[a as usize]);
                a = fast_field_reader.get(a as u32);
            }
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
        let path = Path::new("test");
        let permutation = generate_permutation();
        let mut directory: RAMDirectory = RAMDirectory::create();
        let mut schema = Schema::new();
        let field = schema.add_u32_field("field", FAST_U32);
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldWriters::from_schema(&schema);
            for x in permutation.iter() {
                add_single_field_doc(&mut fast_field_writers, &field, x.clone());
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = U32FastFieldReaders::open(&source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(&field).unwrap();
            b.iter(|| {
                let n = test::black_box(7000u32);
                let mut a = 0u32;
                for i in (0u32..n).step_by(7) {
                    a ^= fast_field_reader.get(i);
                }
                a
            });
        }
    }

    #[bench]
    fn bench_intfastfield_fflookup(b: &mut Bencher) {
        let path = Path::new("test");
        let permutation = generate_permutation();
        let mut directory: RAMDirectory = RAMDirectory::create();
        let mut schema = Schema::new();
        let field = schema.add_u32_field("field", FAST_U32);
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldWriters::from_schema(&schema);
            for x in permutation.iter() {
                add_single_field_doc(&mut fast_field_writers, &field, x.clone());
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = U32FastFieldReaders::open(&source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(&field).unwrap();
            b.iter(|| {
                let n = test::black_box(1000u32);
                let mut a = 0u32;
                for _ in 0u32..n {
                    a = fast_field_reader.get(a);
                }
                a
            });
        }
    }
}
