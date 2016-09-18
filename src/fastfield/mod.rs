mod reader;
mod writer;
mod serializer;

pub use self::writer::{U32FastFieldsWriter, U32FastFieldWriter};
pub use self::reader::{U32FastFieldsReader, U32FastFieldReader};
pub use self::serializer::FastFieldSerializer;

fn count_leading_zeros(mut val: u32) -> u8 {
    if val == 0 {
        return 32;
    }
    let mut result = 0u8;
    while (val & (1u32 << 31)) == 0 {
        val <<= 1;
        result += 1;
    }
    result
}


fn compute_num_bits(amplitude: u32) -> u8 {
    32u8 - count_leading_zeros(amplitude)
}

#[cfg(test)]
mod tests {

    use super::compute_num_bits;
    use super::U32FastFieldsReader;
    use super::U32FastFieldsWriter;
    use super::FastFieldSerializer;
    use schema::Field;
    use std::path::Path;
    use directory::{Directory, WritePtr, RAMDirectory};
    use schema::Document;
    use schema::{Schema, SchemaBuilder};
    use schema::FAST;
    use test::Bencher;
    use test;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::XorShiftRng;

    lazy_static! {
        static ref SCHEMA: Schema = {
            let mut schema_builder = SchemaBuilder::default();
            schema_builder.add_u32_field("field", FAST);
            schema_builder.build()
        };
        static ref FIELD: Field = { 
            SCHEMA.get_field("field").unwrap()
        };
    }

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

    fn add_single_field_doc(fast_field_writers: &mut U32FastFieldsWriter, field: Field, value: u32) {
        let mut doc = Document::default();
        doc.add_u32(field, value);
        fast_field_writers.add_document(&doc);
    }

    #[test]
    fn test_intfastfield_small() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&SCHEMA);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 13u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 14u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 2u32);
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 26 as usize);
        }
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(*FIELD).unwrap();
            assert_eq!(fast_field_reader.get(0), 13u32);
            assert_eq!(fast_field_reader.get(1), 14u32);
            assert_eq!(fast_field_reader.get(2), 2u32);
        }
    }

    #[test]
    fn test_intfastfield_large() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&SCHEMA);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 4u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 14_082_001u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 3_052u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 9002u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 15_001u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 777u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 1_002u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 1_501u32);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 215u32);
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 50 as usize);
        }
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(*FIELD).unwrap();
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
    
     #[test]
     fn test_intfastfield_null_amplitude() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();


        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&SCHEMA);
            for _ in 0..10_000 {
                add_single_field_doc(&mut fast_field_writers, *FIELD, 100_000u32);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 26 as usize);
        }
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(*FIELD).unwrap();
            for doc in 0..10_000 {
                assert_eq!(fast_field_reader.get(doc), 100_000u32);
            }
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
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&SCHEMA);
            for x in &permutation {
                add_single_field_doc(&mut fast_field_writers, *FIELD, *x);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(*FIELD).unwrap();
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
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&SCHEMA);
            for x in &permutation {
                add_single_field_doc(&mut fast_field_writers, *FIELD, *x);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(*FIELD).unwrap();
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
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&SCHEMA);
            for x in &permutation {
                add_single_field_doc(&mut fast_field_writers, *FIELD, *x);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
            let fast_field_reader = fast_field_readers.get_field(*FIELD).unwrap();
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
