mod fastdivide;
mod reader;
mod writer;
mod serializer;


pub use self::fastdivide::DividerU32;
pub use self::writer::{U32FastFieldsWriter, U32FastFieldWriter};
pub use self::reader::{U32FastFieldsReader, U32FastFieldReader};
pub use self::serializer::FastFieldSerializer;

use self::fastdivide::count_leading_zeros;

fn compute_num_bits(amplitude: u32) -> u8 {
    32u8 - count_leading_zeros(amplitude)
}

#[cfg(test)]
mod tests {

    use super::compute_num_bits;
    use super::U32FastFieldsReader;
    use super::U32FastFieldsWriter;
    use super::FastFieldSerializer;
    use schema::U32Field;
    use std::path::Path;
    use directory::{Directory, WritePtr, RAMDirectory};
    use schema::Document;
    use schema::Schema;
    use schema::FAST_U32;
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

    fn add_single_field_doc(fast_field_writers: &mut U32FastFieldsWriter, field: &U32Field, value: u32) {
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
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&schema);
            add_single_field_doc(&mut fast_field_writers, &field, 13u32);
            add_single_field_doc(&mut fast_field_writers, &field, 14u32);
            add_single_field_doc(&mut fast_field_writers, &field, 2u32);
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 26 as usize);
        }
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
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
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&schema);
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
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 58 as usize);
        }
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
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
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&schema);
            for x in permutation.iter() {
                add_single_field_doc(&mut fast_field_writers, &field, x.clone());
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
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
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&schema);
            for x in permutation.iter() {
                add_single_field_doc(&mut fast_field_writers, &field, x.clone());
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
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
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&schema);
            for x in permutation.iter() {
                add_single_field_doc(&mut fast_field_writers, &field, x.clone());
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
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
