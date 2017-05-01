/// Fast field module
///
/// Fast fields are the equivalent of `DocValues` in `Lucene`.
/// Fast fields are stored in column-oriented fashion and allow fast
/// random access given a `DocId`.
///
/// Their performance is comparable to that of an array lookup.
/// They are useful when a field is required for all or most of
/// the `DocSet` : for instance for scoring, grouping, filtering, or facetting.
/// 
/// Currently only u64 fastfield are supported.

mod reader;
mod writer;
mod serializer;
pub mod delete;

pub use self::writer::{U64FastFieldsWriter, U64FastFieldWriter};
pub use self::reader::{FastFieldsReader, U64FastFieldReader};
pub use self::reader::FastFieldReader;
pub use self::serializer::FastFieldSerializer;

#[cfg(test)]
mod tests {
    use super::*;
    use schema::Field;
    use std::path::Path;
    use directory::{Directory, WritePtr, RAMDirectory};
    use schema::Document;
    use schema::{Schema, SchemaBuilder};
    use schema::FAST;
    use test::Bencher;
    use test;
    use fastfield::FastFieldReader;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::XorShiftRng;

    lazy_static! {
        static ref SCHEMA: Schema = {
            let mut schema_builder = SchemaBuilder::default();
            schema_builder.add_u64_field("field", FAST);
            schema_builder.build()
        };
        static ref FIELD: Field = { 
            SCHEMA.get_field("field").unwrap()
        };
    }

    fn add_single_field_doc(fast_field_writers: &mut U64FastFieldsWriter, field: Field, value: u64) {
        let mut doc = Document::default();
        doc.add_u64(field, value);
        fast_field_writers.add_document(&doc);
    }
     
    #[test]
    pub fn test_fastfield() {
        let test_fastfield = U64FastFieldReader::from(vec!(100,200,300));
        assert_eq!(test_fastfield.get(0), 100);
        assert_eq!(test_fastfield.get(1), 200);
        assert_eq!(test_fastfield.get(2), 300); 
    }

    #[test]
    fn test_intfastfield_small() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U64FastFieldsWriter::from_schema(&SCHEMA);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 13u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 14u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 2u64);
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 31 as usize);
        }
        {
            let fast_field_readers = FastFieldsReader::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader = fast_field_readers.open_reader(*FIELD).unwrap();
            assert_eq!(fast_field_reader.get(0), 13u64);
            assert_eq!(fast_field_reader.get(1), 14u64);
            assert_eq!(fast_field_reader.get(2), 2u64);
        }
    }

    #[test]
    fn test_intfastfield_large() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U64FastFieldsWriter::from_schema(&SCHEMA);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 4u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 14_082_001u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 3_052u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 9002u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 15_001u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 777u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 1_002u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 1_501u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 215u64);
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 56 as usize);
        }
        {
            let fast_field_readers = FastFieldsReader::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader = fast_field_readers.open_reader(*FIELD).unwrap();
            assert_eq!(fast_field_reader.get(0), 4u64);
            assert_eq!(fast_field_reader.get(1), 14_082_001u64);
            assert_eq!(fast_field_reader.get(2), 3_052u64);
            assert_eq!(fast_field_reader.get(3), 9002u64);
            assert_eq!(fast_field_reader.get(4), 15_001u64);
            assert_eq!(fast_field_reader.get(5), 777u64);
            assert_eq!(fast_field_reader.get(6), 1_002u64);
            assert_eq!(fast_field_reader.get(7), 1_501u64);
            assert_eq!(fast_field_reader.get(8), 215u64);
        }
    }
    
     #[test]
     fn test_intfastfield_null_amplitude() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();


        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U64FastFieldsWriter::from_schema(&SCHEMA);
            for _ in 0..10_000 {
                add_single_field_doc(&mut fast_field_writers, *FIELD, 100_000u64);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 29 as usize);
        }
        {
            let fast_field_readers = FastFieldsReader::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader = fast_field_readers.open_reader(*FIELD).unwrap();
            for doc in 0..10_000 {
                assert_eq!(fast_field_reader.get(doc), 100_000u64);
            }
        }
    }


     #[test]
     fn test_intfastfield_large_numbers() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U64FastFieldsWriter::from_schema(&SCHEMA);
            // forcing the amplitude to be high
            add_single_field_doc(&mut fast_field_writers, *FIELD, 0u64);
            for i in 0u64..10_000u64 {
                add_single_field_doc(&mut fast_field_writers, *FIELD, 5_000_000_000_000_000_000u64 + i);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 80037 as usize);
        }
        {
            let fast_field_readers = FastFieldsReader::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader = fast_field_readers.open_reader(*FIELD).unwrap();
            assert_eq!(fast_field_reader.get(0), 0u64);
            for doc in 1..10_001 {
                assert_eq!(fast_field_reader.get(doc), 5_000_000_000_000_000_000u64 + doc as u64 - 1u64);
            }
        }
    }

    fn generate_permutation() -> Vec<u64> {
        let seed: &[u32; 4] = &[1, 2, 3, 4];
        let mut rng = XorShiftRng::from_seed(*seed);
        let mut permutation: Vec<u64> = (0u64..1_000_000u64).collect();
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
            let mut fast_field_writers = U64FastFieldsWriter::from_schema(&SCHEMA);
            for x in &permutation {
                add_single_field_doc(&mut fast_field_writers, *FIELD, *x);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = FastFieldsReader::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader = fast_field_readers.open_reader(*FIELD).unwrap();
            let mut a = 0u64;
            for _ in 0..n {
                println!("i {}=> {} {}", a, fast_field_reader.get(a as u32), permutation[a as usize]);
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
            let mut a = 0u64;
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
            let mut a = 0u64;
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
            let mut fast_field_writers = U64FastFieldsWriter::from_schema(&SCHEMA);
            for x in &permutation {
                add_single_field_doc(&mut fast_field_writers, *FIELD, *x);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = FastFieldsReader::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader = fast_field_readers.open_reader(*FIELD).unwrap();
            b.iter(|| {
                let n = test::black_box(7000u32);
                let mut a = 0u64;
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
            let mut fast_field_writers = U64FastFieldsWriter::from_schema(&SCHEMA);
            for x in &permutation {
                add_single_field_doc(&mut fast_field_writers, *FIELD, *x);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_field_readers = FastFieldsReader::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader = fast_field_readers.open_reader(*FIELD).unwrap();
            b.iter(|| {
                let n = test::black_box(1000u32);
                let mut a = 0u32;
                for _ in 0u32..n {
                    a = fast_field_reader.get(a) as u32;
                }
                a
            });
        }
    }
}
