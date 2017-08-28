/*!
Fast fields is a column oriented storage storage.

It is the equivalent of `Lucene`'s `DocValues`.

Fast fields is a column-oriented fashion storage of `tantivy`.

It is designed for the fast random access of some document
fields given a document id.

`FastField` are useful when a field is required for all or most of
the `DocSet` : for instance for scoring, grouping, filtering, or facetting.


Fields have to be declared as `FAST` in the  schema.
Currently only 64-bits integers (signed or unsigned) are
supported.

They are stored in a bitpacked fashion so that their
memory usage is directly linear with the amplitude of the
values stored.

Read access performance is comparable to that of an array lookup.
*/

mod reader;
mod writer;
mod serializer;
mod error;
mod delete;

pub use self::delete::write_delete_bitset;
pub use self::delete::DeleteBitSet;
pub use self::writer::{FastFieldsWriter, IntFastFieldWriter};
pub use self::reader::{U64FastFieldReader, I64FastFieldReader};
pub use self::reader::FastFieldReader;
pub use self::serializer::FastFieldSerializer;
pub use self::error::{Result, FastFieldNotAvailableError};

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
    use common::CompositeFile;
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

    fn add_single_field_doc(fast_field_writers: &mut FastFieldsWriter, field: Field, value: u64) {
        let mut doc = Document::default();
        doc.add_u64(field, value);
        fast_field_writers.add_document(&doc);
    }

    #[test]
    pub fn test_fastfield() {
        let test_fastfield = U64FastFieldReader::from(vec![100, 200, 300]);
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
            let mut serializer = FastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 13u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 14u64);
            add_single_field_doc(&mut fast_field_writers, *FIELD, 2u64);
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 35 as usize);
        }
        {
            let composite_file = CompositeFile::open(source).unwrap();
            let field_source = composite_file.open_read(*FIELD).unwrap();
            let fast_field_reader: U64FastFieldReader = U64FastFieldReader::open(field_source);
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
            let mut serializer = FastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
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
            assert_eq!(source.len(), 60 as usize);
        }
        {
            let fast_fields_composite = CompositeFile::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader =
                U64FastFieldReader::open(fast_fields_composite.open_read(*FIELD).unwrap());
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
            let mut serializer = FastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for _ in 0..10_000 {
                add_single_field_doc(&mut fast_field_writers, *FIELD, 100_000u64);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 33 as usize);
        }
        {
            let fast_fields_composite = CompositeFile::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader =
                U64FastFieldReader::open(fast_fields_composite.open_read(*FIELD).unwrap());
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
            let mut serializer = FastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            // forcing the amplitude to be high
            add_single_field_doc(&mut fast_field_writers, *FIELD, 0u64);
            for i in 0u64..10_000u64 {
                add_single_field_doc(
                    &mut fast_field_writers,
                    *FIELD,
                    5_000_000_000_000_000_000u64 + i,
                );
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 80041 as usize);
        }
        {
            let fast_fields_composite = CompositeFile::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader =
                U64FastFieldReader::open(fast_fields_composite.open_read(*FIELD).unwrap());

            assert_eq!(fast_field_reader.get(0), 0u64);
            for doc in 1..10_001 {
                assert_eq!(
                    fast_field_reader.get(doc),
                    5_000_000_000_000_000_000u64 + doc as u64 - 1u64
                );
            }
        }
    }

    #[test]
    fn test_signed_intfastfield() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        let mut schema_builder = SchemaBuilder::new();

        let i64_field = schema_builder.add_i64_field("field", FAST);
        let schema = schema_builder.build();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            for i in -100i64..10_000i64 {
                let mut doc = Document::default();
                doc.add_i64(i64_field, i);
                fast_field_writers.add_document(&doc);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 17708 as usize);
        }
        {
            let fast_fields_composite = CompositeFile::open(source).unwrap();
            let fast_field_reader: I64FastFieldReader =
                I64FastFieldReader::open(fast_fields_composite.open_read(i64_field).unwrap());

            assert_eq!(fast_field_reader.min_value(), -100i64);
            assert_eq!(fast_field_reader.max_value(), 9_999i64);
            for (doc, i) in (-100i64..10_000i64).enumerate() {
                assert_eq!(fast_field_reader.get(doc as u32), i);
            }
            let mut buffer = vec![0i64; 100];
            fast_field_reader.get_range(53, &mut buffer[..]);
            for i in 0..100 {
                assert_eq!(buffer[i], -100i64 + 53i64 + i as i64);
            }
        }
    }

    #[test]
    fn test_signed_intfastfield_default_val() {
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        let mut schema_builder = SchemaBuilder::new();
        let i64_field = schema_builder.add_i64_field("field", FAST);
        let schema = schema_builder.build();

        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            let doc = Document::default();
            fast_field_writers.add_document(&doc);
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }

        let source = directory.open_read(&path).unwrap();
        {

            let fast_fields_composite = CompositeFile::open(source).unwrap();
            let fast_field_reader: I64FastFieldReader =
                I64FastFieldReader::open(fast_fields_composite.open_read(i64_field).unwrap());
            assert_eq!(fast_field_reader.get(0u32), 0i64);
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
            let mut serializer = FastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for x in &permutation {
                add_single_field_doc(&mut fast_field_writers, *FIELD, *x);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader =
                U64FastFieldReader::open(fast_fields_composite.open_read(*FIELD).unwrap());

            let mut a = 0u64;
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
            let mut a = 0u64;
            for i in Iterator::step_by((0u32..n), 7) {
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
            let mut serializer = FastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for x in &permutation {
                add_single_field_doc(&mut fast_field_writers, *FIELD, *x);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader =
                U64FastFieldReader::open(fast_fields_composite.open_read(*FIELD).unwrap());


            b.iter(|| {
                let n = test::black_box(7000u32);
                let mut a = 0u64;
                for i in Iterator::step_by((0u32..n), 7) {
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
            let mut serializer = FastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for x in &permutation {
                add_single_field_doc(&mut fast_field_writers, *FIELD, *x);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(source).unwrap();
            let fast_field_reader: U64FastFieldReader =
                U64FastFieldReader::open(fast_fields_composite.open_read(*FIELD).unwrap());

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
