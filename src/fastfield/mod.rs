/*!
Column oriented field storage for tantivy.

It is the equivalent of `Lucene`'s `DocValues`.

Fast fields is a column-oriented fashion storage of `tantivy`.

It is designed for the fast random access of some document
fields given a document id.

`FastField` are useful when a field is required for all or most of
the `DocSet` : for instance for scoring, grouping, filtering, or faceting.


Fields have to be declared as `FAST` in the  schema.
Currently only 64-bits integers (signed or unsigned) are
supported.

They are stored in a bit-packed fashion so that their
memory usage is directly linear with the amplitude of the
values stored.

Read access performance is comparable to that of an array lookup.
*/

pub use self::bytes::{BytesFastFieldReader, BytesFastFieldWriter};
pub use self::delete::write_delete_bitset;
pub use self::delete::DeleteBitSet;
pub use self::error::{FastFieldNotAvailableError, Result};
pub use self::facet_reader::FacetReader;
pub use self::multivalued::{MultiValueIntFastFieldReader, MultiValueIntFastFieldWriter};
pub use self::reader::FastFieldReader;
pub use self::serializer::FastFieldSerializer;
pub use self::writer::{FastFieldsWriter, IntFastFieldWriter};
use common;
use schema::Cardinality;
use schema::FieldType;
use schema::Value;

mod bytes;
mod delete;
mod error;
mod facet_reader;
mod multivalued;
mod reader;
mod serializer;
mod writer;

/// Trait for types that are allowed for fast fields: (u64 or i64).
pub trait FastValue: Default + Clone + Copy {
    /// Converts a value from u64
    ///
    /// Internally all fast field values are encoded as u64.
    fn from_u64(val: u64) -> Self;

    /// Converts a value to u64.
    ///
    /// Internally all fast field values are encoded as u64.
    fn to_u64(&self) -> u64;

    /// Returns the fast field cardinality that can be extracted from the given
    /// `FieldType`.
    ///
    /// If the type is not a fast field, `None` is returned.
    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality>;

    /// Cast value to `u64`.
    /// The value is just reinterpreted in memory.
    fn as_u64(&self) -> u64;
}

impl FastValue for u64 {
    fn from_u64(val: u64) -> Self {
        val
    }

    fn to_u64(&self) -> u64 {
        *self
    }

    fn as_u64(&self) -> u64 {
        *self
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::U64(ref integer_options) => integer_options.get_fastfield_cardinality(),
            FieldType::HierarchicalFacet => Some(Cardinality::MultiValues),
            _ => None,
        }
    }
}

impl FastValue for i64 {
    fn from_u64(val: u64) -> Self {
        common::u64_to_i64(val)
    }

    fn to_u64(&self) -> u64 {
        common::i64_to_u64(*self)
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::I64(ref integer_options) => integer_options.get_fastfield_cardinality(),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        *self as u64
    }
}

fn value_to_u64(value: &Value) -> u64 {
    match *value {
        Value::U64(ref val) => *val,
        Value::I64(ref val) => common::i64_to_u64(*val),
        _ => panic!("Expected a u64/i64 field, got {:?} ", value),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use common::CompositeFile;
    use directory::{Directory, RAMDirectory, WritePtr};
    use fastfield::FastFieldReader;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::XorShiftRng;
    use schema::Document;
    use schema::Field;
    use schema::FAST;
    use schema::{Schema, SchemaBuilder};
    use std::collections::HashMap;
    use std::path::Path;

    lazy_static! {
        pub static ref SCHEMA: Schema = {
            let mut schema_builder = SchemaBuilder::default();
            schema_builder.add_u64_field("field", FAST);
            schema_builder.build()
        };
        pub static ref FIELD: Field = { SCHEMA.get_field("field").unwrap() };
    }

    #[test]
    pub fn test_fastfield() {
        let test_fastfield = FastFieldReader::<u64>::from(vec![100, 200, 300]);
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
            fast_field_writers.add_document(&doc!(*FIELD=>13u64));
            fast_field_writers.add_document(&doc!(*FIELD=>14u64));
            fast_field_writers.add_document(&doc!(*FIELD=>2u64));
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 36 as usize);
        }
        {
            let composite_file = CompositeFile::open(&source).unwrap();
            let field_source = composite_file.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(field_source);
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
            fast_field_writers.add_document(&doc!(*FIELD=>4u64));
            fast_field_writers.add_document(&doc!(*FIELD=>14_082_001u64));
            fast_field_writers.add_document(&doc!(*FIELD=>3_052u64));
            fast_field_writers.add_document(&doc!(*FIELD=>9_002u64));
            fast_field_writers.add_document(&doc!(*FIELD=>15_001u64));
            fast_field_writers.add_document(&doc!(*FIELD=>777u64));
            fast_field_writers.add_document(&doc!(*FIELD=>1_002u64));
            fast_field_writers.add_document(&doc!(*FIELD=>1_501u64));
            fast_field_writers.add_document(&doc!(*FIELD=>215u64));
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 61 as usize);
        }
        {
            let fast_fields_composite = CompositeFile::open(&source).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data);
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
                fast_field_writers.add_document(&doc!(*FIELD=>100_000u64));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 34 as usize);
        }
        {
            let fast_fields_composite = CompositeFile::open(&source).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data);
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
            fast_field_writers.add_document(&doc!(*FIELD=>0u64));
            for i in 0u64..10_000u64 {
                fast_field_writers.add_document(&doc!(*FIELD=>5_000_000_000_000_000_000u64 + i));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 80042 as usize);
        }
        {
            let fast_fields_composite = CompositeFile::open(&source).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data);
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
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            assert_eq!(source.len(), 17709 as usize);
        }
        {
            let fast_fields_composite = CompositeFile::open(&source).unwrap();
            let data = fast_fields_composite.open_read(i64_field).unwrap();
            let fast_field_reader = FastFieldReader::<i64>::open(data);

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
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }

        let source = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&source).unwrap();
            let data = fast_fields_composite.open_read(i64_field).unwrap();
            let fast_field_reader = FastFieldReader::<i64>::open(data);
            assert_eq!(fast_field_reader.get(0u32), 0i64);
        }
    }

    pub fn generate_permutation() -> Vec<u64> {
        let seed: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut rng = XorShiftRng::from_seed(seed);
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
            for &x in &permutation {
                fast_field_writers.add_document(&doc!(*FIELD=>x));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&source).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data);

            let mut a = 0u64;
            for _ in 0..n {
                assert_eq!(fast_field_reader.get(a as u32), permutation[a as usize]);
                a = fast_field_reader.get(a as u32);
            }
        }
    }

}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::tests::FIELD;
    use super::tests::{generate_permutation, SCHEMA};
    use super::*;
    use common::CompositeFile;
    use directory::{Directory, RAMDirectory, WritePtr};
    use fastfield::FastFieldReader;
    use std::collections::HashMap;
    use std::path::Path;
    use test::{self, Bencher};

    #[bench]
    fn bench_intfastfield_linear_veclookup(b: &mut Bencher) {
        let permutation = generate_permutation();
        b.iter(|| {
            let n = test::black_box(7000u32);
            let mut a = 0u64;
            for i in (0u32..n / 7).map(|v| v * 7) {
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
            for &x in &permutation {
                fast_field_writers.add_document(&doc!(*FIELD=>x));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&source).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data);

            b.iter(|| {
                let n = test::black_box(7000u32);
                let mut a = 0u64;
                for i in (0u32..n / 7).map(|val| val * 7) {
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
            for &x in &permutation {
                fast_field_writers.add_document(&doc!(*FIELD=>x));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&source).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data);

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
