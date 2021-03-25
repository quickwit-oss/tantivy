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
pub use self::multivalued::{MultiValuedFastFieldReader, MultiValuedFastFieldWriter};
pub use self::reader::FastFieldReader;
pub use self::readers::FastFieldReaders;
pub use self::serializer::FastFieldSerializer;
pub use self::writer::{FastFieldsWriter, IntFastFieldWriter};
use crate::common;
use crate::schema::Cardinality;
use crate::schema::FieldType;
use crate::schema::Value;
use crate::{
    chrono::{NaiveDateTime, Utc},
    schema::Type,
};

mod bytes;
mod delete;
mod error;
mod facet_reader;
mod multivalued;
mod reader;
mod readers;
mod serializer;
mod writer;

/// Trait for types that are allowed for fast fields: (u64, i64 and f64).
pub trait FastValue: Clone + Copy + Send + Sync + PartialOrd + 'static {
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

    /// Build a default value. This default value is never used, so the value does not
    /// really matter.
    fn make_zero() -> Self {
        Self::from_u64(0i64.to_u64())
    }

    /// Returns the `schema::Type` for this FastValue.
    fn to_type() -> Type;
}

impl FastValue for u64 {
    fn from_u64(val: u64) -> Self {
        val
    }

    fn to_u64(&self) -> u64 {
        *self
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::U64(ref integer_options) => integer_options.get_fastfield_cardinality(),
            FieldType::HierarchicalFacet(_) => Some(Cardinality::MultiValues),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        *self
    }

    fn to_type() -> Type {
        Type::U64
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

    fn to_type() -> Type {
        Type::I64
    }
}

impl FastValue for f64 {
    fn from_u64(val: u64) -> Self {
        common::u64_to_f64(val)
    }

    fn to_u64(&self) -> u64 {
        common::f64_to_u64(*self)
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::F64(ref integer_options) => integer_options.get_fastfield_cardinality(),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        self.to_bits()
    }

    fn to_type() -> Type {
        Type::F64
    }
}

impl FastValue for crate::DateTime {
    fn from_u64(timestamp_u64: u64) -> Self {
        let timestamp_i64 = i64::from_u64(timestamp_u64);
        crate::DateTime::from_utc(NaiveDateTime::from_timestamp(timestamp_i64, 0), Utc)
    }

    fn to_u64(&self) -> u64 {
        self.timestamp().to_u64()
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::Date(ref integer_options) => integer_options.get_fastfield_cardinality(),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        self.timestamp().as_u64()
    }

    fn to_type() -> Type {
        Type::Date
    }
}

fn value_to_u64(value: &Value) -> u64 {
    match *value {
        Value::U64(ref val) => *val,
        Value::I64(ref val) => common::i64_to_u64(*val),
        Value::F64(ref val) => common::f64_to_u64(*val),
        Value::Date(ref datetime) => common::i64_to_u64(datetime.timestamp()),
        _ => panic!("Expected a u64/i64/f64 field, got {:?} ", value),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::common::CompositeFile;
    use crate::directory::{Directory, RAMDirectory, WritePtr};
    use crate::fastfield::FastFieldReader;
    use crate::merge_policy::NoMergePolicy;
    use crate::schema::Field;
    use crate::schema::Schema;
    use crate::schema::FAST;
    use crate::schema::{Document, IntOptions};
    use crate::{Index, SegmentId, SegmentReader};
    use common::HasLen;
    use once_cell::sync::Lazy;
    use rand::prelude::SliceRandom;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::collections::HashMap;
    use std::path::Path;

    pub static SCHEMA: Lazy<Schema> = Lazy::new(|| {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("field", FAST);
        schema_builder.build()
    });

    pub static FIELD: Lazy<Field> = Lazy::new(|| SCHEMA.get_field("field").unwrap());

    #[test]
    pub fn test_fastfield() {
        let test_fastfield = FastFieldReader::<u64>::from(vec![100, 200, 300]);
        assert_eq!(test_fastfield.get(0), 100);
        assert_eq!(test_fastfield.get(1), 200);
        assert_eq!(test_fastfield.get(2), 300);
    }

    #[test]
    pub fn test_fastfield_i64_u64() {
        let datetime = crate::DateTime::from_utc(NaiveDateTime::from_timestamp(0i64, 0), Utc);
        assert_eq!(i64::from_u64(datetime.to_u64()), 0i64);
    }

    #[test]
    fn test_intfastfield_small() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RAMDirectory = RAMDirectory::create();
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
        let file = directory.open_read(&path).unwrap();
        assert_eq!(file.len(), 36 as usize);
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(*FIELD).unwrap();
        let fast_field_reader = FastFieldReader::<u64>::open(file)?;
        assert_eq!(fast_field_reader.get(0), 13u64);
        assert_eq!(fast_field_reader.get(1), 14u64);
        assert_eq!(fast_field_reader.get(2), 2u64);
        Ok(())
    }

    #[test]
    fn test_intfastfield_large() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RAMDirectory = RAMDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test"))?;
            let mut serializer = FastFieldSerializer::from_write(write)?;
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
            fast_field_writers.serialize(&mut serializer, &HashMap::new())?;
            serializer.close()?;
        }
        let file = directory.open_read(&path)?;
        assert_eq!(file.len(), 61 as usize);
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data)?;
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
        Ok(())
    }

    #[test]
    fn test_intfastfield_null_amplitude() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RAMDirectory = RAMDirectory::create();

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
        let file = directory.open_read(&path).unwrap();
        assert_eq!(file.len(), 34 as usize);
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data)?;
            for doc in 0..10_000 {
                assert_eq!(fast_field_reader.get(doc), 100_000u64);
            }
        }
        Ok(())
    }

    #[test]
    fn test_intfastfield_large_numbers() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RAMDirectory = RAMDirectory::create();

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
        let file = directory.open_read(&path).unwrap();
        assert_eq!(file.len(), 80042 as usize);
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data)?;
            assert_eq!(fast_field_reader.get(0), 0u64);
            for doc in 1..10_001 {
                assert_eq!(
                    fast_field_reader.get(doc),
                    5_000_000_000_000_000_000u64 + doc as u64 - 1u64
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_signed_intfastfield() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RAMDirectory = RAMDirectory::create();
        let mut schema_builder = Schema::builder();

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
        let file = directory.open_read(&path).unwrap();
        assert_eq!(file.len(), 17709 as usize);
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite.open_read(i64_field).unwrap();
            let fast_field_reader = FastFieldReader::<i64>::open(data)?;

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
        Ok(())
    }

    #[test]
    fn test_signed_intfastfield_default_val() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RAMDirectory = RAMDirectory::create();
        let mut schema_builder = Schema::builder();
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

        let file = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite.open_read(i64_field).unwrap();
            let fast_field_reader = FastFieldReader::<i64>::open(data)?;
            assert_eq!(fast_field_reader.get(0u32), 0i64);
        }
        Ok(())
    }

    // Warning: this generates the same permutation at each call
    pub fn generate_permutation() -> Vec<u64> {
        let mut permutation: Vec<u64> = (0u64..100_000u64).collect();
        permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
        permutation
    }

    #[test]
    fn test_intfastfield_permutation() -> crate::Result<()> {
        let path = Path::new("test");
        let permutation = generate_permutation();
        let n = permutation.len();
        let directory = RAMDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test"))?;
            let mut serializer = FastFieldSerializer::from_write(write)?;
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for &x in &permutation {
                fast_field_writers.add_document(&doc!(*FIELD=>x));
            }
            fast_field_writers.serialize(&mut serializer, &HashMap::new())?;
            serializer.close()?;
        }
        let file = directory.open_read(&path)?;
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data)?;

            let mut a = 0u64;
            for _ in 0..n {
                assert_eq!(fast_field_reader.get(a as u32), permutation[a as usize]);
                a = fast_field_reader.get(a as u32);
            }
        }
        Ok(())
    }

    #[test]
    fn test_merge_missing_date_fast_field() {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        index_writer.add_document(doc!(date_field =>crate::chrono::prelude::Utc::now()));
        index_writer.commit().unwrap();
        index_writer.add_document(doc!());
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let segment_ids: Vec<SegmentId> = reader
            .searcher()
            .segment_readers()
            .iter()
            .map(SegmentReader::segment_id)
            .collect();
        assert_eq!(segment_ids.len(), 2);
        let merge_future = index_writer.merge(&segment_ids[..]);
        let merge_res = futures::executor::block_on(merge_future);
        assert!(merge_res.is_ok());
        assert!(reader.reload().is_ok());
        assert_eq!(reader.searcher().segment_readers().len(), 1);
    }

    #[test]
    fn test_default_datetime() {
        assert_eq!(crate::DateTime::make_zero().timestamp(), 0i64);
    }

    #[test]
    fn test_datefastfield() {
        use crate::fastfield::FastValue;
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", FAST);
        let multi_date_field = schema_builder.add_date_field(
            "multi_date",
            IntOptions::default().set_fast(Cardinality::MultiValues),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        index_writer.add_document(doc!(
            date_field => crate::DateTime::from_u64(1i64.to_u64()),
            multi_date_field => crate::DateTime::from_u64(2i64.to_u64()),
            multi_date_field => crate::DateTime::from_u64(3i64.to_u64())
        ));
        index_writer.add_document(doc!(
            date_field => crate::DateTime::from_u64(4i64.to_u64())
        ));
        index_writer.add_document(doc!(
            multi_date_field => crate::DateTime::from_u64(5i64.to_u64()),
            multi_date_field => crate::DateTime::from_u64(6i64.to_u64())
        ));
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();
        let date_fast_field = fast_fields.date(date_field).unwrap();
        let dates_fast_field = fast_fields.dates(multi_date_field).unwrap();
        let mut dates = vec![];
        {
            assert_eq!(date_fast_field.get(0u32).timestamp(), 1i64);
            dates_fast_field.get_vals(0u32, &mut dates);
            assert_eq!(dates.len(), 2);
            assert_eq!(dates[0].timestamp(), 2i64);
            assert_eq!(dates[1].timestamp(), 3i64);
        }
        {
            assert_eq!(date_fast_field.get(1u32).timestamp(), 4i64);
            dates_fast_field.get_vals(1u32, &mut dates);
            assert!(dates.is_empty());
        }
        {
            assert_eq!(date_fast_field.get(2u32).timestamp(), 0i64);
            dates_fast_field.get_vals(2u32, &mut dates);
            assert_eq!(dates.len(), 2);
            assert_eq!(dates[0].timestamp(), 5i64);
            assert_eq!(dates[1].timestamp(), 6i64);
        }
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::tests::FIELD;
    use super::tests::{generate_permutation, SCHEMA};
    use super::*;
    use crate::common::CompositeFile;
    use crate::directory::{Directory, RAMDirectory, WritePtr};
    use crate::fastfield::FastFieldReader;
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
        let directory: RAMDirectory = RAMDirectory::create();
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
        let file = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data).unwrap();

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
        let directory: RAMDirectory = RAMDirectory::create();
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
        let file = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = FastFieldReader::<u64>::open(data).unwrap();

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
