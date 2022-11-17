//! Column oriented field storage for tantivy.
//!
//! It is the equivalent of `Lucene`'s `DocValues`.
//!
//! A fast field is a column-oriented fashion storage for `tantivy`.
//!
//! It is designed for the fast random access of some document
//! fields given a document id.
//!
//! Fast fields are useful when a field is required for all or most of
//! the `DocSet`: for instance for scoring, grouping, aggregation, filtering, or faceting.
//!
//!
//! Fields have to be declared as `FAST` in the schema.
//! Currently supported fields are: u64, i64, f64, bytes and text.
//!
//! Fast fields are stored in with [different codecs](fastfield_codecs). The best codec is detected
//! automatically, when serializing.
//!
//! Read access performance is comparable to that of an array lookup.

use fastfield_codecs::MonotonicallyMappableToU64;

pub use self::alive_bitset::{intersect_alive_bitsets, write_alive_bitset, AliveBitSet};
pub use self::bytes::{BytesFastFieldReader, BytesFastFieldWriter};
pub use self::error::{FastFieldNotAvailableError, Result};
pub use self::facet_reader::FacetReader;
pub(crate) use self::multivalued::{get_fastfield_codecs_for_multivalue, MultivalueStartIndex};
pub use self::multivalued::{
    MultiValueIndex, MultiValueU128FastFieldWriter, MultiValuedFastFieldReader,
    MultiValuedFastFieldWriter, MultiValuedU128FastFieldReader,
};
pub use self::readers::FastFieldReaders;
pub(crate) use self::readers::{type_and_cardinality, FastType};
pub use self::serializer::{Column, CompositeFastFieldSerializer};
use self::writer::unexpected_value;
pub use self::writer::{FastFieldsWriter, IntFastFieldWriter};
use crate::schema::{Type, Value};
use crate::DateTime;

mod alive_bitset;
mod bytes;
mod error;
mod facet_reader;
mod multivalued;
mod readers;
mod serializer;
mod writer;

/// Trait for types that are allowed for fast fields:
/// (u64, i64 and f64, bool, DateTime).
pub trait FastValue:
    MonotonicallyMappableToU64 + Copy + Send + Sync + PartialOrd + 'static
{
    /// Returns the `schema::Type` for this FastValue.
    fn to_type() -> Type;

    /// Build a default value. This default value is never used, so the value does not
    /// really matter.
    fn make_zero() -> Self {
        Self::from_u64(0u64)
    }
}

impl FastValue for u64 {
    fn to_type() -> Type {
        Type::U64
    }
}

impl FastValue for i64 {
    fn to_type() -> Type {
        Type::I64
    }
}

impl FastValue for f64 {
    fn to_type() -> Type {
        Type::F64
    }
}

impl FastValue for bool {
    fn to_type() -> Type {
        Type::Bool
    }
}

impl MonotonicallyMappableToU64 for DateTime {
    fn to_u64(self) -> u64 {
        self.timestamp_micros.to_u64()
    }

    fn from_u64(val: u64) -> Self {
        let timestamp_micros = i64::from_u64(val);
        DateTime { timestamp_micros }
    }
}

impl FastValue for DateTime {
    fn to_type() -> Type {
        Type::Date
    }

    fn make_zero() -> Self {
        DateTime {
            timestamp_micros: 0,
        }
    }
}

fn value_to_u64(value: &Value) -> crate::Result<u64> {
    let value = match value {
        Value::U64(val) => val.to_u64(),
        Value::I64(val) => val.to_u64(),
        Value::F64(val) => val.to_u64(),
        Value::Bool(val) => val.to_u64(),
        Value::Date(val) => val.to_u64(),
        _ => return Err(unexpected_value("u64/i64/f64/bool/date", value)),
    };
    Ok(value)
}

/// The fast field type
pub enum FastFieldType {
    /// Numeric type, e.g. f64.
    Numeric,
    /// Fast field stores string ids.
    String,
    /// Fast field stores string ids for facets.
    Facet,
}

impl FastFieldType {
    fn is_storing_term_ids(&self) -> bool {
        matches!(self, FastFieldType::String | FastFieldType::Facet)
    }

    fn is_facet(&self) -> bool {
        matches!(self, FastFieldType::Facet)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::ops::Range;
    use std::path::Path;
    use std::sync::Arc;

    use common::HasLen;
    use fastfield_codecs::{open, FastFieldCodecType};
    use once_cell::sync::Lazy;
    use rand::prelude::SliceRandom;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::directory::{CompositeFile, Directory, RamDirectory, WritePtr};
    use crate::merge_policy::NoMergePolicy;
    use crate::schema::{Cardinality, Document, Field, Schema, SchemaBuilder, FAST, STRING, TEXT};
    use crate::time::OffsetDateTime;
    use crate::{DateOptions, DatePrecision, Index, SegmentId, SegmentReader};

    pub static SCHEMA: Lazy<Schema> = Lazy::new(|| {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("field", FAST);
        schema_builder.build()
    });
    pub static FIELD: Lazy<Field> = Lazy::new(|| SCHEMA.get_field("field").unwrap());

    #[test]
    pub fn test_fastfield() {
        let test_fastfield = fastfield_codecs::serialize_and_load(&[100u64, 200u64, 300u64][..]);
        assert_eq!(test_fastfield.get_val(0), 100);
        assert_eq!(test_fastfield.get_val(1), 200);
        assert_eq!(test_fastfield.get_val(2), 300);
    }

    #[test]
    pub fn test_fastfield_i64_u64() {
        let datetime = DateTime::from_utc(OffsetDateTime::UNIX_EPOCH);
        assert_eq!(i64::from_u64(datetime.to_u64()), 0i64);
    }

    #[test]
    fn test_intfastfield_small() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            fast_field_writers
                .add_document(&doc!(*FIELD=>13u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>14u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>2u64))
                .unwrap();
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 25);
        let composite_file = CompositeFile::open(&file)?;
        let fast_field_bytes = composite_file.open_read(*FIELD).unwrap().read_bytes()?;
        let fast_field_reader = open::<u64>(fast_field_bytes)?.to_full().unwrap();
        assert_eq!(fast_field_reader.get_val(0), 13u64);
        assert_eq!(fast_field_reader.get_val(1), 14u64);
        assert_eq!(fast_field_reader.get_val(2), 2u64);
        Ok(())
    }

    #[test]
    fn test_intfastfield_large() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test"))?;
            let mut serializer = CompositeFastFieldSerializer::from_write(write)?;
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            fast_field_writers
                .add_document(&doc!(*FIELD=>4u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>14_082_001u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>3_052u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>9_002u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>15_001u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>777u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>1_002u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>1_501u64))
                .unwrap();
            fast_field_writers
                .add_document(&doc!(*FIELD=>215u64))
                .unwrap();
            fast_field_writers.serialize(&mut serializer, &HashMap::new(), None)?;
            serializer.close()?;
        }
        let file = directory.open_read(path)?;
        assert_eq!(file.len(), 53);
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite
                .open_read(*FIELD)
                .unwrap()
                .read_bytes()?;
            let fast_field_reader = open::<u64>(data)?.to_full().unwrap();
            assert_eq!(fast_field_reader.get_val(0), 4u64);
            assert_eq!(fast_field_reader.get_val(1), 14_082_001u64);
            assert_eq!(fast_field_reader.get_val(2), 3_052u64);
            assert_eq!(fast_field_reader.get_val(3), 9002u64);
            assert_eq!(fast_field_reader.get_val(4), 15_001u64);
            assert_eq!(fast_field_reader.get_val(5), 777u64);
            assert_eq!(fast_field_reader.get_val(6), 1_002u64);
            assert_eq!(fast_field_reader.get_val(7), 1_501u64);
            assert_eq!(fast_field_reader.get_val(8), 215u64);
        }
        Ok(())
    }

    #[test]
    fn test_intfastfield_null_amplitude() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();

        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for _ in 0..10_000 {
                fast_field_writers
                    .add_document(&doc!(*FIELD=>100_000u64))
                    .unwrap();
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 26);
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite
                .open_read(*FIELD)
                .unwrap()
                .read_bytes()?;
            let fast_field_reader = open::<u64>(data)?.to_full().unwrap();
            for doc in 0..10_000 {
                assert_eq!(fast_field_reader.get_val(doc), 100_000u64);
            }
        }
        Ok(())
    }

    #[test]
    fn test_intfastfield_large_numbers() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();

        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            // forcing the amplitude to be high
            fast_field_writers
                .add_document(&doc!(*FIELD=>0u64))
                .unwrap();
            for i in 0u64..10_000u64 {
                fast_field_writers
                    .add_document(&doc!(*FIELD=>5_000_000_000_000_000_000u64 + i))
                    .unwrap();
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 80040);
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite
                .open_read(*FIELD)
                .unwrap()
                .read_bytes()?;
            let fast_field_reader = open::<u64>(data)?.to_full().unwrap();
            assert_eq!(fast_field_reader.get_val(0), 0u64);
            for doc in 1..10_001 {
                assert_eq!(
                    fast_field_reader.get_val(doc),
                    5_000_000_000_000_000_000u64 + doc as u64 - 1u64
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_signed_intfastfield_normal() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        let mut schema_builder = Schema::builder();

        let i64_field = schema_builder.add_i64_field("field", FAST);
        let schema = schema_builder.build();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            for i in -100i64..10_000i64 {
                let mut doc = Document::default();
                doc.add_i64(i64_field, i);
                fast_field_writers.add_document(&doc).unwrap();
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 40_usize);

        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite
                .open_read(i64_field)
                .unwrap()
                .read_bytes()?;
            let fast_field_reader = open::<i64>(data)?.to_full().unwrap();

            assert_eq!(fast_field_reader.min_value(), -100i64);
            assert_eq!(fast_field_reader.max_value(), 9_999i64);
            for (doc, i) in (-100i64..10_000i64).enumerate() {
                assert_eq!(fast_field_reader.get_val(doc as u32), i);
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
        let directory: RamDirectory = RamDirectory::create();
        let mut schema_builder = Schema::builder();
        let i64_field = schema_builder.add_i64_field("field", FAST);
        let schema = schema_builder.build();

        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            let doc = Document::default();
            fast_field_writers.add_document(&doc).unwrap();
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }

        let file = directory.open_read(path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite
                .open_read(i64_field)
                .unwrap()
                .read_bytes()?;
            let fast_field_reader = open::<i64>(data)?.to_full().unwrap();
            assert_eq!(fast_field_reader.get_val(0), 0i64);
        }
        Ok(())
    }

    // Warning: this generates the same permutation at each call
    pub fn generate_permutation() -> Vec<u64> {
        let mut permutation: Vec<u64> = (0u64..100_000u64).collect();
        permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
        permutation
    }

    // Warning: this generates the same permutation at each call
    pub fn generate_permutation_gcd() -> Vec<u64> {
        let mut permutation: Vec<u64> = (1u64..100_000u64).map(|el| el * 1000).collect();
        permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
        permutation
    }

    fn test_intfastfield_permutation_with_data(permutation: Vec<u64>) -> crate::Result<()> {
        let path = Path::new("test");
        let n = permutation.len();
        let directory = RamDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test"))?;
            let mut serializer = CompositeFastFieldSerializer::from_write(write)?;
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for &x in &permutation {
                fast_field_writers.add_document(&doc!(*FIELD=>x)).unwrap();
            }
            fast_field_writers.serialize(&mut serializer, &HashMap::new(), None)?;
            serializer.close()?;
        }
        let file = directory.open_read(path)?;
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite
                .open_read(*FIELD)
                .unwrap()
                .read_bytes()?;
            let fast_field_reader = open::<u64>(data)?.to_full().unwrap();

            for a in 0..n {
                assert_eq!(fast_field_reader.get_val(a as u32), permutation[a as usize]);
            }
        }
        Ok(())
    }

    #[test]
    fn test_intfastfield_permutation_gcd() -> crate::Result<()> {
        let permutation = generate_permutation_gcd();
        test_intfastfield_permutation_with_data(permutation)?;
        Ok(())
    }

    #[test]
    fn test_intfastfield_permutation() -> crate::Result<()> {
        let permutation = generate_permutation();
        test_intfastfield_permutation_with_data(permutation)?;
        Ok(())
    }

    #[test]
    fn test_merge_missing_date_fast_field() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        index_writer
            .add_document(doc!(date_field =>DateTime::from_utc(OffsetDateTime::now_utc())))?;
        index_writer.commit()?;
        index_writer.add_document(doc!())?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let segment_ids: Vec<SegmentId> = reader
            .searcher()
            .segment_readers()
            .iter()
            .map(SegmentReader::segment_id)
            .collect();
        assert_eq!(segment_ids.len(), 2);
        index_writer.merge(&segment_ids[..]).wait().unwrap();
        reader.reload()?;
        assert_eq!(reader.searcher().segment_readers().len(), 1);
        Ok(())
    }

    #[test]
    fn test_default_date() {
        assert_eq!(0, DateTime::make_zero().into_timestamp_secs());
    }

    fn get_vals_for_docs(ff: &MultiValuedFastFieldReader<u64>, docs: Range<u32>) -> Vec<u64> {
        let mut all = vec![];

        for doc in docs {
            let mut out: Vec<u64> = vec![];
            ff.get_vals(doc, &mut out);
            all.extend(out);
        }
        all
    }

    #[test]
    fn test_text_fastfield() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            // first segment
            let mut index_writer = index.writer_for_tests()?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            index_writer.add_document(doc!(
                text_field => "BBBBB AAAAA", // term_ord 1,2
            ))?;
            index_writer.add_document(doc!())?;
            index_writer.add_document(doc!(
                text_field => "AAAAA", // term_ord 0
            ))?;
            index_writer.add_document(doc!(
                text_field => "AAAAA BBBBB", // term_ord 0
            ))?;
            index_writer.add_document(doc!(
                text_field => "zumberthree", // term_ord 2, after merge term_ord 3
            ))?;

            index_writer.add_document(doc!())?;
            index_writer.commit()?;

            let reader = index.reader()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            let segment_reader = searcher.segment_reader(0);
            let fast_fields = segment_reader.fast_fields();
            let text_fast_field = fast_fields.u64s(text_field).unwrap();

            assert_eq!(
                get_vals_for_docs(&text_fast_field, 0..5),
                vec![1, 0, 0, 0, 1, 2]
            );

            let mut out = vec![];
            text_fast_field.get_vals(3, &mut out);
            assert_eq!(out, vec![0, 1]);

            let inverted_index = segment_reader.inverted_index(text_field)?;
            assert_eq!(inverted_index.terms().num_terms(), 3);
            let mut bytes = vec![];
            assert!(inverted_index.terms().ord_to_term(0, &mut bytes)?);
            // default tokenizer applies lower case
            assert_eq!(bytes, "aaaaa".as_bytes());
        }

        {
            // second segment
            let mut index_writer = index.writer_for_tests()?;

            index_writer.add_document(doc!(
                text_field => "AAAAA", // term_ord 0
            ))?;

            index_writer.add_document(doc!(
                text_field => "CCCCC AAAAA", // term_ord 1, after merge 2
            ))?;

            index_writer.add_document(doc!())?;
            index_writer.commit()?;

            let reader = index.reader()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 2);
            let segment_reader = searcher.segment_reader(1);
            let fast_fields = segment_reader.fast_fields();
            let text_fast_field = fast_fields.u64s(text_field).unwrap();

            assert_eq!(get_vals_for_docs(&text_fast_field, 0..3), vec![0, 1, 0]);
        }
        // Merging the segments
        {
            let segment_ids = index.searchable_segment_ids()?;
            let mut index_writer = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();
        let text_fast_field = fast_fields.u64s(text_field).unwrap();

        assert_eq!(
            get_vals_for_docs(&text_fast_field, 0..8),
            vec![1, 0, 0, 0, 1, 3 /* next segment */, 0, 2, 0]
        );

        Ok(())
    }

    #[test]
    fn test_string_fastfield() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", STRING | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            // first segment
            let mut index_writer = index.writer_for_tests()?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            index_writer.add_document(doc!(
                text_field => "BBBBB", // term_ord 1
            ))?;
            index_writer.add_document(doc!())?;
            index_writer.add_document(doc!(
                text_field => "AAAAA", // term_ord 0
            ))?;
            index_writer.add_document(doc!(
                text_field => "AAAAA", // term_ord 0
            ))?;
            index_writer.add_document(doc!(
                text_field => "zumberthree", // term_ord 2, after merge term_ord 3
            ))?;

            index_writer.add_document(doc!())?;
            index_writer.commit()?;

            let reader = index.reader()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            let segment_reader = searcher.segment_reader(0);
            let fast_fields = segment_reader.fast_fields();
            let text_fast_field = fast_fields.u64s(text_field).unwrap();

            assert_eq!(get_vals_for_docs(&text_fast_field, 0..6), vec![1, 0, 0, 2]);

            let inverted_index = segment_reader.inverted_index(text_field)?;
            assert_eq!(inverted_index.terms().num_terms(), 3);
            let mut bytes = vec![];
            assert!(inverted_index.terms().ord_to_term(0, &mut bytes)?);
            assert_eq!(bytes, "AAAAA".as_bytes());
        }

        {
            // second segment
            let mut index_writer = index.writer_for_tests()?;

            index_writer.add_document(doc!(
                text_field => "AAAAA", // term_ord 0
            ))?;

            index_writer.add_document(doc!(
                text_field => "CCCCC", // term_ord 1, after merge 2
            ))?;

            index_writer.add_document(doc!())?;
            index_writer.commit()?;

            let reader = index.reader()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 2);
            let segment_reader = searcher.segment_reader(1);
            let fast_fields = segment_reader.fast_fields();
            let text_fast_field = fast_fields.u64s(text_field).unwrap();

            assert_eq!(get_vals_for_docs(&text_fast_field, 0..2), vec![0, 1]);
        }
        // Merging the segments
        {
            let segment_ids = index.searchable_segment_ids()?;
            let mut index_writer = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();
        let text_fast_field = fast_fields.u64s(text_field).unwrap();

        assert_eq!(
            get_vals_for_docs(&text_fast_field, 0..9),
            vec![1, 0, 0, 3 /* next segment */, 0, 2]
        );

        Ok(())
    }

    #[test]
    fn test_datefastfield() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field(
            "date",
            DateOptions::from(FAST).set_precision(DatePrecision::Microseconds),
        );
        let multi_date_field = schema_builder.add_date_field(
            "multi_date",
            DateOptions::default()
                .set_precision(DatePrecision::Microseconds)
                .set_fast(Cardinality::MultiValues),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        index_writer.add_document(doc!(
            date_field => DateTime::from_u64(1i64.to_u64()),
            multi_date_field => DateTime::from_u64(2i64.to_u64()),
            multi_date_field => DateTime::from_u64(3i64.to_u64())
        ))?;
        index_writer.add_document(doc!(
            date_field => DateTime::from_u64(4i64.to_u64())
        ))?;
        index_writer.add_document(doc!(
            multi_date_field => DateTime::from_u64(5i64.to_u64()),
            multi_date_field => DateTime::from_u64(6i64.to_u64())
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();
        let date_fast_field = fast_fields.date(date_field).unwrap();
        let dates_fast_field = fast_fields.dates(multi_date_field).unwrap();
        let mut dates = vec![];
        {
            assert_eq!(
                date_fast_field.get_val(0).unwrap().into_timestamp_micros(),
                1i64
            );
            dates_fast_field.get_vals(0u32, &mut dates);
            assert_eq!(dates.len(), 2);
            assert_eq!(dates[0].into_timestamp_micros(), 2i64);
            assert_eq!(dates[1].into_timestamp_micros(), 3i64);
        }
        {
            assert_eq!(
                date_fast_field.get_val(1).unwrap().into_timestamp_micros(),
                4i64
            );
            dates_fast_field.get_vals(1u32, &mut dates);
            assert!(dates.is_empty());
        }
        {
            assert_eq!(
                date_fast_field.get_val(2).unwrap().into_timestamp_micros(),
                0i64
            );
            dates_fast_field.get_vals(2u32, &mut dates);
            assert_eq!(dates.len(), 2);
            assert_eq!(dates[0].into_timestamp_micros(), 5i64);
            assert_eq!(dates[1].into_timestamp_micros(), 6i64);
        }
        Ok(())
    }

    #[test]
    pub fn test_fastfield_bool() {
        let test_fastfield: Arc<dyn Column<bool>> =
            fastfield_codecs::serialize_and_load::<bool>(&[true, false, true, false]);
        assert_eq!(test_fastfield.get_val(0), true);
        assert_eq!(test_fastfield.get_val(1), false);
        assert_eq!(test_fastfield.get_val(2), true);
        assert_eq!(test_fastfield.get_val(3), false);
    }

    #[test]
    pub fn test_fastfield_bool_small() -> crate::Result<()> {
        let path = Path::new("test_bool");
        let directory: RamDirectory = RamDirectory::create();

        let mut schema_builder = Schema::builder();
        schema_builder.add_bool_field("field_bool", FAST);
        let schema = schema_builder.build();
        let field = schema.get_field("field_bool").unwrap();

        {
            let write: WritePtr = directory.open_write(path).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            fast_field_writers.add_document(&doc!(field=>true)).unwrap();
            fast_field_writers
                .add_document(&doc!(field=>false))
                .unwrap();
            fast_field_writers.add_document(&doc!(field=>true)).unwrap();
            fast_field_writers
                .add_document(&doc!(field=>false))
                .unwrap();
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 24);
        let composite_file = CompositeFile::open(&file)?;
        let data = composite_file.open_read(field).unwrap().read_bytes()?;
        let fast_field_reader = open::<bool>(data)?.to_full().unwrap();
        assert_eq!(fast_field_reader.get_val(0), true);
        assert_eq!(fast_field_reader.get_val(1), false);
        assert_eq!(fast_field_reader.get_val(2), true);
        assert_eq!(fast_field_reader.get_val(3), false);

        Ok(())
    }

    #[test]
    pub fn test_fastfield_bool_large() -> crate::Result<()> {
        let path = Path::new("test_bool");
        let directory: RamDirectory = RamDirectory::create();

        let mut schema_builder = Schema::builder();
        schema_builder.add_bool_field("field_bool", FAST);
        let schema = schema_builder.build();
        let field = schema.get_field("field_bool").unwrap();

        {
            let write: WritePtr = directory.open_write(path).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            for _ in 0..50 {
                fast_field_writers.add_document(&doc!(field=>true)).unwrap();
                fast_field_writers
                    .add_document(&doc!(field=>false))
                    .unwrap();
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 36);
        let composite_file = CompositeFile::open(&file)?;
        let data = composite_file.open_read(field).unwrap().read_bytes()?;
        let fast_field_reader = open::<bool>(data)?.to_full().unwrap();
        for i in 0..25 {
            assert_eq!(fast_field_reader.get_val(i * 2), true);
            assert_eq!(fast_field_reader.get_val(i * 2 + 1), false);
        }

        Ok(())
    }

    #[test]
    pub fn test_fastfield_bool_default_value() -> crate::Result<()> {
        let path = Path::new("test_bool");
        let directory: RamDirectory = RamDirectory::create();

        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_bool_field("field_bool", FAST);
        let schema = schema_builder.build();

        {
            let write: WritePtr = directory.open_write(path).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write)?;
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            let doc = Document::default();
            fast_field_writers.add_document(&doc).unwrap();
            fast_field_writers.serialize(&mut serializer, &HashMap::new(), None)?;
            serializer.close()?;
        }
        let file = directory.open_read(path).unwrap();
        let composite_file = CompositeFile::open(&file)?;
        assert_eq!(file.len(), 23);
        let data = composite_file.open_read(field).unwrap().read_bytes()?;
        let fast_field_reader = open::<bool>(data)?.to_full().unwrap();
        assert_eq!(fast_field_reader.get_val(0), false);

        Ok(())
    }

    fn get_index(
        docs: &[crate::Document],
        schema: &Schema,
        codec_types: &[FastFieldCodecType],
    ) -> crate::Result<RamDirectory> {
        let directory: RamDirectory = RamDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer =
                CompositeFastFieldSerializer::from_write_with_codec(write, codec_types).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(schema);
            for doc in docs {
                fast_field_writers.add_document(doc).unwrap();
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        Ok(directory)
    }

    #[test]
    pub fn test_gcd_date() -> crate::Result<()> {
        let size_prec_sec =
            test_gcd_date_with_codec(FastFieldCodecType::Bitpacked, DatePrecision::Seconds)?;
        assert_eq!(size_prec_sec, 28 + (1_000 * 13) / 8); // 13 bits per val = ceil(log_2(number of seconds in 2hours);
        let size_prec_micro =
            test_gcd_date_with_codec(FastFieldCodecType::Bitpacked, DatePrecision::Microseconds)?;
        assert_eq!(size_prec_micro, 26 + (1_000 * 33) / 8); // 33 bits per val = ceil(log_2(number of microsecsseconds in 2hours);
        Ok(())
    }

    fn test_gcd_date_with_codec(
        codec_type: FastFieldCodecType,
        precision: DatePrecision,
    ) -> crate::Result<usize> {
        let mut rng = StdRng::seed_from_u64(2u64);
        const T0: i64 = 1_662_345_825_012_529i64;
        const ONE_HOUR_IN_MICROSECS: i64 = 3_600 * 1_000_000;
        let times: Vec<DateTime> = std::iter::repeat_with(|| {
            // +- One hour.
            let t = T0 + rng.gen_range(-ONE_HOUR_IN_MICROSECS..ONE_HOUR_IN_MICROSECS);
            DateTime::from_timestamp_micros(t)
        })
        .take(1_000)
        .collect();
        let date_options = DateOptions::default()
            .set_fast(Cardinality::SingleValue)
            .set_precision(precision);
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_date_field("field", date_options);
        let schema = schema_builder.build();

        let docs: Vec<Document> = times.iter().map(|time| doc!(field=>*time)).collect();

        let directory = get_index(&docs[..], &schema, &[codec_type])?;
        let path = Path::new("test");
        let file = directory.open_read(path).unwrap();
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(*FIELD).unwrap();
        let len = file.len();
        let test_fastfield = open::<DateTime>(file.read_bytes()?)?
            .to_full()
            .expect("temp migration solution");

        for (i, time) in times.iter().enumerate() {
            assert_eq!(test_fastfield.get_val(i as u32), time.truncate(precision));
        }
        Ok(len)
    }
}
