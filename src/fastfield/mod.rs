//! Column oriented field storage for tantivy.
//!
//! It is the equivalent of `Lucene`'s `DocValues`.
//!
//! A fast field is a column-oriented fashion storage for `tantivy`.
//!
//! It is designed for the fast random access of some document
//! fields given a document id.
//!
//! `FastField` are useful when a field is required for all or most of
//! the `DocSet` : for instance for scoring, grouping, filtering, or faceting.
//!
//!
//! Fields have to be declared as `FAST` in the  schema.
//! Currently supported fields are: u64, i64, f64 and bytes.
//!
//! u64, i64 and f64 fields are stored in a bit-packed fashion so that
//! their memory usage is directly linear with the amplitude of the
//! values stored.
//!
//! Read access performance is comparable to that of an array lookup.

pub use self::alive_bitset::{intersect_alive_bitsets, write_alive_bitset, AliveBitSet};
pub use self::bytes::{BytesFastFieldReader, BytesFastFieldWriter};
pub use self::error::{FastFieldNotAvailableError, Result};
pub use self::facet_reader::FacetReader;
pub use self::multivalued::{MultiValuedFastFieldReader, MultiValuedFastFieldWriter};
pub use self::reader::{DynamicFastFieldReader, FastFieldReader};
pub use self::readers::FastFieldReaders;
pub(crate) use self::readers::{type_and_cardinality, FastType};
pub use self::serializer::{CompositeFastFieldSerializer, FastFieldDataAccess, FastFieldStats};
pub use self::writer::{FastFieldsWriter, IntFastFieldWriter};
use crate::schema::{Cardinality, FieldType, Type, Value};
use crate::{DateTime, DocId};

mod alive_bitset;
mod bytes;
mod error;
mod facet_reader;
mod multivalued;
mod reader;
mod readers;
mod serializer;
mod writer;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub(crate) enum FastFieldCodecName {
    Bitpacked,
    LinearInterpol,
    BlockwiseLinearInterpol,
}
pub(crate) const ALL_CODECS: &[FastFieldCodecName; 3] = &[
    FastFieldCodecName::Bitpacked,
    FastFieldCodecName::LinearInterpol,
    FastFieldCodecName::BlockwiseLinearInterpol,
];

/// Trait for `BytesFastFieldReader` and `MultiValuedFastFieldReader` to return the length of data
/// for a doc_id
pub trait MultiValueLength {
    /// returns the num of values associated to a doc_id
    fn get_len(&self, doc_id: DocId) -> u64;
    /// returns the sum of num values for all doc_ids
    fn get_total_len(&self) -> u64;
}

/// Trait for types that are allowed for fast fields:
/// (u64, i64 and f64, bool, DateTime).
pub trait FastValue: Clone + Copy + Send + Sync + PartialOrd + 'static {
    /// Converts a value from u64
    ///
    /// Internally all fast field values are encoded as u64.
    /// **Note: To be used for converting encoded Term, Posting values.**
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
            FieldType::Facet(_) => Some(Cardinality::MultiValues),
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

impl FastValue for bool {
    fn from_u64(val: u64) -> Self {
        val != 0u64
    }

    fn to_u64(&self) -> u64 {
        match self {
            false => 0,
            true => 1,
        }
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::Bool(ref integer_options) => integer_options.get_fastfield_cardinality(),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        *self as u64
    }

    fn to_type() -> Type {
        Type::Bool
    }
}

impl FastValue for DateTime {
    /// Converts a timestamp microseconds into DateTime.
    ///
    /// **Note the timestamps is expected to be in microseconds.**
    fn from_u64(timestamp_micros_u64: u64) -> Self {
        let timestamp_micros = i64::from_u64(timestamp_micros_u64);
        Self::from_timestamp_micros(timestamp_micros)
    }

    fn to_u64(&self) -> u64 {
        common::i64_to_u64(self.into_timestamp_micros())
    }

    fn fast_field_cardinality(field_type: &FieldType) -> Option<Cardinality> {
        match *field_type {
            FieldType::Date(ref options) => options.get_fastfield_cardinality(),
            _ => None,
        }
    }

    fn as_u64(&self) -> u64 {
        self.into_timestamp_micros().as_u64()
    }

    fn to_type() -> Type {
        Type::Date
    }
}

fn value_to_u64(value: &Value) -> u64 {
    match value {
        Value::U64(val) => val.to_u64(),
        Value::I64(val) => val.to_u64(),
        Value::F64(val) => val.to_u64(),
        Value::Bool(val) => val.to_u64(),
        Value::Date(val) => val.to_u64(),
        _ => panic!("Expected a u64/i64/f64/bool/date field, got {:?} ", value),
    }
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

    use common::HasLen;
    use once_cell::sync::Lazy;
    use rand::prelude::SliceRandom;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::directory::{CompositeFile, Directory, RamDirectory, WritePtr};
    use crate::merge_policy::NoMergePolicy;
    use crate::schema::{Document, Field, Schema, FAST, STRING, TEXT};
    use crate::time::OffsetDateTime;
    use crate::{DateOptions, DatePrecision, Index, SegmentId, SegmentReader};

    pub static SCHEMA: Lazy<Schema> = Lazy::new(|| {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("field", FAST);
        schema_builder.build()
    });

    pub static SCHEMAI64: Lazy<Schema> = Lazy::new(|| {
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("field", FAST);
        schema_builder.build()
    });

    pub static FIELD: Lazy<Field> = Lazy::new(|| SCHEMA.get_field("field").unwrap());
    pub static FIELDI64: Lazy<Field> = Lazy::new(|| SCHEMAI64.get_field("field").unwrap());

    #[test]
    pub fn test_fastfield() {
        let test_fastfield = DynamicFastFieldReader::<u64>::from(vec![100, 200, 300]);
        assert_eq!(test_fastfield.get(0), 100);
        assert_eq!(test_fastfield.get(1), 200);
        assert_eq!(test_fastfield.get(2), 300);
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
            fast_field_writers.add_document(&doc!(*FIELD=>13u64));
            fast_field_writers.add_document(&doc!(*FIELD=>14u64));
            fast_field_writers.add_document(&doc!(*FIELD=>2u64));
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 37);
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(*FIELD).unwrap();
        let fast_field_reader = DynamicFastFieldReader::<u64>::open(file)?;
        assert_eq!(fast_field_reader.get(0), 13u64);
        assert_eq!(fast_field_reader.get(1), 14u64);
        assert_eq!(fast_field_reader.get(2), 2u64);
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
            fast_field_writers.add_document(&doc!(*FIELD=>4u64));
            fast_field_writers.add_document(&doc!(*FIELD=>14_082_001u64));
            fast_field_writers.add_document(&doc!(*FIELD=>3_052u64));
            fast_field_writers.add_document(&doc!(*FIELD=>9_002u64));
            fast_field_writers.add_document(&doc!(*FIELD=>15_001u64));
            fast_field_writers.add_document(&doc!(*FIELD=>777u64));
            fast_field_writers.add_document(&doc!(*FIELD=>1_002u64));
            fast_field_writers.add_document(&doc!(*FIELD=>1_501u64));
            fast_field_writers.add_document(&doc!(*FIELD=>215u64));
            fast_field_writers.serialize(&mut serializer, &HashMap::new(), None)?;
            serializer.close()?;
        }
        let file = directory.open_read(path)?;
        assert_eq!(file.len(), 62);
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = DynamicFastFieldReader::<u64>::open(data)?;
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
        let directory: RamDirectory = RamDirectory::create();

        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for _ in 0..10_000 {
                fast_field_writers.add_document(&doc!(*FIELD=>100_000u64));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 35);
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = DynamicFastFieldReader::<u64>::open(data)?;
            for doc in 0..10_000 {
                assert_eq!(fast_field_reader.get(doc), 100_000u64);
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
            fast_field_writers.add_document(&doc!(*FIELD=>0u64));
            for i in 0u64..10_000u64 {
                fast_field_writers.add_document(&doc!(*FIELD=>5_000_000_000_000_000_000u64 + i));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 80043);
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = DynamicFastFieldReader::<u64>::open(data)?;
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
                fast_field_writers.add_document(&doc);
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        // assert_eq!(file.len(), 17710 as usize); //bitpacked size
        assert_eq!(file.len(), 10175_usize); // linear interpol size
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite.open_read(i64_field).unwrap();
            let fast_field_reader = DynamicFastFieldReader::<i64>::open(data)?;

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
        let directory: RamDirectory = RamDirectory::create();
        let mut schema_builder = Schema::builder();
        let i64_field = schema_builder.add_i64_field("field", FAST);
        let schema = schema_builder.build();

        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            let doc = Document::default();
            fast_field_writers.add_document(&doc);
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }

        let file = directory.open_read(path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite.open_read(i64_field).unwrap();
            let fast_field_reader = DynamicFastFieldReader::<i64>::open(data)?;
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
                fast_field_writers.add_document(&doc!(*FIELD=>x));
            }
            fast_field_writers.serialize(&mut serializer, &HashMap::new(), None)?;
            serializer.close()?;
        }
        let file = directory.open_read(path)?;
        {
            let fast_fields_composite = CompositeFile::open(&file)?;
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = DynamicFastFieldReader::<u64>::open(data)?;

            for a in 0..n {
                assert_eq!(fast_field_reader.get(a as u32), permutation[a as usize]);
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
            let mut out = vec![];
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
        use crate::fastfield::FastValue;
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
            assert_eq!(date_fast_field.get(0u32).into_timestamp_micros(), 1i64);
            dates_fast_field.get_vals(0u32, &mut dates);
            assert_eq!(dates.len(), 2);
            assert_eq!(dates[0].into_timestamp_micros(), 2i64);
            assert_eq!(dates[1].into_timestamp_micros(), 3i64);
        }
        {
            assert_eq!(date_fast_field.get(1u32).into_timestamp_micros(), 4i64);
            dates_fast_field.get_vals(1u32, &mut dates);
            assert!(dates.is_empty());
        }
        {
            assert_eq!(date_fast_field.get(2u32).into_timestamp_micros(), 0i64);
            dates_fast_field.get_vals(2u32, &mut dates);
            assert_eq!(dates.len(), 2);
            assert_eq!(dates[0].into_timestamp_micros(), 5i64);
            assert_eq!(dates[1].into_timestamp_micros(), 6i64);
        }
        Ok(())
    }

    #[test]
    pub fn test_fastfield_bool() {
        let test_fastfield = DynamicFastFieldReader::<bool>::from(vec![true, false, true, false]);
        assert_eq!(test_fastfield.get(0), true);
        assert_eq!(test_fastfield.get(1), false);
        assert_eq!(test_fastfield.get(2), true);
        assert_eq!(test_fastfield.get(3), false);
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
            fast_field_writers.add_document(&doc!(field=>true));
            fast_field_writers.add_document(&doc!(field=>false));
            fast_field_writers.add_document(&doc!(field=>true));
            fast_field_writers.add_document(&doc!(field=>false));
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 36);
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(field).unwrap();
        let fast_field_reader = DynamicFastFieldReader::<bool>::open(file)?;
        assert_eq!(fast_field_reader.get(0), true);
        assert_eq!(fast_field_reader.get(1), false);
        assert_eq!(fast_field_reader.get(2), true);
        assert_eq!(fast_field_reader.get(3), false);

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
                fast_field_writers.add_document(&doc!(field=>true));
                fast_field_writers.add_document(&doc!(field=>false));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 48);
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(field).unwrap();
        let fast_field_reader = DynamicFastFieldReader::<bool>::open(file)?;
        for i in 0..25 {
            assert_eq!(fast_field_reader.get(i * 2), true);
            assert_eq!(fast_field_reader.get(i * 2 + 1), false);
        }

        Ok(())
    }

    #[test]
    pub fn test_fastfield_bool_default_value() -> crate::Result<()> {
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
            let doc = Document::default();
            fast_field_writers.add_document(&doc);
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 35);
        let composite_file = CompositeFile::open(&file)?;
        let file = composite_file.open_read(field).unwrap();
        let fast_field_reader = DynamicFastFieldReader::<bool>::open(file)?;
        assert_eq!(fast_field_reader.get(0), false);

        Ok(())
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use std::collections::HashMap;
    use std::path::Path;

    use test::{self, Bencher};

    use super::tests::{generate_permutation, FIELD, SCHEMA};
    use super::*;
    use crate::directory::{CompositeFile, Directory, RamDirectory, WritePtr};
    use crate::fastfield::tests::generate_permutation_gcd;
    use crate::fastfield::FastFieldReader;

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
        let directory: RamDirectory = RamDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for &x in &permutation {
                fast_field_writers.add_document(&doc!(*FIELD=>x));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = DynamicFastFieldReader::<u64>::open(data).unwrap();

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
        let directory: RamDirectory = RamDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for &x in &permutation {
                fast_field_writers.add_document(&doc!(*FIELD=>x));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = DynamicFastFieldReader::<u64>::open(data).unwrap();

            b.iter(|| {
                let mut a = 0u32;
                for i in 0u32..permutation.len() as u32 {
                    a = fast_field_reader.get(i) as u32;
                }
                a
            });
        }
    }

    #[bench]
    fn bench_intfastfield_fflookup_gcd(b: &mut Bencher) {
        let path = Path::new("test");
        let permutation = generate_permutation_gcd();
        let directory: RamDirectory = RamDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = CompositeFastFieldSerializer::from_write(write).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for &x in &permutation {
                fast_field_writers.add_document(&doc!(*FIELD=>x));
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new(), None)
                .unwrap();
            serializer.close().unwrap();
        }
        let file = directory.open_read(&path).unwrap();
        {
            let fast_fields_composite = CompositeFile::open(&file).unwrap();
            let data = fast_fields_composite.open_read(*FIELD).unwrap();
            let fast_field_reader = DynamicFastFieldReader::<u64>::open(data).unwrap();

            b.iter(|| {
                let mut a = 0u32;
                for i in 0u32..permutation.len() as u32 {
                    a = fast_field_reader.get(i) as u32;
                }
                a
            });
        }
    }
}
