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
//! Currently supported fields are: u64, i64, f64, bytes, ip and text.
//!
//! Fast fields are stored in with [different codecs](fastfield_codecs). The best codec is detected
//! automatically, when serializing.
//!
//! Read access performance is comparable to that of an array lookup.

use std::net::Ipv6Addr;

pub use columnar::Column;

pub use self::alive_bitset::{intersect_alive_bitsets, write_alive_bitset, AliveBitSet};
pub use self::error::{FastFieldNotAvailableError, Result};
pub use self::facet_reader::FacetReader;
pub use self::readers::FastFieldReaders;
pub use self::writer::FastFieldsWriter;
use crate::schema::Type;
use crate::DateTime;

mod alive_bitset;
mod error;
mod facet_reader;
mod readers;
mod writer;

/// Trait for types that provide a zero value.
///
/// The resulting value is never used, just as placeholder, e.g. for `vec.resize()`.
pub trait MakeZero {
    /// Build a default value. This default value is never used, so the value does not
    /// really matter.
    fn make_zero() -> Self;
}

impl<T: FastValue> MakeZero for T {
    fn make_zero() -> Self {
        T::from_u64(0)
    }
}

impl MakeZero for u128 {
    fn make_zero() -> Self {
        0
    }
}

impl MakeZero for Ipv6Addr {
    fn make_zero() -> Self {
        Ipv6Addr::from(0u128.to_be_bytes())
    }
}

/// Trait for types that are allowed for fast fields:
/// (u64, i64 and f64, bool, DateTime).
pub trait FastValue:
    Copy + Send + Sync + columnar::MonotonicallyMappableToU64 + PartialOrd + 'static
{
    /// Returns the `schema::Type` for this FastValue.
    fn to_type() -> Type;
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
impl FastValue for DateTime {
    fn to_type() -> Type {
        Type::Date
    }
}

#[cfg(test)]
mod tests {

    use std::ops::{Range, RangeInclusive};
    use std::path::Path;

    use columnar::{Column, MonotonicallyMappableToU64, StrColumn};
    use common::{HasLen, TerminatingWrite};
    use once_cell::sync::Lazy;
    use rand::prelude::SliceRandom;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::directory::{Directory, RamDirectory, WritePtr};
    use crate::merge_policy::NoMergePolicy;
    use crate::schema::{
        Document, Facet, FacetOptions, Field, Schema, SchemaBuilder, FAST, INDEXED, STORED, STRING,
        TEXT,
    };
    use crate::time::OffsetDateTime;
    use crate::{DateOptions, DatePrecision, Index, SegmentId, SegmentReader};

    pub static SCHEMA: Lazy<Schema> = Lazy::new(|| {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("field", FAST);
        schema_builder.build()
    });
    pub static FIELD: Lazy<Field> = Lazy::new(|| SCHEMA.get_field("field").unwrap());

    #[test]
    pub fn test_convert_i64_u64() {
        let datetime = DateTime::from_utc(OffsetDateTime::UNIX_EPOCH);
        assert_eq!(i64::from_u64(datetime.to_u64()), 0i64);
    }

    #[test]
    fn test_intfastfield_small() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
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
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();

        assert_eq!(file.len(), 161);
        let fast_field_readers = FastFieldReaders::open(file).unwrap();
        let column = fast_field_readers
            .u64("field")
            .unwrap()
            .first_or_default_col(0);
        assert_eq!(column.get_val(0), 13u64);
        assert_eq!(column.get_val(1), 14u64);
        assert_eq!(column.get_val(2), 2u64);
        Ok(())
    }

    #[test]
    fn test_intfastfield_large() {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
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
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 189);
        let fast_field_readers = FastFieldReaders::open(file).unwrap();
        let col = fast_field_readers
            .u64("field")
            .unwrap()
            .first_or_default_col(0);
        assert_eq!(col.get_val(0), 4u64);
        assert_eq!(col.get_val(1), 14_082_001u64);
        assert_eq!(col.get_val(2), 3_052u64);
        assert_eq!(col.get_val(3), 9002u64);
        assert_eq!(col.get_val(4), 15_001u64);
        assert_eq!(col.get_val(5), 777u64);
        assert_eq!(col.get_val(6), 1_002u64);
        assert_eq!(col.get_val(7), 1_501u64);
        assert_eq!(col.get_val(8), 215u64);
    }

    #[test]
    fn test_intfastfield_null_amplitude() {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for _ in 0..10_000 {
                fast_field_writers
                    .add_document(&doc!(*FIELD=>100_000u64))
                    .unwrap();
            }
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 162);
        let fast_field_readers = FastFieldReaders::open(file).unwrap();
        let fast_field_reader = fast_field_readers
            .u64("field")
            .unwrap()
            .first_or_default_col(0);
        for doc in 0..10_000 {
            assert_eq!(fast_field_reader.get_val(doc), 100_000u64);
        }
    }

    #[test]
    fn test_intfastfield_large_numbers() {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();

        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            // forcing the amplitude to be high
            fast_field_writers
                .add_document(&doc!(*FIELD=>0u64))
                .unwrap();
            for doc_id in 1u64..10_000u64 {
                fast_field_writers
                    .add_document(&doc!(*FIELD=>5_000_000_000_000_000_000u64 + doc_id))
                    .unwrap();
            }
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 4557);
        {
            let fast_field_readers = FastFieldReaders::open(file).unwrap();
            let col = fast_field_readers
                .u64("field")
                .unwrap()
                .first_or_default_col(0);
            for doc in 1..10_000 {
                assert_eq!(col.get_val(doc), 5_000_000_000_000_000_000u64 + doc as u64);
            }
        }
    }

    #[test]
    fn test_signed_intfastfield_normal() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        let mut schema_builder = Schema::builder();

        let i64_field = schema_builder.add_i64_field("field", FAST);
        let schema = schema_builder.build();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            for i in -100i64..10_000i64 {
                let mut doc = Document::default();
                doc.add_i64(i64_field, i);
                fast_field_writers.add_document(&doc).unwrap();
            }
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 333_usize);

        {
            let fast_field_readers = FastFieldReaders::open(file).unwrap();
            let col = fast_field_readers
                .i64("field")
                .unwrap()
                .first_or_default_col(0);
            assert_eq!(col.min_value(), -100i64);
            assert_eq!(col.max_value(), 9_999i64);
            for (doc, i) in (-100i64..10_000i64).enumerate() {
                assert_eq!(col.get_val(doc as u32), i);
            }
            let mut buffer = vec![0i64; 100];
            col.get_range(53, &mut buffer[..]);
            for i in 0..100 {
                assert_eq!(buffer[i], -100i64 + 53i64 + i as i64);
            }
        }
        Ok(())
    }

    #[test]
    fn test_signed_intfastfield_default_val() {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("field", FAST);
        let schema = schema_builder.build();

        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            let doc = Document::default();
            fast_field_writers.add_document(&doc).unwrap();
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }

        let file = directory.open_read(path).unwrap();
        let fast_field_readers = FastFieldReaders::open(file).unwrap();
        let col = fast_field_readers.i64("field").unwrap();
        assert_eq!(col.first(0), None);

        let col = fast_field_readers
            .i64("field")
            .unwrap()
            .first_or_default_col(0);
        assert_eq!(col.get_val(0), 0);
        let col = fast_field_readers
            .i64("field")
            .unwrap()
            .first_or_default_col(-100);
        assert_eq!(col.get_val(0), -100);
    }

    #[test]
    fn test_date_fastfield_default() {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        let mut schema_builder = Schema::builder();
        schema_builder.add_date_field("date", FAST);
        let schema = schema_builder.build();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            let doc = Document::default();
            fast_field_writers.add_document(&doc).unwrap();
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }

        let file = directory.open_read(path).unwrap();
        let fast_field_readers = FastFieldReaders::open(file).unwrap();
        let col = fast_field_readers
            .date("date")
            .unwrap()
            .first_or_default_col(DateTime::default());
        assert_eq!(col.get_val(0), DateTime::default());
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

    fn test_intfastfield_permutation_with_data(permutation: Vec<u64>) {
        let path = Path::new("test");
        let n = permutation.len();
        let directory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&SCHEMA);
            for &x in &permutation {
                fast_field_writers.add_document(&doc!(*FIELD=>x)).unwrap();
            }
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        let fast_field_readers = FastFieldReaders::open(file).unwrap();
        let col = fast_field_readers
            .u64("field")
            .unwrap()
            .first_or_default_col(0);
        for a in 0..n {
            assert_eq!(col.get_val(a as u32), permutation[a]);
        }
    }

    #[test]
    fn test_intfastfield_permutation_gcd() {
        let permutation = generate_permutation_gcd();
        test_intfastfield_permutation_with_data(permutation);
    }

    #[test]
    fn test_intfastfield_permutation() {
        let permutation = generate_permutation();
        test_intfastfield_permutation_with_data(permutation);
    }

    #[test]
    fn test_merge_missing_date_fast_field() {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        index_writer
            .add_document(doc!(date_field => DateTime::from_utc(OffsetDateTime::now_utc())))
            .unwrap();
        index_writer.commit().unwrap();
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let segment_ids: Vec<SegmentId> = reader
            .searcher()
            .segment_readers()
            .iter()
            .map(SegmentReader::segment_id)
            .collect();
        assert_eq!(segment_ids.len(), 2);
        index_writer.merge(&segment_ids[..]).wait().unwrap();
        reader.reload().unwrap();
        assert_eq!(reader.searcher().segment_readers().len(), 1);
    }

    fn get_vals_for_docs(column: &Column<u64>, docs: Range<u32>) -> Vec<u64> {
        docs.into_iter()
            .flat_map(|doc| column.values(doc))
            .collect()
    }

    #[test]
    fn test_text_fastfield() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            // first segment
            let mut index_writer = index.writer_for_tests().unwrap();
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            index_writer
                .add_document(doc!(
                text_field => "BBBBB", // term ord 1
                text_field => "AAAAA", // term ord 0
                ))
                .unwrap();
            index_writer.add_document(doc!()).unwrap();
            index_writer
                .add_document(doc!(
                text_field => "AAAAA", // term_ord 0
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    text_field => "AAAAA",
                    text_field => "BBBBB",
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                text_field => "zumberthree", // term_ord 2, after merge term_ord 3
                ))
                .unwrap();

            index_writer.add_document(doc!()).unwrap();
            index_writer.commit().unwrap();

            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            let segment_reader = searcher.segment_reader(0);
            let fast_fields = segment_reader.fast_fields();
            let str_column = fast_fields.str("text").unwrap().unwrap();
            assert!(str_column.ords().values(0u32).eq([1, 0]),);
            assert!(str_column.ords().values(1u32).next().is_none());
            assert!(str_column.ords().values(2u32).eq([0]),);
            assert!(str_column.ords().values(3u32).eq([0, 1]),);
            assert!(str_column.ords().values(4u32).eq([2]),);

            let mut str_term = String::default();
            assert!(str_column.ord_to_str(0, &mut str_term).unwrap());
            assert_eq!("AAAAA", &str_term);

            let inverted_index = segment_reader.inverted_index(text_field).unwrap();
            assert_eq!(inverted_index.terms().num_terms(), 3);
            let mut bytes = vec![];
            assert!(inverted_index.terms().ord_to_term(0, &mut bytes).unwrap());
            assert_eq!(bytes, "aaaaa".as_bytes());
        }

        {
            // second segment
            let mut index_writer = index.writer_for_tests().unwrap();

            index_writer
                .add_document(doc!(
                text_field => "AAAAA", // term_ord 0
                ))
                .unwrap();

            index_writer
                .add_document(doc!(
                text_field => "CCCCC AAAAA", // term_ord 1, after merge 2
                ))
                .unwrap();

            index_writer.add_document(doc!()).unwrap();
            index_writer.commit().unwrap();

            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 2);
            let segment_reader = searcher.segment_reader(1);
            let fast_fields = segment_reader.fast_fields();
            let text_fast_field = fast_fields.str("text").unwrap().unwrap();

            assert_eq!(&get_vals_for_docs(text_fast_field.ords(), 0..2), &[0, 1]);
        }

        // TODO uncomment once merging is available
        // Merging the segments
        {
            let segment_ids = index.searchable_segment_ids().unwrap();
            let mut index_writer = index.writer_for_tests().unwrap();
            index_writer.merge(&segment_ids).wait().unwrap();
            index_writer.wait_merging_threads().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();
        let text_column = fast_fields.str("text").unwrap().unwrap();

        assert_eq!(
            get_vals_for_docs(text_column.ords(), 0..8),
            vec![1, 0, 0, 0, 1, 3 /* next segment */, 0, 2]
        );
    }

    #[test]
    fn test_string_fastfield_simple() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc!(text_field=>"hello happy tax payer", text_field=>"aaa this string comes lexicographically before the other one.")).unwrap();
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);
        let str_column = segment_reader.fast_fields().str("text").unwrap().unwrap();
        // The string values are not sorted here.
        let term_ords: Vec<u64> = str_column.term_ords(0u32).collect();
        assert_eq!(&term_ords, &[1, 0]);
    }

    #[test]
    fn test_facet_fastfield_simple() {
        let mut schema_builder = Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(facet_field=>Facet::from("/a/2"), facet_field=>Facet::from("/a/1")))
            .unwrap();
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);
        let facet_reader = segment_reader.facet_reader("facet").unwrap();
        // facets, contrary to strings are sorted.
        let mut facet_ords = Vec::new();
        facet_ords.extend(facet_reader.facet_ords(0u32));
        assert_eq!(&facet_ords, &[0, 1]);
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
            let text_col = fast_fields.str("text").unwrap().unwrap();

            assert_eq!(get_vals_for_docs(text_col.ords(), 0..6), vec![1, 0, 0, 2]);

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
            let text_fast_field = fast_fields.str("text").unwrap().unwrap();

            assert_eq!(&get_vals_for_docs(text_fast_field.ords(), 0..2), &[0, 1]);
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
        let text_fast_field = fast_fields.str("text").unwrap().unwrap();

        assert_eq!(
            get_vals_for_docs(text_fast_field.ords(), 0..9),
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
                .set_fast(),
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
        let date_fast_field = fast_fields
            .column_opt::<DateTime>("date")
            .unwrap()
            .unwrap()
            .first_or_default_col(Default::default());
        let dates_fast_field = fast_fields
            .column_opt::<DateTime>("multi_date")
            .unwrap()
            .unwrap();
        let mut dates = vec![];
        {
            assert_eq!(date_fast_field.get_val(0).into_timestamp_micros(), 1i64);
            dates_fast_field.fill_vals(0u32, &mut dates);
            assert_eq!(dates.len(), 2);
            assert_eq!(dates[0].into_timestamp_micros(), 2i64);
            assert_eq!(dates[1].into_timestamp_micros(), 3i64);
        }
        {
            assert_eq!(date_fast_field.get_val(1).into_timestamp_micros(), 4i64);
            dates_fast_field.fill_vals(1u32, &mut dates);
            assert!(dates.is_empty());
        }
        {
            assert_eq!(date_fast_field.get_val(2).into_timestamp_micros(), 0i64);
            dates_fast_field.fill_vals(2u32, &mut dates);
            assert_eq!(dates.len(), 2);
            assert_eq!(dates[0].into_timestamp_micros(), 5i64);
            assert_eq!(dates[1].into_timestamp_micros(), 6i64);
        }
        Ok(())
    }

    #[test]
    pub fn test_fastfield_bool_small() {
        let path = Path::new("test_bool");
        let directory: RamDirectory = RamDirectory::create();

        let mut schema_builder = Schema::builder();
        schema_builder.add_bool_field("field_bool", FAST);
        let schema = schema_builder.build();
        let field = schema.get_field("field_bool").unwrap();

        {
            let mut write: WritePtr = directory.open_write(path).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            fast_field_writers.add_document(&doc!(field=>true)).unwrap();
            fast_field_writers
                .add_document(&doc!(field=>false))
                .unwrap();
            fast_field_writers.add_document(&doc!(field=>true)).unwrap();
            fast_field_writers
                .add_document(&doc!(field=>false))
                .unwrap();
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 175);
        let fast_field_readers = FastFieldReaders::open(file).unwrap();
        let bool_col = fast_field_readers.bool("field_bool").unwrap();
        assert_eq!(bool_col.first(0), Some(true));
        assert_eq!(bool_col.first(1), Some(false));
        assert_eq!(bool_col.first(2), Some(true));
        assert_eq!(bool_col.first(3), Some(false));
    }

    #[test]
    pub fn test_fastfield_bool_large() {
        let path = Path::new("test_bool");
        let directory: RamDirectory = RamDirectory::create();

        let mut schema_builder = Schema::builder();
        schema_builder.add_bool_field("field_bool", FAST);
        let schema = schema_builder.build();
        let field = schema.get_field("field_bool").unwrap();

        {
            let mut write: WritePtr = directory.open_write(path).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            for _ in 0..50 {
                fast_field_writers.add_document(&doc!(field=>true)).unwrap();
                fast_field_writers
                    .add_document(&doc!(field=>false))
                    .unwrap();
            }
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 187);
        let readers = FastFieldReaders::open(file).unwrap();
        let bool_col = readers.bool("field_bool").unwrap();
        for i in 0..25 {
            assert_eq!(bool_col.first(i * 2), Some(true));
            assert_eq!(bool_col.first(i * 2 + 1), Some(false));
        }
    }

    #[test]
    pub fn test_fastfield_bool_default_value() {
        let path = Path::new("test_bool");
        let directory: RamDirectory = RamDirectory::create();
        let mut schema_builder = Schema::builder();
        schema_builder.add_bool_field("field_bool", FAST);
        let schema = schema_builder.build();
        {
            let mut write: WritePtr = directory.open_write(path).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            let doc = Document::default();
            fast_field_writers.add_document(&doc).unwrap();
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert_eq!(file.len(), 177);
        let fastfield_readers = FastFieldReaders::open(file).unwrap();
        let col = fastfield_readers.bool("field_bool").unwrap();
        assert_eq!(col.first(0), None);
        let col = fastfield_readers
            .bool("field_bool")
            .unwrap()
            .first_or_default_col(false);
        assert_eq!(col.get_val(0), false);
        let col = fastfield_readers
            .bool("field_bool")
            .unwrap()
            .first_or_default_col(true);
        assert_eq!(col.get_val(0), true);
    }

    fn get_index(docs: &[crate::Document], schema: &Schema) -> crate::Result<RamDirectory> {
        let directory: RamDirectory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut fast_field_writers = FastFieldsWriter::from_schema(schema);
            for doc in docs {
                fast_field_writers.add_document(doc).unwrap();
            }
            fast_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        Ok(directory)
    }

    #[test]
    pub fn test_gcd_date() {
        let size_prec_sec = test_gcd_date_with_codec(DatePrecision::Seconds);
        assert!((1000 * 13 / 8..100 + 1000 * 13 / 8).contains(&size_prec_sec)); // 13 bits per val = ceil(log_2(number of seconds in 2hours);
        let size_prec_micros = test_gcd_date_with_codec(DatePrecision::Microseconds);
        assert!((1000 * 33 / 8..100 + 1000 * 33 / 8).contains(&size_prec_micros));
        // 33 bits per
        // val = ceil(log_2(number
        // of microsecsseconds
        // in 2hours);
    }

    fn test_gcd_date_with_codec(precision: DatePrecision) -> usize {
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
        let date_options = DateOptions::default().set_fast().set_precision(precision);
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_date_field("field", date_options);
        let schema = schema_builder.build();

        let docs: Vec<Document> = times.iter().map(|time| doc!(field=>*time)).collect();

        let directory = get_index(&docs[..], &schema).unwrap();
        let path = Path::new("test");
        let file = directory.open_read(path).unwrap();
        let readers = FastFieldReaders::open(file).unwrap();
        let col = readers.date("field").unwrap();

        for (i, time) in times.iter().enumerate() {
            let dt: DateTime = col.first(i as u32).unwrap();
            assert_eq!(dt, time.truncate(precision));
        }
        readers.column_num_bytes("field").unwrap()
    }

    #[test]
    fn test_gcd_bug_regression_1757() {
        let mut schema_builder = Schema::builder();
        let num_field = schema_builder.add_u64_field("url_norm_hash", FAST | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut writer = index.writer_for_tests().unwrap();
            writer
                .add_document(doc! {
                    num_field => 100u64,
                })
                .unwrap();
            writer
                .add_document(doc! {
                    num_field => 200u64,
                })
                .unwrap();
            writer
                .add_document(doc! {
                    num_field => 300u64,
                })
                .unwrap();

            writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment = &searcher.segment_readers()[0];
        let field = segment
            .fast_fields()
            .u64("url_norm_hash")
            .unwrap()
            .first_or_default_col(0);

        let numbers = vec![100, 200, 300];
        let test_range = |range: RangeInclusive<u64>| {
            let expexted_count = numbers.iter().filter(|num| range.contains(num)).count();
            let mut vec = vec![];
            field.get_row_ids_for_value_range(range, 0..u32::MAX, &mut vec);
            assert_eq!(vec.len(), expexted_count);
        };
        test_range(50..=50);
        test_range(150..=150);
        test_range(350..=350);
        test_range(100..=250);
        test_range(101..=200);
        test_range(101..=199);
        test_range(100..=300);
        test_range(100..=299);
    }

    #[test]
    fn test_ip_addr_columnar_simple() {
        let mut schema_builder = Schema::builder();
        let ip_field = schema_builder.add_u64_field("ip", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();
        let ip_addr = Ipv6Addr::new(1, 2, 3, 4, 5, 1, 2, 3);
        index_writer.add_document(Document::default()).unwrap();
        index_writer.add_document(doc!(ip_field=>ip_addr)).unwrap();
        index_writer.add_document(Document::default()).unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let fastfields = searcher.segment_reader(0u32).fast_fields();
        let column: Column<Ipv6Addr> = fastfields.column_opt("ip").unwrap().unwrap();
        assert_eq!(column.num_docs(), 3);
        assert_eq!(column.first(0), None);
        assert_eq!(column.first(1), Some(ip_addr));
        assert_eq!(column.first(2), None);
    }

    #[test]
    fn test_mapping_bug_docids_for_value_range() {
        let mut schema_builder = Schema::builder();
        let num_field = schema_builder.add_u64_field("url_norm_hash", FAST | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // Values without gcd, but with min_value
            let mut writer = index.writer_for_tests().unwrap();
            writer
                .add_document(doc! {
                    num_field => 1000u64,
                })
                .unwrap();
            writer
                .add_document(doc! {
                    num_field => 1001u64,
                })
                .unwrap();
            writer
                .add_document(doc! {
                    num_field => 1003u64,
                })
                .unwrap();
            writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment = &searcher.segment_readers()[0];
        let field = segment
            .fast_fields()
            .u64("url_norm_hash")
            .unwrap()
            .first_or_default_col(0);

        let numbers = vec![1000, 1001, 1003];
        let test_range = |range: RangeInclusive<u64>| {
            let expexted_count = numbers.iter().filter(|num| range.contains(num)).count();
            let mut vec = vec![];
            field.get_row_ids_for_value_range(range, 0..u32::MAX, &mut vec);
            assert_eq!(vec.len(), expexted_count);
        };
        let test_range_variant = |start, stop| {
            let start_range = start..=stop;
            test_range(start_range);
            let start_range = start..=(stop - 1);
            test_range(start_range);
            let start_range = start..=(stop + 1);
            test_range(start_range);
            let start_range = (start - 1)..=stop;
            test_range(start_range);
            let start_range = (start - 1)..=(stop - 1);
            test_range(start_range);
            let start_range = (start - 1)..=(stop + 1);
            test_range(start_range);
            let start_range = (start + 1)..=stop;
            test_range(start_range);
            let start_range = (start + 1)..=(stop - 1);
            test_range(start_range);
            let start_range = (start + 1)..=(stop + 1);
            test_range(start_range);
        };
        test_range_variant(50, 50);
        test_range_variant(1000, 1000);
        test_range_variant(1000, 1002);
    }

    #[test]
    fn test_json_object_fast_field() {
        let mut schema_builder = Schema::builder();
        let without_fast_field = schema_builder.add_json_field("without", STORED);
        let with_fast_field = schema_builder.add_json_field("with", STORED | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(without_fast_field=>json!({"hello": "without"})))
            .unwrap();
        writer
            .add_document(doc!(with_fast_field=>json!({"hello": "with"})))
            .unwrap();
        writer
            .add_document(doc!(with_fast_field=>json!({"hello": "with2"})))
            .unwrap();
        writer
            .add_document(doc!(with_fast_field=>json!({"hello": "with1"})))
            .unwrap();
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0u32);
        let fast_fields = segment_reader.fast_fields();
        let column_without_opt: Option<StrColumn> = fast_fields.str("without\u{1}hello").unwrap();
        assert!(column_without_opt.is_none());
        let column_with_opt: Option<StrColumn> = fast_fields.str("with\u{1}hello").unwrap();
        let column_with: StrColumn = column_with_opt.unwrap();
        assert!(column_with.term_ords(0).next().is_none());
        assert!(column_with.term_ords(1).eq([0]));
        assert!(column_with.term_ords(2).eq([2]));
        assert!(column_with.term_ords(3).eq([1]));
    }
}
