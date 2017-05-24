#![doc(html_logo_url = "http://fulmicoton.com/tantivy-logo/tantivy-logo.png")]
#![cfg_attr(feature = "cargo-clippy", allow(module_inception))]
#![cfg_attr(feature = "cargo-clippy", allow(inline_always))]

#![feature(box_syntax)]
#![feature(optin_builtin_traits)]
#![feature(conservative_impl_trait)]
#![feature(integer_atomics)]

#![cfg_attr(test, feature(test))]
#![cfg_attr(test, feature(step_by))]

#![doc(test(attr(allow(unused_variables), deny(warnings))))]

#![allow(unknown_lints)]

#![warn(missing_docs)]

//! # `tantivy`
//!
//! Tantivy is a search engine library.
//! Think `Lucene`, but in Rust.
//!
//! A good place for you to get started is to check out
//! the example code (
//! [literate programming](http://fulmicoton.com/tantivy-examples/simple_search.html) /
//! [source code](https://github.com/fulmicoton/tantivy/blob/master/examples/simple_search.rs))

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate log;

#[macro_use]
extern crate version;
extern crate fst;
extern crate byteorder;
extern crate memmap;
extern crate regex;
extern crate tempfile;
extern crate atomicwrites;
extern crate tempdir;
extern crate serde;
extern crate bincode;
extern crate serde_json;
extern crate time;
extern crate lz4;
extern crate uuid;
extern crate num_cpus;
extern crate combine;
extern crate itertools;
extern crate chan;
extern crate crossbeam;
extern crate bit_set;
extern crate futures;
extern crate futures_cpupool;

#[cfg(test)]
extern crate env_logger;

#[cfg(feature="simdcompression")]
extern crate libc;

#[cfg(windows)]
extern crate winapi;

#[cfg(test)]
extern crate test;
#[cfg(test)]
extern crate rand;


#[cfg(test)]
mod functional_test;




#[macro_use]
mod macros;

pub use error::Error;

/// Tantivy result.
pub type Result<T> = std::result::Result<T, Error>;

mod core;
mod compression;
mod indexer;
pub mod common;
mod error;
mod analyzer;
mod datastruct;

pub mod termdict;

// Row-oriented, slow, compressed storage of documents
pub mod store;

/// Query module
pub mod query;

pub mod directory;

/// Collector module
pub mod collector;

/// Postings module (also called inverted index)
pub mod postings;
/// Schema
pub mod schema;

pub mod fastfield;


pub use directory::Directory;
pub use core::{Index, Segment, SegmentId, SegmentMeta, Searcher};
pub use indexer::IndexWriter;
pub use schema::{Term, Document};
pub use core::SegmentReader;
pub use self::common::TimerTree;

pub use postings::DocSet;
pub use postings::Postings;
pub use core::SegmentComponent;
pub use postings::SegmentPostingsOption;


/// Expose the current version of tantivy, as well
/// whether it was compiled with the simd compression.
pub fn version() -> &'static str {
    if cfg!(feature = "simdcompression") {
        concat!(version!(), "-simd")
    } else {
        concat!(version!(), "-nosimd")
    }
}

/// Defines tantivy's merging strategy
pub mod merge_policy {
    pub use indexer::MergePolicy;
    pub use indexer::LogMergePolicy;
    pub use indexer::NoMergePolicy;
    pub use indexer::DefaultMergePolicy;
}

/// u32 identifying a document within a segment.
/// Documents have their doc id assigned incrementally,
/// as they are added in the segment.
pub type DocId = u32;

/// f32 the score of a document.
pub type Score = f32;

/// A segment local id identifies a segment.
/// It only makes sense for a given searcher.
pub type SegmentLocalId = u32;

impl DocAddress {
    /// Return the segment ordinal.
    /// The segment ordinal is an id identifying the segment
    /// hosting the document. It is only meaningful, in the context
    /// of a searcher.
    pub fn segment_ord(&self) -> SegmentLocalId {
        self.0
    }

    /// Return the segment local `DocId`
    pub fn doc(&self) -> DocId {
        self.1
    }
}


/// `DocAddress` contains all the necessary information
/// to identify a document given a `Searcher` object.
///
/// It consists in an id identifying its segment, and
/// its segment-local `DocId`.
///
/// The id used for the segment is actually an ordinal
/// in the list of segment hold by a `Searcher`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DocAddress(pub SegmentLocalId, pub DocId);


#[cfg(test)]
mod tests {

    use collector::tests::TestCollector;
    use Index;
    use core::SegmentReader;
    use query::BooleanQuery;
    use postings::SegmentPostingsOption;
    use schema::*;
    use DocSet;
    use IndexWriter;
    use postings::SegmentPostingsOption::FreqAndPositions;
    use fastfield::{FastFieldReader, U64FastFieldReader, I64FastFieldReader};
    use Postings;
    use rand::{XorShiftRng, Rng, SeedableRng};

    fn generate_array_with_seed(n: usize, ratio: f32, seed_val: u32) -> Vec<u32> {
        let seed: &[u32; 4] = &[1, 2, 3, seed_val];
        let mut rng: XorShiftRng = XorShiftRng::from_seed(*seed);
        (0..u32::max_value())
            .filter(|_| rng.next_f32() < ratio)
            .take(n)
            .collect()
    }

    pub fn generate_array(n: usize, ratio: f32) -> Vec<u32> {
        generate_array_with_seed(n, ratio, 4)
    }

    fn sample_with_seed(n: u32, ratio: f32, seed_val: u32) -> Vec<u32> {
        let seed: &[u32; 4] = &[1, 2, 3, seed_val];
        let mut rng: XorShiftRng = XorShiftRng::from_seed(*seed);
        (0..n).filter(|_| rng.next_f32() < ratio).collect()
    }

    pub fn sample(n: u32, ratio: f32) -> Vec<u32> {
        sample_with_seed(n, ratio, 4)
    }

    #[test]
    fn test_indexing() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_from_tempdir(schema).unwrap();
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field=>"af b");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field=>"a b c");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field=>"a b c d");
                index_writer.add_document(doc);
            }
            assert!(index_writer.commit().is_ok());
        }

    }

    #[test]
    fn test_docfreq() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
        {
            index_writer.add_document(doc!(text_field=>"a b c"));
            index_writer.commit().unwrap();
        }
        {
            {
                let doc = doc!(text_field=>"a");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field=>"a a");
                index_writer.add_document(doc);
            }
            index_writer.commit().unwrap();
        }
        {
            let doc = doc!(text_field=>"c");
            index_writer.add_document(doc);
            index_writer.commit().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let term_a = Term::from_field_text(text_field, "a");
            assert_eq!(searcher.doc_freq(&term_a), 3);
            let term_b = Term::from_field_text(text_field, "b");
            assert_eq!(searcher.doc_freq(&term_b), 1);
            let term_c = Term::from_field_text(text_field, "c");
            assert_eq!(searcher.doc_freq(&term_c), 2);
            let term_d = Term::from_field_text(text_field, "d");
            assert_eq!(searcher.doc_freq(&term_d), 0);
        }
    }


    #[test]
    fn test_fieldnorm() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field=>"a b c");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!();
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field=>"a b");
                index_writer.add_document(doc);
            }
            index_writer.commit().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let segment_reader: &SegmentReader = searcher.segment_reader(0);
            let fieldnorms_reader = segment_reader.get_fieldnorms_reader(text_field).unwrap();
            assert_eq!(fieldnorms_reader.get(0), 3);
            assert_eq!(fieldnorms_reader.get(1), 0);
            assert_eq!(fieldnorms_reader.get(2), 2);
        }
    }


    #[test]
    fn test_delete_postings1() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let term_abcd = Term::from_field_text(text_field, "abcd");
        let term_a = Term::from_field_text(text_field, "a");
        let term_b = Term::from_field_text(text_field, "b");
        let term_c = Term::from_field_text(text_field, "c");
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                // 0
                let doc = doc!(text_field=>"a b");
                index_writer.add_document(doc);
            }
            {
                // 1
                let doc = doc!(text_field=>" a c");
                index_writer.add_document(doc);
            }
            {
                // 2
                let doc = doc!(text_field=>" b c");
                index_writer.add_document(doc);
            }
            {
                // 3
                let doc = doc!(text_field=>" b d");
                index_writer.add_document(doc);
            }
            {
                index_writer.delete_term(Term::from_field_text(text_field, "c"));
            }
            {
                index_writer.delete_term(Term::from_field_text(text_field, "a"));
            }
            {
                // 4
                let doc = doc!(text_field=>" b c");
                index_writer.add_document(doc);
            }
            {
                // 5
                let doc = doc!(text_field=>" a");
                index_writer.add_document(doc);
            }
            index_writer.commit().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let reader = searcher.segment_reader(0);
            assert!(reader.read_postings(&term_abcd, FreqAndPositions).is_none());
            {
                let mut postings = reader.read_postings(&term_a, FreqAndPositions).unwrap();
                assert!(postings.advance());
                assert_eq!(postings.doc(), 5);
                assert!(!postings.advance());
            }
            {
                let mut postings = reader.read_postings(&term_b, FreqAndPositions).unwrap();
                assert!(postings.advance());
                assert_eq!(postings.doc(), 3);
                assert!(postings.advance());
                assert_eq!(postings.doc(), 4);
                assert!(!postings.advance());
            }
        }
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                // 0
                let doc = doc!(text_field=>"a b");
                index_writer.add_document(doc);
            }
            {
                // 1
                index_writer.delete_term(Term::from_field_text(text_field, "c"));
            }
            index_writer.rollback().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let reader = searcher.segment_reader(0);

            assert!(reader.read_postings(&term_abcd, FreqAndPositions).is_none());
            {
                let mut postings = reader.read_postings(&term_a, FreqAndPositions).unwrap();
                assert!(postings.advance());
                assert_eq!(postings.doc(), 5);
                assert!(!postings.advance());
            }
            {
                let mut postings = reader.read_postings(&term_b, FreqAndPositions).unwrap();
                assert!(postings.advance());
                assert_eq!(postings.doc(), 3);
                assert!(postings.advance());
                assert_eq!(postings.doc(), 4);
                assert!(!postings.advance());
            }
        }
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field=>"a b");
                index_writer.add_document(doc);
            }
            {
                index_writer.delete_term(Term::from_field_text(text_field, "c"));
            }
            index_writer = index_writer.rollback().unwrap();
            index_writer.delete_term(Term::from_field_text(text_field, "a"));
            index_writer.commit().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let reader = searcher.segment_reader(0);
            assert!(reader.read_postings(&term_abcd, FreqAndPositions).is_none());
            {
                let mut postings = reader.read_postings(&term_a, FreqAndPositions).unwrap();
                assert!(!postings.advance());
            }
            {
                let mut postings = reader.read_postings(&term_b, FreqAndPositions).unwrap();
                assert!(postings.advance());
                assert_eq!(postings.doc(), 3);
                assert!(postings.advance());
                assert_eq!(postings.doc(), 4);
                assert!(!postings.advance());
            }
            {
                let mut postings = reader.read_postings(&term_c, FreqAndPositions).unwrap();
                assert!(postings.advance());
                assert_eq!(postings.doc(), 4);
                assert!(!postings.advance());
            }
        }
    }


    #[test]
    fn test_indexed_u64() {
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_u64_field("value", INT_INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
        index_writer.add_document(doc!(field=>1u64));
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let term = Term::from_field_u64(field, 1u64);
        let mut postings = searcher
            .segment_reader(0)
            .read_postings(&term, SegmentPostingsOption::NoFreq)
            .unwrap();
        assert!(postings.advance());
        assert_eq!(postings.doc(), 0);
        assert!(!postings.advance());
    }

    #[test]
    fn test_indexed_i64() {
        let mut schema_builder = SchemaBuilder::default();
        let value_field = schema_builder.add_i64_field("value", INT_INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
        let negative_val = -1i64;
        index_writer.add_document(doc!(value_field => negative_val));
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let term = Term::from_field_i64(value_field, negative_val);
        let mut postings = searcher
            .segment_reader(0)
            .read_postings(&term, SegmentPostingsOption::NoFreq)
            .unwrap();
        assert!(postings.advance());
        assert_eq!(postings.doc(), 0);
        assert!(!postings.advance());
    }

    #[test]
    fn test_delete_postings2() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        // writing the segment
        let mut index_writer = index.writer_with_num_threads(2, 40_000_000).unwrap();

        let add_document = |index_writer: &mut IndexWriter, val: &'static str| {
            let doc = doc!(text_field=>val);
            index_writer.add_document(doc);
        };

        let remove_document = |index_writer: &mut IndexWriter, val: &'static str| {
            let delterm = Term::from_field_text(text_field, val);
            index_writer.delete_term(delterm);
        };

        add_document(&mut index_writer, "63");
        add_document(&mut index_writer, "70");
        add_document(&mut index_writer, "34");
        add_document(&mut index_writer, "1");
        add_document(&mut index_writer, "38");
        add_document(&mut index_writer, "33");
        add_document(&mut index_writer, "40");
        add_document(&mut index_writer, "17");
        remove_document(&mut index_writer, "38");
        remove_document(&mut index_writer, "34");
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        assert_eq!(searcher.num_docs(), 6);
    }

    #[test]
    fn test_termfreq() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field=>"af af af bc bc");
                index_writer.add_document(doc);
            }
            index_writer.commit().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let reader = searcher.segment_reader(0);
            let term_abcd = Term::from_field_text(text_field, "abcd");
            assert!(reader.read_postings(&term_abcd, FreqAndPositions).is_none());
            let term_af = Term::from_field_text(text_field, "af");
            let mut postings = reader.read_postings(&term_af, FreqAndPositions).unwrap();
            assert!(postings.advance());
            assert_eq!(postings.doc(), 0);
            assert_eq!(postings.term_freq(), 3);
            assert!(!postings.advance());
        }
    }

    #[test]
    fn test_searcher_1() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field=>"af af af b");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field=>"a b c");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field=>"a b c d");
                index_writer.add_document(doc);
            }
            index_writer.commit().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let get_doc_ids = |terms: Vec<Term>| {
                let query = BooleanQuery::new_multiterms_query(terms);
                let mut collector = TestCollector::default();
                assert!(searcher.search(&query, &mut collector).is_ok());
                collector.docs()
            };
            {
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "a")]),
                           vec![1, 2]);
            }
            {
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "af")]),
                           vec![0]);
            }
            {
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "b")]),
                           vec![0, 1, 2]);
            }
            {
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "c")]),
                           vec![1, 2]);
            }
            {
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "d")]),
                           vec![2]);
            }
            {
                assert_eq!(get_doc_ids(vec![Term::from_field_text(text_field, "b"),
                                            Term::from_field_text(text_field, "a")]),
                           vec![0, 1, 2]);
            }
        }
    }

    #[test]
    fn test_searcher_2() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field=>"af b");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field=>"a b c");
                index_writer.add_document(doc);
            }
            {
                let doc = doc!(text_field=>"a b c d");
                index_writer.add_document(doc);
            }
            index_writer.commit().unwrap();
        }
        index.searcher();
    }

    #[test]
    fn test_doc_macro() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let other_text_field = schema_builder.add_text_field("text2", TEXT);
        let document = doc!(text_field => "tantivy",
                            text_field => "some other value",
                            other_text_field => "short");
        assert_eq!(document.len(), 3);
        let values = document.get_all(text_field);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].text(), "tantivy");
        assert_eq!(values[1].text(), "some other value");
        let values = document.get_all(other_text_field);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].text(), "short");
    }

    #[test]
    fn test_wrong_fast_field_type() {
        let mut schema_builder = SchemaBuilder::default();
        let fast_field_unsigned = schema_builder.add_u64_field("unsigned", FAST);
        let fast_field_signed = schema_builder.add_i64_field("signed", FAST);
        let text_field = schema_builder.add_text_field("text", TEXT);
        let stored_int_field = schema_builder.add_u64_field("text", INT_STORED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
        {
            let document = doc!(fast_field_unsigned => 4u64, fast_field_signed=>4i64);
            index_writer.add_document(document);
            index_writer.commit().unwrap();
        }

        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let segment_reader: &SegmentReader = searcher.segment_reader(0);
        {
            let fast_field_reader_res =
                segment_reader.get_fast_field_reader::<U64FastFieldReader>(text_field);
            assert!(fast_field_reader_res.is_err());
        }
        {
            let fast_field_reader_res =
                segment_reader.get_fast_field_reader::<U64FastFieldReader>(stored_int_field);
            assert!(fast_field_reader_res.is_err());
        }
        {
            let fast_field_reader_res =
                segment_reader.get_fast_field_reader::<U64FastFieldReader>(fast_field_signed);
            assert!(fast_field_reader_res.is_err());
        }
        {
            let fast_field_reader_res =
                segment_reader.get_fast_field_reader::<I64FastFieldReader>(fast_field_signed);
            assert!(fast_field_reader_res.is_ok());
            let fast_field_reader = fast_field_reader_res.unwrap();
            assert_eq!(fast_field_reader.get(0), 4i64)
        }

        {
            let fast_field_reader_res =
                segment_reader.get_fast_field_reader::<I64FastFieldReader>(fast_field_signed);
            assert!(fast_field_reader_res.is_ok());
            let fast_field_reader = fast_field_reader_res.unwrap();
            assert_eq!(fast_field_reader.get(0), 4i64)
        }

    }
}
