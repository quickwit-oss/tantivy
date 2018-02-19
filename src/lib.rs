#![doc(html_logo_url = "http://fulmicoton.com/tantivy-logo/tantivy-logo.png")]
#![cfg_attr(feature = "cargo-clippy", allow(module_inception))]
#![cfg_attr(feature = "cargo-clippy", allow(inline_always))]
#![feature(box_syntax)]
#![feature(optin_builtin_traits)]
#![feature(conservative_impl_trait)]
#![feature(collections_range)]
#![feature(integer_atomics)]
#![feature(drain_filter)]
#![cfg_attr(test, feature(test))]
#![cfg_attr(test, feature(iterator_step_by))]
#![doc(test(attr(allow(unused_variables), deny(warnings))))]
#![allow(unknown_lints)]
#![allow(new_without_default)]
#![warn(missing_docs)]

//! # `tantivy`
//!
//! Tantivy is a search engine library.
//! Think `Lucene`, but in Rust.
//!
//! ```rust

//! # extern crate tempdir;
//! #
//! #[macro_use]
//! extern crate tantivy;
//!
//! // ...
//!
//! # use std::path::Path;
//! # use tempdir::TempDir;
//! # use tantivy::Index;
//! # use tantivy::schema::*;
//! # use tantivy::collector::TopCollector;
//! # use tantivy::query::QueryParser;
//! #
//! # fn main() {
//! #     // Let's create a temporary directory for the
//! #     // sake of this example
//! #     if let Ok(dir) = TempDir::new("tantivy_example_dir") {
//! #         run_example(dir.path()).unwrap();
//! #         dir.close().unwrap();
//! #     }
//! # }
//! #
//! # fn run_example(index_path: &Path) -> tantivy::Result<()> {
//! // First we need to define a schema ...
//!
//! // `TEXT` means the field should be tokenized and indexed,
//! // along with its term frequency and term positions.
//! //
//! // `STORED` means that the field will also be saved
//! // in a compressed, row-oriented key-value store.
//! // This store is useful to reconstruct the
//! // documents that were selected during the search phase.
//! let mut schema_builder = SchemaBuilder::default();
//! let title = schema_builder.add_text_field("title", TEXT | STORED);
//! let body = schema_builder.add_text_field("body", TEXT);
//! let schema = schema_builder.build();
//!
//! // Indexing documents
//!
//! let index = Index::create(index_path, schema.clone())?;
//!
//! // Here we use a buffer of 100MB that will be split
//! // between indexing threads.
//! let mut index_writer = index.writer(100_000_000)?;
//!
//! // Let's index one documents!
//! index_writer.add_document(doc!(
//!     title => "The Old Man and the Sea",
//!     body => "He was an old man who fished alone in a skiff in \
//!             the Gulf Stream and he had gone eighty-four days \
//!             now without taking a fish."
//! ));
//!
//! // We need to call .commit() explicitly to force the
//! // index_writer to finish processing the documents in the queue,
//! // flush the current index to the disk, and advertise
//! // the existence of new documents.
//! index_writer.commit()?;
//!
//! // # Searching
//!
//! index.load_searchers()?;
//!
//! let searcher = index.searcher();
//!
//! let query_parser = QueryParser::for_index(&index, vec![title, body]);
//!
//! // QueryParser may fail if the query is not in the right
//! // format. For user facing applications, this can be a problem.
//! // A ticket has been opened regarding this problem.
//! let query = query_parser.parse_query("sea whale")?;
//!
//! let mut top_collector = TopCollector::with_limit(10);
//! searcher.search(&*query, &mut top_collector)?;
//!
//! // Our top collector now contains the 10
//! // most relevant doc ids...
//! let doc_addresses = top_collector.docs();
//! for doc_address in doc_addresses {
//!     let retrieved_doc = searcher.doc(&doc_address)?;
//!     println!("{}", schema.to_json(&retrieved_doc));
//! }
//!
//! # Ok(())
//! # }
//! ```
//!
//!
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
extern crate error_chain;

extern crate atomicwrites;
extern crate bit_set;
extern crate byteorder;
extern crate chan;
extern crate combine;
extern crate crossbeam;
extern crate fst;
extern crate futures;
extern crate futures_cpupool;
extern crate itertools;
extern crate lz4;
extern crate num_cpus;
extern crate owning_ref;
extern crate regex;
extern crate rust_stemmers;
extern crate serde;
extern crate serde_json;
extern crate stable_deref_trait;
extern crate tempdir;
extern crate tempfile;
extern crate time;
extern crate uuid;

#[cfg(test)]
extern crate env_logger;

#[cfg(feature = "simdcompression")]
extern crate libc;

#[cfg(windows)]
extern crate winapi;

#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate test;

extern crate tinysegmenter;

#[macro_use]
extern crate downcast;

#[cfg(test)]
mod functional_test;

#[macro_use]
mod macros;

pub use error::{Error, ErrorKind, ResultExt};

/// Tantivy result.
pub type Result<T> = std::result::Result<T, Error>;

mod core;
mod compression;
mod indexer;
mod common;

#[allow(unused_doc_comment)]
mod error;
pub mod tokenizer;
mod datastruct;

pub mod termdict;
pub mod store;
pub mod query;
pub mod directory;
pub mod collector;
pub mod postings;
pub mod schema;
pub mod fastfield;

mod docset;
pub use self::docset::{SkipResult, DocSet};

pub use directory::Directory;
pub use core::{Index, Searcher, Segment, SegmentId, SegmentMeta};
pub use indexer::IndexWriter;
pub use schema::{Document, Term};
pub use core::{InvertedIndexReader, SegmentReader};
pub use self::common::TimerTree;

pub use postings::Postings;
pub use core::SegmentComponent;

pub use common::{i64_to_u64, u64_to_i64};

/// Expose the current version of tantivy, as well
/// whether it was compiled with the simd compression.
pub fn version() -> &'static str {
    if cfg!(feature = "simdcompression") {
        concat!(env!("CARGO_PKG_VERSION"), "-simd")
    } else {
        concat!(env!("CARGO_PKG_VERSION"), "-nosimd")
    }
}

/// Defines tantivy's merging strategy
pub mod merge_policy {
    pub use indexer::MergePolicy;
    pub use indexer::LogMergePolicy;
    pub use indexer::NoMergePolicy;
    pub use indexer::DefaultMergePolicy;
}

/// A `u32` identifying a document within a segment.
/// Documents have their `DocId` assigned incrementally,
/// as they are added in the segment.
pub type DocId = u32;

/// A f32 that represents the relevance of the document to the query
///
/// This is modelled internally as a `f32`. The
/// larger the number, the more relevant the document
/// to the search
pub type Score = f32;

/// A `SegmentLocalId` identifies a segment.
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
    use schema::*;
    use docset::DocSet;
    use IndexWriter;
    use fastfield::{FastFieldReader, I64FastFieldReader, U64FastFieldReader};
    use Postings;
    use rand::{Rng, SeedableRng, XorShiftRng};
    use rand::distributions::{IndependentSample, Range};

    fn generate_array_with_seed(n: usize, ratio: f32, seed_val: u32) -> Vec<u32> {
        let seed: &[u32; 4] = &[1, 2, 3, seed_val];
        let mut rng: XorShiftRng = XorShiftRng::from_seed(*seed);
        (0..u32::max_value())
            .filter(|_| rng.next_f32() < ratio)
            .take(n)
            .collect()
    }

    pub fn generate_nonunique_unsorted(max_value: u32, n_elems: usize) -> Vec<u32> {
        let seed: &[u32; 4] = &[1, 2, 3, 4];
        let mut rng: XorShiftRng = XorShiftRng::from_seed(*seed);
        let between = Range::new(0u32, max_value);
        (0..n_elems)
            .map(|_| between.ind_sample(&mut rng))
            .collect::<Vec<u32>>()
    }

    pub fn generate_array(n: usize, ratio: f32) -> Vec<u32> {
        generate_array_with_seed(n, ratio, 4)
    }

    pub fn sample_with_seed(n: u32, ratio: f32, seed_val: u32) -> Vec<u32> {
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
    fn test_docfreq1() {
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
            let inverted_index = reader.inverted_index(text_field);
            assert!(
                inverted_index
                    .read_postings(&term_abcd, IndexRecordOption::WithFreqsAndPositions)
                    .is_none()
            );
            {
                let mut postings = inverted_index
                    .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
                assert!(postings.advance());
                assert_eq!(postings.doc(), 5);
                assert!(!postings.advance());
            }
            {
                let mut postings = inverted_index
                    .read_postings(&term_b, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
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
            let inverted_index = reader.inverted_index(term_abcd.field());

            assert!(
                inverted_index
                    .read_postings(&term_abcd, IndexRecordOption::WithFreqsAndPositions)
                    .is_none()
            );
            {
                let mut postings = inverted_index
                    .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
                assert!(postings.advance());
                assert_eq!(postings.doc(), 5);
                assert!(!postings.advance());
            }
            {
                let mut postings = inverted_index
                    .read_postings(&term_b, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
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
            index_writer.rollback().unwrap();
            index_writer.delete_term(Term::from_field_text(text_field, "a"));
            index_writer.commit().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let reader = searcher.segment_reader(0);
            let inverted_index = reader.inverted_index(term_abcd.field());
            assert!(
                inverted_index
                    .read_postings(&term_abcd, IndexRecordOption::WithFreqsAndPositions)
                    .is_none()
            );
            {
                let mut postings = inverted_index
                    .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
                assert!(!postings.advance());
            }
            {
                let mut postings = inverted_index
                    .read_postings(&term_b, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
                assert!(postings.advance());
                assert_eq!(postings.doc(), 3);
                assert!(postings.advance());
                assert_eq!(postings.doc(), 4);
                assert!(!postings.advance());
            }
            {
                let mut postings = inverted_index
                    .read_postings(&term_c, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
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
            .inverted_index(term.field())
            .read_postings(&term, IndexRecordOption::Basic)
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
            .inverted_index(term.field())
            .read_postings(&term, IndexRecordOption::Basic)
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
            let inverted_index = reader.inverted_index(text_field);
            let term_abcd = Term::from_field_text(text_field, "abcd");
            assert!(
                inverted_index
                    .read_postings(&term_abcd, IndexRecordOption::WithFreqsAndPositions)
                    .is_none()
            );
            let term_af = Term::from_field_text(text_field, "af");
            let mut postings = inverted_index
                .read_postings(&term_af, IndexRecordOption::WithFreqsAndPositions)
                .unwrap();
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
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "a")]),
                    vec![1, 2]
                );
            }
            {
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "af")]),
                    vec![0]
                );
            }
            {
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "b")]),
                    vec![0, 1, 2]
                );
            }
            {
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "c")]),
                    vec![1, 2]
                );
            }
            {
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "d")]),
                    vec![2]
                );
            }
            {
                assert_eq!(
                    get_doc_ids(vec![
                        Term::from_field_text(text_field, "b"),
                        Term::from_field_text(text_field, "a"),
                    ]),
                    vec![0, 1, 2]
                );
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
