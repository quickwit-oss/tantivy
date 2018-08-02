#![doc(html_logo_url = "http://fulmicoton.com/tantivy-logo/tantivy-logo.png")]
#![cfg_attr(feature = "cargo-clippy", allow(module_inception))]
#![cfg_attr(feature = "cargo-clippy", allow(inline_always))]
#![cfg_attr(all(feature = "unstable", test), feature(test))]
#![doc(test(attr(allow(unused_variables), deny(warnings))))]
#![allow(unknown_lints)]
#![allow(new_without_default)]
#![allow(decimal_literal_representation)]
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
//! let index = Index::create_in_dir(index_path, schema.clone())?;
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

#[cfg_attr(test, macro_use)]
extern crate serde_json;

#[macro_use]
extern crate log;

#[macro_use]
extern crate error_chain;

#[cfg(feature = "mmap")]
extern crate atomicwrites;
extern crate base64;
extern crate bit_set;
extern crate bitpacking;
extern crate byteorder;

#[macro_use]
extern crate combine;

extern crate crossbeam;
extern crate crossbeam_channel;
extern crate fnv;
extern crate fst;
extern crate fst_regex;
extern crate futures;
extern crate futures_cpupool;
extern crate itertools;
extern crate levenshtein_automata;
extern crate num_cpus;
extern crate owning_ref;
extern crate regex;
extern crate rust_stemmers;
extern crate serde;
extern crate stable_deref_trait;
extern crate tempdir;
extern crate tempfile;
extern crate uuid;

#[cfg(test)]
#[macro_use]
extern crate matches;

#[cfg(windows)]
extern crate winapi;

#[cfg(test)]
extern crate rand;

#[cfg(all(test, feature = "unstable"))]
extern crate test;

extern crate tinysegmenter;

#[macro_use]
extern crate downcast;

#[cfg(test)]
mod functional_test;

#[macro_use]
mod macros;

pub use error::{Error, ErrorKind, ResultExt};

extern crate census;
extern crate owned_read;

/// Tantivy result.
pub type Result<T> = std::result::Result<T, Error>;

mod common;
mod core;
mod indexer;

#[allow(unused_doc_comments)]
mod error;
pub mod tokenizer;

pub mod collector;
pub mod directory;
pub mod fastfield;
pub mod fieldnorm;
pub mod postings;
pub(crate) mod positions;
pub mod query;
pub mod schema;
pub mod store;
pub mod termdict;

mod docset;
pub use self::docset::{DocSet, SkipResult};

pub use core::SegmentComponent;
pub use core::{Index, Searcher, Segment, SegmentId, SegmentMeta};
pub use core::{InvertedIndexReader, SegmentReader};
pub use directory::Directory;
pub use indexer::IndexWriter;
pub use postings::Postings;
pub use schema::{Document, Term};

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
    pub use indexer::DefaultMergePolicy;
    pub use indexer::LogMergePolicy;
    pub use indexer::MergePolicy;
    pub use indexer::NoMergePolicy;
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
    use core::SegmentReader;
    use docset::DocSet;
    use query::BooleanQuery;
    use rand::distributions::Range;
    use rand::{Rng, SeedableRng, XorShiftRng};
    use schema::*;
    use Index;
    use IndexWriter;
    use Postings;
    use rand::distributions::Bernoulli;

    pub fn assert_nearly_equals(expected: f32, val: f32) {
        assert!(
            nearly_equals(val, expected),
            "Got {}, expected {}.",
            val,
            expected
        );
    }

    pub fn nearly_equals(a: f32, b: f32) -> bool {
        (a - b).abs() < 0.0005 * (a + b).abs()
    }

    pub fn generate_nonunique_unsorted(max_value: u32, n_elems: usize) -> Vec<u32> {
        let seed: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        XorShiftRng::from_seed(seed)
            .sample_iter(&Range::new(0u32, max_value))
            .take(n_elems)
            .collect::<Vec<u32>>()
    }

    pub fn sample_with_seed(n: u32, ratio: f64, seed_val: u8) -> Vec<u32> {
        let seed: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, seed_val];
        XorShiftRng::from_seed(seed)
            .sample_iter(&Bernoulli::new(ratio))
            .take(n as usize)
            .enumerate()
            .filter_map(|(val, keep)| {
                if keep {
                    Some(val as u32)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn sample(n: u32, ratio: f64) -> Vec<u32> {
        sample_with_seed(n, ratio, 4)
    }

    #[test]
    #[cfg(feature = "mmap")]
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
    fn test_fieldnorm_no_docs_with_field() {
        let mut schema_builder = SchemaBuilder::default();
        let title_field = schema_builder.add_text_field("title", TEXT);
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let doc = doc!(text_field=>"a b c");
                index_writer.add_document(doc);
            }
            index_writer.commit().unwrap();
        }
        {
            index.load_searchers().unwrap();
            let searcher = index.searcher();
            let reader = searcher.segment_reader(0);
            {
                let fieldnorm_reader = reader.get_fieldnorms_reader(text_field);
                assert_eq!(fieldnorm_reader.fieldnorm(0), 3);
            }
            {
                let fieldnorm_reader = reader.get_fieldnorms_reader(title_field);
                assert_eq!(fieldnorm_reader.fieldnorm_id(0), 0);
            }
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
            let fieldnorms_reader = segment_reader.get_fieldnorms_reader(text_field);
            assert_eq!(fieldnorms_reader.fieldnorm(0), 3);
            assert_eq!(fieldnorms_reader.fieldnorm(1), 0);
            assert_eq!(fieldnorms_reader.fieldnorm(2), 2);
        }
    }

    fn advance_undeleted(docset: &mut DocSet, reader: &SegmentReader) -> bool {
        while docset.advance() {
            if !reader.is_deleted(docset.doc()) {
                return true;
            }
        }
        false
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
                assert!(advance_undeleted(&mut postings, reader));
                assert_eq!(postings.doc(), 5);
                assert!(!advance_undeleted(&mut postings, reader));
            }
            {
                let mut postings = inverted_index
                    .read_postings(&term_b, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
                assert!(advance_undeleted(&mut postings, reader));
                assert_eq!(postings.doc(), 3);
                assert!(advance_undeleted(&mut postings, reader));
                assert_eq!(postings.doc(), 4);
                assert!(!advance_undeleted(&mut postings, reader));
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
                assert!(advance_undeleted(&mut postings, reader));
                assert_eq!(postings.doc(), 5);
                assert!(!advance_undeleted(&mut postings, reader));
            }
            {
                let mut postings = inverted_index
                    .read_postings(&term_b, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
                assert!(advance_undeleted(&mut postings, reader));
                assert_eq!(postings.doc(), 3);
                assert!(advance_undeleted(&mut postings, reader));
                assert_eq!(postings.doc(), 4);
                assert!(!advance_undeleted(&mut postings, reader));
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
                assert!(!advance_undeleted(&mut postings, reader));
            }
            {
                let mut postings = inverted_index
                    .read_postings(&term_b, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
                assert!(advance_undeleted(&mut postings, reader));
                assert_eq!(postings.doc(), 3);
                assert!(advance_undeleted(&mut postings, reader));
                assert_eq!(postings.doc(), 4);
                assert!(!advance_undeleted(&mut postings, reader));
            }
            {
                let mut postings = inverted_index
                    .read_postings(&term_c, IndexRecordOption::WithFreqsAndPositions)
                    .unwrap();
                assert!(advance_undeleted(&mut postings, reader));
                assert_eq!(postings.doc(), 4);
                assert!(!advance_undeleted(&mut postings, reader));
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
    fn test_indexedfield_not_in_documents() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let absent_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(2, 40_000_000).unwrap();
        index_writer.add_document(doc!(text_field=>"a"));
        assert!(index_writer.commit().is_ok());
        assert!(index.load_searchers().is_ok());
        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);
        segment_reader.inverted_index(absent_field); //< should not panic
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
            let fast_field_reader_res = segment_reader.fast_field_reader::<u64>(text_field);
            assert!(fast_field_reader_res.is_err());
        }
        {
            let fast_field_reader_res = segment_reader.fast_field_reader::<u64>(stored_int_field);
            assert!(fast_field_reader_res.is_err());
        }
        {
            let fast_field_reader_res = segment_reader.fast_field_reader::<u64>(fast_field_signed);
            assert!(fast_field_reader_res.is_err());
        }
        {
            let fast_field_reader_res = segment_reader.fast_field_reader::<i64>(fast_field_signed);
            assert!(fast_field_reader_res.is_ok());
            let fast_field_reader = fast_field_reader_res.unwrap();
            assert_eq!(fast_field_reader.get(0), 4i64)
        }

        {
            let fast_field_reader_res = segment_reader.fast_field_reader::<i64>(fast_field_signed);
            assert!(fast_field_reader_res.is_ok());
            let fast_field_reader = fast_field_reader_res.unwrap();
            assert_eq!(fast_field_reader.get(0), 4i64)
        }
    }
}
