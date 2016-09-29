#![allow(unknown_lints)] // for the clippy lint options
#![allow(module_inception)]

#![feature(binary_heap_extras)]
#![feature(conservative_impl_trait)]
#![cfg_attr(test, feature(test))]
#![cfg_attr(test, feature(step_by))]
#![doc(test(attr(allow(unused_variables), deny(warnings))))]
#![feature(conservative_impl_trait)]

#![warn(missing_docs)]

//! # `tantivy`
//!
//! Tantivy is a search engine library. 
//! Think `Lucene`, but in Rust.

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

#[macro_use]
extern crate fst;
extern crate byteorder;
extern crate memmap;
extern crate regex;
extern crate tempfile;
extern crate rustc_serialize;
extern crate atomicwrites;
extern crate tempdir;
extern crate bincode;
extern crate time;
extern crate libc;
extern crate lz4;
extern crate uuid;
extern crate num_cpus;
extern crate combine;
extern crate itertools;
extern crate chan;
extern crate crossbeam;



#[cfg(test)] extern crate test;
#[cfg(test)] extern crate rand;

#[macro_use]
mod macros {
    macro_rules! get(
        ($e:expr) => (match $e { Some(e) => e, None => return None })
    );
}

mod core;
mod compression;
mod fastfield;
mod store;
mod indexer;
mod common;
mod error;

pub use error::{Result, Error};

mod analyzer;
mod datastruct;


/// Query module
pub mod query;
/// Directory module
pub mod directory;
/// Collector module
pub mod collector;
/// Postings module (also called inverted index)
pub mod postings;
/// Schema
pub mod schema;


pub use directory::Directory;
pub use core::searcher::Searcher;

pub use core::Index;
pub use indexer::IndexWriter;
pub use schema::Term;
pub use schema::Document;
pub use core::SegmentReader;
pub use self::common::TimerTree;


pub use postings::DocSet;
pub use postings::Postings;
pub use postings::SegmentPostingsOption;


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
    pub fn segment_ord(&self,) -> SegmentLocalId {
        self.0
    }
        
    /// Return the segment local `DocId`
    pub fn doc(&self,) -> DocId {
        self.1
    }
}

/// A scored doc is simply a couple `(score, doc_id)`
#[derive(Clone, Copy)]
pub struct ScoredDoc(Score, DocId);

impl ScoredDoc {
    
    /// Returns the score
    pub fn score(&self,) -> Score {
        self.0
    }
    
    /// Returns the doc
    pub fn doc(&self,) -> DocId {
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
    use query::MultiTermQuery;
    use Index;
    use core::SegmentReader;
    use schema::*;
    use DocSet;
    use Postings;

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
                let mut doc = Document::default();
                doc.add_text(text_field, "af b");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a b c d");
                index_writer.add_document(doc).unwrap();
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
            let mut doc = Document::default();
            doc.add_text(text_field, "a b c");
            index_writer.add_document(doc).unwrap();
            index_writer.commit().unwrap();
        }
        {
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a a");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        }
        {
            let mut doc = Document::default();
            doc.add_text(text_field, "c");
            index_writer.add_document(doc).unwrap();
            index_writer.commit().unwrap();
        }
        {
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
                let mut doc = Document::default();
                doc.add_text(text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let doc = Document::default();
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a b");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        }
        {
            
            let searcher = index.searcher();
            let segment_reader: &SegmentReader = searcher.segment_reader(0);
            let fieldnorms_reader = segment_reader.get_fieldnorms_reader(text_field).unwrap();
            assert_eq!(fieldnorms_reader.get(0), 3);
            assert_eq!(fieldnorms_reader.get(1), 0);
            assert_eq!(fieldnorms_reader.get(2), 2);
        }
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
                let mut doc = Document::default();
                doc.add_text(text_field, "af af af bc bc");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        }
        {
            let searcher = index.searcher();
            let reader = searcher.segment_reader(0);
            assert!(reader.read_postings_all_info(&Term::from_field_text(text_field, "abcd")).is_none());
            let mut postings = reader.read_postings_all_info(&Term::from_field_text(text_field, "af")).unwrap();
            assert!(postings.advance());
            assert_eq!(postings.doc(), 0);
            assert_eq!(postings.term_freq(), 3);
            assert!(!postings.advance());
        }
    }

    #[test]
    fn test_searcher() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "af af af b");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a b c d");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        }
        {
            let searcher = index.searcher();
            let get_doc_ids = |terms: Vec<Term>| {
                let query = MultiTermQuery::from(terms);
                let mut collector = TestCollector::default();
                assert!(searcher.search(&query, &mut collector).is_ok());
                collector.docs()
            };
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "a"))),
                    vec!(1, 2));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "af"))),
                    vec!(0));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "b"))),
                    vec!(0, 1, 2));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "c"))),
                    vec!(1, 2));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "d"))),
                    vec!(2));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(text_field, "b"),
                                     Term::from_field_text(text_field, "a"), )),
                    vec!(0, 1, 2));
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
                let mut doc = Document::default();
                doc.add_text(text_field, "af b");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a b c d");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        }
        index.searcher();
    }
}
