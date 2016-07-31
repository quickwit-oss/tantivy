#![feature(binary_heap_extras)]
#![cfg_attr(test, feature(test))]
#![cfg_attr(test, feature(step_by))]
#![doc(test(attr(allow(unused_variables), deny(warnings))))]


#[macro_use]
extern crate lazy_static;

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
mod common;

pub mod postings;
pub mod query;
pub mod directory;
pub mod datastruct;
pub mod analyzer;
pub mod collector;

pub mod schema;

pub use directory::Directory;
pub use core::searcher::Searcher;
pub use core::index::Index;
pub use schema::Term;
pub use schema::Document;
pub use core::SegmentReader;
pub use self::common::TimerTree;

/// u32 identifying a document within a segment.
/// Document gets their doc id assigned incrementally,
/// as they are added in the segment.
pub type DocId = u32;

/// f32 the score of a document.
pub type Score = f32;

/// A segment local id identifies a segment.
/// It only makes sense for a given searcher.
pub type SegmentLocalId = u32;

#[derive(Debug, Clone, Copy)]
pub struct DocAddress(pub SegmentLocalId, pub DocId);

impl DocAddress {
    pub fn segment_ord(&self,) -> SegmentLocalId {
        self.0
    }

    pub fn doc(&self,) -> DocId {
        self.1
    }
}

#[derive(Clone, Copy)]
pub struct ScoredDoc(Score, DocId);

impl ScoredDoc {
    pub fn score(&self,) -> Score {
        self.0
    }
    pub fn doc(&self,) -> DocId {
        self.1
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use collector::TestCollector;
    use query::MultiTermQuery;
    use postings::Postings;
    use postings::DocSet;

    #[test]
    fn test_indexing() {
        let mut schema = schema::Schema::new();
        let text_field = schema.add_text_field("text", schema::TEXT);

        let index = Index::create_from_tempdir(schema).unwrap();
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "af b");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a b c d");
                index_writer.add_document(doc).unwrap();
            }
            assert!(index_writer.wait().is_ok());
            // TODO reenable this test
            // let segment = commit_result.unwrap();
            // let segment_reader = SegmentReader::open(segment).unwrap();
            // assert_eq!(segment_reader.max_doc(), 3);
        }

    }

    #[test]
    fn test_docfreq() {
        let mut schema = schema::Schema::new();
        let text_field = schema.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            let mut doc = Document::new();
            doc.add_text(text_field, "a b c");
            index_writer.add_document(doc).unwrap();
            index_writer.wait().unwrap();
        }
        {
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a a");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.wait().unwrap();
        }
        {
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            let mut doc = Document::new();
            doc.add_text(text_field, "c");
            index_writer.add_document(doc).unwrap();
            index_writer.wait().unwrap();
        }
        {
            let searcher = index.searcher().unwrap();
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
    fn test_termfreq() {
        let mut schema = schema::Schema::new();
        let text_field = schema.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "af af af bc bc");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.wait().unwrap();
        }
        {
            let searcher = index.searcher().unwrap();
            let reader = &searcher.segments()[0];
            let mut postings = reader.read_postings(&Term::from_field_text(text_field, "af")).unwrap();
            assert!(postings.next());
            assert_eq!(postings.doc(), 0);
            assert_eq!(postings.term_freq(), 3);
            assert!(!postings.next());
        }
    }

    #[test]
    fn test_searcher() {
        let mut schema = schema::Schema::new();
        let text_field = schema.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema);

        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "af af af b");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a b c d");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.wait().unwrap();
        }
        {
            let searcher = index.searcher().unwrap();
            let get_doc_ids = |terms: Vec<Term>| {
                let query = MultiTermQuery::new(terms);
                let mut collector = TestCollector::new();
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
        let mut schema = schema::Schema::new();
        let text_field = schema.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema);

        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "af b");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a b c d");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.wait().unwrap();
        }
        index.searcher().unwrap();
    }
}
