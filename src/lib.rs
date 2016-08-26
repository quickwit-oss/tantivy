/*!

# Creating a new index, adding documents and searching.

```

 
```
*/

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

pub mod postings;
pub mod query;
pub mod directory;

pub mod collector;
pub mod schema;

pub use directory::Directory;
pub use core::searcher::Searcher;
pub use core::Index;
pub use schema::Term;
pub use schema::Document;
pub use core::SegmentReader;
pub use self::common::TimerTree;


pub use postings::DocSet;
pub use postings::Postings;
pub use postings::SegmentPostingsOption;


/// u32 identifying a document within a segment.
/// Document gets their doc id assigned incrementally,
/// as they are added in the segment.
pub type DocId = u32;

/// f32 the score of a document.
pub type Score = f32;

/// A segment local id identifies a segment.
/// It only makes sense for a given searcher.
pub type SegmentLocalId = u32;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

    use collector::TestCollector;
    use query::MultiTermQuery;
    use Index;
    use core::SegmentReader;
    use schema::*;
    use DocSet;
    use Postings;

    #[test]
    fn test_indexing() {
        let mut schema_builder = SchemaBuilder::new();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
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
            assert!(index_writer.commit().is_ok());
            // TODO reenable this test
            // let segment = commit_result.unwrap();
            // let segment_reader = SegmentReader::open(segment).unwrap();
            // assert_eq!(segment_reader.max_doc(), 3);
        }

    }

    #[test]
    fn test_docfreq() {
        let mut schema_builder = SchemaBuilder::new();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            let mut doc = Document::new();
            doc.add_text(text_field, "a b c");
            index_writer.add_document(doc).unwrap();
            index_writer.commit().unwrap();
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
            index_writer.commit().unwrap();
        }
        {
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            let mut doc = Document::new();
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
        let mut schema_builder = SchemaBuilder::new();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let doc = Document::new();
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a b");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        }
        {
            
            let searcher = index.searcher();
            let segment_reader: &SegmentReader = searcher.segment_readers().iter().next().unwrap();
            let fieldnorms_reader = segment_reader.get_fieldnorms_reader(text_field).unwrap();
            assert_eq!(fieldnorms_reader.get(0), 3);
            assert_eq!(fieldnorms_reader.get(1), 0);
            assert_eq!(fieldnorms_reader.get(2), 2);
        }
    }

    #[test]
    fn test_termfreq() {
        let mut schema_builder = SchemaBuilder::new();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "af af af bc bc");
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        }
        {
            let searcher = index.searcher();
            let reader = searcher.segment_reader(0);
            let mut postings = reader.read_postings_all_info(&Term::from_field_text(text_field, "af")).unwrap();
            assert!(postings.advance());
            assert_eq!(postings.doc(), 0);
            assert_eq!(postings.term_freq(), 3);
            assert!(!postings.advance());
        }
    }

    #[test]
    fn test_searcher() {
        let mut schema_builder = SchemaBuilder::new();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
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
            index_writer.commit().unwrap();
        }
        {
            let searcher = index.searcher();
            let get_doc_ids = |terms: Vec<Term>| {
                let query = MultiTermQuery::from(terms);
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
        let mut schema_builder = SchemaBuilder::new();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
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
            index_writer.commit().unwrap();
        }
        index.searcher();
    }
}
