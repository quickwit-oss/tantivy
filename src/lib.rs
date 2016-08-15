/*!

# Creating a new index, adding documents and searching.

```
# extern crate rustc_serialize;
# extern crate tantivy;
# use std::fs;
use tantivy::{Document, Index};
use tantivy::schema::{Schema, TEXT, STORED};
use tantivy::collector::TopCollector;
use tantivy::query::QueryParser;
use tantivy::query::Query;
use std::path::PathBuf; 

# fn main() {
# fn wrapper_err() -> tantivy::Result<()> {
// We need to declare a schema
// to create a new index.
let mut schema = Schema::new();

// TEXT | STORED is some syntactic sugar to describe
// how tantivy should index this field.
// It means the field should be tokenized and indexed,
// along with its term frequency and term positions.  
let title = schema.add_text_field("title", TEXT | STORED);
let body = schema.add_text_field("body", TEXT);
// the path in which our index will be created.
# fs::create_dir("./tantivy-index").unwrap();
let index_path = PathBuf::from("./tantivy-index");
// this will actually just create a meta.json 
// file in the directory.
let index = try!(Index::create(&index_path, schema));

// There can be only one writer at one time.
// The writer will use more than one thread
// to use your CPU.
let mut index_writer = try!(index.writer());


// Let's now create one document and index it.
let mut doc = Document::new();
doc.add_text(title, "The Old Man and the Sea");
doc.add_text(body, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four
days now without taking a fish.");

// We can now add our document
try!(index_writer.add_document(doc));

// ... in the real world, we would add way more documents
// here.

// At this point the document is not indexed.
// It has been pushed to a queue where
// it will be eventually processed.
//
// There is even no guarantee that 
// the document will be indexed if there
// is a power outage for instance.
// 
// We can call .wait() to force the index_writer to 
// commit to disk. 
try!(index_writer.wait());

// At this point we are guaranteed that
// all documents that were added are index, and
// ready for search.
//

// Let's search our index. This starts
// by creating a searcher. There can be more
// than one search at a time.
let searcher = try!(index.searcher());

// The query parser can interpret human queries.
// Here, if the user does not specify which
// field he wants to search, tantivy will search in both title and body.
let query_parser = QueryParser::new(index.schema(), vec!(title, body));
let query = query_parser.parse_query("sea whale").unwrap();

// A query defines a set of documents, as
// well as the way they should be scored. 
// By default the query_parser is scoring according
// to a metric called TfIdf, and will consider
// any document matching at least one of our terms.

// We are not interested in all of the document but 
// only in the top 10.
let mut top_collector = TopCollector::with_limit(10);

try!(query.search(&searcher, &mut top_collector));

// Our top collector now contains are 10 
// most relevant doc ids...
let doc_ids = top_collector.docs();

// The actual documents still need to be 
// retrieved from Tantivy's store.
// Since body was not configured as stored,
// the document returned will only contain
// a title.
let retrieved_doc = searcher.doc(&doc_ids[0]);
# Ok(())
# }
# wrapper_err().unwrap();
# fs::remove_dir_all("./tantivy-index").unwrap();
# }
 
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
pub use core::index::Index;
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

    use super::*;
    use collector::TestCollector;
    use query::MultiTermQuery;

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
    fn test_fieldnorm() {
        let mut schema = schema::Schema::new();
        let text_field = schema.add_text_field("text", schema::TEXT);
        let index = Index::create_in_ram(schema);
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
            index_writer.wait().unwrap();
        }
        {
            
            let searcher = index.searcher().unwrap();
            let segment_reader: &SegmentReader = searcher.segments().iter().next().unwrap();
            let fieldnorms_reader = segment_reader.get_fieldnorms_reader(text_field).unwrap();
            assert_eq!(fieldnorms_reader.get(0), 3);
            assert_eq!(fieldnorms_reader.get(1), 0);
            assert_eq!(fieldnorms_reader.get(2), 2);
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
            let mut postings = reader.read_postings_all_info(&Term::from_field_text(text_field, "af")).unwrap();
            assert!(postings.advance());
            assert_eq!(postings.doc(), 0);
            assert_eq!(postings.term_freq(), 3);
            assert!(!postings.advance());
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
