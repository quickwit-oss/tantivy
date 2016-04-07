//#![feature(test,associated_consts)]
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
extern crate combine;
extern crate atomicwrites;
extern crate tempdir;
extern crate bincode;
extern crate time;
extern crate serde;
extern crate libc;
extern crate lz4;
extern crate uuid;
extern crate num_cpus;

#[cfg(test)] extern crate test;
#[cfg(test)] extern crate rand;

mod core;

pub use core::directory::Directory;
pub use core::searcher::Searcher;
pub use core::index::Index;
pub use core::schema;
pub use core::schema::Term;
pub use core::schema::Document;
pub use core::collector;
pub use core::schema::DocId;
pub use core::reader::SegmentReader;
pub use core::searcher::SegmentLocalId;


#[cfg(test)]
mod tests {

    use super::*;
    use collector::TestCollector;

    #[test]
    fn test_indexing() {
        let mut schema = schema::Schema::new();
        let text_fieldtype = schema::TextOptions::new().set_tokenized_indexed();
        let text_field = schema.add_text_field("text", &text_fieldtype);

        let index = Index::create_from_tempdir(schema).unwrap();

        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            {
                let mut doc = Document::new();
                doc.set(&text_field, "af b");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c d");
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
    fn test_searcher() {
        let mut schema = schema::Schema::new();
        let text_fieldtype = schema::TextOptions::new().set_tokenized_indexed();
        let text_field = schema.add_text_field("text", &text_fieldtype);
        let index = Index::create_in_ram(schema);

        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1).unwrap();
            {
                let mut doc = Document::new();
                doc.set(&text_field, "af b");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c d");
                index_writer.add_document(doc).unwrap();
            }
            //let commit_result = index_writer.commit();
            index_writer.wait().unwrap();
            //commit_result.unwrap();
        }
        {
            let searcher = index.searcher().unwrap();
            let get_doc_ids = |terms: Vec<Term>| {
                let mut collector = TestCollector::new();
                assert!(searcher.search(&terms, &mut collector).is_ok());
                collector.docs()
            };
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "a"))),
                    vec!(1, 2));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "af"))),
                    vec!(0));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "b"))),
                    vec!(0, 1, 2));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "c"))),
                    vec!(1, 2));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "d"))),
                    vec!(2));
            }
            {
                assert_eq!(
                    get_doc_ids(vec!(Term::from_field_text(&text_field, "b"), Term::from_field_text(&text_field, "a"), )),
                    vec!(1, 2));
            }
        }
    }
}
