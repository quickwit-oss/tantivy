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
    use collector::Collector;

    // only make sense for a single segment
    struct TestCollector {
        docs: Vec<DocId>,
    }

    impl TestCollector {
        pub fn new() -> TestCollector {
            TestCollector {
                docs: Vec::new(),
            }
        }

        pub fn docs(self,) -> Vec<DocId> {
            self.docs
        }
    }

    impl Collector for TestCollector {

        fn set_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) {}

        fn collect(&mut self, doc_id: DocId) {
            self.docs.push(doc_id);
        }
    }


    #[test]
    fn test_indexing() {
        let mut schema = schema::Schema::new();
        let text_fieldtype = schema::TextOptions::new().set_tokenized_indexed();
        let text_field = schema.add_text_field("text", &text_fieldtype);

        let index = Index::create_from_tempdir(schema).unwrap();

        {
            // writing the segment
            let mut index_writer = index.writer().unwrap();
            {
                let mut doc = Document::new();
                doc.set(&text_field, "af b");
                index_writer.add(&doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c");
                index_writer.add(&doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c d");
                index_writer.add(&doc).unwrap();
            }

            let commit_result = index_writer.commit();
            assert!(commit_result.is_ok());

            let segment = commit_result.unwrap();
            let segment_reader = SegmentReader::open(segment).unwrap();
            assert_eq!(segment_reader.max_doc(), 3);
        }

    }


    #[test]
    fn test_searcher() {
        let mut schema = schema::Schema::new();
        let text_fieldtype = schema::TextOptions::new().set_tokenized_indexed();
        let text_field = schema.add_text_field("text", &text_fieldtype);
        let index = Index::create_from_tempdir(schema).unwrap();

        {
            // writing the segment
            let mut index_writer = index.writer().unwrap();
            {
                let mut doc = Document::new();
                doc.set(&text_field, "af b");
                index_writer.add(&doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c");
                index_writer.add(&doc).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c d");
                index_writer.add(&doc).unwrap();
            }
            let commit_result = index_writer.commit();
            commit_result.unwrap();
        }
        {
            let searcher = index.searcher().unwrap();
            let get_doc_ids = |terms: Vec<Term>| {
                let mut collector = TestCollector::new();
                searcher.search(&terms, &mut collector);
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
