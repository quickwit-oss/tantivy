#![feature(test)]
#[allow(unused_imports)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate fst;
extern crate byteorder;
extern crate memmap;
extern crate rand;
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

#[cfg(test)] extern crate test;

mod core;

pub use core::schema::DocId;
pub use core::index::Index;
pub use core::schema::Schema;
pub use core::schema::Term;
pub use core::schema::FieldOptions;
pub use core::schema::Document;
pub use core::collector;
pub use core::reader::SegmentReader;

#[cfg(test)]
mod tests {

    use super::*;
    use core::serial::DebugSegmentSerializer;
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

        fn set_segment(&mut self, _: &SegmentReader) {}

        fn collect(&mut self, doc_id: DocId) {
            self.docs.push(doc_id);
        }
    }


    #[test]
    fn test_indexing() {
        let mut schema = Schema::new();
        let text_fieldtype = FieldOptions::new().set_tokenized_indexed();
        let text_field = schema.add_field("text", &text_fieldtype);

        let index = Index::create_from_tempdir(schema).unwrap();

        {
            // writing the segment
            let mut index_writer = index.writer();
            {
                let mut doc = Document::new();
                doc.set(&text_field, "af b");
                index_writer.add(doc);
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c");
                index_writer.add(doc);
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c d");
                index_writer.add(doc);
            }

            let segment_str_before_writing = DebugSegmentSerializer::debug_string(index_writer.current_segment_writer());
            println!("{:?}", segment_str_before_writing);


            let commit_result = index_writer.commit();
            assert!(commit_result.is_ok());
            let segment = commit_result.unwrap();
            SegmentReader::open(segment).unwrap();

            // let segment_reader = SegmentReader::open(segment).unwrap();
            // TODO ENABLE TEST
            // let segment_str_after_reading = DebugSegmentSerializer::debug_string(&segment_reader);
            // assert_eq!(segment_str_before_writing, segment_str_after_reading);
        }

    }


    #[test]
    fn test_searcher() {
        let mut schema = Schema::new();
        let text_fieldtype = FieldOptions::new().set_tokenized_indexed();
        let text_field = schema.add_field("text", &text_fieldtype);
        let index = Index::create_from_tempdir(schema).unwrap();

        {
            // writing the segment
            let mut index_writer = index.writer();
            {
                let mut doc = Document::new();
                doc.set(&text_field, "af b");
                index_writer.add(doc);
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c");
                index_writer.add(doc);
            }
            {
                let mut doc = Document::new();
                doc.set(&text_field, "a b c d");
                index_writer.add(doc);
            }
            let commit_result = index_writer.commit();
            commit_result.unwrap();
        }
        println!("index {:?}", index.schema());
        {

            let searcher = index.searcher();
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
