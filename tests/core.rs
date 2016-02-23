extern crate tantivy;
extern crate regex;
extern crate tempdir;

use tantivy::core::schema::*;
use tantivy::core::writer::IndexWriter;
use tantivy::core::collector::Collector;
use tantivy::core::searcher::{Searcher, DocAddress};
use tantivy::core::directory::{Directory, generate_segment_name, SegmentId};
use tantivy::core::reader::SegmentReader;
use regex::Regex;



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

    fn set_segment(&mut self, segment: &SegmentReader) {}

    fn collect(&mut self, doc_id: DocId) {
        self.docs.push(doc_id);
    }
}




#[test]
fn test_indexing() {
    let mut schema = Schema::new();
    let text_fieldtype = FieldOptions::new().set_tokenized_indexed();
    let text_field = schema.add_field("text", &text_fieldtype);

    let mut directory = Directory::create_from_tempdir(schema).unwrap();

    {
        // writing the segment
        let mut index_writer = IndexWriter::open(&directory);
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

        //let debug_serializer = DebugSegmentSerializer::new();
        //let segment_str_before_writing = DebugSegmentSerializer::debug_string(index_writer.current_segment_writer());
        let commit_result = index_writer.commit();
        assert!(commit_result.is_ok());
        let segment = commit_result.unwrap();
        SegmentReader::open(segment).unwrap();
        // TODO ENABLE TEST
        //let segment_str_after_reading = DebugSegmentSerializer::debug_string(&segment_reader);
        //assert_eq!(segment_str_before_writing, segment_str_after_reading);
    }
}


#[test]
fn test_searcher() {
    let mut schema = Schema::new();
    let text_fieldtype = FieldOptions::new().set_tokenized_indexed();
    let text_field = schema.add_field("text", &text_fieldtype);
    let mut directory = Directory::create_from_tempdir(schema).unwrap();

    {
        // writing the segment
        let mut index_writer = IndexWriter::open(&directory);
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
        let segment = commit_result.unwrap();
    }
    {

        let searcher = Searcher::for_directory(directory);
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

#[test]
fn test_new_segment() {
    let SegmentId(segment_name) = generate_segment_name();
    let segment_ptn = Regex::new(r"^_[a-z0-9]{8}$").unwrap();
    assert!(segment_ptn.is_match(&segment_name));
}
