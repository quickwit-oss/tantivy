extern crate tantivy;
extern crate regex;
extern crate tempdir;

use tantivy::core::collector::TestCollector;
use tantivy::core::schema::*;
use tantivy::core::global::*;
use tantivy::core::writer::IndexWriter;
use tantivy::core::searcher::Searcher;
use tantivy::core::directory::{Directory, generate_segment_name, SegmentId};
use tantivy::core::reader::SegmentReader;
use regex::Regex;



pub struct TestCollector {
    docs: Vec<DocAddress>,
    current_segment: Option<SegmentId>,
}

impl TestCollector {
    pub fn new() -> TestCollector {
        TestCollector {
            docs: Vec::new(),
            current_segment: None,
        }
    }

    pub fn docs(self,) -> Vec<DocAddress> {
        self.docs
    }
}

impl Collector for TestCollector {

    fn set_segment(&mut self, segment: &SegmentReader) {
        self.current_segment = Some(segment.id());
    }

    fn collect(&mut self, doc_id: DocId) {
        self.docs.push(DocAddress(self.current_segment.clone().unwrap(), doc_id));
    }
}


#[test]
fn test_indexing() {
    let mut schema = Schema::new();
    let text_fieldtype = FieldOptions::new().set_tokenized_indexed();
    let text_field = schema.add_field("text", &text_fieldtype);

    let mut directory = Directory::from_tempdir().unwrap();
    directory.set_schema(&schema);

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
    let mut directory = Directory::from_tempdir().unwrap();
    directory.set_schema(&schema);

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
        let terms = vec!(Term::from_field_text(&text_field, "b"), Term::from_field_text(&text_field, "a"), );
        let mut collector = TestCollector::new();
        searcher.search(&terms, &mut collector);
        let vals: Vec<DocId> = collector.docs().iter()
            .map(|doc| doc.1)
            .collect::<Vec<DocId>>();
        assert_eq!(vals, [1, 2]);
    }
}

#[test]
fn test_new_segment() {
    let SegmentId(segment_name) = generate_segment_name();
    let segment_ptn = Regex::new(r"^_[a-z0-9]{8}$").unwrap();
    assert!(segment_ptn.is_match(&segment_name));
}
