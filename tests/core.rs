extern crate tantivy;
extern crate regex;
extern crate tempdir;

use tantivy::core::postings::VecPostings;
use tantivy::core::postings::Postings;
use tantivy::core::analyzer::tokenize;
use tantivy::core::collector::TestCollector;
use tantivy::core::serial::*;
use tantivy::core::schema::*;
use tantivy::core::codec::SimpleCodec;
use tantivy::core::global::*;
use tantivy::core::postings::IntersectionPostings;
use tantivy::core::writer::IndexWriter;
use tantivy::core::searcher::Searcher;
use tantivy::core::directory::{Directory, generate_segment_name, SegmentId};
use std::ops::DerefMut;
use tantivy::core::reader::SegmentReader;
use std::io::{ BufWriter, Write};
use regex::Regex;
use std::convert::From;
use std::path::PathBuf;
use tantivy::core::query;
use tantivy::core::query::parse_query;
#[test]
fn test_parse_query() {
    {
        let (parsed_query, _) = parse_query("toto:titi toto:tutu").unwrap();
        assert_eq!(parsed_query, vec!(query::Term(String::from("toto"), String::from("titi")), query::Term(String::from("toto"), String::from("tutu"))));
    }
}

#[test]
fn test_intersection() {
    {
        let left = VecPostings::new(vec!(1, 3, 9));
        let right = VecPostings::new(vec!(3, 4, 9, 18));
        let inter = IntersectionPostings::from_postings(vec!(left, right));
        let vals: Vec<DocId> = inter.collect();
        assert_eq!(vals, vec!(3, 9));
    }
    {
        let a = VecPostings::new(vec!(1, 3, 9));
        let b = VecPostings::new(vec!(3, 4, 9, 18));
        let c = VecPostings::new(vec!(1, 5, 9, 111));
        let inter = IntersectionPostings::from_postings(vec!(a, b, c));
        let vals: Vec<DocId> = inter.collect();
        assert_eq!(vals, vec!(9));
    }
}

#[test]
fn test_tokenizer() {
    let words: Vec<&str> = tokenize("hello happy tax payer!").collect();
    assert_eq!(words, vec!("hello", "happy", "tax", "payer"));
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

        let debug_serializer = DebugSegmentSerializer::new();
        let segment_str_before_writing = DebugSegmentSerializer::debug_string(index_writer.current_segment_writer());
        let commit_result = index_writer.commit();
        assert!(commit_result.is_ok());
        let segment = commit_result.unwrap();
        let segment_reader = SegmentReader::open(segment).unwrap();
        let segment_str_after_reading = DebugSegmentSerializer::debug_string(&segment_reader);
        assert_eq!(segment_str_before_writing, segment_str_after_reading);
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
        let terms = vec!(Term::from_field_text(&text_field, "a"), Term::from_field_text(&text_field, "b"), );
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
