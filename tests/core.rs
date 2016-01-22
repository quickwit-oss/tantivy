extern crate tantivy;
extern crate regex;
extern crate tempdir;

use tantivy::core::postings::{VecPostings, intersection};
use tantivy::core::postings::Postings;
use tantivy::core::analyzer::tokenize;
use tantivy::core::serial::*;
use tantivy::core::schema::*;
use tantivy::core::codec::SimpleCodec;
use tantivy::core::global::*;
use tantivy::core::writer::IndexWriter;
use tantivy::core::directory::{Directory, generate_segment_name, SegmentId};
use std::ops::DerefMut;
use tantivy::core::reader::SegmentIndexReader;
use std::io::{ BufWriter, Write};
use regex::Regex;
use std::convert::From;
use std::path::PathBuf;

#[test]
fn test_intersection() {
    let left = VecPostings::new(vec!(1, 3, 9));
    let right = VecPostings::new(vec!(3, 4, 9, 18));
    let inter = intersection(&left, &right);
    let vals: Vec<DocId> = inter.iter().collect();
    assert_eq!(vals, vec!(3, 9));
}

#[test]
fn test_tokenizer() {
    let words: Vec<&str> = tokenize("hello happy tax payer!").collect();
    assert_eq!(words, vec!("hello", "happy", "tax", "payer"));
}

#[test]
fn test_indexing() {
    let tmp_dir = tempdir::TempDir::new("test_indexing").unwrap();
    let directory = Directory::open(tmp_dir.path()).unwrap();
    {
        // writing the segment
        let mut index_writer = IndexWriter::open(&directory);
        {
            let mut doc = Document::new();
            doc.set(Field(1), "af b");
            index_writer.add(doc);
        }
        {
            let mut doc = Document::new();
            doc.set(Field(1), "a b c");
            index_writer.add(doc);
        }
        {
            let mut doc = Document::new();
            doc.set(Field(1), "a b c d");
            index_writer.add(doc);
        }
        let mut debug_serializer = DebugSegmentSerializer::new();
        let segment_str_before_writing = DebugSegmentSerializer::debug_string(index_writer.current_segment_writer());

        let commit_result = index_writer.commit();
        assert!(commit_result.is_ok());
        let segment = commit_result.unwrap();
        let index_reader = SegmentIndexReader::open(segment).unwrap();
        let segment_str_after_reading = DebugSegmentSerializer::debug_string(&index_reader);
        assert_eq!(segment_str_before_writing, segment_str_after_reading);
    }
}




#[test]
fn test_new_segment() {
    let SegmentId(segment_name) = generate_segment_name();
    let segment_ptn = Regex::new(r"^_[a-z0-9]{8}$").unwrap();
    assert!(segment_ptn.is_match(&segment_name));
}
