extern crate tantivy;
extern crate itertools;
extern crate byteorder;
extern crate regex;

use tantivy::core::DocId;
use tantivy::core::postings::{VecPostings, intersection};
use tantivy::core::postings::Postings;
use tantivy::core::analyzer::tokenize;
use tantivy::core::writer::IndexWriter;
use tantivy::core::directory::{Directory, generate_segment_name, SegmentId};
use tantivy::core::schema::{Field, Document};
use tantivy::core::reader::IndexReader;
use tantivy::core::writer::SimplePostingsWriter;
use tantivy::core::postings::PostingsWriter;
use tantivy::core::global::Flushable;
use std::io::{ BufWriter, Write};
use regex::Regex;
use std::convert::From;

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
    let directory = Directory::in_mem();
    {
        let mut index_writer = IndexWriter::open(&directory);
        let mut doc = Document::new();
        doc.set(Field("text"), "toto");
        index_writer.add(doc);
        index_writer.sync().unwrap();
    }
    {
        let index_reader = IndexReader::open(&directory);
    }
}

#[test]
fn test_postings_writer() {
    let mut postings_writer = SimplePostingsWriter::new();
    postings_writer.suscribe(1);
    postings_writer.suscribe(4);
    postings_writer.suscribe(5);
    postings_writer.suscribe(17);
    let mut buffer: Vec<u8> = Vec::new();
    assert_eq!(buffer.len(), 0);
    postings_writer.flush(&mut buffer);
    assert_eq!(buffer.len(), 5 * 8);
}

#[test]
fn test_new_segment() {
    let SegmentId(segment_name) = generate_segment_name();
    let segment_ptn = Regex::new(r"^_[a-z0-9]{8}$").unwrap();
    assert!(segment_ptn.is_match(&segment_name));
}
