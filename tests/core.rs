extern crate tantivy;
extern crate itertools;

use tantivy::core::DocId;
use tantivy::core::postings::{VecPostings, intersection};
use tantivy::core::postings::Postings;
use tantivy::core::analyzer::tokenize;
use tantivy::core::writer::IndexWriter;
use tantivy::core::directory::Directory;
use tantivy::core::schema::{Field, Document};
use tantivy::core::reader::IndexReader;

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
