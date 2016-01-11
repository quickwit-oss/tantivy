extern crate parici;
extern crate itertools;

use parici::core::DocId;
use parici::core::postings::{VecPostings, intersection};
use parici::core::postings::Postings;
use parici::core::analyzer::tokenize;
use parici::core::writer::IndexWriter;
use parici::core::directory::Directory;
use parici::core::schema::{Field, Document};

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
    let directory = Directory::open("toto");
    let mut index_writer = IndexWriter::open(&directory);
    let mut doc = Document::new();
    doc.set(Field("text"), &String::from("toto"));
    index_writer.add(doc);
}
