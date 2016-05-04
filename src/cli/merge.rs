extern crate argparse;
extern crate tantivy;

use argparse::{ArgumentParser, Store};
use tantivy::Index;
use std::path::Path;

fn main() {
    let mut directory = String::from(".");
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Merge a few segments together");
        ap.refer(&mut directory)
          .add_option(&["-d", "--directory"],
                      Store,
                      "Path to the tantivy index directory");
        ap.parse_args_or_exit();
    }
    
    println!("Directory : {:?}", directory);
    let index = Index::open(Path::new(&directory)).unwrap();
    let mut index_writer = index.writer().unwrap();
    let segments = index.segments();
    println!("Merging {} segments", segments.len());
    index_writer.merge(&segments).unwrap();
}
