use std::env;
use std::path::PathBuf;
use std::fs::File;
use std::io::Write;
extern crate tantivy;
use tantivy::directory::{StaticDirectory, write_static_from_directory};
use tantivy::Index;
use tantivy::query::QueryParser;
use tantivy::collector::TopCollector;


static DATA: &'static [u8] = include_bytes!("output.bin");

fn run() -> tantivy::Result<()> {
    // Prints each argument on a separate line
    let directory = StaticDirectory::open(DATA).unwrap();
    let index = Index::open_directory(directory).unwrap();
    index.load_searchers().unwrap();
    let searcher = index.searcher();

    let schema = index.schema();
    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();

    let query_parser = QueryParser::for_index(&index, vec![title, body]);
    let query = query_parser.parse_query("sea whale")?;

    let mut top_collector = TopCollector::with_limit(10);

    searcher.search(&*query, &mut top_collector)?;

    let doc_addresses = top_collector.docs();

    // The actual documents still need to be
    // retrieved from Tantivy's store.
    //
    // Since the body field was not configured as stored,
    // the document returned will only contain
    // a title.

    for doc_address in doc_addresses {
        let retrieved_doc = searcher.doc(&doc_address)?;
        println!("{}", schema.to_json(&retrieved_doc));
    }
    Ok(())
}


fn main() {
    run().unwrap();
}
