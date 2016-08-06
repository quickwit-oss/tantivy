extern crate argparse;
extern crate tantivy;

use tantivy::Result;
use argparse::{ArgumentParser, Store};
use tantivy::Index;
use tantivy::schema::{Field, Schema};
use tantivy::query::QueryParser;
use tantivy::query::Query;
use std::path::Path;
use tantivy::TimerTree;
use std::io::BufReader;
use std::io::BufRead;
use std::io;
use std::fs::File;
use tantivy::collector::chain;
use tantivy::collector::TopCollector;
use tantivy::collector::CountCollector;


fn extract_search_fields(schema: &Schema) -> Vec<Field> {
    schema.fields()
          .iter()
          .enumerate()
          .filter(|&(_, field_entry)| {
              field_entry.is_indexed()
          })
          .map(|(field_id, _)| field_id as u8)
          .map(Field)
          .collect()
}

fn read_query_file(query_path: &String) -> io::Result<Vec<String>> {
    let query_file: File = try!(File::open(&query_path));
    let file = BufReader::new(&query_file);
    Ok(file.lines()
        .map(|l| l.unwrap())
        .map(|q| String::from(q.trim()))
        .collect())
}


fn run(directory: String,
       query_filepath: String,
       num_repeat: usize) -> Result<()> {
    
    println!("Directory : {:?}", directory);
    println!("Query : {:?}", directory);
    println!("-------------------------------\n\n\n");
    
    let index = try!(Index::open(Path::new(&directory)));
    let searcher = try!(index.searcher());
    let default_search_fields: Vec<Field> = extract_search_fields(&index.schema());
    let queries = try!(read_query_file(&query_filepath));
    let query_parser = QueryParser::new(index.schema(), default_search_fields);
    
    println!("SEARCH\n");
    println!("{}\t{}\t{}\t{}", "query", "num_terms", "num hits", "time in microsecs");
    for _ in 0..num_repeat {
        for query_txt in &queries {
            let query = query_parser.parse_query(&query_txt).unwrap();
            let num_terms = query.num_terms();
            let mut top_collector = TopCollector::with_limit(10);
            let mut count_collector = CountCollector::new();
            let timing;
            {
                let mut collector = chain().add(&mut top_collector).add(&mut count_collector);
                timing = try!(query.search(&searcher, &mut collector));
            }
            println!("{}\t{}\t{}\t{}", query_txt, num_terms, count_collector.count(), timing.total_time());
        }
    }
    
    
    println!("\n\nFETCH STORE\n");
    println!("{}\t{}", "query", "time in microsecs");
    for _ in 0..num_repeat {
        for query_txt in &queries {
            let query = query_parser.parse_query(&query_txt).unwrap();
            let mut top_collector = TopCollector::with_limit(10);
            try!(query.search(&searcher, &mut top_collector));
            let mut timer = TimerTree::new();
            {
                let h = timer.open("total");
                for doc_address in top_collector.docs() {
                    searcher.doc(&doc_address).unwrap();
                }
            }
            println!("{}\t{}", query_txt, timer.total_time());
        }
    }
    
    
    
    Ok(()) 
}

fn main() {
    let mut directory = String::from(".");
    let mut query_file = String::from("query.txt");
    let mut repeat: usize = 1; 
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Merge a few segments together");
        ap.refer(&mut directory)
          .add_option(&["-i", "--index"],
                      Store,
                      "Path to the tantivy index directory");
        ap.refer(&mut query_file)
          .add_option(&["-q", "--queries"],
                      Store,
                      "Path to the tantivy index directory");
        ap.refer(&mut repeat)
          .add_option(&["-n", "--repeat"],
                      Store,
                      "Number of iterations");
        ap.parse_args_or_exit();
    }
    run(directory, query_file, repeat).unwrap();       
}
