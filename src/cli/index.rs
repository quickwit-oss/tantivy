extern crate rustc_serialize;
extern crate tantivy;
extern crate time;
#[macro_use]
extern crate lazy_static;

extern crate regex;

use std::fs::File;
use tantivy::Index;
use std::io::BufReader;
use std::io::BufRead;
use rustc_serialize::json;
use std::convert::From;
use std::path::PathBuf;
use rustc_serialize::json::DecodeResult;
use time::PreciseTime;
use tantivy::schema::*;

#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct WikiArticle {
    pub url: String,
    pub title: String,
    pub body: String,
}

fn create_schema() -> Schema {
    let mut schema = Schema::new();
    schema.add_text_field("url", STRING | STORED);
    schema.add_text_field("title", TEXT | STORED);
    schema.add_text_field("body", TEXT | STORED);
    schema
}

fn main() {
    let articles = BufReader::new(File::open(&PathBuf::from("wiki-articles-1000.json")).unwrap());

    let schema = create_schema();
    let directory_path = PathBuf::from("/Users/pmasurel/wiki-index");
    let index = Index::create(&directory_path, schema.clone()).unwrap();
    let mut index_writer = index.writer_with_num_threads(1).unwrap();

    let mut num_docs = 0;
    let mut cur = PreciseTime::now();
    let group_count = 10000;

    let title = schema.get_field("title").unwrap();
    let url = schema.get_field("url").unwrap();
    let body = schema.get_field("body").unwrap();

    for article_line_res in articles.lines() {
        let article_line = article_line_res.unwrap();
        let article_res: DecodeResult<WikiArticle> = json::decode(&article_line);
        match article_res {
            Ok(article) => {
                let mut doc = Document::new();
                doc.add_text(title, &article.title);
                doc.add_text(body, &article.body);
                doc.add_text(url, &article.url);
                index_writer.add_document(doc).unwrap();
            }
            Err(_) => {}
        }

        if num_docs > 0 && (num_docs % group_count == 0) {
            println!("{} Docs", num_docs);
            let new = PreciseTime::now();
            let elapsed = cur.to(new);
            println!("{:?} docs / hour", group_count * 3600 * 1e6 as u64 / (elapsed.num_microseconds().unwrap() as u64));
            cur = new;
        }

        num_docs += 1;

    }

    index_writer.wait().unwrap();
}
