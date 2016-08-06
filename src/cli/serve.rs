extern crate tantivy;
extern crate persistent;
extern crate time;
extern crate urlencoded;
extern crate rustc_serialize;
extern crate iron;
extern crate staticfile;
extern crate mount;

use tantivy::Result;
use tantivy::schema::Field;
use tantivy::collector::CountCollector;
use tantivy::Index;
use tantivy::collector;
use tantivy::Score;
use urlencoded::UrlEncodedQuery;
use iron::status;
use rustc_serialize::json::as_pretty_json;
use std::path::Path;
use staticfile::Static;
use iron::mime::Mime;
use mount::Mount;
use tantivy::query::Query;
use tantivy::query::QueryParser;
use tantivy::query::Explanation;
use tantivy::Document;
use tantivy::collector::TopCollector;
use persistent::Read;
use iron::typemap::Key;
use iron::prelude::*;


#[derive(RustcDecodable, RustcEncodable)]
struct Serp {
    q: String,
    num_hits: usize,
    hits: Vec<Hit>,
    timings: Vec<Timing>,
}

#[derive(RustcDecodable, RustcEncodable)]
struct Hit {
    title: String,
    body: String,
    explain: String,
    score: Score,
}

#[derive(RustcDecodable, RustcEncodable)]
struct Timing {
    name: String,
    duration: i64,
}

struct IndexServer {
    index: Index,
    query_parser: QueryParser,
    body_field: Field,
    title_field: Field,
}

impl IndexServer {
    
    fn load(path: &Path) -> IndexServer {
        let index = Index::open(path).unwrap();
        let schema = index.schema();
        let body_field = schema.get_field("body").unwrap();
        let title_field = schema.get_field("title").unwrap();
        let query_parser = QueryParser::new(schema, vec!(body_field, title_field));
        IndexServer {
            index: index,
            query_parser: query_parser,
            title_field: title_field,
            body_field: body_field,
        }
    }

    fn create_hit(&self, doc: &Document, explain: Explanation) -> Hit {
        Hit {
            title: String::from(doc.get_first(self.title_field).unwrap().text()),
            body: String::from(doc.get_first(self.body_field).unwrap().text().clone()),
            explain: format!("{:?}", explain),
            score: explain.val(),
        }
    }
    
    fn search(&self, q: String) -> Result<Serp> {
        let query = self.query_parser.parse_query(&q).unwrap();
        let searcher = self.index.searcher().unwrap();
        let mut count_collector = CountCollector::new();
        let mut top_collector = TopCollector::with_limit(10);

        {
            let mut chained_collector = collector::chain()
                    .add(&mut top_collector)
                    .add(&mut count_collector);
            try!(query.search(&searcher, &mut chained_collector));
        }
        let hits: Vec<Hit> = top_collector.docs()
                .iter()
                .map(|doc_address| {
                    let doc: Document = searcher.doc(doc_address).unwrap();
                    let explanation = query.explain(&searcher, doc_address).unwrap();
                    self.create_hit(&doc, explanation)
                })
                .collect();
        Ok(Serp {
            q: q,
            hits: hits,
            num_hits: count_collector.count(),
            timings: Vec::new(),
        })
    }

    
}

impl Key for IndexServer {
    type Value = IndexServer;
}


fn search(req: &mut Request) -> IronResult<Response> {
    let index_server = req.get::<Read<IndexServer>>().unwrap();
    match req.get_ref::<UrlEncodedQuery>() {
        Ok(ref qs_map) => {
            match qs_map.get("q") {
                Some(qs) => {
                    let query = qs[0].clone();
                    let serp = index_server.search(query).unwrap();
                    let resp_json = as_pretty_json(&serp).indent(4);
                    let content_type = "application/json".parse::<Mime>().unwrap();
                    Ok(
                        Response::with((content_type, status::Ok, format!("{}", resp_json)))
                    )
                }
                None => {
                    Ok(Response::with((status::BadRequest, "Query not defined")))
                }
            }
        }
        Err(_) => Ok(Response::with((status::BadRequest, "Failed to parse query string")))
    }
}



fn main() {
    let mut mount = Mount::new();
    let server = IndexServer::load(&Path::new("/Users/pmasurel/wiki-index/"));
    
    mount.mount("/api", search);
    mount.mount("/", Static::new(Path::new("static/")));
    
    let mut middleware = Chain::new(mount);
    middleware.link(Read::<IndexServer>::both(server));
    
    let host = "127.0.0.1:3000";
    println!("listening on http://{}", host);
    Iron::new(middleware).http("127.0.0.1:3000").unwrap();
}
