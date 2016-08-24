extern crate rustc_serialize;
extern crate tantivy;
extern crate tempdir;

use std::fs;
use std::path::Path;
use tempdir::TempDir;
use tantivy::Index;
use tantivy::schema::*;
use tantivy::collector::TopCollector;
use tantivy::query::QueryParser;
use tantivy::query::Query;
use std::path::PathBuf; 

fn main() {

    // this example creates its index in a temporary
    // directory.
    if let Ok(dir) = TempDir::new("tantivy_example_dir") {
        run(&dir.path()).unwrap();
        dir.close().unwrap();
    }   
}


fn create_schema() -> Schema {
    // We need to declare a schema
    // to create a new index.
    let mut schema_builder = SchemaBuilder::new();

    // TEXT | STORED is some syntactic sugar to describe
    // how tantivy should index this field.
    // It means the field should be tokenized and indexed,
    // along with its term frequency and term positions.  
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT);

    schema_builder.build()
}



fn run(index_path: &Path) -> tantivy::Result<()> {

    // first we need to define a schema ...
    let schema = create_schema(); 

    // Creates an empty index.
    // 
    // This will actually just save a meta.json 
    // file in the directory.
    let index = try!(Index::create(index_path, schema.clone()));

    // There can be only one writer at one time.
    // The writer will use more than one thread
    // to use your multicore CPU.
    let mut index_writer = try!(index.writer());





    // Let's now index our documents!

    // We need a handle on the title and the body field.
    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();


    let mut old_man_doc = Document::new();
    old_man_doc.add_text(title, "The Old Man and the Sea");
    old_man_doc.add_text(body, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.");


    // We can now add our document
    try!(index_writer.add_document(old_man_doc));

    // Alternatively, we can use our schema to parse
    // a document object from json.
    let mice_and_men_doc = try!(schema.parse_document(r#"{
       "title": "Of Mice and Men",
       "body": "few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter’s flooding; and sycamores with mottled, white,recumbent limbs and branches that arch over the pool"  
    }"#));
    try!(index_writer.add_document(mice_and_men_doc));


    // Multi-valued field are allowed, they are
    // expressed in JSON by an array.
    // The following document has two titles.
    let frankenstein_doc = try!(schema.parse_document(r#"{
       "title": ["Frankenstein", "The Modern Promotheus"],
       "body": "You will rejoice to hear that no disaster has accompanied the commencement of an enterprise which you have regarded with such evil forebodings.  I arrived here yesterday, and my first task is to assure my dear sister of my welfare and increasing confidence in the success of my undertaking."  
    }"#));
    try!(index_writer.add_document(frankenstein_doc));

    //
    // ... in the real world, we would add way more documents
    // here.
    // Tantivy is rather fast. Indexing 5 millions articles of
    // the English wikipedia takes around 6 minutes on
    // my desktop.
    //

    // At this point our documents are not necessarily
    // indexed.
    // 
    // It has been pushed to a queue where
    // it will be eventually processed.
    //
    // We have no guarantee that 
    // the document will be indexed if there
    // is a power outage for instance.
    // 
    // We can call .commit() to force the index_writer to 
    // commit to disk. This call is blocking.
    try!(index_writer.commit());
    // If a commit returns correctly, then all of the
    // documents added before have been indexed.
    // 
    // In the scenario of a crash or a power failure
    // - as long as the hard disk is spared - tantivy
    // will be able to rollback to its last commit.


    // Let's search our index. This starts
    // by creating a searcher. There can be more
    // than one searcher at a time.
    let searcher = try!(index.searcher());

    // The query parser can interpret human queries.
    // Here, if the user does not specify which
    // field he wants to search, tantivy will search in both title and body.
    let query_parser = QueryParser::new(index.schema(), vec!(title, body));

    // QueryParser may fail if the query is not in the right
    // format. For user facing applications, this can be a problem.
    // A ticket has been filled regarding this problem.
    let query = try!(query_parser.parse_query("sea whale"));

    // A query defines a set of documents, as
    // well as the way they should be scored.
    //  
    // Query created by the query parser are scoring according
    // to a metric called Tf-Idf, and will consider
    // any document matching at least one of our terms.
    // 
    // We are not interested in all of the document but 
    // only in the top 10.
    let mut top_collector = TopCollector::with_limit(10);

    try!(query.search(&searcher, &mut top_collector));

    // Our top collector now contains are 10 
    // most relevant doc ids...
    let doc_addresses = top_collector.docs();

    // The actual documents still need to be 
    // retrieved from Tantivy's store.
    //
    // Since body was not configured as stored,
    // the document returned will only contain
    // a title.
    
    for doc_address in doc_addresses {
         let retrieved_doc = try!(searcher.doc(&doc_address));
         println!("{}", schema.to_json(&retrieved_doc));
    }

    Ok(())
}