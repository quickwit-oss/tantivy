extern crate rustc_serialize;
extern crate tantivy;
extern crate tempdir;

use std::path::Path;
use tempdir::TempDir;
use tantivy::Index;
use tantivy::schema::*;
use tantivy::collector::TopCollector;
use tantivy::query::QueryParser;
use tantivy::query::Query;

fn main() {
    // Let's create a temporary directory for the 
    // sake of this example
    if let Ok(dir) = TempDir::new("tantivy_example_dir") {
        run_example(&dir.path()).unwrap();
        dir.close().unwrap();
    }   
}


fn run_example(index_path: &Path) -> tantivy::Result<()> {
    
    
    // # Defining the schema
    //
    // Tantivy index require to have a very strict schema.
    // The schema declares which fields are in the index,
    // and for each field, its type and "the way it should 
    // be indexed".
    
    
    // first we need to define a schema ...
    let mut schema_builder = SchemaBuilder::default();
    
    // Our first field is title.
    // We want full-text search for it, and we want to be able
    // to retrieve the document after the search.
    //
    // TEXT | STORED is some syntactic sugar to describe
    // that. 
    // 
    // `TEXT` means the field should be tokenized and indexed,
    // along with its term frequency and term positions.
    //
    // `STORED` means that the field will also be saved
    // in a compressed, row-oriented key-value store.
    // This store is useful to reconstruct the 
    // document that were selected during the search phase.
    schema_builder.add_text_field("title", TEXT | STORED);
    
    // Our first field is body.
    // We want full-text search for it, and we want to be able
    // to retrieve the body after the search.
    schema_builder.add_text_field("body", TEXT);
    
    let schema = schema_builder.build(); 



    // # Indexing documents
    //
    // Let's create a brand new index.
    // 
    // This will actually just save a meta.json
    // with our schema the directory.
    let index = try!(Index::create(index_path, schema.clone()));

    
    
    // To insert document we need an index writer.
    // There shall be only one writer at a time.
    // Besides, this single `IndexWriter` is already
    // multithreaded.
    //
    // Here we used a buffer of 1 GB. Using a bigger 
    // heap for the indexer can increase its throughput.
    // This buffer will be split between the indexing
    // threads.
    let mut index_writer = try!(index.writer(1_000_000_000));

    // Let's now index our documents!
    // We first need a handle on the title and the body field.
    
    
    // ### Create a document "manually".
    //
    // We can create a document manually, by setting adding the fields
    // one by one in a Document object.
    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();
     
    let mut old_man_doc = Document::default();
    old_man_doc.add_text(title, "The Old Man and the Sea");
    old_man_doc.add_text(body, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.");
    
    // ... and add it to the `IndexWriter`.
    try!(index_writer.add_document(old_man_doc));
    
    // ### Create a document directly from json.
    //
    // Alternatively, we can use our schema to parse
    // a document object directly from json.
    
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
    
    // This is an example, so we will only index 3 documents
    // here. You can check out tantivy's tutorial to index
    // the English wikipedia. Tantivy's indexing is rather fast. 
    // Indexing 5 millions articles of the English wikipedia takes
    // around 4 minutes on my computer!
    
    
    // ### Committing
    // 
    // At this point our documents are not searchable.
    //
    // 
    // We need to call .commit() explicitely to force the
    // index_writer to finish processing the documents in the queue,
    // flush the current index on the disk, and advertise
    // the existence of new documents.
    //
    // This call is blocking.
    try!(index_writer.commit());
    
    // If `.commit()` returns correctly, then all of the
    // documents have been added before are guaranteed to be
    // persistently indexed.
    // 
    // In the scenario of a crash or a power failure,
    // tantivy behaves as if it rollbacked to its last
    // commit.
    
    
    // # Searching
    //
    // Let's search our index. This starts
    // by creating a searcher. There can be more
    // than one searcher at a time.
    // 
    // You are supposed to acquire a search
    // every time you start a "search query".
    let searcher = index.searcher();

    // The query parser can interpret human queries.
    // Here, if the user does not specify which
    // field he wants to search, tantivy will search
    // in both title and body.
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
    
    // ### Collectors 
    //
    // We are not interested in all of the document but 
    // only in the top 10. Keep track of our top 10 best documents
    // is the role of the TopCollector.
    
    let mut top_collector = TopCollector::with_limit(10);
    
    // We can now perform our query.
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