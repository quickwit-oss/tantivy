// # Basic Example
//
// This example covers the basic functionalities of
// tantivy.
//
// We will :
// - define our schema
// = create an index in a directory
// - index few documents in our index
// - search for the best document matchings "sea whale"
// - retrieve the best document original content.


extern crate tempdir;

// ---
// Importing tantivy...
#[macro_use]
extern crate tantivy;
use tantivy::collector::TopCollector;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;

fn main() -> tantivy::Result<()> {
    // Let's create a temporary directory for the
    // sake of this example
    let index_path = TempDir::new("tantivy_example_dir")?;

    // # Defining the schema
    //
    // The Tantivy index requires a very strict schema.
    // The schema declares which fields are in the index,
    // and for each field, its type and "the way it should
    // be indexed".

    // first we need to define a schema ...
    let mut schema_builder = SchemaBuilder::default();

    // Our first field is title.
    // We want full-text search for it, and we also want
    // to be able to retrieve the document after the search.
    //
    // `TEXT | STORED` is some syntactic sugar to describe
    // that.
    //
    // `TEXT` means the field should be tokenized and indexed,
    // along with its term frequency and term positions.
    //
    // `STORED` means that the field will also be saved
    // in a compressed, row-oriented key-value store.
    // This store is useful to reconstruct the
    // documents that were selected during the search phase.
    schema_builder.add_text_field("title", TEXT | STORED);

    // Our second field is body.
    // We want full-text search for it, but we do not
    // need to be able to be able to retrieve it
    // for our application.
    //
    // We can make our index lighter and
    // by omitting `STORED` flag.
    schema_builder.add_text_field("body", TEXT);

    let schema = schema_builder.build();

    // # Indexing documents
    //
    // Let's create a brand new index.
    //
    // This will actually just save a meta.json
    // with our schema in the directory.
    let index = Index::create_in_dir(&index_path, schema.clone())?;

    // To insert document we need an index writer.
    // There must be only one writer at a time.
    // This single `IndexWriter` is already
    // multithreaded.
    //
    // Here we give tantivy a budget of `50MB`.
    // Using a bigger heap for the indexer may increase
    // throughput, but 50 MB is already plenty.
    let mut index_writer = index.writer(50_000_000)?;

    // Let's index our documents!
    // We first need a handle on the title and the body field.

    // ### Adding documents
    //
    // We can create a document manually, by setting the fields
    // one by one in a Document object.
    //
    // To demonstrate ranking easier we only have single field.
    let title = schema.get_field("title").unwrap();

    index_writer.add_document(doc!(
        title => "Dogfish Shark",
    ));
    index_writer.add_document(doc!(
        title => "Horn Shark",
    ));
    index_writer.add_document(doc!(
        title => "Shark",
    ));

    // This is an example, so we will only index 3 documents
    // here. You can check out tantivy's tutorial to index
    // the English wikipedia. Tantivy's indexing is rather fast.
    // Indexing 5 million articles of the English wikipedia takes
    // around 3 minutes on my computer!

    // ### Committing
    //
    // At this point our documents are not searchable.
    //
    //
    // We need to call .commit() explicitly to force the
    // index_writer to finish processing the documents in the queue,
    // flush the current index to the disk, and advertise
    // the existence of new documents.
    //
    // This call is blocking.
    index_writer.commit()?;

    // If `.commit()` returns correctly, then all of the
    // documents that have been added are guaranteed to be
    // persistently indexed.
    //
    // In the scenario of a crash or a power failure,
    // tantivy behaves as if has rolled back to its last
    // commit.

    // # Searching
    //
    // ### Searcher
    //
    // Let's search our index. Start by reloading
    // searchers in the index. This should be done
    // after every `commit()`.
    index.load_searchers()?;

    // We now need to acquire a searcher.
    // Some search experience might require more than
    // one query.
    //
    // The searcher ensure that we get to work
    // with a consistent version of the index.
    //
    // Acquiring a `searcher` is very cheap.
    //
    // You should acquire a searcher every time you
    // start processing a request and
    // and release it right after your query is finished.
    let searcher = index.searcher();


    // Trying normal scorer first:
    let query_parser = QueryParser::for_index(&index, vec![title]);
    let query = query_parser.parse_query("shark")?;

    let mut top_collector = TopCollector::with_limit(10);

    searcher.search(&*query, &mut top_collector)?;
    let doc_addresses = top_collector.score_docs();
    println!("Normal scorer:");
    for (score, doc_address) in doc_addresses {
        let retrieved_doc = searcher.doc(&doc_address)?;
        println!("    {}: {}", schema.to_json(&retrieved_doc), score);
    }

    // Setting `b` of BM25 to zero:
    //
    // Note: the scoring changes. Sorting probably too, but since scores in
    // this simple example are equal the actual order is unpredictable.
    let query_parser = QueryParser::for_index_with_settings(
        &index, vec![title], 1.2, 0.0);

    let query = query_parser.parse_query("shark")?;

    let mut top_collector = TopCollector::with_limit(10);

    searcher.search(&*query, &mut top_collector)?;
    let doc_addresses = top_collector.score_docs();
    println!("Custom scorer (b=0):");
    for (score, doc_address) in doc_addresses {
        let retrieved_doc = searcher.doc(&doc_address)?;
        println!("    {}: {}", schema.to_json(&retrieved_doc), score);
    }


    Ok(())
}


use tempdir::TempDir;
