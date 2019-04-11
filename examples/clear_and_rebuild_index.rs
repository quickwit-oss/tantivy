// # Clearing and rebuilding an index
//
// This example shows how to build, clear and rebuild an index

// Importing tantivy...
#[macro_use]
extern crate tantivy;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;

fn main() -> tantivy::Result<()> {
    // We first create a schema for the sake of the
    // example. Check the `basic_search` example for more information.
    let mut schema_builder = Schema::builder();

    // For this example, we need to make sure to index positions for our title
    // field. `TEXT` precisely does this.
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());

    let mut index_writer = index.writer_with_num_threads(1, 50_000_000)?;
    index_writer.add_document(doc!(title => "The Old Man and the Sea"));
    index_writer.add_document(doc!(title => "The modern Promotheus"));
    index_writer.commit()?;

    // clears the index
    index_writer.clear()?;
    // have to commit, otherwise deleted terms remain available
    index_writer.commit()?;

    // searching to make sure, none of the documents show up
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![title]);
    let query_promo = query_parser.parse_query("Promotheus")?;
    let top_docs_promo = searcher.search(&query_promo, &TopDocs::with_limit(10))?;

    assert!(top_docs_promo.is_empty());

    let query_sea = query_parser.parse_query("Sea")?;
    let top_docs_sea = searcher.search(&query_sea, &TopDocs::with_limit(10))?;

    assert!(top_docs_sea.is_empty());

    // Add the same documents again
    index_writer.add_document(doc!(title => "The Old Man and the Sea"));
    index_writer.add_document(doc!(title => "The modern Promotheus"));
    index_writer.commit()?;


    // Get a new reader, so you don't refer to old files
    let new_reader = index.reader()?;
    let new_searcher = new_reader.searcher();
    let top_docs_sea = new_searcher.search(&query_sea, &TopDocs::with_limit(10))?;

    // now you will find something
    assert!(!top_docs_sea.is_empty());
    Ok(())
}
