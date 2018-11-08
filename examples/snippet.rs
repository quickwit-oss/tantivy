// # Snippet example
//
// This example shows how to return a representative snippet of
// your hit result.
// Snippet are an extracted of a target document, and returned in HTML format.
// The keyword searched by the user are highlighted with a `<b>` tag.
extern crate tempdir;

// ---
// Importing tantivy...
#[macro_use]
extern crate tantivy;
use tantivy::collector::TopCollector;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::SnippetGenerator;
use tempdir::TempDir;

fn main() -> tantivy::Result<()> {
    // Let's create a temporary directory for the
    // sake of this example
    let index_path = TempDir::new("tantivy_example_dir")?;

    // # Defining the schema
    let mut schema_builder = SchemaBuilder::default();
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT | STORED);
    let schema = schema_builder.build();

    // # Indexing documents
    let index = Index::create_in_dir(&index_path, schema.clone())?;

    let mut index_writer = index.writer(50_000_000)?;

    // we'll only need one doc for this example.
    index_writer.add_document(doc!(
        title => "Of Mice and Men",
        body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
                bank and runs deep and green. The water is warm too, for it has slipped twinkling \
                over the yellow sands in the sunlight before reaching the narrow pool. On one \
                side of the river the golden foothill slopes curve up to the strong and rocky \
                Gabilan Mountains, but on the valley side the water is lined with trees—willows \
                fresh and green with every spring, carrying in their lower leaf junctures the \
                debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
                limbs and branches that arch over the pool"
    ));
    // ...
    index_writer.commit()?;

    index.load_searchers()?;

    let searcher = index.searcher();
    let query_parser = QueryParser::for_index(&index, vec![title, body]);
    let query = query_parser.parse_query("sycamore spring")?;

    let mut top_collector = TopCollector::with_limit(10);
    let top_docs = searcher.search(&*query, &mut top_collector)?;

    let snippet_generator = SnippetGenerator::new(&searcher, &*query, body)?;

    for doc_address in top_docs.docs() {
        let doc = searcher.doc(doc_address)?;
        let snippet = snippet_generator.snippet_from_doc(&doc);
        println!("title: {}", doc.get_first(title).unwrap().text().unwrap());
        println!("snippet: {}", snippet.to_html());
    }

    Ok(())
}
