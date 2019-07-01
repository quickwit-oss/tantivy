// # Snippet example
//
// This example shows how to return a representative snippet of
// your hit result.
// Snippet are an extracted of a target document, and returned in HTML format.
// The keyword searched by the user are highlighted with a `<b>` tag.

// ---
// Importing tantivy...
#[macro_use]
extern crate tantivy;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::{Snippet, SnippetGenerator};
use tempdir::TempDir;

fn main() -> tantivy::Result<()> {
    // Let's create a temporary directory for the
    // sake of this example
    let index_path = TempDir::new("tantivy_example_dir")?;

    // # Defining the schema
    let mut schema_builder = Schema::builder();
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

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![title, body]);
    let query = query_parser.parse_query("sycamore spring")?;

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

    let snippet_generator = SnippetGenerator::create(&searcher, &*query, body)?;

    for (score, doc_address) in top_docs {
        let doc = searcher.doc(doc_address)?;
        let snippet = snippet_generator.snippet_from_doc(&doc);
        println!("Document score {}:", score);
        println!("title: {}", doc.get_first(title).unwrap().text().unwrap());
        println!("snippet: {}", snippet.to_html());
        println!("custom highlighting: {}", highlight(snippet));
    }

    Ok(())
}

fn highlight(snippet: Snippet) -> String {
    let mut result = String::new();
    let mut start_from = 0;

    for (start, end) in snippet.highlighted().iter().map(|h| h.bounds()) {
        result.push_str(&snippet.fragments()[start_from..start]);
        result.push_str(" --> ");
        result.push_str(&snippet.fragments()[start..end]);
        result.push_str(" <-- ");
        start_from = end;
    }

    result.push_str(&snippet.fragments()[start_from..]);
    result
}
