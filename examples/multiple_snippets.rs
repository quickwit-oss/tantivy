// # Multiple Snippets Example
//
// This example demonstrates how to return multiple text fragments
// from a document, useful for long documents with matches in different locations.

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::snippet::SnippetGenerator;
use tantivy::{doc, Index, IndexWriter};
use tempfile::TempDir;

fn main() -> tantivy::Result<()> {
    let index_path = TempDir::new()?;

    // Define the schema
    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT | STORED);
    let schema = schema_builder.build();

    // Create the index
    let index = Index::create_in_dir(&index_path, schema)?;
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Index a long document with multiple occurrences of "rust"
    index_writer.add_document(doc!(
        title => "The Rust Programming Language",
        body => "Rust is a systems programming language that runs blazingly fast, prevents \
                 segfaults, and guarantees thread safety. Lorem ipsum dolor sit amet, \
                 consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore. \
                 Rust empowers everyone to build reliable and efficient software. More filler \
                 text to create distance between matches. Ut enim ad minim veniam, quis nostrud \
                 exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. \
                 The Rust compiler is known for its helpful error messages. Duis aute irure \
                 dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla \
                 pariatur. Rust has a strong type system and ownership model."
    ))?;

    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![title, body]);
    let query = query_parser.parse_query("rust")?;

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;

    // Create snippet generator
    let mut snippet_generator = SnippetGenerator::create(&searcher, &*query, body)?;

    println!("=== Single Snippet (Default Behavior) ===\n");
    for (score, doc_address) in &top_docs {
        let doc = searcher.doc::<TantivyDocument>(*doc_address)?;
        let snippet = snippet_generator.snippet_from_doc(&doc);
        println!("Document score: {}", score);
        println!("Title: {}", doc.get_first(title).unwrap().as_str().unwrap());
        println!("Single snippet: {}\n", snippet.to_html());
    }

    println!("\n=== Multiple Snippets (New Feature) ===\n");

    // Configure to return multiple snippets
    // Get up to 3 snippets
    snippet_generator.set_snippets_limit(3);
    // Smaller fragments
    snippet_generator.set_max_num_chars(80);
    // By default, multiple snippets are sorted by score. You can change this to sort by position.
    // snippet_generator.set_sort_order(SnippetSortOrder::Position);

    for (score, doc_address) in top_docs {
        let doc = searcher.doc::<TantivyDocument>(doc_address)?;
        let snippets = snippet_generator.snippets_from_doc(&doc);

        println!("Document score: {}", score);
        println!("Title: {}", doc.get_first(title).unwrap().as_str().unwrap());
        println!("Found {} snippets:", snippets.len());

        for (i, snippet) in snippets.iter().enumerate() {
            println!("  Snippet {}: {}", i + 1, snippet.to_html());
        }
        println!();
    }

    Ok(())
}
