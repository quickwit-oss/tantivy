// # Pre-tokenized text example
//
// This example shows how to use pre-tokenized text. Sometimes yout might
// want to index and search through text which is already split into
// tokens by some external tool.
//
// In this example we will:
// - use tantivy tokenizer to create tokens and load them directly into tantivy,
// - import tokenized text straight from json,
// - perform a search on documents with pre-tokenized text

use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{Index, ReloadPolicy};
use tempfile::TempDir;


fn main() -> tantivy::Result<()> {
    let index_path = TempDir::new()?;

    let mut schema_builder = Schema::builder();


    let indexing = tantivy::schema::TextFieldIndexing::default()
        .set_index_option(tantivy::schema::IndexRecordOption::WithScore);
    let option = tantivy::schema::TextOptions::default().set_indexing_options(indexing).set_stored();
    schema_builder.add_text_field("title", option);

    let schema = schema_builder.build();

    let index = Index::create_in_dir(&index_path, schema.clone())?;

    let mut index_writer = index.writer(50_000_000)?;

    // We can create a document manually, by setting the fields
    // one by one in a Document object.
    let title = schema.get_field("title").unwrap();

    // Pretokenized text can also be fed as JSON
    let short_man_json = r#"{
        "title":[{
            "text":"the old man",
            "tokens":[
                {"offset_from":0,"offset_to":3,"position":0,"text":"the","position_length":1, "score": 10},
                {"offset_from":4,"offset_to":7,"position":1,"text":"old","position_length":1, "score": 100},
                {"offset_from":8,"offset_to":11,"position":2,"text":"man","position_length":1, "score": 500}
            ]
        }]
    }"#;

    let short_man_doc = schema.parse_document(&short_man_json)?;

    index_writer.add_document(short_man_doc);

    // Let's commit changes
    index_writer.commit()?;

    // ... and now is the time to query our index

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;

    let searcher = reader.searcher();

    // We want to get documents with token "Man", we will use TermQuery to do it
    // Using PreTokenizedString means the tokens are stored as is avoiding stemming
    // and lowercasing, which preserves full words in their original form
    let query_parser = QueryParser::for_index(&index, vec![title]);
    let query = query_parser.parse_query_with_method("the", "score")?;


    let (top_docs, count) = searcher
        .search(&query, &(TopDocs::with_limit(2), Count))
        .unwrap();
    count;

    // Now let's print out the results.
    // Note that the tokens are not stored along with the original text
    // in the document store
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{} Document: {}", _score, schema.to_json(&retrieved_doc));
    }

    Ok(())
}
