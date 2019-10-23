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

use tantivy::tokenizer::{SimpleTokenizer, Token, TokenStream, TokenizedString, Tokenizer};

use tantivy::collector::{Count, TopDocs};
use tantivy::query::TermQuery;
use tantivy::schema::*;
use tantivy::{doc, Index, ReloadPolicy};
use tempfile::TempDir;

fn tokenize_it(text: &str) -> Vec<Token> {
    let mut ts = SimpleTokenizer.token_stream(text);
    let mut tokens = vec![];
    while ts.advance() {
        tokens.push(ts.token().clone());
    }
    tokens
}

fn main() -> tantivy::Result<()> {
    let index_path = TempDir::new()?;

    let mut schema_builder = Schema::builder();

    // now we add `TOKENIZED` `TextOptions` to mark field as pre-tokenized
    // in addition the title will be also stored, so we can see it in
    // returned results
    schema_builder.add_text_field("title", TEXT | STORED | TOKENIZED);
    schema_builder.add_text_field("body", TEXT | TOKENIZED);

    let schema = schema_builder.build();

    let index = Index::create_in_dir(&index_path, schema.clone())?;

    let mut index_writer = index.writer(50_000_000)?;

    // We can create a document manually, by setting the fields
    // one by one in a Document object.
    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();

    let title_text = "The Old Man and the Sea";
    let body_text = "He was an old man who fished alone in a skiff in the Gulf Stream";

    // Content of our first document
    // We create `TokenizedString` which contains original text and vector of tokens
    let title_tok = TokenizedString {
        text: String::from(title_text),
        tokens: tokenize_it(title_text),
    };

    println!(
        "Original text: \"{}\" and tokens: {:?}",
        title_tok.text, title_tok.tokens
    );

    let body_tok = TokenizedString {
        text: String::from(body_text),
        tokens: tokenize_it(body_text),
    };

    // Now lets create a document and add our `TokenizedString` using
    // `add_tokenized_text` method of `Document`
    let mut old_man_doc = Document::default();
    old_man_doc.add_tokenized_text(title, &title_tok);
    old_man_doc.add_tokenized_text(body, &body_tok);

    // ... now let's just add it to the IndexWriter
    index_writer.add_document(old_man_doc);

    // `Document` can be obtained directly from JSON:
    let short_man_json = r#"{
        "title":[{
            "text":"The Old Man",
            "tokens":[
                {"offset_from":0,"offset_to":3,"position":0,"text":"The","position_length":1},
                {"offset_from":4,"offset_to":7,"position":1,"text":"Old","position_length":1},
                {"offset_from":8,"offset_to":11,"position":2,"text":"Man","position_length":1}
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
    let query = TermQuery::new(
        Term::from_field_text(title, "Man"),
        IndexRecordOption::Basic,
    );

    let (top_docs, count) = searcher
        .search(&query, &(TopDocs::with_limit(2), Count))
        .unwrap();

    assert_eq!(count, 2);

    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("Document: {}", schema.to_json(&retrieved_doc));
    }

    // In contrary to the previous query, when we search for the "man" term we
    // should get no results, as it's not one of the indexed tokens. SimpleTokenizer
    // only splits text on whitespace / interpunction.

    let query = TermQuery::new(
        Term::from_field_text(title, "nan"),
        IndexRecordOption::Basic,
    );

    let (top_docs, count) = searcher
        .search(&query, &(TopDocs::with_limit(2), Count))
        .unwrap();

    assert_eq!(count, 0);

    Ok(())
}
