use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();

    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer("default")
        .set_search_tokenizer("raw")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_field_indexing)
        .set_stored();
    let title = schema_builder.add_text_field("title", text_options);

    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());

    let mut index_writer = index.writer(50_000_000)?;
    index_writer.add_document(doc!(
        title => "The Old Man and the Sea",
    ));
    index_writer.add_document(doc!(
        title => "Of Mice and Men",
    ));
    index_writer.add_document(doc!(
        title => "Frankenstein",
    ));
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![title]);

    let query = query_parser.parse_query("mice")?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    assert_eq!(top_docs.len(), 1);
    for (_, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{}", schema.to_json(&retrieved_doc));
    }

    // This would return 1 document if the tokenizer was "default"
    let query = query_parser.parse_query("Mice")?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    assert_eq!(top_docs.len(), 0);

    Ok(())
}
