// # Json field example
//
// This example shows how the json field can be used
// to make tantivy partially schemaless.

use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, STORED, STRING, TEXT};
use tantivy::Index;

fn main() -> tantivy::Result<()> {
    // # Defining the schema
    //
    // We need two fields:
    // - a timestamp
    // - a json object field
    let mut schema_builder = Schema::builder();
    schema_builder.add_date_field("timestamp", FAST | STORED);
    let event_type = schema_builder.add_text_field("event_type", STRING | STORED);
    let attributes = schema_builder.add_json_field("attributes", STORED | TEXT);
    let schema = schema_builder.build();

    // # Indexing documents
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer = index.writer(50_000_000)?;
    let doc = schema.parse_document(
        r#"{
        "timestamp": "2022-02-22T23:20:50.53Z",
        "event_type": "click",
        "attributes": {
            "target": "submit-button",
            "cart": {"product_id": 103},
            "description": "the best vacuum cleaner ever"
        }
    }"#,
    )?;
    index_writer.add_document(doc)?;
    let doc = schema.parse_document(
        r#"{
        "timestamp": "2022-02-22T23:20:51.53Z",
        "event_type": "click",
        "attributes": {
            "target": "submit-button",
            "cart": {"product_id": 133},
            "description": "das keyboard"
        }
    }"#,
    )?;
    index_writer.add_document(doc)?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![event_type, attributes]);
    {
        let query = query_parser.parse_query("target:submit-button")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(2))?;
        assert_eq!(count_docs.len(), 2);
    }
    {
        let query = query_parser.parse_query("target:submit")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(2))?;
        assert_eq!(count_docs.len(), 2);
    }
    {
        let query = query_parser.parse_query("cart.product_id:103")?;
        let count_docs = searcher.search(&*query, &Count)?;
        assert_eq!(count_docs, 1);
    }
    {
        let query = query_parser
            .parse_query("event_type:click AND cart.product_id:133")
            .unwrap();
        let hits = searcher.search(&*query, &TopDocs::with_limit(2)).unwrap();
        assert_eq!(hits.len(), 1);
    }
    Ok(())
}
