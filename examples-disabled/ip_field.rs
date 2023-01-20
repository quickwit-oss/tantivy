// # IP Address example
//
// This example shows how the ip field can be used
// with IpV6 and IpV4.

use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, INDEXED, STORED, STRING};
use tantivy::Index;

fn main() -> tantivy::Result<()> {
    // # Defining the schema
    let mut schema_builder = Schema::builder();
    let event_type = schema_builder.add_text_field("event_type", STRING | STORED);
    let ip = schema_builder.add_ip_addr_field("ip", STORED | INDEXED | FAST);
    let schema = schema_builder.build();

    // # Indexing documents
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer = index.writer(50_000_000)?;
    let doc = schema.parse_document(
        r#"{
        "ip": "192.168.0.33",
        "event_type": "login"
    }"#,
    )?;
    index_writer.add_document(doc)?;
    let doc = schema.parse_document(
        r#"{
        "ip": "192.168.0.80",
        "event_type": "checkout"
    }"#,
    )?;
    index_writer.add_document(doc)?;
    let doc = schema.parse_document(
        r#"{
        "ip": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        "event_type": "checkout"
    }"#,
    )?;

    index_writer.add_document(doc)?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![event_type, ip]);
    {
        let query = query_parser.parse_query("ip:[192.168.0.0 TO 192.168.0.100]")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(5))?;
        assert_eq!(count_docs.len(), 2);
    }
    {
        let query = query_parser.parse_query("ip:[192.168.1.0 TO 192.168.1.100]")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(2))?;
        assert_eq!(count_docs.len(), 0);
    }
    {
        let query = query_parser.parse_query("ip:192.168.0.80")?;
        let count_docs = searcher.search(&*query, &Count)?;
        assert_eq!(count_docs, 1);
    }
    {
        // IpV6 needs to be escaped because it contains `:`
        let query = query_parser.parse_query("ip:\"2001:0db8:85a3:0000:0000:8a2e:0370:7334\"")?;
        let count_docs = searcher.search(&*query, &Count)?;
        assert_eq!(count_docs, 1);
    }

    Ok(())
}
